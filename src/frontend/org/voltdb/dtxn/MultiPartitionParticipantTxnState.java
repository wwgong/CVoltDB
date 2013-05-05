/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.dtxn;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.voltdb.ClientResponseImpl;
import org.voltdb.ExecutionSite;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.CompleteTransactionResponseMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.MultiPartitionParticipantMessage;
import org.voltdb.messaging.TransactionInfoBaseMessage;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.catalog.Site;
import org.voltdb.exceptions.*;


public class MultiPartitionParticipantTxnState extends TransactionState {

    protected final ArrayDeque<WorkUnit> m_readyWorkUnits = new ArrayDeque<WorkUnit>();
    protected boolean m_isCoordinator;
    protected final int m_siteId;
    protected int[] m_nonCoordinatingSites;
    protected boolean m_shouldResumeProcedure = false;
    protected boolean m_hasStartedWork = false;
    protected HashMap<Integer, WorkUnit> m_missingDependencies = null;
    protected ArrayList<WorkUnit> m_stackFrameDropWUs = null;
    protected Map<Integer, List<VoltTable>> m_previousStackFrameDropDependencies = null;

    private HashSet<Integer> m_outstandingAcks = null;
    private final java.util.concurrent.atomic.AtomicBoolean m_durabilityFlag;
    private final InitiateTaskMessage m_task;

    private int[] m_replicaSites = null;

    private static final VoltLogger hostLog = new VoltLogger("HOST");
    
    // GWW: to support early fragment distribution 
    private ProcedureThread m_pThread;
   
    public boolean startProcedureThread(MultiPartitionParticipantTxnState tx, VoltMessage msg) {
    	m_pThread = new ProcedureThread(tx, msg);
        Thread procThread = new Thread(m_pThread, "ProcThread");
        procThread.start();
        return true;
    }
    
    // GWW: procedure thread wait this sem for result executed by ES
    // the semaphore is released by ES after execution
	public void waitResult() {
		try {
			m_pThread.sem.acquire();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// GWW: after execution of current shot, ES let procedure thread continue
	public void resumeProcedure() {
		m_pThread.sem.release();
	}
	
    public void resumeExecutionSite() {
    	((ExecutionSite)m_site).resume();
    }
    
    // GWW: this class represents the thread created for each multi-site txn
    // it calls slowPath() to do early fragments distribution, and then wait
    // ES to get result for each shot, and finally stores response and exit
    class ProcedureThread implements Runnable {
    	private MultiPartitionParticipantTxnState ts;
    	private Semaphore sem;
    	private VoltMessage msg;
    	
    	ProcedureThread(MultiPartitionParticipantTxnState ts, VoltMessage msg) {
    		this.ts = ts;
    		this.msg = msg;
    		sem = new Semaphore(0, true);
    	}

    	@Override
		public void run() {	
    		assert(ts instanceof MultiPartitionParticipantTxnState);
    		assert(ts.isCoordinator());
    		m_response = m_site.processInitiateTask(ts, msg);
    		
    		// check the response and setup m_needsRollback
            if (!m_response.shouldCommit()) {
                if (m_missingDependencies != null)
                {
                    m_missingDependencies.clear();
                }
                m_needsRollback = true;
            }
            
            if(((ExecutionSite)m_site).debugFlag)
            	((ExecutionSite)m_site).cVoltTrace("MPPTS have m_response ready: " + m_response);
            
            // send complete msg to participants
            sendCompleteMessage();
            
    		resumeExecutionSite();
    		return;
    	}
    }
    

    /**
     *  This is thrown by the TransactionState instance when something
     *  goes wrong mid-fragment, and execution needs to back all the way
     *  out to the stored procedure call.
     */
    public static class FragmentFailureException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    public MultiPartitionParticipantTxnState(Mailbox mbox, ExecutionSite site,
                                             TransactionInfoBaseMessage notice)
    {
        super(mbox, site, notice);
        m_siteId = site.getSiteId();
        m_nonCoordinatingSites = null;
        m_isCoordinator = false;
        
        //Check to make sure we are the coordinator, it is possible to get an initiate task
        //where we aren't the coordinator because we are a replica of the coordinator.
        if (notice instanceof InitiateTaskMessage)
        {
            if (notice.getCoordinatorSiteId() == m_siteId) {
                m_isCoordinator = true;
                m_task = (InitiateTaskMessage) notice;
                m_durabilityFlag = m_task.getDurabilityFlagIfItExists();
                SiteTracker tracker = site.getSiteTracker();
                // Add this check for tests which use a mock execution site
                if (tracker != null) {
                	// GWW: get all replica sites for the coordinator
                	m_replicaSites = getReplicaSiteIds();
                	
                	// GWW: check all participant sites passed in to see whether it is alive
                	int participantSiteIds[] = ((InitiateTaskMessage) notice).getParticipantSiteIds();
                	ArrayList<Integer> participantSites = new ArrayList<Integer>();
                	ArrayDeque<Integer> upExecutionSites = tracker.getUpExecutionSitesArray();
                	if(participantSiteIds != null) {
	                	for(int siteId : participantSiteIds) {
	                		if(upExecutionSites.contains(siteId))
	                			participantSites.add(siteId);
	                	}
                	}
                	
                	int i = 0;
                	if(m_replicaSites != null && m_replicaSites.length > 0) {
                		m_nonCoordinatingSites = new int[m_replicaSites.length + participantSites.size()];
                		for(i = 0; i < m_replicaSites.length; i++)
                			m_nonCoordinatingSites[i] = m_replicaSites[i];
                	}
                	else
                		m_nonCoordinatingSites = new int[participantSites.size()];
                	
                	for(Integer siteId : participantSites)
                		m_nonCoordinatingSites[i++] = siteId;
                }
                m_readyWorkUnits.add(new WorkUnit(tracker, m_task,
                                                  null, m_siteId,
                                                  null, false));
            } else {
                m_durabilityFlag = ((InitiateTaskMessage)notice).getDurabilityFlagIfItExists();
                m_task = null;
            }
            
            m_isCVoltDBExecution = ((InitiateTaskMessage) notice).isCVoltDBExecution();

        } else {
            m_task = null;
            m_durabilityFlag = null;
            
            // GWW
            if(notice instanceof MultiPartitionParticipantMessage) {
            	String msg = "Received new MPPartMsg for txn " + txnId + " :" + notice;
            	 ((ExecutionSite)m_site).cVoltTrace(msg);
            	m_isCVoltDBExecution = ((MultiPartitionParticipantMessage)notice).isCVoltDBExecution();
            }
        }
               
    }

    @Override
    public String toString() {
        return "MultiPartitionParticipantTxnState initiator: " + initiatorSiteId +
            " coordinator: " + m_isCoordinator +
            " in-progress: " + m_hasStartedWork +
            " txnId: " + TransactionIdManager.toString(txnId);
    }

    @Override
    public boolean isInProgress() {
        return m_hasStartedWork;
    }

    @Override
    public boolean isSinglePartition()
    {
        return false;
    }

    @Override
    public boolean isBlocked()
    {
        // GWW: if txn is data "blocked", return false too
    	return m_readyWorkUnits.isEmpty() || m_isDataConflict;
    }

    @Override
    public boolean isCoordinator()
    {
        return m_isCoordinator;
    }

    @Override
    public boolean hasTransactionalWork()
    {
        // this check is a little bit of a lie.  It depends not only on
        // whether or not any of the pending WorkUnits touch tables in the EE,
        // but also whether or not these pending WorkUnits are part of the final
        // batch of SQL statements that a stored procedure is going to execute
        // (Otherwise, we might see no transactional work remaining in this
        // batch but the stored procedure could send us another batch that
        // not only touches tables but does writes to them).  The
        // WorkUnit.nonTransactional boolean ends up ultimately being gated on this
        // condition in VoltProcedure.slowPath().  This is why the null case
        // below is pessimistic.
        if (m_missingDependencies == null)
        {
            return true;
        }

        boolean has_transactional_work = false;
        for (WorkUnit pendingWu : m_missingDependencies.values()) {
            if (pendingWu.nonTransactional == false) {
                has_transactional_work = true;
                break;
            }
        }
        return has_transactional_work;
    }

    private boolean m_startedWhileRecovering;

    @Override
    public boolean doWork(boolean recovering) {
        if (!m_hasStartedWork) {
            m_site.beginNewTxn(this);
            m_hasStartedWork = true;
            m_startedWhileRecovering = recovering;
        }

        assert(m_startedWhileRecovering == recovering);
             
        // GWW: now return m_prepared because doWork() means txn has finished execution
        // either it waits acks back or it has decides to abort
        if(m_prepared)
        	return true;
        
        // GWW: correct ordering
        if(m_response != null) {
        	m_prepared = true;
        	return m_prepared;
        }

        while (!isBlocked())
        {
            WorkUnit wu = m_readyWorkUnits.poll();
            if (wu.shouldResumeProcedure()) {
                assert(m_stackFrameDropWUs != null);
                m_stackFrameDropWUs.remove(wu);

                m_shouldResumeProcedure = true;
                m_previousStackFrameDropDependencies = wu.getDependencies();

                for (WorkUnit sfd : m_stackFrameDropWUs) {
                    sfd.m_stackCount--;
                    if (sfd.allDependenciesSatisfied()) {
                        m_readyWorkUnits.add(sfd);
                    }
                }

                return m_prepared;
            }

            VoltMessage payload = wu.getPayload();
            
            if(((ExecutionSite)m_site).debugFlag)
            	((ExecutionSite)m_site).cVoltTrace("MPPTS doing one task message " + payload);

            if (payload instanceof InitiateTaskMessage) {
            	 assert(m_isCoordinator);
            	// GWW: do first part of initiateProcedure() in the new thread
            	startProcedureThread(this, payload);
            	((ExecutionSite)m_site).waitOnProcedure();
            	return m_prepared;
            }
            else if (payload instanceof FragmentTaskMessage) {
                if (recovering && (wu.nonTransactional == false)) {
                    processRecoveringFragmentWork((FragmentTaskMessage) payload, wu.getDependencies());
                }
                else {
                    // when recovering, still do non-transactional work
                    processFragmentWork((FragmentTaskMessage) payload, wu.getDependencies());
                    if(m_isDataConflict)
                    	m_readyWorkUnits.addFirst(wu);
                }
            }
        }

        return m_prepared;
    }
	@Override
    public boolean shouldResumeProcedure() {
        if (m_shouldResumeProcedure) {
            m_shouldResumeProcedure = false;
            return true;
        }
        return false;
    }

    public CompleteTransactionMessage createCompleteTransactionMessage(boolean rollback,
                                                                       boolean requiresAck)
    {
        CompleteTransactionMessage ft =
            new CompleteTransactionMessage(initiatorSiteId,
                                           coordinatorSiteId,
                                           txnId,
                                           true,
                                           rollback,
                                           requiresAck);
        return ft;
    }
    
    // GWW: second part of initiateProcedure()
    void sendCompleteMessage() {
    	assert(m_response != null);

        m_outstandingAcks = new HashSet<Integer>();
        // if this transaction was readonly then don't require acks.
        // We still need to send the completion message, however,
        // since there are a number of circumstances where the coordinator
        // is the only site that knows whether or not the transaction has
        // completed.
        if (!isReadOnly())
        {
            for (int site_id : m_nonCoordinatingSites)
            {
                m_outstandingAcks.add(site_id);
            }
        }
        // send commit notices to everyone
        CompleteTransactionMessage complete_msg =
            createCompleteTransactionMessage(m_response.shouldCommit() == false,
                                             !isReadOnly());

        try
        {
            m_mbox.send(m_nonCoordinatingSites, 0, complete_msg);
        }
        catch (MessagingException e) {
            throw new RuntimeException(e);
        }
        
        m_prepared = true;

        if (m_outstandingAcks.size() == 0)
        {
            m_done = true;
        }
    }


    void initiateProcedure(InitiateTaskMessage itask) {
        assert(m_isCoordinator);

        // Cache the response locally and create accounting
        // to track the outstanding acks.
        m_response = m_site.processInitiateTask(this, itask);
      
        // GWW: extract following codes to toCompleteTransaction() method.
        sendCompleteMessage();
    }

    void processFragmentWork(FragmentTaskMessage ftask, HashMap<Integer, List<VoltTable>> dependencies) {
        assert(ftask.getFragmentCount() > 0);

        FragmentResponseMessage response = m_site.processFragmentTask(this, dependencies, ftask);
        if (response.getStatusCode() != FragmentResponseMessage.SUCCESS)
        {

            if (m_missingDependencies != null)
                m_missingDependencies.clear();
            
            m_readyWorkUnits.clear();

            if (m_isCoordinator)
            {
                // GWW: store exception and let procedure thread to fetch and throw it
            	// the reason is the exception thrown by the original code will not be
            	// caught by slowPath() anymore
                if(response.getException() != null)
                	m_procedureException = response.getException();
                else
                	m_procedureException = new FragmentFailureException();

                m_shouldResumeProcedure = true;
                return;
            }
            else
            {
                m_needsRollback = true;
                m_done = true;
                // GWW: set m_prepared to true too, because one txn finishes means
                // it must has prepared
                m_prepared = true;
            }
        }
        
        if (m_isCoordinator &&
            (response.getDestinationSiteId() == response.getExecutorSiteId()))
        {
            processFragmentResponseDependencies(response);
        }
        else
        {
            try {
                m_mbox.send(response.getDestinationSiteId(), 0, response);
            } catch (MessagingException e) {
                throw new RuntimeException(e);
            }
            // If we're not the coordinator, the transaction is read-only,
            // and this was the final task, then we can try to move on after
            // we've finished this work.
            if(ftask.isFinalTask() && !isCoordinator())
            	m_prepared = true;
        }
    }

    void processRecoveringFragmentWork(FragmentTaskMessage ftask, HashMap<Integer, List<VoltTable>> dependencies) {
        assert(ftask.getFragmentCount() > 0);

        FragmentResponseMessage response = new FragmentResponseMessage(ftask, m_siteId);
        response.setRecovering(true);
        response.setStatus(FragmentResponseMessage.SUCCESS, null);

        // add a dummy table for all of the expected dependency ids
        for (int i = 0; i < ftask.getFragmentCount(); i++) {
            response.addDependency(ftask.getOutputDepId(i),
                    new VoltTable(new VoltTable.ColumnInfo("DUMMY", VoltType.BIGINT)));
        }

        try {
            m_mbox.send(response.getDestinationSiteId(), 0, response);
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setupProcedureResume(boolean isFinal, int[] dependencies) {
        assert(dependencies != null);
        assert(dependencies.length > 0);

        WorkUnit w = new WorkUnit(m_site.getSiteTracker(), null, dependencies,
                                  m_siteId, m_nonCoordinatingSites, true);
        if (isFinal)
            w.nonTransactional = true;
        for (int depId : dependencies) {
            if (m_missingDependencies == null) {
                m_missingDependencies = new HashMap<Integer, WorkUnit>();
            }
            // We are missing the dependency: record this fact
            assert(!m_missingDependencies.containsKey(depId));
            m_missingDependencies.put(depId, w);
        }
        if (m_stackFrameDropWUs == null)
            m_stackFrameDropWUs = new ArrayList<WorkUnit>();
        for (WorkUnit sfd : m_stackFrameDropWUs)
            sfd.m_stackCount++;
        m_stackFrameDropWUs.add(w);

        // Find any stack frame drop work marked ready in the ready set,
        // and if it's not really ready, take it out.
        for (WorkUnit wu : m_readyWorkUnits) {
            if (wu.shouldResumeProcedure()) {
                if (wu.m_stackCount > 0)
                    m_readyWorkUnits.remove(wu);
            }
        }
    }

    @Override
    public void createAllParticipatingFragmentWork(FragmentTaskMessage task) {
        assert(m_isCoordinator); // Participant can't set m_nonCoordinatingSites
        try {
            // send to all non-coordinating sites
            m_mbox.send(m_nonCoordinatingSites, 0, task);
            // send to this site
            createLocalFragmentWork(task, false);
        }
        catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }
    
    private int[] getReplicaSiteIds() {
    	SiteTracker tracker = m_site.getSiteTracker();
    	int partitionId = tracker.getPartitionForSite(m_siteId);
    	ArrayList<Integer> siteIds = tracker.getAllSitesForPartition(partitionId);
    	ArrayList<Integer> replicaSiteIds = new ArrayList<Integer>();
    	ArrayDeque<Integer> upExecutionSites = tracker.getUpExecutionSitesArray();
    	for(Integer siteId : siteIds){
    		int site = siteId.intValue();
    		if(site != m_siteId && upExecutionSites.contains(siteId))
    			replicaSiteIds.add(siteId);
    	}
    	
    	int[] replicaSites = new int[replicaSiteIds.size()];
    	int i = 0;
    	for(Integer id : replicaSiteIds)
    		replicaSites[i++] = id;
    	return replicaSites;
    }
    
    // GWW: after divide all fragments to 3 categories:
    //			local-task: only execute on coordinator
    //			local-distributed-task: execute on coordinator
    //			remote-distributed-task: execute on all participants, the same as local-distributed
    // createLocalFragmentWork() put local-task in to-do list, and this function sends remote-distributed-task 
    // to all participant sites and put local-distributed-task in this site's to-do list
    public void createAllParticipatingFragmentWork(FragmentTaskMessage remoteTask, 
    												FragmentTaskMessage localTask,
    												boolean isCoordinatorOnly) {
        assert(m_isCoordinator); // Participant can't set m_nonCoordinatingSites
        try {
        	if(isCoordinatorOnly) {
        		// send to all replica sites if it is not read-only
        		if(!remoteTask.isReadOnly()) {
        			if(m_replicaSites.length > 0)
        				m_mbox.send(m_replicaSites, 0, remoteTask);
        		}
        	} else
        		// send to all non-coordinating sites
        		m_mbox.send(m_nonCoordinatingSites, 0, remoteTask);
            
        	// send to this site
            createLocalFragmentWork(localTask, false, isCoordinatorOnly);
        }
        catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createLocalFragmentWork(FragmentTaskMessage task, boolean nonTransactional) {
        if (task.getFragmentCount() > 0) {
            createLocalFragmentWorkDependencies(task, nonTransactional, false);
        }
    }
    
    public void createLocalFragmentWork(FragmentTaskMessage task, boolean nonTransactional, boolean isBatchCoordinatorPartitionOnly) {
        if (task.getFragmentCount() > 0) {
            createLocalFragmentWorkDependencies(task, nonTransactional, isBatchCoordinatorPartitionOnly);
        }
    }

    private void createLocalFragmentWorkDependencies(FragmentTaskMessage task, boolean nonTransactional, boolean isBatchCoordinatorPartitionOnly)
    {
        if (task.getFragmentCount() <= 0) return;

        if(((ExecutionSite)m_site).debugFlag)
        	((ExecutionSite)m_site).cVoltTrace("createLocalFragmentWorkDependencies - txnid: " + this.txnId
        			+ ", isCoordinatorOnly: " + isBatchCoordinatorPartitionOnly);
        
        WorkUnit w;
        
        if (isBatchCoordinatorPartitionOnly){
        	if(!task.isReadOnly() && m_replicaSites.length > 0) {
                if(((ExecutionSite)m_site).debugFlag)
                	((ExecutionSite)m_site).cVoltTrace("createLocalFragmentWorkDependencies - has replica sites, txnId: "
                						+ this.txnId);

        		w = new WorkUnit(m_site.getSiteTracker(), task,
	                  task.getAllUnorderedInputDepIds(),
	                  m_siteId, m_replicaSites, false);
        	}
        	else {
                if(((ExecutionSite)m_site).debugFlag)
                	((ExecutionSite)m_site).cVoltTrace("createLocalFragmentWorkDependencies - no replica sites, txnId: "
                						+ this.txnId);
        		w = new WorkUnit(m_site.getSiteTracker(), task,
		                  task.getAllUnorderedInputDepIds(),
		                  m_siteId, null, false);
        	}
        }
        else
        	w = new WorkUnit(m_site.getSiteTracker(), task,
                    task.getAllUnorderedInputDepIds(),
                    m_siteId, m_nonCoordinatingSites, false);
        
        w.nonTransactional = nonTransactional;

        for (int i = 0; i < task.getFragmentCount(); i++) {
            ArrayList<Integer> inputDepIds = task.getInputDepIds(i);
            if (inputDepIds == null) continue;
            for (int inputDepId : inputDepIds) {
                if (m_missingDependencies == null)
                    m_missingDependencies = new HashMap<Integer, WorkUnit>();
                assert(!m_missingDependencies.containsKey(inputDepId));
                m_missingDependencies.put(inputDepId, w);
            }
        }

        if (w.allDependenciesSatisfied())
            m_readyWorkUnits.add(w);
    }

    @Override
    public void processRemoteWorkResponse(FragmentResponseMessage response) {
        // if we've already decided that we're rolling back, then we just
        // want to discard any incoming FragmentResponses that were
        // possibly in flight
        if (m_needsRollback)
        {
            return;
        }

        if (response.getStatusCode() != FragmentResponseMessage.SUCCESS)
        {
            if (m_missingDependencies != null)
                m_missingDependencies.clear();
            m_readyWorkUnits.clear();

            if (m_isCoordinator)
            {
            	// GWW: store the exception and let procedure thread to fetch and throw it
            	// the reason is the exception thrown by the original code will not be
            	// caught by slowPath() anymore
            	if(response.getException() != null)
            		m_procedureException = response.getException();
            	else
            		m_procedureException = new FragmentFailureException();
            	m_shouldResumeProcedure = true;
            	return;
            }
            else
            {
                m_needsRollback = true;
                m_done = true;
                // GWW: set m_preprared to true too, since one txn is finished means
                // it must has prepared
                m_prepared = true;
            }
        }

        processFragmentResponseDependencies(response);
    }

    private void processFragmentResponseDependencies(FragmentResponseMessage response)
    {
        int depCount = response.getTableCount();
        for (int i = 0; i < depCount; i++) {
            int dependencyId = response.getTableDependencyIdAtIndex(i);
            VoltTable payload = response.getTableAtIndex(i);
            assert(payload != null);

            // if we're getting a dependency, i hope we know about it
            assert(m_missingDependencies != null);

            WorkUnit w = m_missingDependencies.get(dependencyId);
            if (w == null) {
                String msg = "Unable to find WorkUnit for dependency: " +
                             dependencyId +
                             " as part of TXN: " + txnId +
                             " received from execution site: " +
                             response.getExecutorSiteId();
                hostLog.warn(msg);
                //throw new FragmentFailureException();
                return;
            }

            // if the node is recovering, it doesn't matter if the payload matches
            if (response.isRecovering()) {
                w.putDummyDependency(dependencyId, response.getExecutorSiteId());
            }
            else {
                w.putDependency(dependencyId, response.getExecutorSiteId(), payload);
            }
           if (w.allDependenciesSatisfied()) {
                handleWorkUnitComplete(w);
            }
        }
        
        if(((ExecutionSite)m_site).debugFlag)
        	((ExecutionSite)m_site).cVoltTrace("GWWW - finished processFragmentResponseDependencies: " + response);
    }

    @Override
    public void processCompleteTransaction(CompleteTransactionMessage complete)
    {
        m_done = m_prepared = true;
        if (complete.isRollback()) {
            if (m_missingDependencies != null) {
                m_missingDependencies.clear();
            }
            m_readyWorkUnits.clear();
            m_needsRollback = true;
        }
        if (complete.requiresAck())
        {
            CompleteTransactionResponseMessage ctrm =
                new CompleteTransactionResponseMessage(complete, m_siteId);
            try
            {
            	if(((ExecutionSite)m_site).debugFlag)
            		((ExecutionSite)m_site).cVoltTrace("sending back CompleteTransactionResponseMessage");
            	            	
                m_mbox.send(complete.getCoordinatorSiteId(), 0, ctrm);
            }
            catch (MessagingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void
    processCompleteTransactionResponse(CompleteTransactionResponseMessage response)
    {
        // need to do ack accounting
        m_outstandingAcks.remove(response.getExecutionSiteId());
        if (m_outstandingAcks.size() == 0)
        {
        	// GWW: will send reponse back when polling from m_committedAbortedTxnQueue
            m_done = true;
        }
    }
    
    // GWW
    public boolean isWaitCompleteTransactionResponse() {
    	return !(m_outstandingAcks.size() == 0);
    }

    void handleWorkUnitComplete(WorkUnit w)
    {
        for (int depId : w.getDependencyIds()) {
            m_missingDependencies.remove(depId);
        }
        // slide this new stack frame drop into the right position
        // (before any other stack frame drops)
        if ((w.shouldResumeProcedure()) &&
                (m_readyWorkUnits.peekLast() != null) &&
                (m_readyWorkUnits.peekLast().shouldResumeProcedure())) {

            ArrayDeque<WorkUnit> deque = new ArrayDeque<WorkUnit>();
            while ((m_readyWorkUnits.peekLast() != null) &&
                    (m_readyWorkUnits.peekLast().shouldResumeProcedure())) {
                deque.add(m_readyWorkUnits.pollLast());
            }
            deque.add(w);
            while (deque.size() > 0)
                m_readyWorkUnits.add(deque.pollLast());
        }
        else {
            m_readyWorkUnits.add(w);
        }
    }

    public void checkWorkUnits()
    {
        // Find any workunits with previously unmet dependencies
        // that may now be satisfied.  We can't remove them from
        // the map in this loop because we induce a
        // ConcurrentModificationException
        if (m_missingDependencies != null)
        {
            ArrayList<WorkUnit> done_wus = new ArrayList<WorkUnit>();
            for (WorkUnit w : m_missingDependencies.values())
            {
                if (w.allDependenciesSatisfied()) {
                    done_wus.add(w);
                }
            }

            for (WorkUnit w : done_wus)
            {
                handleWorkUnitComplete(w);
            }
        }

        // Also, check to see if we're just waiting on acks from
        // participants, and, if so, if the fault we just experienced
        // freed us up.
        if (m_outstandingAcks != null)
        {
            if (m_outstandingAcks.size() == 0)
            {
                m_done = true;
            }
        }
    }

    // for test only
    @Deprecated
    public int[] getNonCoordinatingSites() {
        return m_nonCoordinatingSites;
    }

    @Override
    public Map<Integer, List<VoltTable>> getPreviousStackFrameDropDependendencies() {
        return m_previousStackFrameDropDependencies;
    }

    public InitiateTaskMessage getInitiateTaskMessage() {
        return m_task;
    }

    /**
     * Clean up internal state in response to a set of failed sites.
     * Note that failedSites contains both initiators and execution
     * sites. An external agent will drive the deletion/fault or
     * commit of this transaction state block. Only responsibility
     * here is to make internal book keeping right.
     */
    @Override
    public void handleSiteFaults(HashSet<Integer> failedSites)
    {
        // remove failed sites from the non-coordinating lists
        // and decrement expected dependency response count
        if (m_nonCoordinatingSites != null) {
            ArrayDeque<Integer> newlist = new ArrayDeque<Integer>(m_nonCoordinatingSites.length);
            for (int i=0; i < m_nonCoordinatingSites.length; ++i) {
                if (!failedSites.contains(m_nonCoordinatingSites[i])) {
                newlist.addLast(m_nonCoordinatingSites[i]);
                }
            }

            m_nonCoordinatingSites = new int[newlist.size()];
            for (int i=0; i < m_nonCoordinatingSites.length; ++i) {
                m_nonCoordinatingSites[i] = newlist.removeFirst();
            }
        }
        
        // GWW: remove failed sites from replica site list
        if(m_replicaSites != null && m_replicaSites.length > 0) {
        	ArrayDeque<Integer> newReplica = new ArrayDeque<Integer>(m_replicaSites.length);
        	for(int i = 0; i < m_replicaSites.length; ++i) {
        		if(!failedSites.contains(m_replicaSites[i]))
        			newReplica.addLast(m_replicaSites[i]);
        	}
        	
        	m_replicaSites = new int[newReplica.size()];
            for (int i=0; i < m_replicaSites.length; ++i) {
            	m_replicaSites[i] = newReplica.removeFirst();
            }
        }

        // Remove failed sites from all of the outstanding work units that
        // may be expecting dependencies from now-dead sites.
        if (m_missingDependencies != null)
        {
            for (WorkUnit wu : m_missingDependencies.values())
            {
                for (Integer site_id : failedSites)
                {
                    wu.removeSite(site_id);
                }
            }
        }

        // Also, if we're waiting on transaction completion acks from
        // participants, remove any sites that failed that we still may
        // be waiting on.
        if (m_outstandingAcks != null)
        {
            for (Integer site_id : failedSites)
            {
                m_outstandingAcks.remove(site_id);
            }
        }
    }

    // STUFF BELOW HERE IS REALY ONLY FOR SYSPROCS
    // SOME DAY MAYBE FOR MORE GENERAL TRANSACTIONS

    @Override
    public void createFragmentWork(int[] partitions, FragmentTaskMessage task) {
        try {
            // send to all specified sites (possibly including this one)
            m_mbox.send(partitions, 0, task);
        }
        catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isDurable() {
        return m_durabilityFlag == null ? true : m_durabilityFlag.get();
    }

}
