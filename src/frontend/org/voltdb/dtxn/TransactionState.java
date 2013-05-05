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

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.voltdb.ExecutionSite;
import org.voltdb.VoltTable;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.CompleteTransactionResponseMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.TransactionInfoBaseMessage;

/**
 * Controls the state of a transaction. Encapsulates from the SimpleDTXNConnection
 * all the logic about what needs to happen next in a transaction. The DTXNConn just
 * pumps events into the TransactionState and it takes the appropriate actions,
 * ultimately it will return true from finished().
 *
 */
public abstract class TransactionState extends OrderableTransaction  {
    public final int coordinatorSiteId;
    protected final boolean m_isReadOnly;
    protected int m_nextDepId = 1;
    protected final Mailbox m_mbox;
    protected final SiteTransactionConnection m_site;
    protected boolean m_done = false;
    protected long m_beginUndoToken;
    protected boolean m_needsRollback = false;
    
    // GWW: now used by MPPTX and SPTX for correct txn ordering
    protected InitiateResponseMessage m_response;
    
    // GWW: txn is prepared only when it finishes all execution and has result ready
    // or it decides to abort.
    //							m_prepared	m_done			m_needRollback
    //		preparing phase:	false		false			false
    //		prepared phase:		true		true / false	true / false
    //		to commit / abort:	true		true			true / false
    protected boolean m_prepared = false;
    
    // GWW: last undo token for this txn
    protected long m_endUndoToken;
    
    // GWW: 
    //protected int m_executedPlanNodesForCurrentFragment = -1;
    
    // GWW: data conflict exception, been blocked
    protected boolean m_isDataConflict = false;
    
    protected RuntimeException m_procedureException = null;
    
    // GWW:
    protected boolean m_isCVoltDBExecution = false;

	// GWW: for stats use   
    public long receivedT = 0;
    public long startedT = 0;
    public long executedT = 0;
    public long fragsStartT = 0; 	// start computing last frags (accumulation for coord.)
    public long fragsDoneT = 0;  	// finished computing last frags
    public long resumeT = 0; 		// coord. only
    public long commitDecisionT = 0;  	// coord. only
    public long preparedT = 0;
    
    // for multi-partition txn: 
    // coordinator: time when receive last completeResponseMsg
    // participants: time when receive completeTxnMsg
    public long receivedCompleteTxnMsgT = 0;	
    
    // for multi-shot txn
    public long firstFragsStartT = 0; 
    public long firstFragsDoneT = 0;
    public long lastFragsStartT = 0;				
    public long lastFragsDoneT = 0;
    
    public long completedT = 0;
    
    /**
     * Set up the final member variables from the parameters. This will
     * be called exclusively by subclasses.
     *
     * @param mbox The mailbox for the site.
     * @param notice The information about the new transaction.
     */
    protected TransactionState(Mailbox mbox,
                               ExecutionSite site,
                               TransactionInfoBaseMessage notice)
    {
        super(notice.getTxnId(), notice.getInitiatorSiteId());
        m_mbox = mbox;
        m_site = site;
        coordinatorSiteId = notice.getCoordinatorSiteId();
        m_isReadOnly = notice.isReadOnly();
        m_beginUndoToken = ExecutionSite.kInvalidUndoToken;
        // GWW:initialize endUndoToken
        m_endUndoToken = ExecutionSite.kInvalidUndoToken;
    }

    final public boolean isDone() {
        return m_done;
    }

    public boolean isInProgress() {
        return false;
    }

    public boolean isReadOnly()
    {
        return m_isReadOnly;
    }
    
    public boolean isCVoltDBExecution() {
    	return m_isCVoltDBExecution;
    }
    
    public void unblockForDataConflict() {
    	assert(this.isDataConflict());
    	assert(!this.isPrepared());
    	m_isDataConflict = false;
    	((ExecutionSite)m_site).cVoltTrace("GWW - unblock txn" + txnId + "\n");
    }
    
    public void blockForDataConflict() {
    	m_isDataConflict = true;
    }
    
    public boolean isDataConflict() {
    	return m_isDataConflict;
    }
    
    public RuntimeException getException() {
    	return m_procedureException;
    }
    public boolean hasException() {
    	return m_procedureException == null ? false : true;
    }
    
    public void addException(RuntimeException ex) {
    	m_procedureException = ex;
    }
    
    // GWW
    public void sendResponse() {
    	assert(m_response != null);
        try {
            m_mbox.send(initiatorSiteId, 0, m_response);
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }
        m_done = true;
    }

    /**
     * Indicate whether or not the transaction represented by this
     * TransactionState is single-partition.  Should be overridden to provide
     * sane results by subclasses.
     */
    public abstract boolean isSinglePartition();

    public abstract boolean isCoordinator();

    public abstract boolean isBlocked();

    public abstract boolean hasTransactionalWork();

    public abstract boolean doWork(boolean recovering);

    public boolean shouldResumeProcedure() {
        return false;
    }

    public void setBeginUndoToken(long undoToken)
    {
        m_beginUndoToken = undoToken;
    }

    public long getBeginUndoToken()
    {
        return m_beginUndoToken;
    }
    
    // GWW: getter and setter
    public long getEndUndoToken() {
		return m_endUndoToken;
	}

	public void setEndUndoToken(long endUndoToken) {
		m_endUndoToken = endUndoToken;
	}
    
    public boolean done() {
    	return m_done;
    }
    
    public boolean isPrepared() {
    	return m_prepared;
    }

    public boolean needsRollback()
    {
        return m_needsRollback;
    }

    public void createFragmentWork(int[] partitions, FragmentTaskMessage task) {
        String msg = "The current transaction context of type " + this.getClass().getName();
        msg += " doesn't support creating fragment tasks.";
        throw new UnsupportedOperationException(msg);
    }

    public void createAllParticipatingFragmentWork(FragmentTaskMessage task) {
        String msg = "The current transaction context of type " + this.getClass().getName();
        msg += " doesn't support creating fragment tasks.";
        throw new UnsupportedOperationException(msg);
    }

    public void createLocalFragmentWork(FragmentTaskMessage task, boolean nonTransactional) {
        String msg = "The current transaction context of type " + this.getClass().getName();
        msg += " doesn't support accepting fragment tasks.";
        throw new UnsupportedOperationException(msg);
    }

    public void setupProcedureResume(boolean isFinal, int[] dependencies) {
        String msg = "The current transaction context of type " + this.getClass().getName();
        msg += " doesn't support receiving dependencies.";
        throw new UnsupportedOperationException(msg);
    }

    public void processRemoteWorkResponse(FragmentResponseMessage response) {
        String msg = "The current transaction context of type ";
        msg += this.getClass().getName();
        msg += " doesn't support receiving fragment responses.";
        throw new UnsupportedOperationException(msg);
    }

    public void processCompleteTransaction(CompleteTransactionMessage complete)
    {
        String msg = "The current transaction context of type ";
        msg += this.getClass().getName();
        msg += " doesn't support receiving CompleteTransactionMessages.";
        throw new UnsupportedOperationException(msg);
    }

    public void
    processCompleteTransactionResponse(CompleteTransactionResponseMessage response)
    {
        String msg = "The current transaction context of type ";
        msg += this.getClass().getName();
        msg += " doesn't support receiving CompleteTransactionResponseMessages.";
        throw new UnsupportedOperationException(msg);
    }

    public Map<Integer, List<VoltTable>> getPreviousStackFrameDropDependendencies() {
        String msg = "The current transaction context of type ";
        msg += this.getClass().getName();
        msg += " doesn't support collecting stack frame drop dependencies.";
        throw new UnsupportedOperationException(msg);
    }

    public int getNextDependencyId() {
        return m_nextDepId++;
    }

    /**
     * Process the failure of failedSites.
     * @param globalCommitPoint greatest committed transaction id in the cluster
     * @param failedSites list of execution and initiator sites that have failed
     */
    public abstract void handleSiteFaults(HashSet<Integer> failedSites);
}
