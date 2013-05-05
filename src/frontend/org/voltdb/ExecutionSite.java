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

package org.voltdb;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.RecoverySiteProcessor.MessageHandler;
import org.voltdb.SnapshotSiteProcessor.SnapshotTableTask;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.catalog.Table;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.dtxn.MultiPartitionParticipantTxnState;
import org.voltdb.dtxn.RestrictedPriorityQueue;
import org.voltdb.dtxn.RestrictedPriorityQueue.QueueState;
import org.voltdb.dtxn.SinglePartitionTxnState;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.dtxn.SiteTransactionConnection;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SQLException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.export.processors.RawProcessor;
import org.voltdb.fault.FaultDistributorInterface.PPDPolicyDecision;
import org.voltdb.fault.FaultHandler;
import org.voltdb.fault.NodeFailureFault;
import org.voltdb.fault.VoltFault;
import org.voltdb.fault.VoltFault.FaultType;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.ExecutionEngineIPC;
import org.voltdb.jni.ExecutionEngineJNI;
import org.voltdb.jni.MockExecutionEngine;
import org.voltdb.logging.Level;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.CompleteTransactionResponseMessage;
import org.voltdb.messaging.FailureSiteUpdateMessage;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.HeartbeatMessage;
import org.voltdb.messaging.HeartbeatResponseMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.LocalObjectMessage;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.MultiPartitionParticipantMessage;
import org.voltdb.messaging.RecoveryMessage;
import org.voltdb.messaging.Subject;
import org.voltdb.messaging.TransactionInfoBaseMessage;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.LogKeys;
import org.voltdb.exceptions.DataConflictException;

/**
 * The main executor of transactional work in the system. Controls running
 * stored procedures and manages the execution engine's running of plan
 * fragments. Interacts with the DTXN system to get work to do. The thread might
 * do other things, but this is where the good stuff happens.
 */
public class ExecutionSite
implements Runnable, SiteTransactionConnection, SiteProcedureConnection
{
    private VoltLogger m_txnlog;
    private VoltLogger m_recoveryLog = new VoltLogger("RECOVERY");
    private static final VoltLogger log = new VoltLogger("EXEC");
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    
    // GWW
    private static final VoltLogger cVoltLog = new VoltLogger("CVolt");
    
    private static final AtomicInteger siteIndexCounter = new AtomicInteger(0);
    static final AtomicInteger recoveringSiteCount = new AtomicInteger(0);
    private final int siteIndex = siteIndexCounter.getAndIncrement();
    private final ExecutionSiteNodeFailureFaultHandler m_faultHandler =
        new ExecutionSiteNodeFailureFaultHandler();

    final HashMap<String, VoltProcedure> procs = new HashMap<String, VoltProcedure>(16, (float) .1);
    private final Mailbox m_mailbox;
    final ExecutionEngine ee;
    final HsqlBackend hsql;
    public volatile boolean m_shouldContinue = true;

    /*
     * Recover a site at a time to make the interval in which other sites
     * are blocked as small as possible. The permit will be generated once.
     * The permit is only acquired by recovering partitions and not the source
     * partitions.
     */
    public static final Semaphore m_recoveryPermit = new Semaphore(Integer.MAX_VALUE);

    private boolean m_recovering = false;
    private boolean m_haveRecoveryPermit = false;
    private long m_recoveryStartTime = 0;
    private static AtomicLong m_recoveryBytesTransferred = new AtomicLong();

    // Catalog
    public CatalogContext m_context;
    Site getCatalogSite() {
        return m_context.cluster.getSites().get(Integer.toString(getSiteId()));
    }

    final int m_siteId;
    public final int getSiteId() {
        return m_siteId;
    }

    HashMap<Long, TransactionState> m_transactionsById = new HashMap<Long, TransactionState>();
    private final RestrictedPriorityQueue m_transactionQueue;

    private TransactionState m_currentPreparingTransactionState;

    // GWW: to support early fragment distribution
    // we will have multiple "currentTxnState" because each of them has
    // been retrieved to distribute fragment, but real work has not been
    // started, buf if we keep them in the RestrictedQueue, the head would
    // be read again and again and block later ones
    private  ArrayDeque<TransactionState> m_startedTxnQueue = new ArrayDeque<TransactionState>();
    
//    private ArrayDeque<TransactionState> m_preparedTxnQueue = new ArrayDeque<TransactionState>();
//    private ArrayDeque<TransactionState> m_committedAbortedTxnQueue = new ArrayDeque<TransactionState>();
    // back to finishingQ
    // GWW: not empty when running under escrow-mood, contains all txns which have finished
    // preparation and waiting to abort/commit
    private ArrayDeque<TransactionState> m_finishingTxnQueue = new ArrayDeque<TransactionState>();
    
    // GWW: handle data conflict
    private Long m_blockerTxn = Long.MIN_VALUE;
    
    private boolean m_cVoltDBMode = false;
    public boolean debugFlag = false;
    
    
    // GWW: main-sem to control only one thread executing ES code at one time
    private Semaphore m_semWaitForProcThread = new Semaphore(0, true);
    
    // GWW: wait and release for main-sem
    public void waitOnProcedure() {
    	try {
    		m_semWaitForProcThread.acquire();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    public void resume() {
    	m_semWaitForProcThread.release();
    }
    
   
    // GWW:
    public void newBlockerTxn(Long blockerTxnId, Long waitingTxnId) {
    	if(debugFlag)
    		cVoltTrace("GWW - newBlockerTxn:\tblocker-" + blockerTxnId + "\twaiter-" + waitingTxnId + " on site " + this.m_siteId);
    	
    	if (blockerTxnId > m_blockerTxn) {
    		m_blockerTxn = blockerTxnId;
    	}
    }
    
    // GWW:
    public void cVoltTrace(String msg) {
    	Integer hostId = m_context.siteTracker.getHostForSite(m_siteId);
    	if(debugFlag)
    		cVoltLog.info(System.nanoTime() + " - " + msg);
    }
    
    // The time in ms since epoch of the last call to tick()
    long lastTickTime = 0;
    long lastCommittedTxnId = 0;
    long lastCommittedTxnTime = 0;

    /*
     * Due to failures we may find out about commited multi-part txns
     * before running the commit fragment. Handle node fault will generate
     * the fragment, but it is possible for a new failure to be detected
     * before the fragment can be run due to the order messages are pulled
     * from subjects. Maintain and send this value when discovering/sending
     * failure data.
     *
     * This value only gets updated on multi-partition transactions that are
     * not read-only.
     */
    long lastKnownGloballyCommitedMultiPartTxnId = 0;

    public final static long kInvalidUndoToken = -1L;
    private long latestUndoToken = 0L;

    public long getNextUndoToken() {
        return ++latestUndoToken;
    }
    
    // GWW
    public long getLatestUndoToken() {
    	return latestUndoToken;
    }
    
    // increase endUndoToken automatically
    public long getNextUndoTokenForTxn() {
    	m_currentPreparingTransactionState.setEndUndoToken(++latestUndoToken);
    	return latestUndoToken;
    }

    // Each execution site manages snapshot using a SnapshotSiteProcessor
    private final SnapshotSiteProcessor m_snapshotter;

    private RecoverySiteProcessor m_recoveryProcessor = null;

    // Trigger if shutdown has been run already.
    private boolean haveShutdownAlready;

    private final TableStats m_tableStats;
    private final IndexStats m_indexStats;
    private final StarvationTracker m_starvationTracker;
    private final Watchdog m_watchdog;
    private class Watchdog extends Thread {
        private volatile boolean m_shouldContinue = true;
        private volatile boolean m_petted = false;
        private final int m_siteIndex;
        private final int m_siteId;
        private Thread m_watchThread = null;
        public Watchdog(final int siteIndex, final int siteId) {
            super(null, null, "ExecutionSite " + siteIndex + " siteId: " + siteId + " watchdog ", 262144);
            m_siteIndex = siteIndex;
            m_siteId = siteId;
        }

        public void pet() {
            m_petted = true;
        }

        @Override
        public void run() {
            if (m_watchThread == null) {
                throw new RuntimeException("Use start(Thread watchThread) not Thread.start()");
            }
            try {
                Thread.sleep(30000);
            } catch (final InterruptedException e) {
                return;
            }
            while (m_shouldContinue) {
                try {
                    Thread.sleep(5000);
                } catch (final InterruptedException e) {
                    return;
                }
                if (!m_petted) {
                    final StackTraceElement trace[] = m_watchThread.getStackTrace();
                    final Throwable throwable = new Throwable();
                    throwable.setStackTrace(trace);
                    log.l7dlog( Level.WARN, LogKeys.org_voltdb_ExecutionSite_Watchdog_possibleHang.name(), new Object[]{ m_siteIndex, m_siteId}, throwable);
                }
                m_petted = false;
            }
        }

        @Override
        public void start() {
            throw new UnsupportedOperationException("Use start(Thread watchThread)");
        }

        public void start(final Thread thread) {
            m_watchThread = thread;
            super.start();
        }
    }

    // This message is used to start a local snapshot. The snapshot
    // is *not* automatically coordinated across the full node set.
    // That must be arranged separately.
    public static class ExecutionSiteLocalSnapshotMessage extends VoltMessage
    {
        public final String path;
        public final String nonce;
        public final boolean crash;

        /**
         * @param roadblocktxnid
         * @param path
         * @param nonce
         * @param crash Should Volt crash itself afterwards
         */
        public ExecutionSiteLocalSnapshotMessage(long roadblocktxnid,
                                                 String path,
                                                 String nonce,
                                                 boolean crash) {
            m_roadblockTransactionId = roadblocktxnid;
            this.path = path;
            this.nonce = nonce;
            this.crash = crash;
        }

        @Override
        protected void flattenToBuffer(DBBPool pool) {
            // can be empty if only used locally
        }

        @Override
        protected void initFromBuffer() {
            // can be empty if only used locally
        }

        @Override
        public byte getSubject() {
            return Subject.FAILURE.getId();
        }

        long m_roadblockTransactionId;
    }

    // This message is used locally to schedule a node failure event's
    // required  processing at an execution site.
    static class ExecutionSiteNodeFailureMessage extends VoltMessage
    {
        final HashSet<NodeFailureFault> m_failedHosts;
        ExecutionSiteNodeFailureMessage(HashSet<NodeFailureFault> failedHosts)
        {
            m_failedHosts = failedHosts;
        }

        @Override
        protected void flattenToBuffer(DBBPool pool) {} // can be empty if only used locally

        @Override
        protected void initFromBuffer() {} // can be empty if only used locally

        @Override
        public byte getSubject() {
            return Subject.FAILURE.getId();
        }
    }

    /**
     * Generated when a snapshot buffer is discarded. Reminds the EE thread
     * that there is probably more snapshot work to do.
     */
    static class PotentialSnapshotWorkMessage extends VoltMessage
    {
        @Override
        protected void flattenToBuffer(DBBPool pool) {} // can be empty if only used locally
        @Override
        protected void initFromBuffer() {} // can be empty if only used locally

        @Override
        public byte getSubject() {
            return Subject.DEFAULT.getId();
        }
    }

    // This message is used locally to get the currently active TransactionState
    // to check whether or not its WorkUnit's dependencies have been satisfied.
    // Necessary after handling a node failure.
    static class CheckTxnStateCompletionMessage extends VoltMessage
    {
        final long m_txnId;
        CheckTxnStateCompletionMessage(long txnId)
        {
            m_txnId = txnId;
        }

        @Override
        protected void flattenToBuffer(DBBPool pool) {} // can be empty if only used locally
        @Override
        protected void initFromBuffer() {} // can be empty if only used locally
    }

    private class ExecutionSiteNodeFailureFaultHandler implements FaultHandler
    {
        @Override
        public void faultOccured(Set<VoltFault> faults)
        {
            if (m_shouldContinue == false) {
                return;
            }
            HashSet<NodeFailureFault> failedNodes = new HashSet<NodeFailureFault>();
            for (VoltFault fault : faults) {
                if (fault instanceof NodeFailureFault)
                {
                    NodeFailureFault node_fault = (NodeFailureFault)fault;
                    failedNodes.add(node_fault);
                }
                else
                {
                    VoltDB.instance().getFaultDistributor().reportFaultHandled(this, fault);
                }
            }
            if (!failedNodes.isEmpty()) {
                m_mailbox.deliver(new ExecutionSiteNodeFailureMessage(failedNodes));
            }
        }

        @Override
        public void faultCleared(Set<VoltFault> faults) {
        }
    }

    private final HashMap<Long, VoltSystemProcedure> m_registeredSysProcPlanFragments =
        new HashMap<Long, VoltSystemProcedure>();


    /**
     * Log settings changed. Signal EE to update log level.
     */
    public void updateBackendLogLevels() {
        ee.setLogLevels(org.voltdb.jni.EELoggers.getLogLevels());
    }

    void startShutdown() {
        m_shouldContinue = false;
    }

    /**
     * Shutdown all resources that need to be shutdown for this <code>ExecutionSite</code>.
     * May be called twice if recursing via recursableRun(). Protected against that..
     */
    public void shutdown() {
        if (haveShutdownAlready) {
            return;
        }
        haveShutdownAlready = true;
        m_shouldContinue = false;

        boolean finished = false;
        while (!finished) {
            try {
                if (m_watchdog.isAlive()) {
                    m_watchdog.m_shouldContinue = false;
                    m_watchdog.interrupt();
                    m_watchdog.join();
                }

                m_transactionQueue.shutdown();

                if (hsql != null) {
                    hsql.shutdown();
                }
                if (ee != null) {
                    ee.release();
                }
                finished = true;
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }

        m_snapshotter.shutdown();
    }

    /**
     * Passed to recovery processors which forward non-recovery messages to this handler.
     * Also used when recovery is enabled and there is no recovery processor for messages
     * received once the priority queue is initialized and returning txns. It is necessary
     * to do the special prehandling in this handler where txnids that are earlier then what
     * has been released from the queue during recovery because multi-part txns can involve
     * the recovering partition after the queue has already released work after the multi-part txn.
     * The recovering partition was going to give an empty responses anyways so it is fine to do
     * that in this message handler.
     */
    private final MessageHandler m_recoveryMessageHandler = new MessageHandler() {
        @Override
        public void handleMessage(VoltMessage message, long txnId) {
            if (message instanceof TransactionInfoBaseMessage) {
                long noticeTxnId = ((TransactionInfoBaseMessage)message).getTxnId();
                /**
                 * If the recovery processor and by extension this site receives
                 * a message regarding a txnid < the current supplied txnId then
                 * the message is for a multi-part txn that this site is a member of
                 * but doesn't have any info for. Send an ack with no extra processing.
                 */
                if (noticeTxnId < txnId) {
                    if (message instanceof CompleteTransactionMessage) {
                        CompleteTransactionMessage complete = (CompleteTransactionMessage)message;
                        CompleteTransactionResponseMessage ctrm =
                            new CompleteTransactionResponseMessage(complete, m_siteId);
                        try
                        {
                            m_mailbox.send(complete.getCoordinatorSiteId(), 0, ctrm);
                        }
                        catch (MessagingException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (message instanceof FragmentTaskMessage) {
                        FragmentTaskMessage ftask = (FragmentTaskMessage)message;
                        FragmentResponseMessage response = new FragmentResponseMessage(ftask, m_siteId);
                        response.setRecovering(true);
                        response.setStatus(FragmentResponseMessage.SUCCESS, null);

                        // add a dummy table for all of the expected dependency ids
                        for (int i = 0; i < ftask.getFragmentCount(); i++) {
                            response.addDependency(ftask.getOutputDepId(i),
                                    new VoltTable(new VoltTable.ColumnInfo("DUMMY", VoltType.BIGINT)));
                        }

                        try {
                            m_mailbox.send(response.getDestinationSiteId(), 0, response);
                        } catch (MessagingException e) {
                            throw new RuntimeException(e);
                        }

                    } else {
                        handleMailboxMessageNonRecursable(message);
                    }
                } else {
                    handleMailboxMessageNonRecursable(message);
                }
            } else {
                handleMailboxMessageNonRecursable(message);
            }

        }
    };

    /**
     * This is invoked after all recovery data has been received/sent. The processor can be nulled out for GC.
     */
    private final Runnable m_onRecoveryCompletion = new Runnable() {
        @Override
        public void run() {
            final long now = System.currentTimeMillis();
            final long transferred = m_recoveryProcessor.bytesTransferred();
            final long bytesTransferredTotal = m_recoveryBytesTransferred.addAndGet(transferred);
            final long megabytes = transferred / (1024 * 1024);
            final double megabytesPerSecond = megabytes / ((now - m_recoveryStartTime) / 1000.0);
            m_recoveryProcessor = null;
            m_recovering = false;
            if (m_haveRecoveryPermit) {
                m_haveRecoveryPermit = false;
                m_recoveryPermit.release();
                m_recoveryLog.info(
                        "Destination recovery complete for site " + m_siteId +
                        " partition " + m_context.siteTracker.getPartitionForSite(m_siteId) +
                        " after " + ((now - m_recoveryStartTime) / 1000) + " seconds " +
                        " with " + megabytes + " megabytes transferred " +
                        " at a rate of " + megabytesPerSecond + " megabytes/sec");
                int remaining = recoveringSiteCount.decrementAndGet();
                if (remaining == 0) {
                    ee.toggleProfiler(0);
                    VoltDB.instance().onExecutionSiteRecoveryCompletion(bytesTransferredTotal);
                }
            } else {
                m_recoveryLog.info("Source recovery complete for site " + m_siteId +
                        " partition " + m_context.siteTracker.getPartitionForSite(m_siteId) +
                        " after " + ((now - m_recoveryStartTime) / 1000) + " seconds " +
                        " with " + megabytes + " megabytes transferred " +
                        " at a rate of " + megabytesPerSecond + " megabytes/sec");
            }
        }
    };

    public void tick() {
        // invoke native ee tick if at least one second has passed
        final long time = EstTime.currentTimeMillis();
        final long prevLastTickTime = lastTickTime;
        if ((time - lastTickTime) >= 1000) {
            if ((lastTickTime != 0) && (ee != null)) {
                ee.tick(time, lastCommittedTxnId);
            }
            lastTickTime = time;
        }

        // do other periodic work
        m_snapshotter.doSnapshotWork(ee, false);
        m_watchdog.pet();

        /*
         * grab the table statistics from ee and put it into the statistics
         * agent if at least 1/3 of the statistics broadcast interval has past.
         * This ensures that when the statistics are broadcasted, they are
         * relatively up-to-date.
         */
        if (m_tableStats != null
            && (time - prevLastTickTime) >= StatsManager.POLL_INTERVAL * 2) {
            CatalogMap<Table> tables = m_context.database.getTables();
            int[] tableIds = new int[tables.size()];
            int i = 0;
            for (Table table : tables) {
                tableIds[i++] = table.getRelativeIndex();
            }

            // data to aggregate
            long tupleCount = 0;
            int tupleDataMem = 0;
            int tupleAllocatedMem = 0;
            int indexMem = 0;
            int stringMem = 0;

            // update table stats
            final VoltTable[] s1 =
                ee.getStats(SysProcSelector.TABLE, tableIds, false, time);
            if (s1 != null) {
                VoltTable stats = s1[0];
                assert(stats != null);

                // rollup the table memory stats for this site
                while (stats.advanceRow()) {
                    tupleCount += stats.getLong(7);
                    tupleAllocatedMem += (int) stats.getLong(8);
                    tupleDataMem += (int) stats.getLong(9);
                    stringMem += (int) stats.getLong(10);
                }
                stats.resetRowPosition();

                m_tableStats.setStatsTable(stats);

            }

            // update index stats
            final VoltTable[] s2 =
                ee.getStats(SysProcSelector.INDEX, tableIds, false, time);
            if ((s2 != null) && (s2.length > 0)) {
                VoltTable stats = s2[0];
                assert(stats != null);

                // rollup the index memory stats for this site
                while (stats.advanceRow()) {
                    indexMem += stats.getLong(10);
                }
                stats.resetRowPosition();

                m_indexStats.setStatsTable(stats);
            }

            // update the rolled up memory statistics
            MemoryStats memoryStats = VoltDB.instance().getMemoryStatsSource();
            if (memoryStats != null) {
                memoryStats.eeUpdateMemStats(m_siteId,
                                             tupleCount,
                                             tupleDataMem,
                                             tupleAllocatedMem,
                                             indexMem,
                                             stringMem,
                                             ee.getThreadLocalPoolAllocations());
            }
        }
    }


    /**
     * SystemProcedures are "friends" with ExecutionSites and granted
     * access to internal state via m_systemProcedureContext.
     */
    public interface SystemProcedureExecutionContext {
        public Database getDatabase();
        public Cluster getCluster();
        public Site getSite();
        public ExecutionEngine getExecutionEngine();
        public long getLastCommittedTxnId();
        public long getCurrentTxnId();
        public long getNextUndo();
        public ExecutionSite getExecutionSite();
        public HashMap<String, VoltProcedure> getProcedures();
    }

    protected class SystemProcedureContext implements SystemProcedureExecutionContext {
        @Override
        public Database getDatabase()                         { return m_context.database; }
        @Override
        public Cluster getCluster()                           { return m_context.cluster; }
        @Override
        public Site getSite()                                 { return getCatalogSite(); }
        @Override
        public ExecutionEngine getExecutionEngine()           { return ee; }
        @Override
        public long getLastCommittedTxnId()                   { return lastCommittedTxnId; }
        @Override
        public long getCurrentTxnId()                         { return m_currentPreparingTransactionState.txnId; }
        @Override
        public long getNextUndo()                             { return getNextUndoToken(); }
        @Override
        public ExecutionSite getExecutionSite()               { return ExecutionSite.this; }
        @Override
        public HashMap<String, VoltProcedure> getProcedures() { return procs; }
    }

    SystemProcedureContext m_systemProcedureContext;

    /**
     * Dummy ExecutionSite useful to some tests that require Mock/Do-Nothing sites.
     * @param siteId
     */
    ExecutionSite(int siteId) {
        m_siteId = siteId;
        m_systemProcedureContext = new SystemProcedureContext();
        m_watchdog = null;
        ee = null;
        hsql = null;
        m_snapshotter = null;
        m_mailbox = null;
        m_transactionQueue = null;
        m_starvationTracker = null;
        m_tableStats = null;
        m_indexStats = null;
    }

    ExecutionSite(VoltDBInterface voltdb, Mailbox mailbox,
                  final int siteId, String serializedCatalog,
                  RestrictedPriorityQueue transactionQueue,
                  boolean recovering,
                  HashSet<Integer> failedHostIds,
                  final long txnId)
    {
        hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_Initializing.name(),
                new Object[] { String.valueOf(siteId) }, null);

        m_siteId = siteId;
        String txnlog_name = ExecutionSite.class.getName() + "." + m_siteId;
        m_txnlog = new VoltLogger(txnlog_name);
        m_recovering = recovering;
        m_context = voltdb.getCatalogContext();
        //lastCommittedTxnId = txnId;
        for (Integer failedHostId : failedHostIds) {
            m_knownFailedSites.addAll(m_context.siteTracker.getAllSitesForHost(failedHostId));
        }
        m_handledFailedSites.addAll(m_knownFailedSites);

        VoltDB.instance().getFaultDistributor().
        registerFaultHandler(NodeFailureFault.NODE_FAILURE_EXECUTION_SITE,
                             m_faultHandler,
                             FaultType.NODE_FAILURE);

        if (voltdb.getBackendTargetType() == BackendTarget.NONE) {
            ee = new MockExecutionEngine();
            hsql = null;
        }
        else if (voltdb.getBackendTargetType() == BackendTarget.HSQLDB_BACKEND) {
            hsql = initializeHSQLBackend();
            ee = new MockExecutionEngine();
        }
        else {
            if (serializedCatalog == null) {
                serializedCatalog = voltdb.getCatalogContext().catalog.serialize();
            }
            hsql = null;
            ee = initializeEE(voltdb.getBackendTargetType(), serializedCatalog, txnId);
        }

        // Should pass in the watchdog class to allow sleepy dogs..
        m_watchdog = new Watchdog(siteId, siteIndex);

        m_systemProcedureContext = new SystemProcedureContext();
        m_mailbox = mailbox;

        // allow dependency injection of the transaction queue implementation
        m_transactionQueue =
            (transactionQueue != null) ? transactionQueue : initializeTransactionQueue(siteId);

        loadProcedures(voltdb.getBackendTargetType());

        int snapshotPriority = 6;
        if (m_context.cluster.getDeployment().get("deployment") != null) {
            snapshotPriority = m_context.cluster.getDeployment().get("deployment").
                getSystemsettings().get("systemsettings").getSnapshotpriority();
        }
        m_snapshotter = new SnapshotSiteProcessor(new Runnable() {
            @Override
            public void run() {
                m_mailbox.deliver(new PotentialSnapshotWorkMessage());
            }
        },
         snapshotPriority);

        final StatsAgent statsAgent = VoltDB.instance().getStatsAgent();
        m_starvationTracker = new StarvationTracker(String.valueOf(getCorrespondingSiteId()), getCorrespondingSiteId());
        statsAgent.registerStatsSource(SysProcSelector.STARVATION,
                                       Integer.parseInt(getCorrespondingCatalogSite().getTypeName()),
                                       m_starvationTracker);
        m_tableStats = new TableStats(String.valueOf(getCorrespondingSiteId()), getCorrespondingSiteId());
        statsAgent.registerStatsSource(SysProcSelector.TABLE,
                                       Integer.parseInt(getCorrespondingCatalogSite().getTypeName()),
                                       m_tableStats);
        m_indexStats = new IndexStats(String.valueOf(getCorrespondingSiteId()), getCorrespondingSiteId());
        statsAgent.registerStatsSource(SysProcSelector.INDEX,
                                       Integer.parseInt(getCorrespondingCatalogSite().getTypeName()),
                                       m_indexStats);

    }

    private RestrictedPriorityQueue initializeTransactionQueue(final int siteId)
    {
        // build an array of all the initiators
        int initiatorCount = 0;
        for (final Site s : m_context.siteTracker.getUpSites())
            if (s.getIsexec() == false)
                initiatorCount++;
        final int[] initiatorIds = new int[initiatorCount];
        int index = 0;
        for (final Site s : m_context.siteTracker.getUpSites())
            if (s.getIsexec() == false)
                initiatorIds[index++] = Integer.parseInt(s.getTypeName());

        // turn off the safety dance for single-node voltdb
        boolean useSafetyDance = m_context.numberOfNodes > 1;

        assert(m_mailbox != null);
        RestrictedPriorityQueue retval = new RestrictedPriorityQueue(
                initiatorIds,
                siteId,
                m_mailbox,
                VoltDB.DTXN_MAILBOX_ID,
                useSafetyDance);
        return retval;
    }

    private HsqlBackend initializeHSQLBackend()
    {
        HsqlBackend hsqlTemp = null;
        try {
            hsqlTemp = new HsqlBackend(getSiteId());
            final String hexDDL = m_context.database.getSchema();
            final String ddl = Encoder.hexDecodeToString(hexDDL);
            final String[] commands = ddl.split("\n");
            for (String command : commands) {
                String decoded_cmd = Encoder.hexDecodeToString(command);
                decoded_cmd = decoded_cmd.trim();
                if (decoded_cmd.length() == 0) {
                    continue;
                }
                hsqlTemp.runDDL(decoded_cmd);
            }
        }
        catch (final Exception ex) {
            hostLog.l7dlog( Level.FATAL, LogKeys.host_ExecutionSite_FailedConstruction.name(),
                            new Object[] { getSiteId(), siteIndex }, ex);
            VoltDB.crashVoltDB();
        }
        return hsqlTemp;
    }

    private ExecutionEngine
    initializeEE(BackendTarget target, String serializedCatalog, final long txnId)
    {
        String hostname = ConnectionUtil.getHostnameOrAddress();

        ExecutionEngine eeTemp = null;
        try {
            if (target == BackendTarget.NATIVE_EE_JNI) {
                Site site = getCatalogSite();
                eeTemp =
                    new ExecutionEngineJNI(
                        this,
                        m_context.cluster.getRelativeIndex(),
                        getSiteId(),
                        Integer.valueOf(site.getPartition().getTypeName()),
                        Integer.valueOf(site.getHost().getTypeName()),
                        hostname,
                        m_context.cluster.getDeployment().get("deployment").
                        getSystemsettings().get("systemsettings").getMaxtemptablesize());
                eeTemp.loadCatalog( txnId, serializedCatalog);
                lastTickTime = EstTime.currentTimeMillis();
                eeTemp.tick( lastTickTime, txnId);
            }
            else {
                // set up the EE over IPC
                Site site = getCatalogSite();
                eeTemp =
                    new ExecutionEngineIPC(
                            this,
                            m_context.cluster.getRelativeIndex(),
                            getSiteId(),
                            Integer.valueOf(site.getPartition().getTypeName()),
                            Integer.valueOf(site.getHost().getTypeName()),
                            hostname,
                            m_context.cluster.getDeployment().get("deployment").
                            getSystemsettings().get("systemsettings").getMaxtemptablesize(),
                            target,
                            VoltDB.instance().getConfig().m_ipcPorts.remove(0));
                eeTemp.loadCatalog( 0, serializedCatalog);
                lastTickTime = EstTime.currentTimeMillis();
                eeTemp.tick( lastTickTime, 0);
            }
        }
        // just print error info an bail if we run into an error here
        catch (final Exception ex) {
            hostLog.l7dlog( Level.FATAL, LogKeys.host_ExecutionSite_FailedConstruction.name(),
                            new Object[] { getSiteId(), siteIndex }, ex);
            VoltDB.crashVoltDB();
        }
        return eeTemp;
    }

    public boolean updateClusterState(String catalogDiffCommands) {
        m_context = VoltDB.instance().getCatalogContext();
        m_knownFailedSites.removeAll(m_context.siteTracker.getAllLiveSites());
        m_handledFailedSites.removeAll(m_context.siteTracker.getAllLiveSites());

        // make sure the restricted priority queue knows about all of the up initiators
        // for most catalog changes this will do nothing
        // for rejoin, it will matter
        int newInitiators = 0;
        for (Site s : m_context.catalog.getClusters().get("cluster").getSites()) {
            if (s.getIsexec() == false && s.getIsup()) {
                newInitiators += m_transactionQueue.ensureInitiatorIsKnown(Integer.parseInt(s.getTypeName()));
            }
        }

        return true;
    }

    public boolean updateCatalog(String catalogDiffCommands, CatalogContext context) {
        m_context = context;
        loadProcedures(VoltDB.getEEBackendType());

        //Necessary to quiesce before updating the catalog
        //so export data for the old generation is pushed to Java.
        ee.quiesce(lastCommittedTxnId);
        ee.updateCatalog( context.m_transactionId, catalogDiffCommands);

        return true;
    }

    void loadProcedures(BackendTarget backendTarget) {
        procs.clear();
        m_registeredSysProcPlanFragments.clear();
        loadProceduresFromCatalog(backendTarget);
        loadSystemProcedures(backendTarget);
    }

    private void loadProceduresFromCatalog(BackendTarget backendTarget) {
        // load up all the stored procedures
        final CatalogMap<Procedure> catalogProcedures = m_context.database.getProcedures();
        for (final Procedure proc : catalogProcedures) {

            // Sysprocs used to be in the catalog. Now they aren't. Ignore
            // sysprocs found in old catalog versions. (PRO-365)
            if (proc.getTypeName().startsWith("@")) {
                continue;
            }

            VoltProcedure wrapper = null;
            if (proc.getHasjava()) {
                final String className = proc.getClassname();
                Class<?> procClass = null;
                try {
                    procClass = m_context.classForProcedure(className);
                }
                catch (final ClassNotFoundException e) {

                    hostLog.l7dlog(
                            Level.WARN,
                            LogKeys.host_ExecutionSite_GenericException.name(),
                            new Object[] { getSiteId(), siteIndex },
                            e);
                    VoltDB.crashVoltDB();
                }
                try {
                    wrapper = (VoltProcedure) procClass.newInstance();
                }
                catch (final InstantiationException e) {
                    hostLog.l7dlog( Level.WARN, LogKeys.host_ExecutionSite_GenericException.name(),
                                    new Object[] { getSiteId(), siteIndex }, e);
                }
                catch (final IllegalAccessException e) {
                    hostLog.l7dlog( Level.WARN, LogKeys.host_ExecutionSite_GenericException.name(),
                                    new Object[] { getSiteId(), siteIndex }, e);
                }
            }
            else {
                wrapper = new VoltProcedure.StmtProcedure();
            }

            wrapper.init(m_context.cluster.getPartitions().size(),
                         this, proc, backendTarget, hsql, m_context.cluster);
            procs.put(proc.getTypeName(), wrapper);
        }
    }

    private void loadSystemProcedures(BackendTarget backendTarget) {
        Set<Entry<String,Config>> entrySet = SystemProcedureCatalog.listing.entrySet();
        for (Entry<String, Config> entry : entrySet) {
            Config sysProc = entry.getValue();
            Procedure proc = sysProc.asCatalogProcedure();

            VoltProcedure wrapper = null;
            final String className = sysProc.getClassname();
            Class<?> procClass = null;
            try {
                procClass = m_context.classForProcedure(className);
            }
            catch (final ClassNotFoundException e) {
                // TODO: check community/pro condition here.
                if (sysProc.commercial) {
                    continue;
                }
                hostLog.l7dlog(
                        Level.WARN,
                        LogKeys.host_ExecutionSite_GenericException.name(),
                        new Object[] { getSiteId(), siteIndex },
                        e);
                VoltDB.crashVoltDB();
            }

            try {
                wrapper = (VoltProcedure) procClass.newInstance();
            }
            catch (final InstantiationException e) {
                hostLog.l7dlog( Level.WARN, LogKeys.host_ExecutionSite_GenericException.name(),
                        new Object[] { getSiteId(), siteIndex }, e);
            }
            catch (final IllegalAccessException e) {
                hostLog.l7dlog( Level.WARN, LogKeys.host_ExecutionSite_GenericException.name(),
                        new Object[] { getSiteId(), siteIndex }, e);
            }

            wrapper.init(m_context.cluster.getPartitions().size(),
                         this, proc, backendTarget, hsql, m_context.cluster);
            procs.put(entry.getKey(), wrapper);
        }
    }


    /**
     * @return: true - no message received but done some snapshot work
     * 			false - 1. this site has nothing to do
     * 					2. this site just distributes next ready txn
     * 					3. this site just handles a message
     * 					4. this site handles a message and distributes next ready txn
     */
    public boolean background(boolean doStarvationTracking) {
    	
        VoltMessage message;
        if(doStarvationTracking) {
        	message = m_mailbox.recv();
	        if (message == null) {
	        	// doSnapshotWork() returns null if no snapshot work done
	            if (m_snapshotter.doSnapshotWork(ee, true) != null) {
	                return true;
	            } else {
	                m_starvationTracker.beginStarvation();
	                message = m_mailbox.recvBlocking(5);
	                m_starvationTracker.endStarvation();
	            }
	        }
        } else
        	message = m_mailbox.recvBlocking(5);

        // do periodic work
        tick();
        if (message != null) {
            handleMailboxMessage(message);
        } else {
            //idle, do snapshot work
            m_snapshotter.doSnapshotWork(ee, true);
        }

        if(m_startedTxnQueue.size() < 100) {
        	TransactionState newTxnState = (TransactionState)m_transactionQueue.poll();
 	
	    	if(newTxnState != null) {
				// GWW: for stats
	    		if(stats)
	    			newTxnState.startedT = System.nanoTime();
				
				if(newTxnState instanceof MultiPartitionParticipantTxnState && newTxnState.isCoordinator()) {
					// call doWork() once to distribute fragments
					boolean ret = newTxnState.doWork(m_recovering);
					assert(ret == false || newTxnState.needsRollback());
				}
				m_startedTxnQueue.add(newTxnState);
		    }
        }
		return false;
    }

    // GWW: for stats
    BufferedWriter out;
    String path = "/tmp/voltdb-2.1.3-output/";
    String filename = "-txnTime.txt";
    boolean stats = false;
    
    // GWW: original primary loop with 
    // 					1. replacing recursableRun() with new loop
    //					2. check available Txn ready to start when idle
    //					3. next run Txn is always polled from m_multiPartitionReadyTxnQueue now
    @Override
    public void run() {
        // enumerate site id (pad to 4 digits for sort)
        String name = "ExecutionSite:";
        if (getSiteId() < 10) name += "0";
        if (getSiteId() < 100) name += "0";
        if (getSiteId() < 1000) name += "0";
        name += String.valueOf(getSiteId());
        Thread.currentThread().setName(name);

        // Commenting this out when making logging more abstrace (is that ok?)
        //NDC.push("ExecutionSite - " + getSiteId() + " index " + siteIndex);
        if (VoltDB.getUseWatchdogs()) {
            m_watchdog.start(Thread.currentThread());
        }
        
        // GWW: for stats
        if(stats) {
	        try {	        	
				out = new BufferedWriter(new OutputStreamWriter(
		                  new FileOutputStream(path + "Host-" + InetAddress.getLocalHost().getHostAddress() 
		                		  + "-Node-" + this.m_siteId + "-" + filename, true), "UTF-8"));
				out.write("TxnId, ReceivedTime, StartedTime, ExecutedTime," +
						"FragsStart, FragsDone, ResumeTime, PreparedTime, " +
						"CommitDecisionTime, CompletedTime" +
						",TxnQueue, StartedQueue, FinishingQueue, TotalTime\n");

			} catch (IOException e1) {
				// TODO Auto-generated catch block
				System.err.println("GWW - Error when creating output files.");
				e1.printStackTrace();
			}
        }

        try {
            // Only poll messaging layer if necessary. Allow the poll
            // to block if the execution site is truly idle.
            while (m_shouldContinue) {
                /*
                 * If this partition is recovering, check for a permit and RPQ
                 * readiness. If it is time, create a recovery processor and send
                 * the initiate message.
                 */
                if (m_recovering && !m_haveRecoveryPermit) {
                    Long safeTxnId = m_transactionQueue.safeToRecover();
                    if (safeTxnId != null && m_recoveryPermit.tryAcquire()) {
                        m_haveRecoveryPermit = true;
                        m_recoveryStartTime = System.currentTimeMillis();
                        m_recoveryProcessor =
                            RecoverySiteProcessorDestination.createProcessor(
                                    m_context.database,
                                    m_context.siteTracker,
                                    ee,
                                    m_mailbox,
                                    m_siteId,
                                    m_onRecoveryCompletion,
                                    m_recoveryMessageHandler);
                    }
                }

                TransactionState currentTxnState = m_startedTxnQueue.poll();
                m_currentPreparingTransactionState = currentTxnState;
            
                if (currentTxnState == null) {
                	if(background(m_finishingTxnQueue.isEmpty()))
                		continue;
            	}                
                if (currentTxnState != null) {
                	if(currentTxnState.isCVoltDBExecution() != m_cVoltDBMode) {
                		while(!m_finishingTxnQueue.isEmpty())
                			background(false);
                		m_cVoltDBMode = currentTxnState.isCVoltDBExecution();
                		if(debugFlag)
                			cVoltTrace("changing cVoltDBMode to " + m_cVoltDBMode + " on seeing txn " + currentTxnState.txnId);
                	}
                    /*
                     * Before doing a transaction check if it is time to start recovery
                     * or do recovery work. The recovery processor checks
                     * if the txn is greater than X
                     */
                    if (m_recoveryProcessor != null) {
                        m_recoveryProcessor.doRecoveryWork(currentTxnState.txnId);
                    }
                    recursableRun(currentTxnState);
                }
                else if (m_recoveryProcessor != null) {
                    /*
                     * If there is no work in the system the minimum safe txnId is used to move
                     * recovery forward. This works because heartbeats will move the minimum safe txnId
                     * up even when there is no work for this partition.
                     */
                    Long foo = m_transactionQueue.safeToRecover();
                    if (foo != null) {
                        m_recoveryProcessor.doRecoveryWork(foo);
                    }
                }
            }
        }
        catch (final RuntimeException e) {
            hostLog.l7dlog( Level.ERROR, LogKeys.host_ExecutionSite_RuntimeException.name(), e);
            throw e;
        }
        
        // GWW: for stats
        if(stats) {
	        try {
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        
        shutdown();
    }

    /**
     * Run the execution site execution loop, for tests currently.
     * Will integrate this in to the real run loop soon.. ish.
     */
    public void runLoop(boolean loopUntilPoison) {
        while (m_shouldContinue) {
            TransactionState currentTxnState = (TransactionState)m_transactionQueue.poll();
            if (currentTxnState == null) {
                // poll the messaging layer for a while as this site has nothing to do
                // this will likely have a message/several messages immediately in a heavy workload
                VoltMessage message = m_mailbox.recv();
                tick();
                if (message != null) {
                    handleMailboxMessage(message);
                }
                else if (!loopUntilPoison){
                    // Terminate run loop on empty mailbox AND no currentTxnState
                    return;
                }
            }
            if (currentTxnState != null) {
                recursableRun(currentTxnState);
            }
        }
    }

    private void completeTransaction(TransactionState txnState) {
    	if(debugFlag)
    		cVoltTrace("completeTransaction - " + txnState.txnId + " read-only = " + txnState.isReadOnly());
        if (m_txnlog.isTraceEnabled())
        {
            m_txnlog.trace("FUZZTEST completeTransaction " + txnState.txnId);
        }
        if (!txnState.isReadOnly()) {
            assert(latestUndoToken != kInvalidUndoToken);
            assert(latestUndoToken >= txnState.getBeginUndoToken());

            // on coordinator it is possible that beginUndoToken is not set properly if
            // the txnState has early failure; but rollback a never started txn on participant
            // is not possible because all txn will have to call recursableRun() and gets a 
            // valid beginUndoToken
            if (txnState.getBeginUndoToken() == kInvalidUndoToken && 
            		!txnState.needsRollback() && !txnState.isCoordinator()) {
                if (m_recovering == false) {
                    throw new AssertionError("Non-recovering write txn has invalid undo state.");
                }
            }
            // release everything through the end of the current window.
            else if (txnState.getEndUndoToken() > txnState.getBeginUndoToken()) {
            	if(debugFlag)
            		cVoltTrace("completeTransaction - " + txnState.txnId + "\t" + txnState.getBeginUndoToken() + "\t" + txnState.getEndUndoToken());
            	
            	ee.release2UndoToken(txnState.txnId, txnState.getBeginUndoToken(), txnState.getEndUndoToken()); 
            }

            // reset for error checking purposes
            txnState.setBeginUndoToken(kInvalidUndoToken);
            txnState.setEndUndoToken(kInvalidUndoToken);
        }

        // advance the committed transaction point. Necessary for both Export
        // commit tracking and for fault detection transaction partial-transaction
        // resolution.
        if (!txnState.needsRollback())
        {
            if (txnState.txnId > lastCommittedTxnId) {
                lastCommittedTxnId = txnState.txnId;
                lastCommittedTxnTime = EstTime.currentTimeMillis();
                if (!txnState.isSinglePartition() && !txnState.isReadOnly())
                {
                    lastKnownGloballyCommitedMultiPartTxnId =
                        Math.max(txnState.txnId, lastKnownGloballyCommitedMultiPartTxnId);
                }
            }
        }
    }

    private void handleMailboxMessage(VoltMessage message) {
        if (m_recovering == true && m_recoveryProcessor == null && m_currentPreparingTransactionState != null) {
            m_recoveryMessageHandler.handleMessage(message, m_currentPreparingTransactionState.txnId);
        } else {
            handleMailboxMessageNonRecursable(message);
        }
    }

    private void handleMailboxMessageNonRecursable(VoltMessage message)
    {
        if (message instanceof TransactionInfoBaseMessage) {
            TransactionInfoBaseMessage info = (TransactionInfoBaseMessage)message;
            assertTxnIdOrdering(info);

            // Special case heartbeats which only update RPQ
            if (info instanceof HeartbeatMessage) {
                // use the heartbeat to unclog the priority queue if clogged
                long lastSeenTxnFromInitiator = m_transactionQueue.noteTransactionRecievedAndReturnLastSeen(
                        info.getInitiatorSiteId(), info.getTxnId(),
                        true, ((HeartbeatMessage) info).getLastSafeTxnId());

                // respond to the initiator with the last seen transaction
                HeartbeatResponseMessage response = new HeartbeatResponseMessage(
                        m_siteId, lastSeenTxnFromInitiator,
                        m_transactionQueue.getQueueState() == QueueState.BLOCKED_SAFETY);
                try {
                    m_mailbox.send(info.getInitiatorSiteId(), VoltDB.DTXN_MAILBOX_ID, response);
                } catch (MessagingException e) {
                    // hope this never happens... it doesn't right?
                    throw new RuntimeException(e);
                }
                // we're done here (in the case of heartbeats)
                return;
            }
            else if (info instanceof InitiateTaskMessage) {
                m_transactionQueue.noteTransactionRecievedAndReturnLastSeen(info.getInitiatorSiteId(),
                                                  info.getTxnId(),
                                                  false,
                                                  ((InitiateTaskMessage) info).getLastSafeTxnId());
            }
            //Participant notices are sent enmasse from the initiator to multiple partitions
            // and don't communicate any information about safe replication, hence DUMMY_LAST_SEEN_TXN_ID
            // it can be used for global ordering since it is a valid txnid from an initiator
            else if (info instanceof MultiPartitionParticipantMessage) {
                m_transactionQueue.noteTransactionRecievedAndReturnLastSeen(info.getInitiatorSiteId(),
                                                  info.getTxnId(),
                                                  false,
                                                  DtxnConstants.DUMMY_LAST_SEEN_TXN_ID);
            }

            // Every non-heartbeat notice requires a transaction state.
            TransactionState ts = m_transactionsById.get(info.getTxnId());
            if (info instanceof CompleteTransactionMessage)
            {
                CompleteTransactionMessage complete = (CompleteTransactionMessage)info;
                if (ts != null)
                {
                	if(debugFlag)
                		cVoltTrace("recd completeTxnMsg at site" + this.m_siteId + " for txn " + ts.txnId + " newOrdering - " + complete);
                    
                	if(stats)
                		ts.receivedCompleteTxnMsgT = System.nanoTime();
                	
                	ts.processCompleteTransaction(complete);
                    assert(ts.done());
                    
                    if(complete.isRollback() && !m_finishingTxnQueue.contains(ts) && !ts.isCoordinator()) {
                    	return;
                    }
                    
                    // too early to unblock, safe for ready-to-commit txn, but not for ready-to-abort txn
                    // GWW: txns are prepared in txnid order, and m_blockerTxn
                    // is initialized to be Long.MIN_VALUE
                    // but CompleteTxn msg might be received at non-coordinator
                    // site without order
//                    if(ts.txnId == m_blockerTxn)
//                    	m_currentPreparingTransactionState.unblockForDataConflict();
                   
                    completeFinishedTxns();
                }
                else
                {
                	if(debugFlag)
                		cVoltTrace("recd completeTxnMsg at site" + this.m_siteId + " for null txn");
                    // if we're getting a CompleteTransactionMessage
                    // and there's no transaction state, it's because
                    // we were the cause of the rollback and we bailed
                    // as soon as we signaled our failure to the coordinator.
                    // Just generate an ack to keep the coordinator happy.
                    if (complete.requiresAck())
                    {
                        CompleteTransactionResponseMessage ctrm =
                            new CompleteTransactionResponseMessage(complete, m_siteId);
                        try
                        {
                            m_mailbox.send(complete.getCoordinatorSiteId(), 0, ctrm);
                        }
                        catch (MessagingException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                return;
            }

            if (ts == null) {
                if (info.isSinglePartition()) {
                    ts = new SinglePartitionTxnState(m_mailbox, this, info);
                }
                else {
                    ts = new MultiPartitionParticipantTxnState(m_mailbox, this, info);
                }
                if (m_transactionQueue.add(ts)) {
                    m_transactionsById.put(ts.txnId, ts);
                    
                    if(stats)
                    	// GWW: for stats
                    	ts.receivedT = System.nanoTime();
                    
                } else {
                    hostLog.info(
                            "Dropping txn " + ts.txnId + " data from failed initiatorSiteId: " + ts.initiatorSiteId);
                }
            }

            if (ts != null)
            {
                if (message instanceof FragmentTaskMessage) {
                    ts.createLocalFragmentWork((FragmentTaskMessage)message, false);
                }
            }
        } else if (message instanceof RecoveryMessage) {
            RecoveryMessage rm = (RecoveryMessage)message;
            if (rm.recoveryMessagesAvailable()) {
                return;
            }
            assert(!m_recovering);
            assert(m_recoveryProcessor == null);
            final long recoveringPartitionTxnId = rm.txnId();
            m_recoveryStartTime = System.currentTimeMillis();
            m_recoveryLog.info(
                    "Recovery initiate received at site " + m_siteId +
                    " from site " + rm.sourceSite() + " requesting recovery start before txnid " +
                    recoveringPartitionTxnId);
            m_recoveryProcessor = RecoverySiteProcessorSource.createProcessor(
                    this,
                    rm,
                    m_context.database,
                    m_context.siteTracker,
                    ee,
                    m_mailbox,
                    m_siteId,
                    m_onRecoveryCompletion,
                    m_recoveryMessageHandler);
        }
        else if (message instanceof FragmentResponseMessage) {
            FragmentResponseMessage response = (FragmentResponseMessage)message;
            TransactionState txnState = m_transactionsById.get(response.getTxnId());
            
            if(debugFlag)
            	cVoltTrace("recd FragmentResponseMessage at site" + this.m_siteId + ", txn " + response.getTxnId() + "message: " + message);
            
            // possible in rollback to receive an unnecessary response
            if (txnState != null) {
                assert (txnState instanceof MultiPartitionParticipantTxnState);
                txnState.processRemoteWorkResponse(response);
            }
        }
        else if (message instanceof CompleteTransactionResponseMessage)
        {
            CompleteTransactionResponseMessage response =
                (CompleteTransactionResponseMessage)message;
            TransactionState txnState = m_transactionsById.get(response.getTxnId());
            
            if(debugFlag)
            	cVoltTrace("recd CompleteTransactionResponseMessage at site" + this.m_siteId + ", txn " + txnState.txnId + "message: " + message);
            
            // I believe a null txnState should eventually be impossible, let's
            // check for null for now
            if (txnState != null)
            {
                assert (txnState instanceof MultiPartitionParticipantTxnState);
                txnState.processCompleteTransactionResponse(response);

            	// GWW: when the last response received, the txn is ready to commit.
            	// we will commit the txn, and remove it from finishingQ and txnQ if
                // no txn with smaller id is still waiting to finish
				// GWWW: back to finishingQ
                if(txnState.done()) {
                	if(stats)
                		txnState.receivedCompleteTxnMsgT = System.nanoTime();
            		completeFinishedTxns();
                }
            }
        }
        else if (message instanceof ExecutionSiteNodeFailureMessage) {
            discoverGlobalFaultData((ExecutionSiteNodeFailureMessage)message);
        }
        else if (message instanceof CheckTxnStateCompletionMessage) {
            long txn_id = ((CheckTxnStateCompletionMessage)message).m_txnId;
            TransactionState txnState = m_transactionsById.get(txn_id);
            if (txnState != null)
            {
                assert(txnState instanceof MultiPartitionParticipantTxnState);
                ((MultiPartitionParticipantTxnState)txnState).checkWorkUnits();
            }
        }
        else if (message instanceof RawProcessor.ExportInternalMessage) {
            RawProcessor.ExportInternalMessage exportm =
                (RawProcessor.ExportInternalMessage) message;
            ee.exportAction(exportm.m_m.isSync(),
                                exportm.m_m.getAckOffset(),
                                0,
                                exportm.m_m.getPartitionId(),
                                exportm.m_m.getSignature());
        } else if (message instanceof PotentialSnapshotWorkMessage) {
            m_snapshotter.doSnapshotWork(ee, false);
        }
        else if (message instanceof ExecutionSiteLocalSnapshotMessage) {
            hostLog.info("Executing local snapshot. Completing any on-going snapshots.");

            // first finish any on-going snapshot
            try {
                HashSet<Exception> completeSnapshotWork = m_snapshotter.completeSnapshotWork(ee);
                if (completeSnapshotWork != null && !completeSnapshotWork.isEmpty()) {
                    for (Exception e : completeSnapshotWork) {
                        hostLog.error("Error completing in progress snapshot.", e);
                    }
                }
            } catch (InterruptedException e) {
                hostLog.warn("Interrupted during snapshot completion", e);
            }

            hostLog.info("Executing local snapshot. Creating new snapshot.");

            //Flush export data to the disk before the partition detection snapshot
            ee.quiesce(lastCommittedTxnId);

            // then initiate the local snapshot
            ExecutionSiteLocalSnapshotMessage snapshotMsg =
                    (ExecutionSiteLocalSnapshotMessage) message;
            String nonce = snapshotMsg.nonce + "_" + snapshotMsg.m_roadblockTransactionId;
            SnapshotSaveAPI saveAPI = new SnapshotSaveAPI();
            VoltTable startSnapshotting = saveAPI.startSnapshotting(snapshotMsg.path,
                                      nonce,
                                      (byte) 0x1,
                                      snapshotMsg.m_roadblockTransactionId,
                                      m_systemProcedureContext,
                                      ConnectionUtil.getHostnameOrAddress());
            if (SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.get() == -1 &&
                snapshotMsg.crash) {
                hostLog.info("Executing local snapshot. Finished final snapshot. Shutting down. " +
                        "Result: " + startSnapshotting.toString());
                VoltDB.crashVoltDB();
            }
        } else if (message instanceof LocalObjectMessage) {
              LocalObjectMessage lom = (LocalObjectMessage)message;
              ((Runnable)lom.payload).run();
        } else {
            hostLog.l7dlog(Level.FATAL, LogKeys.org_voltdb_dtxn_SimpleDtxnConnection_UnkownMessageClass.name(),
                           new Object[] { message.getClass().getName() }, null);
            VoltDB.crashVoltDB();
        }
    }

    private void assertTxnIdOrdering(final TransactionInfoBaseMessage notice) {
        // Because of our rollback implementation, fragment tasks can arrive
        // late. This participant can have aborted and rolled back already,
        // for example.
        //
        // Additionally, commit messages for read-only MP transactions can
        // arrive after sneaked-in SP transactions have advanced the last
        // committed transaction point. A commit message is a fragment task
        // with a null payload.
        if (notice instanceof FragmentTaskMessage ||
            notice instanceof CompleteTransactionMessage)
        {
            return;
        }

        if (notice.getTxnId() < lastCommittedTxnId) {
            StringBuilder msg = new StringBuilder();
            msg.append("Txn ordering deadlock (DTXN) at site ").append(m_siteId).append(":\n");
            msg.append("   txn ").append(lastCommittedTxnId).append(" (");
            msg.append(TransactionIdManager.toString(lastCommittedTxnId)).append(" HB: ?");
            msg.append(") before\n");
            msg.append("   txn ").append(notice.getTxnId()).append(" (");
            msg.append(TransactionIdManager.toString(notice.getTxnId())).append(" HB:");
            msg.append(notice instanceof HeartbeatMessage).append(").\n");

            TransactionState txn = m_transactionsById.get(notice.getTxnId());
            if (txn != null) {
                msg.append("New notice transaction already known: " + txn.toString() + "\n");
            }
            else {
                msg.append("New notice is for new or completed transaction.\n");
            }
            msg.append("New notice of type: " + notice.getClass().getName());
            log.fatal(msg.toString());
            VoltDB.crashVoltDB();
        }

        if (notice instanceof InitiateTaskMessage) {
            InitiateTaskMessage task = (InitiateTaskMessage)notice;
            assert (task.getInitiatorSiteId() != getSiteId());
        }
    }

    /**
     * Find the global multi-partition commit point and the global initiator point for the
     * failed host.
     *
     * @param failedHostId the host id of the failed node.
     */
    private void discoverGlobalFaultData(ExecutionSiteNodeFailureMessage message)
    {
        //Keep it simple and don't try to recover on the recovering node.
        if (m_recovering) {
            m_recoveryLog.fatal("Aborting recovery due to a remote node failure. Retry again.");
            VoltDB.crashVoltDB();
        }
        HashSet<NodeFailureFault> failures = message.m_failedHosts;

        // Fix context and associated site tracker first - need
        // an accurate topology to perform discovery.
        m_context = VoltDB.instance().getCatalogContext();

        HashSet<Integer> failedSiteIds = new HashSet<Integer>();
        for (NodeFailureFault fault : failures) {
            failedSiteIds.addAll(m_context.siteTracker.getAllSitesForHost(fault.getHostId()));
        }
        m_knownFailedSites.addAll(failedSiteIds);

        HashMap<Integer, Long> initiatorSafeInitPoint = new HashMap<Integer, Long>();
        int expectedResponses = discoverGlobalFaultData_send();
        Long multiPartitionCommitPoint = discoverGlobalFaultData_rcv(expectedResponses, initiatorSafeInitPoint);

        if (multiPartitionCommitPoint == null) {
            return;
        }


        // Agreed on a fault set.

        // Do the work of patching up the execution site.
        // Do a little work to identify the newly failed site ids and only handle those

        HashSet<Integer> newFailedSiteIds = new HashSet<Integer>(failedSiteIds);
        newFailedSiteIds.removeAll(m_handledFailedSites);

        // Use this agreed new-fault set to make PPD decisions.
        // Since this agreement process should eventually be moved to
        // the fault distributor - this is written with some intentional
        // feature envy.

        PPDPolicyDecision makePPDPolicyDecisions =
            VoltDB.instance().getFaultDistributor().makePPDPolicyDecisions(newFailedSiteIds);

        if (makePPDPolicyDecisions == PPDPolicyDecision.NodeFailure) {
            handleSiteFaults(false,
                    newFailedSiteIds,
                    multiPartitionCommitPoint,
                    initiatorSafeInitPoint);
        }
        else if (makePPDPolicyDecisions == PPDPolicyDecision.PartitionDetection) {
            handleSiteFaults(true,
                    newFailedSiteIds,
                    multiPartitionCommitPoint,
                    initiatorSafeInitPoint);
        }

        m_handledFailedSites.addAll(failedSiteIds);
        for (NodeFailureFault fault : failures) {
            if (newFailedSiteIds.containsAll(m_context.siteTracker.getAllSitesForHost(fault.getHostId()))) {
                VoltDB.instance().getFaultDistributor().
                reportFaultHandled(m_faultHandler, fault);
            }
        }
    }

    /**
     * The list of failed sites we know about. Included with all failure messages
     * to identify what the information was used to generate commit points
     */
    private final HashSet<Integer> m_knownFailedSites = new HashSet<Integer>();

    /**
     * Failed sites for which agreement has been reached.
     */
    private final HashSet<Integer> m_handledFailedSites = new HashSet<Integer>();

    /**
     * Store values from older failed nodes. They are repeated with every failure message
     */
    private final HashMap<Integer, HashMap<Integer, Long>> m_newestSafeTransactionForInitiatorLedger =
        new HashMap<Integer, HashMap<Integer, Long>>();

    /**
     * Send one message to each surviving execution site providing this site's
     * multi-partition commit point and this site's safe txnid
     * (the receiver will filter the later for its
     * own partition). Do this once for each failed initiator that we know about.
     * Sends all data all the time to avoid a need for request/response.
     */
    private int discoverGlobalFaultData_send()
    {
        int expectedResponses = 0;
        int[] survivors = m_context.siteTracker.getUpExecutionSites();
        HashSet<Integer> survivorSet = new HashSet<Integer>();
        for (int survivor : survivors) {
            survivorSet.add(survivor);
        }
        m_recoveryLog.info("Sending fault data " + m_knownFailedSites.toString() + " to "
                + survivorSet.toString() + " survivors with lastKnownGloballyCommitedMultiPartTxnId "
                + lastKnownGloballyCommitedMultiPartTxnId);
        try {
            for (Integer site : m_knownFailedSites) {
                Integer hostId = m_context.siteTracker.getHostForSite(site);
                HashMap<Integer, Long> siteMap = m_newestSafeTransactionForInitiatorLedger.get(hostId);
                if (siteMap == null) {
                    siteMap = new HashMap<Integer, Long>();
                    m_newestSafeTransactionForInitiatorLedger.put(hostId, siteMap);
                }

                if (m_context.siteTracker.getSiteForId(site).getIsexec() == false) {
                    /*
                     * Check the queue for the data and get it from the ledger if necessary.\
                     * It might not even be in the ledger if the site has been failed
                     * since recovery of this node began.
                     */
                    Long txnId = m_transactionQueue.getNewestSafeTransactionForInitiator(site);
                    if (txnId == null) {
                        txnId = siteMap.get(site);
                        //assert(txnId != null);
                    } else {
                        siteMap.put(site, txnId);
                    }

                    FailureSiteUpdateMessage srcmsg =
                        new FailureSiteUpdateMessage(m_siteId,
                                                     m_knownFailedSites,
                                                     site,
                                                     txnId != null ? txnId : Long.MIN_VALUE,
                                                     //txnId,
                                                     lastKnownGloballyCommitedMultiPartTxnId);

                    m_mailbox.send(survivors, 0, srcmsg);
                    expectedResponses += (survivors.length);
                }
            }
        }
        catch (MessagingException e) {
            // TODO: unsure what to do with this. maybe it implies concurrent failure?
            e.printStackTrace();
            VoltDB.crashVoltDB();
        }
        m_recoveryLog.info("Sent fault data. Expecting " + expectedResponses + " responses.");
        return expectedResponses;
    }

    /**
     * Collect the failure site update messages from all sites This site sent
     * its own mailbox the above broadcast the maximum is local to this site.
     * This also ensures at least one response.
     *
     * Concurrent failures can be detected by additional reports from the FaultDistributor
     * or a mismatch in the set of failed hosts reported in a message from another site
     */
    private Long discoverGlobalFaultData_rcv(int expectedResponses, Map<Integer, Long> initiatorSafeInitPoint)
    {
        final int localPartitionId =
            m_context.siteTracker.getPartitionForSite(m_siteId);
        int responses = 0;
        int responsesFromSamePartition = 0;
        long commitPoint = Long.MIN_VALUE;
        java.util.ArrayList<FailureSiteUpdateMessage> messages = new java.util.ArrayList<FailureSiteUpdateMessage>();
        do {
            VoltMessage m = m_mailbox.recvBlocking(new Subject[] { Subject.FAILURE, Subject.FAILURE_SITE_UPDATE }, 5);
            //Invoke tick periodically to ensure that the last snapshot continues in the event that the failure
            //process does not complete
            if (m == null) {
                tick();
                continue;
            }
            FailureSiteUpdateMessage fm = null;

            if (m.getSubject() == Subject.FAILURE_SITE_UPDATE.getId()) {
                fm = (FailureSiteUpdateMessage)m;
                messages.add(fm);
            } else if (m.getSubject() == Subject.FAILURE.getId()) {
                /*
                 * If the fault distributor reports a new fault, assert that the fault currently
                 * being handled is included, redeliver the message to ourself and then abort so
                 * that the process can restart.
                 */
                HashSet<NodeFailureFault> faults = ((ExecutionSiteNodeFailureMessage)m).m_failedHosts;
                HashSet<Integer> newFailedSiteIds = new HashSet<Integer>();
                for (NodeFailureFault fault : faults) {
                    newFailedSiteIds.addAll(m_context.siteTracker.getAllSitesForHost(fault.getHostId()));
                }
                m_mailbox.deliverFront(m);
                m_recoveryLog.info("Detected a concurrent failure from FaultDistributor, new failed sites "
                        + newFailedSiteIds);
                return null;
            }

            /*
             * If the other surviving host saw a different set of failures
             */
            if (!m_knownFailedSites.equals(fm.m_failedSiteIds)) {
                if (!m_knownFailedSites.containsAll(fm.m_failedSiteIds)) {
                    /*
                     * In this case there is a new failed site we didn't know about. Time to
                     * start the process again from square 1 with knowledge of the new failed hosts
                     * First fail all the ones we didn't know about.
                     */
                    HashSet<Integer> difference = new HashSet<Integer>(fm.m_failedSiteIds);
                    difference.removeAll(m_knownFailedSites);
                    Set<Integer> differenceHosts = new HashSet<Integer>();
                    for (Integer siteId : difference) {
                        differenceHosts.add(m_context.siteTracker.getHostForSite(siteId));
                    }
                    for (Integer hostId : differenceHosts) {
                        String hostname = String.valueOf(hostId);
                        if (VoltDB.instance() != null) {
                            if (VoltDB.instance().getHostMessenger() != null) {
                                String hostnameTemp = VoltDB.instance().getHostMessenger().getHostnameForHostID(hostId);
                                if (hostnameTemp != null) hostname = hostnameTemp;
                            }
                        }
                        VoltDB.instance().getFaultDistributor().
                            reportFault(new NodeFailureFault(
                                    hostId,
                                    m_context.siteTracker.getNonExecSitesForHost(hostId),
                                    hostname));
                    }
                    m_recoveryLog.info("Detected a concurrent failure from " +
                            fm.m_sourceSiteId + " with new failed sites " + difference.toString());
                    m_mailbox.deliver(m);
                    /*
                     * Return null and skip handling the fault for now. Will try again
                     * later once the other failed hosts are detected and can be dealt with at once.
                     */
                    return null;
                } else {
                    /*
                     * In this instance they are not equal because the message is missing some
                     * failed sites. Drop the message. The sender will detect the fault and resend
                     * the message later with the correct information.
                     */
                    HashSet<Integer> difference = new HashSet<Integer>(m_knownFailedSites);
                    difference.removeAll(fm.m_failedSiteIds);
                    m_recoveryLog.info("Discarding failure message from " +
                            fm.m_sourceSiteId + " because it was missing failed sites " + difference.toString());
                    continue;
                }
            }

            ++responses;
            m_recoveryLog.info("Received failure message " + responses + " of " + expectedResponses
                    + " from " + fm.m_sourceSiteId + " for failed sites " + fm.m_failedSiteIds +
                    " with commit point " + fm.m_committedTxnId + " safe txn id " + fm.m_safeTxnId);
            commitPoint =
                Math.max(commitPoint, fm.m_committedTxnId);

            final int remotePartitionId =
                m_context.siteTracker.getPartitionForSite(fm.m_sourceSiteId);

            if (remotePartitionId == localPartitionId) {
                Integer initiatorId = fm.m_initiatorForSafeTxnId;
                if (!initiatorSafeInitPoint.containsKey(initiatorId)) {
                    initiatorSafeInitPoint.put(initiatorId, Long.MIN_VALUE);
                }
                initiatorSafeInitPoint.put(
                        initiatorId, Math.max(initiatorSafeInitPoint.get(initiatorId), fm.m_safeTxnId));
                responsesFromSamePartition++;
            }
        } while(responses < expectedResponses);

        assert(commitPoint != Long.MIN_VALUE);
        assert(!initiatorSafeInitPoint.containsValue(Long.MIN_VALUE));
        return commitPoint;
    }


    /**
     * Process a node failure detection.
     *
     * Different sites can process UpdateCatalog sysproc and handleNodeFault()
     * in different orders. UpdateCatalog changes MUST be commutative with
     * handleNodeFault.
     * @param partitionDetected
     *
     * @param siteIds Hashset<Integer> of host ids of failed nodes
     * @param globalCommitPoint the surviving cluster's greatest committed multi-partition transaction id
     * @param globalInitiationPoint the greatest transaction id acknowledged as globally
     * 2PC to any surviving cluster execution site by the failed initiator.
     *
     */
    void handleSiteFaults(boolean partitionDetected,
            HashSet<Integer> failedSites,
            long globalMultiPartCommitPoint,
            HashMap<Integer, Long> initiatorSafeInitiationPoint)
    {
        HashSet<Integer> failedHosts = new HashSet<Integer>();
        for (Integer siteId : failedSites) {
            failedHosts.add(m_context.siteTracker.getHostForSite(siteId));
        }

        StringBuilder sb = new StringBuilder();
        for (Integer hostId : failedHosts) {
            sb.append(hostId).append(' ');
        }
        if (m_txnlog.isTraceEnabled())
        {
            m_txnlog.trace("FUZZTEST handleNodeFault " + sb.toString() +
                    " with globalMultiPartCommitPoint " + globalMultiPartCommitPoint + " and safeInitiationPoints "
                    + initiatorSafeInitiationPoint);
        } else {
            m_recoveryLog.info("Handling node faults " + sb.toString() +
                    " with globalMultiPartCommitPoint " + globalMultiPartCommitPoint + " and safeInitiationPoints "
                    + initiatorSafeInitiationPoint);
        }
        lastKnownGloballyCommitedMultiPartTxnId = globalMultiPartCommitPoint;

        // If possibly partitioned, run through the safe initiated transaction and stall
        // The unsafe txns from each initiators will be dropped. after this partition detected branch
        if (partitionDetected) {
            Long globalInitiationPoint = Long.MIN_VALUE;
            for (Long initiationPoint : initiatorSafeInitiationPoint.values()) {
                globalInitiationPoint = Math.max( initiationPoint, globalInitiationPoint);
            }
            m_recoveryLog.info("Scheduling snapshot after txnId " + globalInitiationPoint +
                               " for cluster partition fault. Current commit point: " + this.lastCommittedTxnId);

            SnapshotSchedule schedule = m_context.cluster.getFaultsnapshots().get("CLUSTER_PARTITION");
            m_transactionQueue.makeRoadBlock(
                globalInitiationPoint,
                QueueState.BLOCKED_CLOSED,
                new ExecutionSiteLocalSnapshotMessage(globalInitiationPoint,
                                                      schedule.getPath(),
                                                      schedule.getPrefix(),
                                                      true));
        }


        // Fix safe transaction scoreboard in transaction queue
        for (Integer i : failedSites)
        {
            if (m_context.siteTracker.getSiteForId(i).getIsexec() == false) {
                m_transactionQueue.gotFaultForInitiator(i);
            }
        }

        /*
         * List of txns that were not initiated or rolled back.
         * This will be synchronously logged to the command log
         * so they can be skipped at replay time.
         */
        Set<Long> faultedTxns = new HashSet<Long>();

        //System.out.println("Site " + m_siteId + " dealing with faultable txns " + m_transactionsById.keySet());
        // Correct transaction state internals and commit
        // or remove affected transactions from RPQ and txnId hash.
        Iterator<Long> it = m_transactionsById.keySet().iterator();
        while (it.hasNext())
        {
            final long tid = it.next();
            TransactionState ts = m_transactionsById.get(tid);
            ts.handleSiteFaults(failedSites);

            // Fault a transaction that was not globally initiated by a failed initiator
            if (initiatorSafeInitiationPoint.containsKey(ts.initiatorSiteId) &&
                    ts.txnId > initiatorSafeInitiationPoint.get(ts.initiatorSiteId) &&
                failedSites.contains(ts.initiatorSiteId))
            {
                m_recoveryLog.info("Site " + m_siteId + " faulting non-globally initiated transaction " + ts.txnId);
                it.remove();
                if (!ts.isReadOnly()) {
                    faultedTxns.add(ts.txnId);
                }
                m_transactionQueue.faultTransaction(ts);
            }

            // Multipartition transaction without a surviving coordinator:
            // Commit a txn that is in progress and committed elsewhere.
            // (Must have lost the commit message during the failure.)
            // Otherwise, without a coordinator, the transaction can't
            // continue. Must rollback, if in progress, or fault it
            // from the queues if not yet started.
            else if (ts instanceof MultiPartitionParticipantTxnState &&
                     failedSites.contains(ts.coordinatorSiteId))
            {
                MultiPartitionParticipantTxnState mpts = (MultiPartitionParticipantTxnState) ts;
                if (ts.isInProgress() && ts.txnId <= globalMultiPartCommitPoint)
                {
                    m_recoveryLog.info("Committing in progress multi-partition txn " + ts.txnId +
                            " even though coordinator was on a failed host because the txnId <= " +
                            "the global multi-part commit point");
                    CompleteTransactionMessage ft =
                        mpts.createCompleteTransactionMessage(false, false);
                    m_mailbox.deliverFront(ft);
                }
                else if (ts.isInProgress() && ts.txnId > globalMultiPartCommitPoint) {
                    m_recoveryLog.info("Rolling back in progress multi-partition txn " + ts.txnId +
                            " because the coordinator was on a failed host and the txnId > " +
                            "the global multi-part commit point");
                    CompleteTransactionMessage ft =
                        mpts.createCompleteTransactionMessage(true, false);
                    if (!ts.isReadOnly()) {
                        faultedTxns.add(ts.txnId);
                    }
                    m_mailbox.deliverFront(ft);
                }
                else
                {
                    m_recoveryLog.info("Faulting multi-part transaction " + ts.txnId +
                            " because the coordinator was on a failed node");
                    it.remove();
                    if (!ts.isReadOnly()) {
                        faultedTxns.add(ts.txnId);
                    }
                    m_transactionQueue.faultTransaction(ts);
                }
            }
            // If we're the coordinator, then after we clean up our internal
            // state due to a failed node, we need to poke ourselves to check
            // to see if all the remaining dependencies are satisfied.  Do this
            // with a message to our mailbox so that happens in the
            // execution site thread
            else if (ts instanceof MultiPartitionParticipantTxnState &&
                     ts.coordinatorSiteId == m_siteId)
            {
                if (ts.isInProgress())
                {
                    m_mailbox.deliverFront(new CheckTxnStateCompletionMessage(ts.txnId));
                }
            }
        }
        if (m_recoveryProcessor != null) {
            m_recoveryProcessor.handleSiteFaults( failedSites, m_context.siteTracker);
        }
        try {
            //Log it and acquire the completion permit from the semaphore
            VoltDB.instance().getCommandLog().logFault( failedSites, faultedTxns).acquire();
        } catch (InterruptedException e) {
            hostLog.fatal("Interrupted while attempting to log a fault", e);
            VoltDB.crashVoltDB();
        }
    }


    private FragmentResponseMessage processSysprocFragmentTask(
            final TransactionState txnState,
            final HashMap<Integer,List<VoltTable>> dependencies,
            final long fragmentId, final FragmentResponseMessage currentFragResponse,
            final ParameterSet params)
    {
        // assume success. errors correct this assumption as they occur
        currentFragResponse.setStatus(FragmentResponseMessage.SUCCESS, null);

        VoltSystemProcedure proc = null;
        synchronized (m_registeredSysProcPlanFragments) {
            proc = m_registeredSysProcPlanFragments.get(fragmentId);
        }

        try {
            // set transaction state for non-coordinator snapshot restore sites
            proc.setTransactionState(txnState);
            final DependencyPair dep
                = proc.executePlanFragment(dependencies,
                                           fragmentId,
                                           params,
                                           m_systemProcedureContext);

            sendDependency(currentFragResponse, dep.depId, dep.dependency);
        }
        catch (final EEException e)
        {
            hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { fragmentId }, e);
            currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
        }
        catch (final SQLException e)
        {
            hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { fragmentId }, e);
            currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
        }
        catch (final Exception e)
        {
            // Just indicate that we failed completely
            currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, new SerializableException(e));
        }

        return currentFragResponse;
    }


    private void sendDependency(
            final FragmentResponseMessage currentFragResponse,
            final int dependencyId,
            final VoltTable dependency)
    {
        if (log.isTraceEnabled()) {
            log.l7dlog(Level.TRACE,
                       LogKeys.org_voltdb_ExecutionSite_SendingDependency.name(),
                       new Object[] { dependencyId }, null);
        }
        currentFragResponse.addDependency(dependencyId, dependency);
    }


    /*
     * Do snapshot work exclusively until there is no more. Also blocks
     * until the syncing and closing of snapshot data targets has completed.
     */
    public void initiateSnapshots(Deque<SnapshotTableTask> tasks, long txnId, int numLiveHosts) {
        m_snapshotter.initiateSnapshots(ee, tasks, txnId, numLiveHosts);
    }

    public HashSet<Exception> completeSnapshotWork() throws InterruptedException {
        return m_snapshotter.completeSnapshotWork(ee);
    }


    /*
     *  SiteConnection Interface (VoltProcedure -> ExecutionSite)
     */

    @Override
    public void registerPlanFragment(final long pfId, final VoltSystemProcedure proc) {
        synchronized (m_registeredSysProcPlanFragments) {
            assert(m_registeredSysProcPlanFragments.containsKey(pfId) == false);
            m_registeredSysProcPlanFragments.put(pfId, proc);
        }
    }

    @Override
    public Site getCorrespondingCatalogSite() {
        return getCatalogSite();
    }

    @Override
    public int getCorrespondingSiteId() {
        return m_siteId;
    }

    @Override
    public int getCorrespondingPartitionId() {
        return Integer.valueOf(getCatalogSite().getPartition().getTypeName());
    }

    @Override
    public int getCorrespondingHostId() {
        return Integer.valueOf(getCatalogSite().getHost().getTypeName());
    }

    @Override
    public void loadTable(
            long txnId,
            String clusterName,
            String databaseName,
            String tableName,
            VoltTable data)
    throws VoltAbortException
    {
        Cluster cluster = m_context.cluster;
        if (cluster == null) {
            throw new VoltAbortException("cluster '" + clusterName + "' does not exist");
        }
        Database db = cluster.getDatabases().get(databaseName);
        if (db == null) {
            throw new VoltAbortException("database '" + databaseName + "' does not exist in cluster " + clusterName);
        }
        Table table = db.getTables().getIgnoreCase(tableName);
        if (table == null) {
            throw new VoltAbortException("table '" + tableName + "' does not exist in database " + clusterName + "." + databaseName);
        }

        long undo_token = getNextUndoToken();
        
        // GWW: can not run in CVoltDB mood
        assert(!m_cVoltDBMode);
        
        ee.loadTable(table.getRelativeIndex(), data,
                     txnId,
                     lastCommittedTxnId,
                     undo_token);
        ee.releaseUndoToken(undo_token);
        getNextUndoToken();
    }

    @Override
    public VoltTable[] executeQueryPlanFragmentsAndGetResults(
            long[] planFragmentIds,
            int numFragmentIds,
            ParameterSet[] parameterSets,
            int numParameterSets,
            long txnId,
            boolean readOnly) throws EEException
    { 	
    	// GWW: handle DataConflictException for single-site execution here
    	DataConflictException ex = null;
    	
    	long undoToken = -1;
    	
    	if(!readOnly)
    		undoToken = getNextUndoTokenForTxn();
    	
    	if(debugFlag)
    		cVoltTrace("entering executeQueryPlanFragmentAndGetResults - txnid: " + txnId
    				+ ", numFrag: " + numFragmentIds);
    	
    	while(true) {  		
    		long startBatch = 0;
    		long startPlanNode = 0;
    		
	       	if(ex != null) {
	        	startBatch = ((DataConflictException)ex).getExecutedBatch();
	        	startPlanNode = ((DataConflictException)ex).getExecutedPlanNodes();
	        	if(debugFlag)
	        		cVoltTrace("continue executeQueryPlanFragmentsAndGetResults: " + startBatch + ", " + startPlanNode);
	        }
        	        	
        	try {
        		VoltTable[] ret = ee.executeQueryPlanFragmentsAndGetResults(
        	            planFragmentIds,
        	            numFragmentIds,
        	            parameterSets,
        	            numParameterSets,
        	            txnId,
        	            lastCommittedTxnId,
        	            readOnly ? Long.MAX_VALUE : undoToken,
        	            0, 0, m_cVoltDBMode);
        		
            	if(debugFlag)
            		cVoltTrace("end executeQueryPlanFragmentAndGetResults - txnid: " + txnId
            				+ ", numFrag: " + numFragmentIds);
            	
            	return ret;
        	} catch (final SQLException e) {
        		if(debugFlag)
        			cVoltTrace("excutequeryplanfragmentsandgetresults - sqlexception " + e);
        		
        		// GWW: add blocker txn if this is a data conflict exception
            	if(e.isDataConflictException()){
    	        	ex = (DataConflictException)e;
    	        	
    	        	m_currentPreparingTransactionState.blockForDataConflict();
    	       		newBlockerTxn(e.getBlockerTxnId(), e.getWaiterTxnId());

    	       		// undo it if not readOnly
    	       		if(!readOnly) {
	    	       		if(debugFlag)
	    	       			cVoltTrace("DCE, undo current undo token " + undoToken + "for txn " + txnId + "\n");
	    	       		ee.undo2UndoToken(txnId, undoToken, undoToken);
    	       		}
    	       		while(m_currentPreparingTransactionState.isDataConflict())
    	      				background(false);
            	} else {
            		throw e;
            	}
        	}
    	}
	}

    @Override
    public void simulateExecutePlanFragments(long txnId, boolean readOnly) {
        if (!readOnly) {
            // pretend real work was done
            getNextUndoToken();
        }
    }
    
	private void checkTxnOnFinishingQueue() {
    	 if(debugFlag) {
    		 for(TransactionState ts : m_finishingTxnQueue) {
				if(!m_transactionsById.containsKey(ts.txnId)) {
					if(debugFlag)
						cVoltTrace("checkTxnOnFinishingQueue - txnid: " + ts.txnId + "; neesRollback: " + ts.needsRollback()
							+ "; done: " + ts.done() + "; isCoordinator: " + ts.isCoordinator() + "; beginUndoToken: " 
							+ ((ts.getBeginUndoToken() == kInvalidUndoToken) ? "invalid" : ts.getBeginUndoToken()));
				}
				assert(m_transactionsById.containsKey(ts.txnId));
			}
    	 }
	}
    
    // GWW: check finishingQ, if the oldest txn is done,it and 
    // possibly others can be completed (actually committed or 
    // aborted); stop when seeing first undone txn to ensure 
    // commit/abort in txnId order.
    private void completeFinishedTxns() {
    	if(debugFlag)
    		cVoltTrace("starting completeFinishedTxns, # on q: " + m_finishingTxnQueue.size());
    	
    	TransactionState ts = null;
    	
		// continue completing all txn until finishingQ is empty or next one is not done
		while(!m_finishingTxnQueue.isEmpty() && (ts = m_finishingTxnQueue.peek()).done()) {
			assert(ts.isPrepared());
			if(debugFlag)
				cVoltTrace("completeFinishedTxns: found done txn " + ts.txnId);
			m_finishingTxnQueue.poll();
			assert(ts.done());
						
			if(ts.needsRollback())
				rollbackTransaction(ts);
			completeTransaction(ts);
			
			// if this txn is the blocker and current site is coordinator
			if(ts.txnId == m_blockerTxn){
				m_currentPreparingTransactionState.unblockForDataConflict();
			}
			
			if(ts.isCoordinator())
				ts.sendResponse();
			
			ts = m_transactionsById.remove(ts.txnId);
			assert(ts != null);

			if (stats) {
	  			ts.completedT = System.nanoTime();
	  			printTxnStateInfo(ts);
			}
		}
    }
    
    private void printTxnStateInfo(TransactionState txnState) {
		// GWW: for stats
		if (stats) {
			txnState.completedT = System.nanoTime();
			try {
				int isUnblocker = (txnState.txnId == m_blockerTxn) ? 1 : 0;
				
				out.write(txnState.txnId + ", "
						+ txnState.receivedT + ", "
						+ txnState.startedT + ", "
						+ txnState.executedT + ", "
						+ txnState.fragsStartT + ", "
						+ txnState.fragsDoneT + ", " 
						+ txnState.resumeT + ", " 
						+ txnState.commitDecisionT + ", "
						+ txnState.preparedT + ", "
						+ txnState.receivedCompleteTxnMsgT + ", "
						+ txnState.completedT + ", "
						+ m_transactionQueue.size() + ", "
						+ m_startedTxnQueue.size() + ", "
						+ m_finishingTxnQueue.size() + ", "
						+ isUnblocker + ", " 
						+ (txnState.completedT - txnState.startedT) / 1000);
				out.newLine();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out
						.println("GWW - write to txnTime file error");
				e.printStackTrace();
			}
		}
    }
    /**
     * Continue doing runnable work for the current transaction.
     * If doWork() returns true, the transaction is over.
     * Otherwise, the procedure may have more java to run
     * or a dependency or fragment to collect from the network.
     *
     * doWork() can sneak in a new SP transaction. Maybe it would
     * be better if transactions didn't trigger other transactions
     * and those optimization decisions where made somewhere closer
     * to this code?
     */
    @Override
    public Map<Integer, List<VoltTable>>
    recursableRun(TransactionState currentTxnState)
    {
    	if(debugFlag)
    		cVoltTrace("txn " + currentTxnState.txnId +" is now executing (" + 
    				m_finishingTxnQueue.size() + " txn in finishingQ)");
    	// GWW: for stats
    	if(stats)
    		currentTxnState.executedT = System.nanoTime();
    	
    	currentTxnState.setBeginUndoToken(latestUndoToken);
    	
        do
        {
            if (currentTxnState.doWork(m_recovering)) {
            	// GWW: entering here means txn has prepared
            	// for single-site txn, no need to wait response, so the txn is ready to
            	// commit/abort; for multi-site txn,  if the txn decides to abort, m_done
            	// is set to be true already, so it's ready to complete too; if it decides
            	// to commit, we must wait until the last complete response processed.
            	if(debugFlag)
            		cVoltTrace("Txn " + currentTxnState.txnId + " has prepared(#finishingQ:" + m_finishingTxnQueue.size() + ")");
            	
            	if(stats)
            		currentTxnState.preparedT = System.nanoTime();
            	
            	// GWW: back to finishingQ
            	m_finishingTxnQueue.add(currentTxnState);
            	
           		completeFinishedTxns();

            	m_currentPreparingTransactionState = null;

            	return null;
            } 
            else if (currentTxnState.shouldResumeProcedure()){    	
                // GWW: when get to this code, the txn must be a multi-site txn.
            	// it means results for current shot are ready to be picked up
            	// so ES releases the sem the procedure thread waiting on, and
            	// and wait the procedure thread to finish its work
            	// if the txn is a single-site txn, the response
            	assert(currentTxnState instanceof MultiPartitionParticipantTxnState);
            	
            	if(stats)
            		currentTxnState.resumeT = System.nanoTime();
            	
        		((MultiPartitionParticipantTxnState)currentTxnState).resumeProcedure();
        		waitOnProcedure();
            }
            // This is a bit ugly; more or less a straight-forward
            // extraction of the logic that used to be in
            // MultiPartitionParticipantTxnState.doWork()
//            else if (currentTxnState.isBlocked() &&
//                     !currentTxnState.isDone() &&
//                     currentTxnState.isCoordinator() &&
//                     currentTxnState.isReadOnly() &&
//                     !currentTxnState.hasTransactionalWork())
//            {
//                assert(!currentTxnState.isSinglePartition());
//                tryToSneakInASinglePartitionProcedure();
//            } 
            else {
            	background(false);

                /**
                 * If this site is the source for a recovering partition the recovering
                 * partition might be blocked waiting for the txn to sync at from here.
                 * Since this site is blocked on the multi-part waiting for the destination to respond
                 * to a plan fragment it is a deadlock.
                 * Poke the destination so that it will execute past the current
                 * multi-part txn.
                 */
                if (m_recoveryProcessor != null) {
                    m_recoveryProcessor.notifyBlockedOnMultiPartTxn( currentTxnState.txnId );
                }
            }
        } while (true);
    }

    /*
     *
     *  SiteTransactionConnection Interface (TransactionState -> ExecutionSite)
     *
     */

    @Override
    public SiteTracker getSiteTracker() {
        return m_context.siteTracker;
    }

    /**
     * Set the txn id from the WorkUnit and set/release undo tokens as
     * necessary. The DTXN currently has no notion of maintaining undo
     * tokens beyond the life of a transaction so it is up to the execution
     * site to release the undo data in the EE up until the current point
     * when the transaction ID changes.
     */
    @Override
    public final void beginNewTxn(TransactionState txnState)
    {
    	if(debugFlag)
    		cVoltTrace("FUZZTEST beginNewTxn " + txnState.txnId + " " +
    				(txnState.isSinglePartition() ? "single" : "multi") + " " +
    				(txnState.isReadOnly() ? "readonly" : "readwrite") + " " +
    				(txnState.isCoordinator() ? "coord" : "part"));
    	
        if (m_txnlog.isTraceEnabled())
        {
            m_txnlog.trace("FUZZTEST beginNewTxn " + txnState.txnId + " " +
                           (txnState.isSinglePartition() ? "single" : "multi") + " " +
                           (txnState.isReadOnly() ? "readonly" : "readwrite") + " " +
                           (txnState.isCoordinator() ? "coord" : "part"));
        }
        if (!txnState.isReadOnly()) {
        	if(txnState.isSinglePartition() || !txnState.isCoordinator()){
        		assert(txnState.getBeginUndoToken() != kInvalidUndoToken);
        	}
        }
    }

    public final void rollbackTransaction(TransactionState txnState)
    {
        if (m_txnlog.isTraceEnabled())
        {
            m_txnlog.trace("FUZZTEST rollbackTransaction " + txnState.txnId);
        }
        if (!txnState.isReadOnly()) {
            assert(latestUndoToken != kInvalidUndoToken);
            assert(latestUndoToken >= txnState.getBeginUndoToken());
            
            // when txnState has early failure
            if(txnState.getBeginUndoToken() == kInvalidUndoToken)
            	return;

            // don't go to the EE if no work was done            
            if(txnState.getEndUndoToken() > txnState.getBeginUndoToken()) {
            	ee.undo2UndoToken(txnState.txnId, txnState.getBeginUndoToken(), txnState.getEndUndoToken());
            }
        }
    }

    @Override
    public FragmentResponseMessage processFragmentTask(
            TransactionState txnState,
            final HashMap<Integer,List<VoltTable>> dependencies,
            final VoltMessage task)
    {
        final FragmentTaskMessage ftask = (FragmentTaskMessage) task;
        final FragmentResponseMessage currentFragResponse = new FragmentResponseMessage(ftask, getSiteId());
        currentFragResponse.setStatus(FragmentResponseMessage.SUCCESS, null);
        
        if(stats)
        	txnState.fragsStartT = System.nanoTime();
        
        if (!ftask.isSysProcTask())
        {
            if (dependencies != null)
            {
                ee.stashWorkUnitDependencies(dependencies);
            }
        }

        for (int frag = 0; frag < ftask.getFragmentCount(); frag++)
        {
            final long fragmentId = ftask.getFragmentId(frag);
            final int outputDepId = ftask.getOutputDepId(frag);

            // this is a horrible performance hack, and can be removed with small changes
            // to the ee interface layer.. (rtb: not sure what 'this' encompasses...)
            ParameterSet params = null;
            final ByteBuffer paramData = ftask.getParameterDataForFragment(frag);
            if (paramData != null) {
                final FastDeserializer fds = new FastDeserializer(paramData);
                try {
                    params = fds.readObject(ParameterSet.class);
                }
                catch (final IOException e) {
                	if(debugFlag)
                		cVoltTrace("processfragmenttask - ioException " + e);
                	
                    hostLog.l7dlog( Level.FATAL,
                                    LogKeys.host_ExecutionSite_FailedDeserializingParamsForFragmentTask.name(), e);
                    VoltDB.crashVoltDB();
                }
            }
            else {
                params = new ParameterSet();
            }

            if (ftask.isSysProcTask()) {
                return processSysprocFragmentTask(txnState, dependencies, fragmentId,
                                                  currentFragResponse, params);
            }
            else {
                final int inputDepId = ftask.getOnlyInputDepId(frag);

                DataConflictException ex = null;
                
                long nextUndoToken = -1L;
                if(!txnState.isReadOnly())
                	nextUndoToken = getNextUndoTokenForTxn();
                
                while(true) {
                	long startBatch = 0;
                	long startPlanNode = 0;
                	
                	if(ex != null) {
                		startBatch = ((DataConflictException)ex).getExecutedBatch();
                		startPlanNode = ((DataConflictException)ex).getExecutedPlanNodes();
                		if(debugFlag)
                			cVoltTrace("continue processFragmentTask: " + startBatch + ", " + startPlanNode);
                	}
                
	                /*
	                 * Currently the error path when executing plan fragments
	                 * does not adequately distinguish between fatal errors and
	                 * abort type errors that should result in a roll back.
	                 * Assume that it is ninja: succeeds or doesn't return.
	                 * No roll back support.
	                 */
	                try {
	                	final DependencyPair dep;
	                	
	                	dep = ee.executePlanFragment(fragmentId,
	                            outputDepId,
	                            inputDepId,
	                            params,
	                            txnState.txnId,
	                            lastCommittedTxnId,
	                            startPlanNode, m_cVoltDBMode,
	                            txnState.isReadOnly() ? Long.MAX_VALUE : nextUndoToken);
	
	                    sendDependency(currentFragResponse, dep.depId, dep.dependency);
	                    
	                    break;
//	                    return currentFragResponse;
	
	                } catch (final SQLException e) {
	                	if(debugFlag)
	                		cVoltTrace("processfragmenttask - SQLException " + e);
	                	
	                	// GWW: add blocker txn if this is a data conflict exception
	                	if(e.isDataConflictException()){
	                		ex = (DataConflictException)e;
	                		m_currentPreparingTransactionState.blockForDataConflict();
	                		newBlockerTxn(e.getBlockerTxnId(), e.getWaiterTxnId());

		                	while(m_currentPreparingTransactionState.isDataConflict())
		                		background(false);
	                	} else {	
		                    hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { fragmentId }, e);
		                    currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
//		                    break;
		                    return currentFragResponse;
	                	}
	                }
                }
            }
        }
        
        if (stats && txnState instanceof MultiPartitionParticipantTxnState) {
        		txnState.fragsDoneT = System.nanoTime();
        }
        
        return currentFragResponse;
    }

    @Override
    public InitiateResponseMessage processInitiateTask(
            TransactionState txnState,
            final VoltMessage task)
    {
        final InitiateTaskMessage itask = (InitiateTaskMessage)task;
        final VoltProcedure wrapper = procs.get(itask.getStoredProcedureName());

        final InitiateResponseMessage response = new InitiateResponseMessage(itask);

        // feasible to receive a transaction initiated with an earlier catalog.
        if (wrapper == null) {
            response.setResults(
                new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE,
                                       new VoltTable[] {},
                                       "Procedure does not exist: " + itask.getStoredProcedureName()));
        }
        else {
            try {
                Object[] callerParams = null;
                /*
                 * Parameters are lazily deserialized. We may not find out until now
                 * that the parameter set is corrupt
                 */
                try {
                    callerParams = itask.getParameters();
                } catch (RuntimeException e) {
                    Writer result = new StringWriter();
                    PrintWriter pw = new PrintWriter(result);
                    e.printStackTrace(pw);
                    response.setResults(
                            new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE,
                                                   new VoltTable[] {},
                                                   "Exception while deserializing procedure params\n" +
                                                   result.toString()));
                }
                if (callerParams != null) {
                    if (wrapper instanceof VoltSystemProcedure) {
                    	if(debugFlag)
                    		cVoltTrace("start processInitiateTask - " + txnState.txnId 
                    			+ " (site" + this.m_siteId + ")"
                    			+ " for procedure " + wrapper.getClass()
                    			+ " proc's CVoltDBMode " + wrapper.getCVoltDBMode()
                    			+ " txn's CVoltDB " + txnState.isCVoltDBExecution()
                    			+ " site CVoltDBMode " + m_cVoltDBMode);
                    	
                        final Object[] combinedParams = new Object[callerParams.length + 1];
                        combinedParams[0] = m_systemProcedureContext;
                        for (int i=0; i < callerParams.length; ++i) combinedParams[i+1] = callerParams[i];
                        final ClientResponseImpl cr = wrapper.call(txnState, combinedParams);
                        response.setResults(cr, itask);
                    }
                    else {
                    	// TODO: give priority to cVoltTrace, print different level of tracing info
                    	if(debugFlag)
                    		cVoltTrace("start processInitiateTask - " + txnState.txnId 
                    			+ " (site" + this.m_siteId + ")"
                    			+ " for procedure " + wrapper.getClass()
                    			+ " proc's CVoltDBMode " + wrapper.getCVoltDBMode()
                    			+ " txn's CVoltDB " + txnState.isCVoltDBExecution()
                    			+ " site CVoltDBMode " + m_cVoltDBMode);
           
                        final ClientResponseImpl cr = wrapper.call(txnState, itask.getParameters());
                        if(debugFlag)
                        	cVoltTrace("processInitiateTask, after call - " + txnState.txnId 
                        		+ " (site" + this.m_siteId + ")"
                        		+ " for procedure " + wrapper.getClass()
                        		+ "status " + cr.getStatus() 
                        		+ "statusString " + cr.getStatusString() 
                        		+ "appstatus " + cr.getAppStatusString());
                        
                        response.setResults(cr, itask);
                        if (stats) {
                  			txnState.commitDecisionT = System.nanoTime();
                 		}
                    }
                }
            }
            catch (final ExpectedProcedureException e) {
                log.l7dlog( Level.TRACE, LogKeys.org_voltdb_ExecutionSite_ExpectedProcedureException.name(), e);
                response.setResults(
                                    new ClientResponseImpl(
                                                           ClientResponse.GRACEFUL_FAILURE,
                                                           new VoltTable[]{},
                                                           e.toString()));
            } catch (DataConflictException e) {
            	if(debugFlag)
            		cVoltTrace("processinitialtask - DataConflictException " + e);
            	
            	System.err.println(e);
            }
            catch (final Exception e) {
                // Should not be able to reach here. VoltProcedure.call caught all invocation target exceptions
                // and converted them to error responses. Java errors are re-thrown, and not caught by this
                // exception clause. A truly unexpected exception reached this point. Crash. It's a defect.
                hostLog.l7dlog( Level.ERROR, LogKeys.host_ExecutionSite_UnexpectedProcedureException.name(), e);
                VoltDB.crashVoltDB();
            }
        }
        log.l7dlog( Level.TRACE, LogKeys.org_voltdb_ExecutionSite_SendingCompletedWUToDtxn.name(), null);
        return response;
    }

    /**
     * Try to execute a single partition procedure if one is available in the
     * priority queue.
     *
     * @return false if there is no possibility for speculative work.
     */
    public boolean tryToSneakInASinglePartitionProcedure() {
        // poll for an available message. don't block
        VoltMessage message = m_mailbox.recv();
        tick(); // unclear if this necessary (rtb)
        if (message != null) {
            handleMailboxMessage(message);
            return true;
        }
        else {
            TransactionState nextTxn = (TransactionState)m_transactionQueue.peek();

            // only sneak in single partition work
            if (nextTxn instanceof SinglePartitionTxnState)
            {
                boolean success = nextTxn.doWork(m_recovering);
                assert(success);
                return true;
            }
            else {
                // multipartition is next or no work
                return false;
            }
        }
    }
}
