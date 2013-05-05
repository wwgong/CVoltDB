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

package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.StoredProcedureInvocation;
import org.voltdb.utils.DBBPool;


/**
 * Message from an initiator to an execution site, instructing the
 * site to begin executing a stored procedure, coordinating other
 * execution sites if needed.
 *
 */
public class InitiateTaskMessage extends TransactionInfoBaseMessage {

    boolean m_isSinglePartition;
    StoredProcedureInvocation m_invocation;
    long m_lastSafeTxnID; // this is the largest txn acked by all partitions running the java for it
    AtomicBoolean m_isDurable;
    // GWW: 
    int m_participantSiteIds[] = null;
    boolean m_isCVoltDBExecution;

	/** Empty constructor for de-serialization */
    InitiateTaskMessage() {
        super();
    }
    
    public InitiateTaskMessage(int initiatorSiteId,
            int coordinatorSiteId,
            long txnId,
            boolean isReadOnly,
            boolean isSinglePartition,
            StoredProcedureInvocation invocation,
            long lastSafeTxnID) {
		super(initiatorSiteId, coordinatorSiteId, txnId, isReadOnly);
		m_isSinglePartition = isSinglePartition;
		m_invocation = invocation;
		m_lastSafeTxnID = lastSafeTxnID;
	}

    public InitiateTaskMessage(int initiatorSiteId,
                        int coordinatorSiteId,
                        long txnId,
                        boolean isReadOnly,
                        boolean isSinglePartition,
                        StoredProcedureInvocation invocation,
                        long lastSafeTxnID,
                        boolean isCVoltDB) {
        super(initiatorSiteId, coordinatorSiteId, txnId, isReadOnly);
        m_isSinglePartition = isSinglePartition;
        m_invocation = invocation;
        m_lastSafeTxnID = lastSafeTxnID;
        m_isCVoltDBExecution = isCVoltDB;
    }
    
    public InitiateTaskMessage(int initiatorSiteId,
            int coordinatorSiteId,
            int participantSiteIds[],
            long txnId,
            boolean isReadOnly,
            boolean isSinglePartition,
            StoredProcedureInvocation invocation,
            long lastSafeTxnID,
            boolean isCVoltDB) {
		super(initiatorSiteId, coordinatorSiteId, txnId, isReadOnly);
		m_isSinglePartition = isSinglePartition;
		m_invocation = invocation;
		m_lastSafeTxnID = lastSafeTxnID;
		m_participantSiteIds = participantSiteIds;
		m_isCVoltDBExecution = isCVoltDB;
}
    

    public boolean isCVoltDBExecution() {
		return m_isCVoltDBExecution;
	}

    @Override
    public boolean isReadOnly() {
        return m_isReadOnly;
    }

    @Override
    public boolean isSinglePartition() {
        return m_isSinglePartition;
    }

    public StoredProcedureInvocation getStoredProcedureInvocation() {
        return m_invocation;
    }

    public String getStoredProcedureName() {
        assert(m_invocation != null);
        return m_invocation.getProcName();
    }

    public int getParameterCount() {
        assert(m_invocation != null);
        if (m_invocation.getParams() == null)
            return 0;
        return m_invocation.getParams().toArray().length;
    }

    public Object[] getParameters() {
        return m_invocation.getParams().toArray();
    }

    public long getLastSafeTxnId() {
        return m_lastSafeTxnID;
    }

    public AtomicBoolean getDurabilityFlag() {
        assert(!m_isReadOnly);
        if (m_isDurable == null) {
            m_isDurable = new AtomicBoolean();
        }
        return m_isDurable;
    }

    public AtomicBoolean getDurabilityFlagIfItExists() {
        return m_isDurable;
    }

    @Override
    protected void flattenToBuffer(final DBBPool pool) {
        // stupid lame flattening of the proc invocation
        FastSerializer fs = new FastSerializer();
        try {
            fs.writeObject(m_invocation);
        } catch (IOException e) {
            e.printStackTrace();
            assert(false);
        }
        ByteBuffer invocationBytes = fs.getBuffer();

        // size of MembershipNotice
        int msgsize = super.getMessageByteCount();
        msgsize += 1 + 8 + invocationBytes.remaining();
        
        // added for CVoltDB: size of array, even if null, size of boolean
        msgsize += 2 + 1;
        
        // serialized array size
        if (m_participantSiteIds != null) {
            msgsize += 4 * m_participantSiteIds.length;
        }

        if (m_buffer == null) {
            m_container = pool.acquire(msgsize + 1 + HEADER_SIZE);
            m_buffer = m_container.b;
        }
        setBufferSize(msgsize + 1, pool);

        m_buffer.position(HEADER_SIZE);
        m_buffer.put(INITIATE_TASK_ID);

        super.writeToBuffer();

        m_buffer.putLong(m_lastSafeTxnID);

        m_buffer.put(m_isSinglePartition ? (byte) 1 : (byte) 0);
        
        // added for CVoltDB--before invocationBytes
        if (m_participantSiteIds == null) {
        	m_buffer.putShort((short) 0);
        } else {
        	m_buffer.putShort((short) m_participantSiteIds.length);
        	for (int i = 0; i < m_participantSiteIds.length; i++) {
        		m_buffer.putInt(m_participantSiteIds[i]);
        	}
        }
        m_buffer.put(m_isCVoltDBExecution ? (byte) 1 : (byte) 0);
        m_buffer.put(invocationBytes);
        m_buffer.limit(m_buffer.position());
    }

    @Override
    protected void initFromBuffer() {
        m_buffer.position(HEADER_SIZE + 1); // skip the msg id
        super.readFromBuffer();

        m_lastSafeTxnID = m_buffer.getLong();

        m_isSinglePartition = m_buffer.get() == 1;
                
        // added for CVoltDB--
        short participantCount = m_buffer.getShort();
        if (participantCount > 0) {
        	m_participantSiteIds = new int[participantCount];
        	for (int i = 0; i < participantCount; i++)
        		m_participantSiteIds[i] = m_buffer.getInt();
        }
        m_isCVoltDBExecution = m_buffer.get() == 1;
        
        FastDeserializer fds = new FastDeserializer(m_buffer);
        try {
            m_invocation = fds.readObject(StoredProcedureInvocation.class);
        } catch (IOException e) {
            e.printStackTrace();
            assert(false);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("INITITATE_TASK (FROM ");
        sb.append(m_initiatorSiteId);
        sb.append(" TO ");
        sb.append(m_coordinatorSiteId);
        sb.append(") FOR TXN ");
        sb.append(m_txnId);

        sb.append("\n");
        if (m_isReadOnly)
            sb.append("  READ, ");
        else
            sb.append("  WRITE, ");
        if (m_isSinglePartition)
            sb.append("SINGLE PARTITION, ");
        else
            sb.append("MULTI PARTITION, ");
        sb.append("COORD ");
        sb.append(m_coordinatorSiteId);

        sb.append("\n  PROCEDURE: ");
        sb.append(m_invocation.getProcName());
        sb.append("\n  PARAMS: ");
        sb.append(m_invocation.getParams().toString());
        
        if (m_participantSiteIds != null) {
        	sb.append("\n PARTICIPANT IDS: ");
        	for (int i = 0; i < m_participantSiteIds.length; i++)
        		sb.append(" " + m_participantSiteIds[i]);
        }
        if (m_isCVoltDBExecution)
        	sb.append("\n EXECUTION MODE: CVoltDB transaction");
        else
        	sb.append("\n EXECUTION MODE: Traditional VoltDB transaction");

        return sb.toString();
    }

    public ByteBuffer getSerializedParams() {
        return m_invocation.getSerializedParams();
    }
    
    // GWW
    public int[] getParticipantSiteIds() {
    	return m_participantSiteIds;
    }
}
