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
#include "storage/DataConflictException.h"
#include "storage/persistenttable.h"
#include "storage/table.h"
#include "common/debuglog.h"
#include <cassert>

namespace voltdb {
/*
 * @param table Table that the operation was performed on
 * @param tableId CatalogId of the table that the operation failed on
 * @param beforeTuple before tuple values or null if before operation is Insert
 * @param afterTuple after tuple values or null if after operation is Delete.
 * @param blockerTxnId txn that modified the tuple
 * @param waitingTxnId txn wanted to perform the operation
 */
DataConflictException::DataConflictException(
        PersistentTable *table,
        TableTuple beforeTuple,
        TableTuple afterTuple,
        int64_t blockerTxnId,
        int64_t waitingTxnId) :
            SQLException(
                    SQLException::integrity_data_conflict,
                    "Attempted modification on Escrow-data",
                    voltdb::VOLT_EE_EXCEPTION_TYPE_DATA_CONFLICT),
            m_table(table), m_beforeTuple(beforeTuple), m_afterTuple(afterTuple),
            m_blockerTxnId(blockerTxnId), m_waitingTxnId(waitingTxnId) {
        assert(table);
        assert(!beforeTuple.isNullTuple() || !afterTuple.isNullTuple());
	m_executedPlanNodes = 0;
	m_executedBatch = 0;
	table->printJournalInfo("DCT");
}

DataConflictException::DataConflictException(
        PersistentTable *table,
        int64_t blockerTxnId,
        int64_t waitingTxnId) :
            SQLException(
                    SQLException::integrity_data_conflict,
                    "Attempted modification on Escrow-data",
                    voltdb::VOLT_EE_EXCEPTION_TYPE_DATA_CONFLICT),
            m_table(table), m_blockerTxnId(blockerTxnId), m_waitingTxnId(waitingTxnId) {
        assert(table);
	m_executedPlanNodes = 0;
	m_executedBatch = 0;

}

void DataConflictException::p_serialize(ReferenceSerializeOutput *output) {
    SQLException::p_serialize(output);
    output->writeLong(m_blockerTxnId);
    output->writeLong(m_waitingTxnId);
    output->writeLong(m_executedBatch);
    output->writeLong(m_executedPlanNodes);
    output->writeTextString(m_table->name());

    VOLT_TRACE("WWWG: blocker %jd waiter %jd %jd %jd %s \n", (intmax_t)m_blockerTxnId, (intmax_t)m_waitingTxnId,
    			(intmax_t)m_executedBatch, (intmax_t)m_executedPlanNodes, m_table->name().c_str());
}
DataConflictException::~DataConflictException() {
    // TODO Auto-generated destructor stub
}

}
