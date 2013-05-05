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

#ifndef DATACONFLICTEXCEPTION_H_
#define DATACONFLICTEXCEPTION_H_

#include "common/SQLException.h"
#include "common/types.h"
#include "common/ids.h"
#include "common/tabletuple.h"

namespace voltdb {
class PersistentTable;
class VoltDBEngine;

/*
 * A escrow failure exception is generated when the operation is not allowed under escrow-mode.
 */
class DataConflictException: public SQLException {
public:
    /*
     * beforeTuple and afterTuple will be used if we implement "what-if"
     * @param table Table that the operation was performed on
     * @param tableId CatalogId of the table that the operation failed on
     * @param beforeTuple before tuple values or null if before operation is Insert
     * @param afterTuple after tuple values or null if after operation is Delete.
     * @param blockingTxnId txn that modified the tuple and blocks current operation
     * @param waitingTxnId txn wanted to perform the operation but gets blocked
     */
	DataConflictException(PersistentTable *table, TableTuple beforeTuple, TableTuple afterTuple,
			int64_t blockerTxnId, int64_t waitingTxnId);

	DataConflictException(PersistentTable *table, int64_t blockerTxnId, int64_t waitingTxnId);

    virtual ~DataConflictException();

    void setExecutedBatch(int executed) {
    	m_executedBatch = (int64_t)executed;
    }

    void setExecutedPlanNodes(int executed) {
    	m_executedPlanNodes = (int64_t)executed;
    }
protected:
    void p_serialize(ReferenceSerializeOutput *output);

    PersistentTable *m_table;
    TableTuple m_beforeTuple;
    TableTuple m_afterTuple;
    int64_t m_blockerTxnId;
    int64_t m_waitingTxnId;
    int64_t m_executedBatch;
    int64_t m_executedPlanNodes;
};

}

#endif /* DATACONFLICTEXCEPTION_H_ */
