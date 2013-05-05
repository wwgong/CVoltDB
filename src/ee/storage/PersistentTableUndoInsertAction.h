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

#ifndef PERSISTENTTABLEUNDOINSERTACTION_H_
#define PERSISTENTTABLEUNDOINSERTACTION_H_

#include "common/UndoAction.h"
#include "common/TupleSchema.h"
#include "common/Pool.hpp"
#include "common/tabletuple.h"
#include "storage/persistenttable.h"

namespace voltdb {


class PersistentTableUndoInsertAction: public voltdb::UndoAction {
public:
    inline PersistentTableUndoInsertAction(voltdb::TableTuple insertedTuple,
                                           voltdb::PersistentTable *table,
                                           voltdb::Pool *pool)
        : m_tuple(insertedTuple), m_table(table), m_tupleJournal(NULL)
    {
        void *tupleData = pool->allocate(m_tuple.tupleLength());
        m_tuple.move(tupleData);
        ::memcpy(tupleData, insertedTuple.address(), m_tuple.tupleLength());

        // GWW
        // m_journalTuplePtr = insertedTuple;

        VOLT_TRACE("INUNDO creating insert undo action for txn %lu\n",
        			ExecutorContext::getExecutorContext()->currentTxnId());
    }

    virtual ~PersistentTableUndoInsertAction();

    /*
     * Undo whatever this undo action was created to undo
     */
    void undo();

    /*
     * Release any resources held by the undo action. It will not need
     * to be undone in the future.
     */
    void release();

    // GWW
    void setJournalPtr(TupleJournal *tuple) {
    	m_tupleJournal = tuple;
    }

    void debug() {
#if defined(DEBUG) || defined(_DEBUG) || defined(_DEBUG_)
    	printf("PTUIA for tj at %lu\n", (long)m_tupleJournal);
#endif
    }

private:
    voltdb::TableTuple m_tuple;
    PersistentTable *m_table;
    // GWW: if INSERT + ... + DELETE and the txn finally commits
    // then after UndoDeleteAction.release(), could not find the tuple
    // anymore, but in release/undo, we only use this pointer to
    // look up tuple journal, so use TableTuple here
    TupleJournal *m_tupleJournal;
};

}

#endif /* PERSISTENTTABLEUNDOINSERTACTION_H_ */
