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

#ifndef PERSISTENTTABLEUNDODELETEACTION_H_
#define PERSISTENTTABLEUNDODELETEACTION_H_

#include "common/UndoAction.h"
#include "common/TupleSchema.h"
#include "common/Pool.hpp"
#include "common/tabletuple.h"
// GWW
#include "persistenttable.h"


namespace voltdb {

class PersistentTable;

class PersistentTableUndoDeleteAction: public voltdb::UndoAction {
public:
    inline PersistentTableUndoDeleteAction(char *deletedTuple,
                                           voltdb::PersistentTable *table)
        : m_tuple(deletedTuple), m_table(table), m_tupleJournal(NULL)
    {}

    virtual ~PersistentTableUndoDeleteAction();

    /*
     * Undo whatever this undo action was created to undo. In this case reinsert the tuple into the table.
     */
    void undo();

    /*
     * Release any resources held by the undo action. It will not need to be undone in the future.
     * In this case free the strings associated with the tuple.
     */
    void release();

    // GWW
    void setJournalPtr(TupleJournal *tuple) {
    	m_tupleJournal = tuple;
    }

    void debug() {
#if defined(DEBUG) || defined(_DEBUG) || defined(_DEBUG_)
    	printf("PTUDA for tj at %lu\n", (long)m_tupleJournal);
#endif
    }

private:
    char *m_tuple;
    PersistentTable *m_table;
    TupleJournal *m_tupleJournal;
};

}

#endif /* PERSISTENTTABLEUNDODELETEACTION_H_ */
