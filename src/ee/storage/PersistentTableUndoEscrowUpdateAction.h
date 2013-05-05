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

#ifndef PERSISTENTTABLEUNDOESCROWUPDATEACTION_H_
#define PERSISTENTTABLEUNDOESCROWUPDATEACTION_H_

#include "common/UndoAction.h"
#include "common/TupleSchema.h"
#include "common/Pool.hpp"
#include "common/tabletuple.h"
#include "common/NValue.hpp"
#include "storage/persistenttable.h"


namespace voltdb {

class PersistentTableUndoEscrowUpdateAction: public voltdb::UndoAction {
public:

    inline PersistentTableUndoEscrowUpdateAction(
            voltdb::PersistentTable *table,
            TupleJournal *journal)
        : m_table(table), m_tupleJournal(journal)
    {}


    /*
     * Undo whatever this undo action was created to undo. In this
     * case the string allocations of the new tuple must be freed and
     * the tuple must be overwritten with the old one.
     */
    void undo();

    /*
     * Release any resources held by the undo action. It will not need
     * to be undone in the future. In this case the string allocations
     * of the old tuple must be released.
     */
    void release();

    virtual ~PersistentTableUndoEscrowUpdateAction(){};

    void debug() {
#if defined(DEBUG) || defined(_DEBUG) || defined(_DEBUG_)
    	printf("PTEUA for tj at %lu\n", (long)m_tupleJournal);
#endif
    }
    

private:
    voltdb::PersistentTable *m_table;
    TupleJournal *m_tupleJournal;
};

}

#endif /* PERSISTENTTABLEUNDOESCROWUPDATEACTION_H_ */
