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

#ifndef ESCROWOUTOFBOUNDEXCEPTION_H_
#define ESCROWOUTOFBOUNDEXCEPTION_H_

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
class EscrowOutOfBoundException: public SQLException {
public:
    /*
     * @param table Table that the operation was performed on
     * @param tuple Tuple that the operation was performed on
     * @param lower NValue of the Escrow Data lower bound
     * @param upper NValue of the Escrow Data upper bound
     * @param requested NValue of the operation wanted to change
     * @param txnId txn wanted to perform the operation but failed
     */
	EscrowOutOfBoundException(PersistentTable *table, TableTuple tuple, NValue lower, NValue upper, NValue requested, int64_t txnId, ConstraintType type);

    virtual ~EscrowOutOfBoundException();

protected:
    void p_serialize(ReferenceSerializeOutput *output);

    PersistentTable *m_table;
    TableTuple m_tuple;
    NValue m_lowerBound;
    NValue m_upperBound;
    NValue m_requestedValue;
    int64_t m_txnId;
    ConstraintType m_type;
};

}

#endif /* ESCROWOUTOFBOUNDEXCEPTION_H_ */
