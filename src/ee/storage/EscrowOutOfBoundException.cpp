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
#include "storage/EscrowOutOfBoundException.h"
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
EscrowOutOfBoundException::EscrowOutOfBoundException(
        PersistentTable *table,
        TableTuple tuple,
        NValue lower,
        NValue upper,
        NValue requested,
        int64_t txnId,
        ConstraintType type) :
            SQLException(
                    SQLException::escrow_out_of_bound,
                    "Attempted modification on Escrow-data exceeded lower bound or upper bound",
                    voltdb::VOLT_EE_EXCEPTION_TYPE_ESCROW_OUT_OF_BOUND),
            m_table(table), m_tuple(tuple),
            m_lowerBound(lower), m_upperBound(upper), m_requestedValue(requested),
            m_txnId(txnId), m_type(type) {
        assert(table);
        assert(!tuple.isNullTuple());
        table->printJournalInfo("Escrow OOB Exception created");
}



void EscrowOutOfBoundException::p_serialize(ReferenceSerializeOutput *output) {
    SQLException::p_serialize(output);
    output->writeTextString(m_table->name());
    output->writeTextString(m_lowerBound.debug());
    output->writeTextString(m_upperBound.debug());
    output->writeTextString(m_requestedValue.debug());
    output->writeLong(m_txnId);

    std::size_t tableSizePosition = output->reserveBytes(4);
    m_table->serializeTupleTo(*output, &m_tuple, 1);
    output->writeIntAt(tableSizePosition, static_cast<int32_t>(output->position() - tableSizePosition - 4));
}
EscrowOutOfBoundException::~EscrowOutOfBoundException() {
    // TODO Auto-generated destructor stub
}

}
