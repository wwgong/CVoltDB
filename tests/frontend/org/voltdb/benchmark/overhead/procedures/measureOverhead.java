/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.benchmark.overhead.procedures;

import org.voltdb.*;

//Notes on Stored Procedure:
//Need to add error handling to catch invalid items, and still return needed values.

@ProcInfo(
    partitionInfo = "NEWORDER.NO_O_ID: 0",
    singlePartition = true
)
public class measureOverhead extends VoltProcedure {
    @SuppressWarnings("unused")
    private final VoltTable item_data_template =
        PrivateVoltTableFactory.createUninitializedVoltTable();
    @SuppressWarnings("unused")
    private final VoltTable misc_template =
        PrivateVoltTableFactory.createUninitializedVoltTable();

    public final SQLStmt getID = new SQLStmt("SELECT NO_O_ITEM FROM NEWORDER WHERE NO_O_ID = ?;");

    public VoltTable[] run(int no_o_id) {
        voltQueueSQL(getID, no_o_id);
        return voltExecuteSQL(true);
    }
}
