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

package org.voltdb_testprocs.regressionsuites.sqlfeatureprocs;

import org.voltdb.*;

@ProcInfo (
    partitionInfo = "ORDER_LINE.OL_W_ID: 0",
    singlePartition = true
)
public class UpdateTests extends VoltProcedure {

    public final SQLStmt updateNoIndexes = new SQLStmt("UPDATE ORDER_LINE SET OL_SUPPLY_W_ID = 5;");
    public final SQLStmt updateParitalIndex = new SQLStmt("UPDATE ORDER_LINE SET OL_D_ID = 6");
    //public final SQLStmt updateFullIndex = new SQLStmt("UPDATE ORDER_LINE SET OL_W_ID = 7, OL_D_ID = 10, OL_O_ID = 50, OL_NUMBER = 12;");

    public VoltTable[] run(byte wid) {
        voltQueueSQL(updateNoIndexes);
        voltQueueSQL(updateParitalIndex);
        //voltQueueQuery(updateFullIndex);
        return voltExecuteSQL();
    }
}