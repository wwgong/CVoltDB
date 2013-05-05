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

package com.tpcbprocedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/*
 * This SP returns the  number of history records in the DB.
 */

@ProcInfo
(
    singlePartition = false
)
public class DBCheck1 extends VoltProcedure
{
    public final SQLStmt getBranchRows =
        new SQLStmt("SELECT * FROM BRANCH;");
    public final SQLStmt getAccountRows =
            new SQLStmt("SELECT * FROM ACCOUNT;");
    public final SQLStmt getTellerRows =
            new SQLStmt("SELECT * FROM TELLER;");
    public final SQLStmt getHistoryCount =
            new SQLStmt("SELECT COUNT (*) FROM HISTORY;");
    public final SQLStmt getOneAccountRow =
            new SQLStmt("SELECT A_BALANCE FROM ACCOUNT WHERE A_ID = 100000;");

    public VoltTable[] run() throws VoltAbortException
    {
        voltQueueSQL(getBranchRows);
        voltQueueSQL(getAccountRows);
        voltQueueSQL(getTellerRows);
        voltQueueSQL(getHistoryCount);
        voltQueueSQL(getOneAccountRow);
        VoltTable[] result = voltExecuteSQL(true);

        return result;
    }
}
