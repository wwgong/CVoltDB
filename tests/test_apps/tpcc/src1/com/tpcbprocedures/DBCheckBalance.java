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
 * This SP returns the branch balances computed various ways that should agree
 */

@ProcInfo
(
    singlePartition = false
)
public class DBCheckBalance extends VoltProcedure
{
    public final SQLStmt getBranchRows =
        new SQLStmt("SELECT b_id, sum(b_balance) FROM BRANCH GROUP BY B_ID;");
    public final SQLStmt getAccountRows =
            new SQLStmt("SELECT  a_b_id, sum(a_balance) FROM ACCOUNT GROUP BY A_B_ID;");
    public final SQLStmt getTellerRows =
            new SQLStmt("SELECT  t_b_id, sum(t_balance) FROM TELLER GROUP BY T_B_ID;");
    public final SQLStmt getHistoryRows =
            new SQLStmt("SELECT  h_b_id, sum(h_delta) FROM HISTORY GROUP BY H_B_ID;");
 
    public VoltTable[] run() throws VoltAbortException
    {
        voltQueueSQL(getBranchRows);
        voltQueueSQL(getAccountRows);
        voltQueueSQL(getTellerRows);
        voltQueueSQL(getHistoryRows);
        VoltTable[] result = voltExecuteSQL(true);

        return result;
    }
}
