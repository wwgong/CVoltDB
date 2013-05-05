/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Michael McCanna
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
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
import org.voltdb.types.TimestampType;

@ProcInfo (
	partitionInfo = "BRANCH.B_ID: 0",
    singlePartition = true
)

public class TPCBTransaction extends VoltProcedure {
    public final SQLStmt getAccountBalance = new SQLStmt("SELECT A_BALANCE FROM ACCOUNT WHERE A_ID = ? AND A_B_ID = ?");
    public final SQLStmt updateBranchBalance = new SQLStmt("UPDATE BRANCH SET B_BALANCE = B_BALANCE + ? WHERE B_ID = ?"); 
    public final SQLStmt updateAccountBalance = new SQLStmt("UPDATE ACCOUNT SET A_BALANCE = A_BALANCE + ? WHERE A_ID = ? AND A_B_ID = ?;");
    public final SQLStmt updateTellerBalance = new SQLStmt("UPDATE TELLER SET T_BALANCE = T_BALANCE + ? WHERE T_ID = ? AND T_B_ID = ?;");

    public final SQLStmt insertHistory = new SQLStmt("INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?);");
    public VoltTable[] run(int bid, int tid, int aid, long delta, TimestampType timestamp) {
    	voltQueueSQL(getAccountBalance, aid, bid);
        voltQueueSQL(updateAccountBalance, delta, aid, bid);
        voltQueueSQL(updateTellerBalance, delta, tid, bid);
        voltQueueSQL(updateBranchBalance, delta, bid);
        voltQueueSQL(insertHistory, bid, aid, tid, timestamp, delta, "hi there");
        final VoltTable[] results = voltExecuteSQL(true);
        return results;
    }
}
