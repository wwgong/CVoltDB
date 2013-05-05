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

package org.voltdb.benchmark.tpcc.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

/*
 * This SP returns the approximate number of warehouses currently loaded into the DB.
 */

@ProcInfo
(
    singlePartition = false,
    limitedToPartitions = true,
    partitionInfo = "WAREHOUSE.W_ID: 0, 1"
)
public class warehouseStatus extends VoltProcedure
{
    public final SQLStmt getWarehouseCount =
        new SQLStmt("SELECT COUNT (*) FROM WAREHOUSE;");
    
    public final SQLStmt getDistrictCount =
            new SQLStmt("SELECT COUNT (*) FROM DISTRICT;");
    
    public final SQLStmt getItemCount =
            new SQLStmt("SELECT COUNT (*) FROM ITEM;");
    
    public final SQLStmt getCustomerCount =
            new SQLStmt("SELECT COUNT (*) FROM CUSTOMER;");
    
    public final SQLStmt getMaxDist = 
    		new SQLStmt("SELECT D_W_ID, MAX(D_ID) FROM DISTRICT GROUP BY D_W_ID;");
    
    public final SQLStmt getDist =
    		new SQLStmt("SELECT D_W_ID, MAX(D_ID) FROM DISTRICT WHERE D_W_ID = ? OR D_W_ID = ? GROUP BY D_W_ID;");
    

    public VoltTable[] run(short w_id1, short w_id2) throws VoltAbortException
    {
        voltQueueSQL(getDist, w_id1, w_id2);
//        voltQueueSQL(getDistrictCount);
//        voltQueueSQL(getItemCount);
//        voltQueueSQL(getCustomerCount);
        VoltTable[] result = voltExecuteSQL(true);
        
        // TODO: add assertions
        return null;
    }
}
