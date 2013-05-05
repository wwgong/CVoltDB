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

package com.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/*
 * This SP returns the  number of records in the DB.
 */

@ProcInfo
(
    singlePartition = false
)
public class DBCheck extends VoltProcedure
{
    public final SQLStmt getWarehouseCount =
        new SQLStmt("SELECT COUNT (*) FROM WAREHOUSE;");
    public final SQLStmt getDistrictCount =
            new SQLStmt("SELECT COUNT (*) FROM DISTRICT;");
    public final SQLStmt getItemCount =
            new SQLStmt("SELECT COUNT (*) FROM ITEM;");
    public final SQLStmt getCustomerCount =
            new SQLStmt("SELECT COUNT (*) FROM CUSTOMER;");
    public final SQLStmt getCustomerNameCount =
            new SQLStmt("SELECT COUNT (*) FROM CUSTOMER_NAME;");
    public final SQLStmt getHistoryCount =
            new SQLStmt("SELECT COUNT (*) FROM HISTORY;");
    public final SQLStmt getStockCount =
            new SQLStmt("SELECT COUNT (*) FROM STOCK;");
    public final SQLStmt getOrdersCount =
            new SQLStmt("SELECT COUNT (*) FROM ORDERS;");
    public final SQLStmt getNewOrdersCount =
            new SQLStmt("SELECT COUNT (*) FROM NEW_ORDER;");

    public final SQLStmt getOrderLineCount =
            new SQLStmt("SELECT COUNT (*) FROM ORDER_LINE;");
  
    public VoltTable[] run() throws VoltAbortException
    {
        voltQueueSQL(getWarehouseCount);
        voltQueueSQL(getDistrictCount);
        voltQueueSQL(getItemCount);
        voltQueueSQL(getCustomerCount);
        voltQueueSQL(getCustomerNameCount);
        voltQueueSQL(getHistoryCount);
        voltQueueSQL(getStockCount);
        voltQueueSQL(getOrdersCount);
        voltQueueSQL(getOrderLineCount);
        
        VoltTable[] result = voltExecuteSQL(true);

        return result;
    }
}
