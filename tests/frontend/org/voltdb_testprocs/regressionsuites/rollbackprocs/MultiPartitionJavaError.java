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

package org.voltdb_testprocs.regressionsuites.rollbackprocs;

import org.voltdb.*;

@ProcInfo (
    singlePartition = false
)
public class MultiPartitionJavaError extends VoltProcedure {

    public final SQLStmt insert = new SQLStmt("INSERT INTO ITEM VALUES (?, ?, ?, ?, ?);");

    public long run(long i_id) {
        voltQueueSQL(insert, i_id, i_id, "foo", 2.5, "foo");
        long tuplesChanged = voltExecuteSQL(true)[0].asScalarLong();
        assert(tuplesChanged > 0);

        // now divide by zero to ensure the transaction gets rolled back

        @SuppressWarnings("unused")
        int x = 5 / 0;

        return tuplesChanged;
    }
}
