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
package org.voltdb.benchmark.bingo;

import org.voltdb.benchmark.bingo.procedures.CreateTournament;
import org.voltdb.benchmark.bingo.procedures.DeleteTournament;
import org.voltdb.benchmark.bingo.procedures.PlayRound;
import org.voltdb.benchmark.bingo.procedures.GetAvgPot;
import org.voltdb.compiler.VoltProjectBuilder;

import java.net.URL;

public class BingoProjectBuilder extends VoltProjectBuilder {

    private static final String m_jarFileName = "bingo.jar";
    private static final URL ddlURL =
        BingoClient.class.getResource("bingo-ddl.sql");

    @Override
    public String[] compileAllCatalogs(int sitesPerHost,
                                       int length,
                                       int kFactor,
                                       String voltRoot) {
        addAllDefaults();
        boolean compile = compile(m_jarFileName, sitesPerHost,
                                  length, kFactor, voltRoot);
        if (!compile) {
            throw new RuntimeException("Bingo project builder failed app compilation.");
        }
        return new String[] {m_jarFileName};
    }

    @Override
    public void addAllDefaults() {
        addProcedures(
                CreateTournament.class,
                PlayRound.class,
                DeleteTournament.class,
                GetAvgPot.class);
        addSchema(ddlURL);
        addPartitionInfo("T", "T_ID");
        addPartitionInfo("B", "T_ID");
        addPartitionInfo("R", "T_ID");
    }
}
