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

package org.voltdb.benchmark.workloads.paramgen;

import org.voltdb.benchmark.workloads.xml.ParamType;

public class RandomDouble extends ParamGenerator
{
    double m_min;
    double m_max;

    public RandomDouble(ParamType paramInfo)
    {
        if (paramInfo != null)
        {
            m_min = paramInfo.getFloat().getMin().floatValue();
            m_max = paramInfo.getFloat().getMax().floatValue();
        }
        else
        {
            m_min = Double.MIN_VALUE;
            m_max = Double.MAX_VALUE;
        }
    }

    @Override
    public Object getNextGeneratedValue()
    {
        if ((Math.abs(m_max / 2) + Math.abs(m_min / 2)) > (Double.MAX_VALUE))
        {
            double randomDouble = (double)(Math.random() * Double.MAX_VALUE);
            if (Math.random() < 0.5)
                return (double)(randomDouble - Double.MAX_VALUE);
            else
                return randomDouble;
        }
        else
        {
            return (double)((Math.random() * (m_max - m_min)) + m_min);
        }
    }

}
