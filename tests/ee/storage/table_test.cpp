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

#include "harness.h"
#include <string>
#include "common/executorcontext.hpp"
#include "common/TupleSchema.h"
#include "common/debuglog.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/tabletuple.h"
#include "common/DummyUndoQuantum.hpp"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "indexes/tableindex.h"

using namespace voltdb;
using namespace std;


string      warehouseColumnNames[9] = {
        "W_ID", "W_NAME", "W_STREET_1", "W_STREET_2", "W_CITY", "W_STATE",
        "W_ZIP", "W_TAX", "W_YTD" };


class TableTest : public Test {
    public:
        TableTest() {
            dummyUndo = new DummyUndoQuantum();
            engine = new ExecutorContext(0, 0, dummyUndo, NULL, false, 0, "", 0);
            mem = 0;

            vector<voltdb::ValueType> districtColumnTypes;
            vector<int32_t> districtColumnLengths;
            vector<bool> districtColumnAllowNull(11, true);
            districtColumnAllowNull[0] = false;

            vector<voltdb::ValueType> warehouseColumnTypes;
            vector<int32_t> warehouseColumnLengths;
            vector<bool> warehouseColumnAllowNull(9, true);
            warehouseColumnAllowNull[0] = false;

            warehouseColumnTypes.push_back(VALUE_TYPE_TINYINT); warehouseColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
            warehouseColumnTypes.push_back(VALUE_TYPE_VARCHAR); warehouseColumnLengths.push_back(16);
            warehouseColumnTypes.push_back(VALUE_TYPE_VARCHAR); warehouseColumnLengths.push_back(32);
            warehouseColumnTypes.push_back(VALUE_TYPE_VARCHAR); warehouseColumnLengths.push_back(32);
            warehouseColumnTypes.push_back(VALUE_TYPE_VARCHAR); warehouseColumnLengths.push_back(32);
            warehouseColumnTypes.push_back(VALUE_TYPE_VARCHAR); warehouseColumnLengths.push_back(2);
            warehouseColumnTypes.push_back(VALUE_TYPE_VARCHAR); warehouseColumnLengths.push_back(9);
            warehouseColumnTypes.push_back(VALUE_TYPE_DOUBLE); warehouseColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));
            warehouseColumnTypes.push_back(VALUE_TYPE_DOUBLE); warehouseColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));

            warehouseTupleSchema = TupleSchema::createTupleSchema(warehouseColumnTypes, warehouseColumnLengths, warehouseColumnAllowNull, true);

            warehouseIndex1ColumnIndices.push_back(0);
            warehouseIndex1ColumnTypes.push_back(VALUE_TYPE_TINYINT);

            warehouseIndex1Scheme = TableIndexScheme("Warehouse primary key index", ARRAY_INDEX, warehouseIndex1ColumnIndices, warehouseIndex1ColumnTypes, true, true, warehouseTupleSchema);


            warehouseTable = voltdb::TableFactory::getPersistentTable(0, engine, "WAREHOUSE",
                                                                      warehouseTupleSchema,
                                                                      warehouseColumnNames,
                                                                      warehouseIndex1Scheme,
                                                                      warehouseIndexes, 0,
                                                                      false, false);

            warehouseTempTable =  dynamic_cast<TempTable*>(
                TableFactory::getCopiedTempTable(0, "WAREHOUSE TEMP", warehouseTable,
                                                 &limits));

        }

        ~TableAndIndexTest() {
            delete engine;
            delete dummyUndo;
            delete warehouseTable;
            delete warehouseTempTable;
        }

    protected:
        int mem;
        TempTableLimits limits;
        UndoQuantum *dummyUndo;
        ExecutorContext *engine;

        TupleSchema      *warehouseTupleSchema;
        vector<TableIndexScheme> warehouseIndexes;
        Table            *warehouseTable;
        TempTable        *warehouseTempTable;
        vector<int>       warehouseIndex1ColumnIndices;
        vector<ValueType> warehouseIndex1ColumnTypes;
        TableIndexScheme  warehouseIndex1Scheme;

};

TEST_F(TableTest, BigTest) {
    vector<NValue> cachedStringValues;//To free at the end of the test
    TableTuple *temp_tuple = &warehouseTempTable->tempTuple();
    temp_tuple->setNValue(0, ValueFactory::getTinyIntValue(static_cast<int8_t>(3)));
    cachedStringValues.push_back(ValueFactory::getStringValue("EZ Street WHouse"));
    temp_tuple->setNValue(1, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("Headquarters"));
    temp_tuple->setNValue(2, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("77 Mass. Ave."));
    temp_tuple->setNValue(3, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("Cambridge"));
    temp_tuple->setNValue(4, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("AZ"));
    temp_tuple->setNValue(5, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("12938"));
    temp_tuple->setNValue(6, cachedStringValues.back());
    temp_tuple->setNValue(7, ValueFactory::getDoubleValue(static_cast<double>(.1234)));
    temp_tuple->setNValue(8, ValueFactory::getDoubleValue(static_cast<double>(15241.45)));
    warehouseTempTable->insertTupleNonVirtual(*temp_tuple);


    TableTuple warehouseTuple = TableTuple(warehouseTempTable->schema());
    TableIterator warehouseIterator = warehouseTempTable->iterator();
    while (warehouseIterator.next(warehouseTuple)) {
        if (!warehouseTable->insertTuple(warehouseTuple)) {
            cout << "Failed to insert tuple from input table '" << warehouseTempTable->name() << "' into target table '" << warehouseTable->name() << "'" << endl;
        }
    }
    warehouseTempTable->deleteAllTuplesNonVirtual(true);

}

int main() {
    return TestSuite::globalInstance()->runAll();
}
