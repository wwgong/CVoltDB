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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
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

#include <cstdlib>
#include <ctime>
#include <string>
#include "harness.h"
#include "common/executorcontext.hpp"
#include "common/DummyUndoQuantum.hpp"

#include "common/common.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "common/debuglog.h"
#include "common/ThreadLocalPool.h"
#include "common/TupleSchema.h"
#include "common/tabletuple.h"
#include "storage/table.h"

#include "storage/temptable.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"
#include "storage/DataConflictException.h"

#include "storage/PersistentTableUndoDeleteAction.h"
#include "storage/PersistentTableUndoInsertAction.h"
#include "storage/PersistentTableUndoUpdateAction.h"
#include "storage/PersistentTableUndoEscrowUpdateAction.h"


using namespace std;
using namespace voltdb;

#define NUM_OF_COLUMNS 5
#define NUM_OF_TUPLES 1

ValueType COLUMN_TYPES[NUM_OF_COLUMNS]  = { VALUE_TYPE_BIGINT,
                                            VALUE_TYPE_TINYINT,
                                            VALUE_TYPE_SMALLINT,
                                            VALUE_TYPE_INTEGER,
                                            VALUE_TYPE_BIGINT };

int32_t COLUMN_SIZES[NUM_OF_COLUMNS] =
    {
        NValue::getTupleStorageSize(VALUE_TYPE_BIGINT),
        NValue::getTupleStorageSize(VALUE_TYPE_TINYINT),
        NValue::getTupleStorageSize(VALUE_TYPE_SMALLINT),
        NValue::getTupleStorageSize(VALUE_TYPE_INTEGER),
        NValue::getTupleStorageSize(VALUE_TYPE_BIGINT)
    };
bool COLUMN_ALLOW_NULLS[NUM_OF_COLUMNS] = { true, true, true, true, true };
bool IS_ESCROW_COLUMN[NUM_OF_COLUMNS] = {false, true, false, false, false};


string      warehouseColumnNames[9] = {
        "W_ID", "W_NAME", "W_STREET_1", "W_STREET_2", "W_CITY", "W_STATE",
        "W_ZIP", "W_TAX", "W_YTD" };

class EscrowTableTest : public Test {
public:
    EscrowTableTest() : table(NULL) //, temp_table(NULL), persistent_table(NULL)
    {
        srand(0);
        init(false); // default is temp_table. call init(true) to make it transactional
    }
    ~EscrowTableTest() {
        delete table;
    }

protected:
    void init(bool xact) {
        CatalogId database_id = 1000;
        vector<boost::shared_ptr<const TableColumn> > columns;
        char buffer[32];

        string *columnNames = new string[NUM_OF_COLUMNS];
        vector<ValueType> columnTypes;
        vector<int32_t> columnLengths;
        vector<bool> columnAllowNull;
        vector<bool> isEscrowColumn;
        for (int ctr = 0; ctr < NUM_OF_COLUMNS; ctr++) {
            snprintf(buffer, 32, "column%02d", ctr);
            columnNames[ctr] = buffer;
            columnTypes.push_back(COLUMN_TYPES[ctr]);
            columnLengths.push_back(COLUMN_SIZES[ctr]);
            columnAllowNull.push_back(COLUMN_ALLOW_NULLS[ctr]);
            isEscrowColumn.push_back(IS_ESCROW_COLUMN[ctr]);

        }
        TupleSchema *schema = TupleSchema::createTupleSchema(columnTypes, columnLengths, columnAllowNull, isEscrowColumn, true);

        m_pool = new Pool();
        undoQuantum =
          new (m_pool->allocate(sizeof(UndoQuantum)))
          UndoQuantum(0, m_pool);

        engine =  new ExecutorContext(0, 0, undoQuantum, NULL, false, 0, "", 0);
        engine->setupForPlanFragments(undoQuantum, 1, 100);

        indexColumnIndices.push_back(0);
        indexColumnTypes.push_back(VALUE_TYPE_BIGINT);
        indexScheme = TableIndexScheme("Primary key index", ARRAY_INDEX, indexColumnIndices, indexColumnTypes, true, true, schema);


        table = (PersistentTable*)TableFactory::getPersistentTable(database_id, engine, "test_table",
        														schema, columnNames, indexScheme, indexes,
        														0, false, false);

        assert(tableutil::addRandomTuples(this->table, NUM_OF_TUPLES));

        // clean up
        delete[] columnNames;
    }

    PersistentTable* table;
    TempTableLimits limits;
    int tempTableMemory;
    Pool *m_pool;
    UndoQuantum* undoQuantum;
    ExecutorContext* engine;

    vector<TableIndexScheme> indexes;
    vector<int>       indexColumnIndices;
    vector<ValueType> indexColumnTypes;
    TableIndexScheme  indexScheme;

};
/*
TEST_F(EscrowTableTest, EscrowUpdate) {

	engine->setupForPlanFragments(undoQuantum, 1, 0);
	ExecutorContext::getExecutorContext()->setCVoltDBExecution(true);
	printf("\n");

	// txn1: update t1 on col2
	TableIterator itt = this->table->iterator();
	TableTuple ttt(this->table->schema());
	itt.next(ttt);
	printf("\t\tSource: %s\n", ttt.debugNoHeader().c_str());
	TableTuple &temp_tuple =this->table->tempTuple();
//	(&temp_tuple)->setNValue(0, ValueFactory::getBigIntValue(static_cast<int8_t>(2)));
	(&temp_tuple)->setNValue(1, ValueFactory::getBigIntValue(static_cast<int8_t>(100)));
	printf("\t\tTarget: %s\n", temp_tuple.debugNoHeader().c_str());

	bool updatedCols[5] = {false, true, false, false, false};

    this->table->updateTuple(temp_tuple, ttt, updatedCols, false);

    EXPECT_EQ(1, this->table->activeTupleCount());
	EXPECT_EQ(1, this->table->getTupleJournals().size());
	EXPECT_EQ(1, this->table->getTupleJournalStorage().size());

    TupleJournal *tJournal = this->table->findTupleJournal((&ttt)->address());
    if(tJournal != NULL) {
    	if(tJournal->escrowData.empty())
    		printf("CANNOT FIND ESCROW DATA!!!!");
    	EXPECT_EQ(1, (int)(tJournal->escrowData.size()));
    } else
    	printf("\t\tno journal found.\n");


    // txn2: update t1 on col2 too
    engine->setupForPlanFragments(undoQuantum, 2, 1);
    (&temp_tuple)->setNValue(1, ValueFactory::getBigIntValue(static_cast<int8_t>(300)));
    try{
    	EXPECT_EQ(true, this->table->updateTuple(temp_tuple, ttt, updatedCols, false));
    } catch (DataConflictException &e) {
    	printf("\t\tException!!!!\n");
    }

    undoQuantum->release();

    itt = this->table->iterator();
    itt.next(ttt);
    printf("\tAfter commit: %s\n", ttt.debugNoHeader().c_str());

    EXPECT_EQ(0, this->table->getTupleJournals().size());

}
*/

TEST_F(EscrowTableTest, Escrow2Inserts) {

	engine->setupForPlanFragments(undoQuantum, 1, 0);
	ExecutorContext::getExecutorContext()->setCVoltDBExecution(true);

	printf("\n\t\tInserting one row...\n");
	// txn1: insert t1
	TableTuple &temp_tuple = this->table->tempTuple();
	ASSERT_EQ(true, tableutil::setRandomTupleValues(this->table, &temp_tuple));
	(&temp_tuple)->setNValue(0, ValueFactory::getBigIntValue(static_cast<int8_t>(1)));

	printf("\t\tStart testing...\n");
	// test
	ASSERT_EQ(true, this->table->insertTuple(temp_tuple));

	printf("\t\tTry to find the journal...\n");
    TableIterator iterator = this->table->iterator();
    TableTuple tuple(table->schema());
    ASSERT_EQ(true, iterator.next(tuple));
    EXPECT_EQ(true, tuple.isActive());
    EXPECT_EQ(1, this->table->getTupleJournals().size());
    TupleJournal *tJournal = this->table->findTupleJournal((&tuple)->address());
    if(tJournal != NULL) {
    	EXPECT_EQ(1, (int)(tJournal->changer->txnid));
    	EXPECT_EQ(NonEscrow_INSERT, tJournal->changer->type);
    	EXPECT_EQ(1, tJournal->changer->operationCount);
    } else
    	printf("\t\tno journal found.\n");

    // txn1: insert t1 again
    try{
    	EXPECT_EQ(true, this->table->insertTuple(temp_tuple));
    } catch (SerializableEEException &e) {
    	printf("\t\tException expected, constraint error.\n");
    }

    // txn2: insert t1 again
    engine->setupForPlanFragments(undoQuantum, 2, 0);
    try{
    	EXPECT_EQ(true, this->table->insertTuple(temp_tuple));
    } catch (SerializableEEException &e) {
    	printf("\t\tException expected, constraint error.\n");
    }

    // txn2: delete t1
	try {
		this->table->deleteAllTuples(true);
	} catch(SerializableEEException &e) {
		printf("\t\tException expected, txn2 can't delete txn1's tuple.\n");
	}

	// txn2: insert t2
    TableTuple &temp_tuple2 = this->table->tempTuple();
	ASSERT_EQ(true, tableutil::setRandomTupleValues(this->table, &temp_tuple2));
	(&temp_tuple)->setNValue(0, ValueFactory::getBigIntValue(static_cast<int8_t>(2)));

	try {
		EXPECT_EQ(true, this->table->insertTuple(temp_tuple2));
	} catch (SerializableEEException &e) {
    	printf("\t\tNo exception expected!!!\n");
    }

	// test changer type
    EXPECT_EQ(2, this->table->getTupleJournals().size());
    iterator = this->table->iterator();
    while(iterator.hasNext()) {
        ASSERT_EQ(true, iterator.next(tuple));
        TupleJournal *tJournal = this->table->findTupleJournal((&tuple)->address());
        if(tJournal != NULL) {
        	EXPECT_EQ(NonEscrow_INSERT, tJournal->changer->type);
        } else
        	printf("\t\tno journal found.\n");
    }

    // txn1: delete t1
    engine->setupForPlanFragments(undoQuantum, 1, 0);
    try {
	    this->table->deleteAllTuples(true);
	} catch(SerializableEEException &e) {
		printf("\t\tException expected, T1 can not delete t2.\n");
	}

	// test type
	std::map<char*, TupleJournal*> journals;
    std::map<char*, TupleJournal*>::iterator it;
    EXPECT_EQ(2, this->table->getTupleJournals().size());
    int type[2];
    int i = 0;
    journals = this->table->getTupleJournals();
    for(it = journals.begin(); it != journals.end(); it++) {
    	type[i++] = it->second->changer->type;
    	printf("\t\tJournal Type: %d\n", it->second->changer->type );
    	printf("\t\tJournal op count: %d\n", it->second->changer->operationCount);
    }
    EXPECT_NE(type[0], type[1]);
    printf("\t\tType test finished!\n");

	// let txn2 delete t2
	engine->setupForPlanFragments(undoQuantum, 2, 0);
	TableTuple t = this->table->lookupTuple(temp_tuple2);
	this->table->deleteTuple(t, true);
	printf("\t\tTwo tuples have been removed!\n");

    // test type
    iterator = this->table->iterator();
    //EXPECT_EQ(2, this->table->getTupleJournals().size());
    printf("\t\tJournal numbers: %lu\n", this->table->getTupleJournals().size());
    EXPECT_EQ(2, this->table->activeTupleCount());
    journals = this->table->getTupleJournals();
    for(it = journals.begin(); it != journals.end(); it++) {
    	printf("\t\tJournal type: %d\n", it->second->changer->type);
    	EXPECT_EQ(NonEscrow_DELETE, it->second->changer->type);
    	printf("\t\tJournal op count: %d\n", it->second->changer->operationCount );
    }
    printf("\t\tJournal type became DELETE!\n");

    // txn1: insert t1 again
    try{
    	EXPECT_EQ(false, this->table->insertTuple(temp_tuple));
    } catch (SerializableEEException &e) {
    	printf("\t\tException expected, can not re-insert.\n");
    }

    printf("\t\tjournal#: %lu\n", this->table->getTupleJournals().size());

    // txn1 and txn2 commit
    undoQuantum->release();
    printf("\t\tjournal#: %lu\n", this->table->getTupleJournals().size());
    journals = this->table->getTupleJournals();
    for(it = journals.begin(); it != journals.end(); it++) {
    	printf("\t\tJournal type: %d\n", it->second->changer->type);
    	EXPECT_EQ(NonEscrow_DELETE, it->second->changer->type);
    	printf("\t\tJournal op count: %d\n", it->second->changer->operationCount );
    }
    EXPECT_EQ(0, this->table->getTupleJournals().size());
    EXPECT_EQ(0, this->table->activeTupleCount());
    EXPECT_EQ(0, this->table->getTupleJournalStorage().size());
}


#if 0
TEST_F(EscrowTableTest, Escrow2Deletes) {

	//this->table->enableEscrow(true);
	ExecutorContext::getExecutorContext()->setUtilityExecution(true);
	engine->setupForPlanFragments(undoQuantum, 50, 10);

	assert(tableutil::addRandomTuples(this->table, 3));
	EXPECT_EQ(3, this->table->getTupleJournals().size());

	printf("\n\t\tJournal #: %lu\n", this->table->getTupleJournals().size());
	std::map<char*, TupleJournal*> journals = this->table->getTupleJournals();
	std::map<char*, TupleJournal*>::iterator it;
	    for(it = journals.begin(); it != journals.end(); it++) {
	    	printf("\t\tJournal type: %d", it->second->changer->type);
	    	EXPECT_EQ(NonEscrow_INSERT, it->second->changer->type);
	    	printf("\t\top#: %d\n", it->second->changer->operationCount );
	}

    this->table->deleteAllTuples(true);
	EXPECT_EQ(3, this->table->getTupleJournals().size());

	printf("\t\tJournal #: %lu\n", this->table->getTupleJournals().size());
	journals = this->table->getTupleJournals();
	for(it = journals.begin(); it != journals.end(); it++) {
	   	printf("\t\tJournal type: %d", it->second->changer->type);
	   	EXPECT_EQ(NonEscrow_DELETE, it->second->changer->type);
	   	printf("\t\top#: %d\n", it->second->changer->operationCount );
	}

    printf("\n\t\t%lu undo actions\n", undoQuantum->size());

    undoQuantum->release();
/*
    for(std::vector<UndoAction*>::reverse_iterator i = undoQuantum->m_undoActions.rbegin();
            i != undoQuantum->m_undoActions.rend(); i++) {

    	PersistentTableUndoDeleteAction *dact = dynamic_cast<PersistentTableUndoDeleteAction*>(*i);
    	PersistentTableUndoInsertAction *iact = dynamic_cast<PersistentTableUndoInsertAction*>(*i);

    	printf("\t\t\tUndo Action Type: ");
    	if(dact != NULL) {
    		printf("DELETE\n");
    	}
    	else if(iact != NULL) {
    		printf("INSERT\n");

//    		if(this->table->findTupleJournal(iact->m_tuple.address()))
//    			printf("option1\n");
//    		else{
//    			TableTuple t = this->table->lookupTuple(iact->m_tuple);
//    			if(t.isNullTuple())
//    				printf("null tuple!\n");
//    			if(this->table->findTupleJournal(t.address())) {
//    				printf("option2\n");
//    				TupleJournal *tj = this->table->findTupleJournal(t.address());
//    				printf("op: %d\n", tj->changer->operationCount);
//    			}
//    		}
    	}
    	(*i)->release();
    	(*i)->~UndoAction();

        journals = this->table->getTupleJournals();
        if(journals.size() > 0)
        for(it = journals.begin(); it != journals.end(); it++) {
        	printf("\t\t\tJournal type: %d", it->second->changer->type);
        	printf("\t\top#: %d\n", it->second->changer->operationCount );
    	}
    }
*/

    EXPECT_EQ(0, this->table->getTupleJournals().size());
    printf("\t\tJournal #: %lu\n", this->table->getTupleJournals().size());
    journals = this->table->getTupleJournals();
    for(it = journals.begin(); it != journals.end(); it++) {
    	printf("\t\tJournal type: %d", it->second->changer->type);
    	printf("\t\top#: %d\n", it->second->changer->operationCount );
	}

    printf("\t\t%ld\n", (long)this->table->activeTupleCount());

//    EXPECT_EQ(0, this->table->activeTupleCount());

    TableTuple tuple3(table->schema());
    TableIterator iterator = this->table->iterator();
    while(iterator.hasNext()) {
       	ASSERT_EQ(true, iterator.next(tuple3));
    	EXPECT_EQ(true, tuple3.isActive());
    	printf("\t\tTuple: %s\n", tuple3.debugNoHeader().c_str());
    }

}


TEST_F(EscrowTableTest, Escrow2Updates) {
	printf("\n");

	this->table->deleteAllTuples(true);
	//this->table->enableEscrow(true);
	//EXPECT_EQ(true, this->table->isEscrowEnabled());
	ExecutorContext::getExecutorContext()->setUtilityExecution(true);

	// txn1: insert t1
	engine->setupForPlanFragments(undoQuantum, 101, 100);
	TableTuple &temp_tuple = this->table->tempTuple();
	ASSERT_EQ(true, tableutil::setRandomTupleValues(this->table, &temp_tuple));
	(&temp_tuple)->setNValue(0, ValueFactory::getBigIntValue(static_cast<int8_t>(1)));
	(&temp_tuple)->setNValue(1, ValueFactory::getBigIntValue(static_cast<int8_t>(1)));

	// test
	ASSERT_EQ(true, this->table->insertTuple(temp_tuple));
    TableIterator iterator = this->table->iterator();
    TableTuple tuple(table->schema());
    ASSERT_EQ(true, iterator.next(tuple));
    EXPECT_EQ(true, tuple.isActive());
    EXPECT_EQ(1, this->table->getTupleJournals().size());
    TupleJournal *tJournal = this->table->findTupleJournal((&tuple)->address());
    if(tJournal != NULL) {
    	EXPECT_EQ(101, (int)(tJournal->changer->txnid));
    	EXPECT_EQ(NonEscrow_INSERT, tJournal->changer->type);
    } else
    	printf("\t\tno journal found.\n");
    printf("\t\tBefore Update: %s\n", tuple.debugNoHeader().c_str());

    // txn1: update t1, change col1 from 1 to 2
    TableTuple &temp_tuple2 =this->table->tempTuple();
    //temp_tuple2.copy(temp_tuple);
    (&temp_tuple2)->setNValue(1, ValueFactory::getBigIntValue(static_cast<int8_t>(2)));
    printf("\t\tTarget tuple: %s\n", temp_tuple2.debugNoHeader().c_str());
    TableTuple t = this->table->lookupTuple(temp_tuple);
    printf("\t\tTable tuple: %s\n", t.debugNoHeader().c_str());
    this->table->updateTuple(temp_tuple2, tuple, false);
    tJournal = this->table->findTupleJournal((&tuple)->address());
    if(tJournal != NULL) {
    	EXPECT_EQ(101, (int)(tJournal->changer->txnid));
    	EXPECT_EQ(NonEscrow_INSERT, tJournal->changer->type);
    } else
    	printf("\t\tno journal found.\n");
    TableTuple tuple1(table->schema());
    iterator = this->table->iterator();
    while(iterator.hasNext()) {
       	ASSERT_EQ(true, iterator.next(tuple1));
    	EXPECT_EQ(true, tuple1.isActive());
    	printf("\t\tAfter Update: %s\n", tuple1.debugNoHeader().c_str());
    }
    EXPECT_EQ(1, this->table->getTupleJournals().size());
    printf("\t\tNow we have %lu journal, since we did INSERT before!\n", this->table->getTupleJournals().size());
    TupleJournal* journals[2];
    char * ptr[2];
    int i = 0;
    std::map<char*, TupleJournal*>::iterator it;
    std::map<char*, TupleJournal*> tjournals = this->table->getTupleJournals();
    for(it = tjournals.begin(); it != tjournals.end(); it++)
    {
    	ptr[i] = it->first;
    	journals[i] = it->second;
    	printf("\n\t\tChanger %d type: %d\n", i, journals[i]->changer->type);
    	printf("\t\tChanger %d txnid: %li\n", i, (long)journals[i]->changer->txnid);
//    	printf("\t\tChanger %d column count: %d\n", i, journals[i].changer->columnCount);
		printf("\t\tChanger %d updated cols: {", i);
		int j = 0;
//		for(j = 0; j < journals[i].changer->columnCount; j++)
		for(j = 0; j < journals[i]->schema->columnCount(); j++)
			printf("%s, ", journals[i]->changer->updatedCols[j] ? "true" : "false");
		printf("}\n");
		printf("\t\tOp count: %d\n", journals[i]->changer->operationCount);
		i++;
    }

//    EXPECT_EQ(journals[0].currentTuple, journals[1].currentTuple);
//    EXPECT_EQ(5, journals[0].changer->columnCount);
//    EXPECT_EQ(journals[0].changer->columnCount, journals[1].changer->columnCount);
//    EXPECT_EQ(true, journals[0].changer->updatedCols[1]);
//    EXPECT_EQ(journals[0].changer->updatedCols[1], journals[1].changer->updatedCols[1]);
//    EXPECT_EQ(journals[0].changer->updatedCols[2], journals[1].changer->updatedCols[2]);
//	assert(ptr[0] != ptr[1]);
   	EXPECT_EQ(1, this->table->activeTupleCount());

    // txn2: update t1 again
    engine->setupForPlanFragments(undoQuantum, 102, 100);
    (&temp_tuple2)->setNValue(1, ValueFactory::getBigIntValue(static_cast<int8_t>(3)));
    try{
    	//EXPECT_EQ(false, this->table->updateTuple(temp_tuple2, temp_tuple, false));
    	TableTuple ttt = this->table->lookupTuple(temp_tuple);
    	EXPECT_EQ(false, this->table->updateTuple(temp_tuple2, ttt, false));
    } catch (DataConflictException &e) {
    	printf("\t\tException expected, txn2 update t1, data conflict error.\n");
    }

    printf("\n\t\tAfter t2 updates...\n");
    printf("\t\tNow, %lu journals\n", this->table->getTupleJournals().size());
    std::map<char*, TupleJournal*> tjournals2 = this->table->getTupleJournals();
    i = 0;
    for(it = tjournals2.begin(); it != tjournals2.end(); it++)
    {
    	ptr[i] = it->first;
    	journals[i] = it->second;
    	printf("\n\t\tChanger %d type: %d\n", i, journals[i]->changer->type);
    	printf("\t\tChanger %d txnid: %li\n", i, (long)journals[i]->changer->txnid);
//    	printf("\t\tChanger %d column count: %d\n", i, journals[i].changer->columnCount);
		printf("\t\tChanger %d updated cols: {", i);
		int j = 0;
//		for(j = 0; j < journals[i].changer->columnCount; j++)
		for(j = 0; j < journals[i]->schema->columnCount(); j++)
			printf("%s, ", journals[i]->changer->updatedCols[j] ? "true" : "false");
		printf("}\n");
		printf("\t\tOp count: %d\n", journals[i]->changer->operationCount);
		i++;
    }

    EXPECT_EQ(1, this->table->activeTupleCount());

    // print out tuples again, should have only 1 tuple
    TableTuple tuple2(table->schema());
    iterator = this->table->iterator();
    while(iterator.hasNext()) {
       	ASSERT_EQ(true, iterator.next(tuple2));
    	EXPECT_EQ(true, tuple2.isActive());
    	printf("\t\tAfter Update: %s\n", tuple2.debugNoHeader().c_str());
    }
/
    engine->setupForPlanFragments(undoQuantum, 101, 100);
    this->table->deleteAllTuples(true);
    tjournals = this->table->getTupleJournals();
    for(it = tjournals.begin(); it != tjournals.end(); it++)
    {
    	ptr[i] = it->first;
    	journals[i] = it->second;
    	printf("\n\t\tChanger %d type: %d\n", i, journals[i].changer->type);
    	printf("\t\tChanger %d txnid: %li\n", i, journals[i].changer->txnid);
//    	printf("\t\tChanger %d column count: %d\n", i, journals[i].changer->columnCount);
		printf("\t\tChanger %d updated cols: {", i);
		int j = 0;
//		for(j = 0; j < journals[i].changer->columnCount; j++)
		for(j = 0; j < journals[i].schema->columnCount(); j++)
			printf("%s, ", journals[i].changer->updatedCols[j] ? "true" : "false");
		printf("}\n");
		printf("\t\tOp count: %d\n", journals[i].operationCount);
		i++;
    }
/

    printf("\t\tReady to finish up...");

    printf("\n\t\t%lu undo actions\n", undoQuantum->size());

    undoQuantum->release();

    for(std::vector<UndoAction*>::reverse_iterator i = undoQuantum->m_undoActions.rbegin();
            i != undoQuantum->m_undoActions.rend(); i++) {

    	PersistentTableUndoDeleteAction *dact = dynamic_cast<PersistentTableUndoDeleteAction*>(*i);
    	PersistentTableUndoInsertAction *iact = dynamic_cast<PersistentTableUndoInsertAction*>(*i);
    	PersistentTableUndoUpdateAction *uact = dynamic_cast<PersistentTableUndoUpdateAction*>(*i);
    	PersistentTableUndoEscrowUpdateAction *euact = dynamic_cast<PersistentTableUndoEscrowUpdateAction*>(*i);


    	printf("\t\t\tUndo Action Type: ");
    	if(dact != NULL) {
    		printf("DELETE\n");
    	}
    	else if (uact != NULL) {
    		printf("UPDATE\t");
    	}
    	else if (euact != NULL) {
    		printf("Escrow UPDATE\n");
    	}
    	else if(iact != NULL) {
    		printf("INSERT\n");

//    		if(this->table->findTupleJournal(iact->m_tuple.address()))
//    			printf("option1\n");
//    		else{
//    			TableTuple t = this->table->lookupTuple(iact->m_tuple);
//    			if(t.isNullTuple())
//    				printf("null tuple!\n");
//    			if(this->table->findTupleJournal(t.address())) {
//    				printf("option2\n");
//    				TupleJournal *tj = this->table->findTupleJournal(t.address());
//    				printf("op: %d\n", tj->changer->operationCount);
//    			}
//    		}
    	}
    	(*i)->release();
    	(*i)->~UndoAction();

        std::map<char*, TupleJournal*> journals = this->table->getTupleJournals();
        if(journals.size() > 0)
        for(it = journals.begin(); it != journals.end(); it++) {
        	printf("\t\t\tJournal type: %d", it->second->changer->type);
        	printf("\t\top#: %d\n", it->second->changer->operationCount );
    	}
    }




    // txn1 and txn2 commit
//    undoQuantum->release();
    EXPECT_EQ(0, this->table->getTupleJournals().size());
//    EXPECT_EQ(0, this->table->activeTupleCount());
    printf("\t\t%ld tuples, and %ld journals left.\n", (long)(this->table->activeTupleCount()),
    											(long)(this->table->getTupleJournals().size()));

	// print out tuples again, should have 1 tuple
    TableTuple tuple3(table->schema());
    iterator = this->table->iterator();
    while(iterator.hasNext()) {
       	ASSERT_EQ(true, iterator.next(tuple3));
    	EXPECT_EQ(true, tuple3.isActive());
    	printf("\t\tAfter commit: %s\n", tuple3.debugNoHeader().c_str());
    }

    i = 0;
    tjournals = this->table->getTupleJournals();
    for(it = tjournals.begin(); it != tjournals.end(); it++)
    {
		TupleJournal* tjournal = it->second;
    	printf("\t\tChanger %d type: %d\n", i, tjournal->changer->type);
    	printf("\t\tChanger %d txnid: %li\n", i, (long)tjournal->changer->txnid);
//    	printf("\t\tChanger %d column count: %d\n", i, tjournal.changer->columnCount);
		printf("\t\tChanger %d updated cols: {", i);
		int j = 0;
//		for(j = 0; j < tjournal.changer->columnCount; j++)
		for(j = 0; j < tjournal->schema->columnCount(); j++)
			printf("%s, ", tjournal->changer->updatedCols[j] ? "true" : "false");
		printf("}\n\n");
		i++;
    }


}



TEST_F(EscrowTableTest, Escrow2Updates) {
	printf("\n");

	ExecutorContext::getExecutorContext()->setUtilityExecution(true);

	// txn1: insert t1
	engine->setupForPlanFragments(undoQuantum, 101, 100);
	TableIterator itt = this->table->iterator();
	TableTuple ttt(this->table->schema());
	itt.next(ttt);
	printf("\t\tSource: %s\n", ttt.debugNoHeader().c_str());
	TableTuple &temp_tuple =this->table->tempTuple();
	(&temp_tuple)->setNValue(0, ValueFactory::getBigIntValue(static_cast<int8_t>(2)));
	(&temp_tuple)->setNValue(1, ValueFactory::getBigIntValue(static_cast<int8_t>(2)));
	printf("\t\tTarget: %s\n", temp_tuple.debugNoHeader().c_str());

	// test
    this->table->updateTuple(temp_tuple, ttt, false);

    EXPECT_EQ(2, this->table->activeTupleCount());
	EXPECT_EQ(2, this->table->getTupleJournals().size());
	EXPECT_EQ(1, this->table->getTupleJournalStorage().size());

    TupleJournal *tJournal = this->table->findTupleJournal((&ttt)->address());
    if(tJournal != NULL) {
    	EXPECT_EQ(101, (int)(tJournal->changer->txnid));
    	EXPECT_EQ(NonEscrow_UPDATE, tJournal->changer->type);
    	TableTuple aaa(tJournal->changer->beforeTuple, this->table->schema());
    	printf("\t\tbeforeTuple: %s\n", aaa.debugNoHeader().c_str());
    	EXPECT_NE(tJournal->changer->beforeTuple, ttt.address());
    } else
    	printf("\t\tno journal found.\n");


    int i = 0;
    std::map<char*, TupleJournal*>::iterator it;
    std::map<char*, TupleJournal*> tjournals = this->table->getTupleJournals();
    for(it = tjournals.begin(); it != tjournals.end(); it++)
    {
    	printf("\n\t\tChanger %d type: %d\n", i, it->second->changer->type);
    	printf("\t\tChanger %d txnid: %li\n", i, (long)it->second->changer->txnid);
		printf("\t\tChanger %d updated cols: {", i);
		int j = 0;
		for(j = 0; j < it->second->schema->columnCount(); j++)
			printf("%s, ", it->second->changer->updatedCols[j] ? "true" : "false");
		printf("}\n");
		printf("\t\tOp count: %d\n", it->second->changer->operationCount);
		if(it->second->changer->beforeTuple != NULL) {
	    	TableTuple aaa(tJournal->changer->beforeTuple, this->table->schema());
	    	printf("\t\tbeforeTuple: %s\n", aaa.debugNoHeader().c_str());
		}
		i++;
    }

    // txn2: update t1 again
    engine->setupForPlanFragments(undoQuantum, 102, 100);
    try{
//    	TableTuple ttt = this->table->lookupTuple(temp_tuple);
    	EXPECT_EQ(false, this->table->updateTuple(temp_tuple, ttt, false));
    } catch (DataConflictException &e) {
    	printf("\t\tException expected, txn2 update t1, data conflict error.\n");
    }
    
    printf("\n\t\tAfter t2 updates...\n");
    printf("\t\tNow, %lu journals\n", this->table->getTupleJournals().size());
    std::map<char*, TupleJournal*> tjournals2 = this->table->getTupleJournals();
    i = 0;
    for(it = tjournals2.begin(); it != tjournals2.end(); it++)
    {
    	printf("\n\t\tChanger %d type: %d\n", i, it->second->changer->type);
    	printf("\t\tChanger %d txnid: %lld\n", i, it->second->changer->txnid);
		printf("\t\tChanger %d updated cols: {", i);
		int j = 0;
		for(j = 0; j < it->second->schema->columnCount(); j++)
			printf("%s, ", it->second->changer->updatedCols[j] ? "true" : "false");
		printf("}\n");
		printf("\t\tOp count: %d\n", it->second->changer->operationCount);
		i++;
    }

    EXPECT_EQ(2, this->table->activeTupleCount());
/*
    // print out tuples again, should have 2 tuples
    TableTuple tuple2(table->schema());
    itt = this->table->iterator();
    while(itt.hasNext()) {
       	ASSERT_EQ(true, itt.next(tuple2));
    	EXPECT_EQ(true, tuple2.isActive());
    	printf("\t\tAfter Update: %s\n", tuple2.debugNoHeader().c_str());
    }
*/
    printf("\t\tReady to finish up...");

    printf("\n\t\t%lu undo actions\n", undoQuantum->size());

//    undoQuantum->release();

    for(std::vector<UndoAction*>::reverse_iterator i = undoQuantum->m_undoActions.rbegin();
            i != undoQuantum->m_undoActions.rend(); i++) {

    	PersistentTableUndoDeleteAction *dact = dynamic_cast<PersistentTableUndoDeleteAction*>(*i);
    	PersistentTableUndoInsertAction *iact = dynamic_cast<PersistentTableUndoInsertAction*>(*i);
    	PersistentTableUndoUpdateAction *uact = dynamic_cast<PersistentTableUndoUpdateAction*>(*i);
    	PersistentTableUndoEscrowUpdateAction *euact = dynamic_cast<PersistentTableUndoEscrowUpdateAction*>(*i);

    	printf("\t\t\tUndo Action Type: ");
    	if(dact != NULL) {
    		printf("DELETE\n");
    	}
    	else if (uact != NULL) {
    		printf("UPDATE\n");
    	}
    	else if (euact != NULL) {
    		printf("Escrow UPDATE\n");
    	}
    	else if(iact != NULL) {
    		printf("INSERT\n");

//    		if(this->table->findTupleJournal(iact->m_tuple.address()))
//    			printf("option1\n");
//    		else{
//    			TableTuple t = this->table->lookupTuple(iact->m_tuple);
//    			if(t.isNullTuple())
//    				printf("null tuple!\n");
//    			if(this->table->findTupleJournal(t.address())) {
//    				printf("option2\n");
//    				TupleJournal *tj = this->table->findTupleJournal(t.address());
//    				printf("op: %d\n", tj->changer->operationCount);
//    			}
//    		}
    	}

    	char* dataPtr = NULL;
    	std::map<char*, TupleJournal*> js = this->table->getTupleJournals();
    	std::map<char*, TupleJournal*>::iterator jst;
    	for(jst = js.begin(); jst != js.end(); jst++){
    		if(jst->second->changer->beforeTuple != NULL)
    			dataPtr = jst->second->changer->beforeTuple;
    	}

    	(*i)->release();
    	(*i)->~UndoAction();

    	if(this->table->findTupleJournal(dataPtr) != NULL)
    		printf("\t\tStill have journals!!!!!\n");
/*
    	if(dataPtr != NULL) {
			TableTuple eee(dataPtr, this->table->schema());
			if(!this->table->lookupTuple(eee).isNullTuple())
				printf("\t\tbeforeTuple still exist!!!!\n");
			this->table->deleteBeforeTuple(eee);
			if(!this->table->lookupTuple(eee).isNullTuple())
				printf("\t\tbeforeTuple deleted manually!!!!\n");
    	}
        std::map<char*, TupleJournal*> journals = this->table->getTupleJournals();
        if(journals.size() > 0){
			for(it = journals.begin(); it != journals.end(); it++) {
				printf("\t\t\tJournal type: %d", it->second->changer->type);
				printf("\t\top#: %d\n", it->second->changer->operationCount );
			}
        }
*/
    }

    // txn1 and txn2 commit
//    undoQuantum->release();
    EXPECT_EQ(0, this->table->getTupleJournals().size());
    EXPECT_EQ(1, this->table->activeTupleCount());
    EXPECT_EQ(0, this->table->getTupleJournalStorage().size());

    printf("\t\t%ld tuples, and %ld journals left.\n", (long)(this->table->activeTupleCount()),
    											(long)(this->table->getTupleJournals().size()));

	// print out tuples again, should have 1 tuple
    TableTuple tuple3(table->schema());
    itt = this->table->iterator();
    while(itt.hasNext()) {
       	ASSERT_EQ(true, itt.next(tuple3));
    	EXPECT_EQ(true, tuple3.isActive());
    	printf("\t\tAfter commit: %s\n", tuple3.debugNoHeader().c_str());
    }

    i = 0;
    tjournals = this->table->getTupleJournals();
    for(it = tjournals.begin(); it != tjournals.end(); it++)
    {
		TupleJournal* tjournal = it->second;
    	printf("\t\tChanger %d type: %d\n", i, tjournal->changer->type);
    	printf("\t\tChanger %d txnid: %li\n", i, (long)tjournal->changer->txnid);
		printf("\t\tChanger %d updated cols: {", i);
		int j = 0;
		for(j = 0; j < tjournal->schema->columnCount(); j++)
			printf("%s, ", tjournal->changer->updatedCols[j] ? "true" : "false");
		printf("}\n\n");
		i++;
    }


}
#endif

/*

TEST_F(EscrowTableTest, EscrowTupleInsert) {
    //
    // All of the values have already been inserted, we just
    // need to make sure that the data makes sense
    //
    TableIterator iterator = this->table->iterator();
    TableTuple tuple(table->schema());
    int insertedCount = 0;
    while (iterator.next(tuple)) {
        //printf("%s\n", tuple->debug(this->table).c_str());
        //
        // Make sure it is not deleted
        //
        EXPECT_EQ(true, tuple.isActive());

        // GWW
        EXPECT_EQ(4, table->findTupleJournal(&tuple)->changer->type);
        insertedCount++;
    }

    // GWW
    //ASSERT_EQ(insertedCount, table->getTupleJournals().size());

    //
    // Make sure that if we insert one tuple, we only get one tuple
    //
    TableTuple &temp_tuple = this->table->tempTuple();
    ASSERT_EQ(true, tableutil::setRandomTupleValues(this->table, &temp_tuple));
    this->table->deleteAllTuples(true);

    // GWW
    //ASSERT_EQ(insertedCount, table->getTupleJournals().size());

    ASSERT_EQ(0, this->table->activeTupleCount());
    ASSERT_EQ(true, this->table->insertTuple(temp_tuple));
    ASSERT_EQ(1, this->table->activeTupleCount());

    // GWW
    ASSERT_EQ(insertedCount + 1, table->getTupleJournals().size());

    //
    // Then check to make sure that it has the same value and type
    //
    iterator = this->table->iterator();
    ASSERT_EQ(true, iterator.next(tuple));
    for (int col_ctr = 0, col_cnt = NUM_OF_COLUMNS; col_ctr < col_cnt; col_ctr++) {
        EXPECT_EQ(COLUMN_TYPES[col_ctr], tuple.getType(col_ctr));
        EXPECT_TRUE(temp_tuple.getNValue(col_ctr).op_equals(tuple.getNValue(col_ctr)).isTrue());
    }

}
*/
/*
TEST_F(EscrowTableTest, TupleUpdate) {
    //
    // Loop through and randomly update values
    // We will keep track of multiple columns to make sure our updates
    // are properly applied to the tuples. We will test two things:
    //
    //      (1) Updating a tuple sets the values correctly
    //      (2) Updating a tuple without changing the values doesn't do anything
    //

    vector<int64_t> totals;
    vector<int64_t> totalsNotSlim;
    totals.reserve(NUM_OF_COLUMNS);
    totalsNotSlim.reserve(NUM_OF_COLUMNS);
    for (int col_ctr = 0; col_ctr < NUM_OF_COLUMNS; col_ctr++) {
        totals[col_ctr] = 0;
        totalsNotSlim[col_ctr] = 0;
    }

    TableIterator iterator = this->table->iterator();
    TableTuple tuple(table->schema());
    while (iterator.next(tuple)) {
        bool update = (rand() % 2 == 0);
        TableTuple &temp_tuple = table->tempTuple();
        for (int col_ctr = 0; col_ctr < NUM_OF_COLUMNS; col_ctr++) {
            //
            // Only check for numeric columns
            //
            if (isNumeric(COLUMN_TYPES[col_ctr])) {
                //
                // Update Column
                //
                if (update) {
                    NValue new_value = getRandomValue(COLUMN_TYPES[col_ctr]);
                    temp_tuple.setNValue(col_ctr, new_value);
                    totals[col_ctr] += ValuePeeker::peekAsBigInt(new_value);
                    totalsNotSlim[col_ctr] += ValuePeeker::peekAsBigInt(new_value);
                } else {
                    totals[col_ctr] += ValuePeeker::peekAsBigInt(tuple.getNValue(col_ctr));
                    totalsNotSlim[col_ctr] += ValuePeeker::peekAsBigInt(tuple.getNValue(col_ctr));
                }
            }
        }
        if (update) EXPECT_EQ(true, temp_table->updateTuple(temp_tuple, tuple, true));
    }

    //
    // Check to make sure our column totals are correct
    //
    for (int col_ctr = 0; col_ctr < NUM_OF_COLUMNS; col_ctr++) {
        if (isNumeric(COLUMN_TYPES[col_ctr])) {
            int64_t new_total = 0;
            iterator = this->table->iterator();
            while (iterator.next(tuple)) {
                new_total += ValuePeeker::peekAsBigInt(tuple.getNValue(col_ctr));
            }
            //printf("\nCOLUMN: %s\n\tEXPECTED: %d\n\tRETURNED: %d\n", this->table->getColumn(col_ctr)->getName().c_str(), totals[col_ctr], new_total);
            EXPECT_EQ(totals[col_ctr], new_total);
            EXPECT_EQ(totalsNotSlim[col_ctr], new_total);
        }
    }
}
*/

// I can't for the life of me make this pass using Valgrind.  I
// suspect that there's an extra reference to the ThreadLocalPool
// which isn't getting deleted, but I can't find it.  Leaving this
// here for now, feel free to fix or delete if you're offended.
// --izzy 7/8/2011
//
// TEST_F(EscrowTableTest, TempTableBoom)
// {
//     init(false);
//     bool threw = false;
//     try
//     {
//         while (true)
//         {
//             TableTuple &tuple = table->tempTuple();
//             if (!tableutil::setRandomTupleValues(table, &tuple)) {
//                 EXPECT_TRUE(false);
//             }
//             if (!table->insertTuple(tuple)) {
//                 EXPECT_TRUE(false);
//             }
//
//             /*
//              * The insert into the table (assuming a persistent table)
//              * will make a copy of the strings so the string
//              * allocations for unlined columns need to be freed here.
//              */
//             for (int ii = 0; ii < tuple.getSchema()->getUninlinedObjectColumnCount(); ii++) {
//                 tuple.getNValue(tuple.getSchema()->getUninlinedObjectColumnInfoIndex(ii)).free();
//             }
//         }
//     }
//     catch (SQLException& e)
//     {
//         TableTuple &tuple = table->tempTuple();
//         for (int ii = 0; ii < tuple.getSchema()->getUninlinedObjectColumnCount(); ii++) {
//             tuple.getNValue(tuple.getSchema()->getUninlinedObjectColumnInfoIndex(ii)).free();
//         }
//         EXPECT_GT(limits.getAllocated(), 1024 * 1024);
//         string state(e.getSqlState());
//         if (state == "V0002")
//         {
//             threw = true;
//         }
//     }
//     EXPECT_TRUE(threw);
// }

/* deleteTuple in TempTable is not supported for performance reason.
TEST_F(EscrowTableTest, TupleDelete) {
    //
    // We are just going to delete all of the odd tuples, then make
    // sure they don't exist anymore
    //
    TableIterator iterator = this->table->iterator();
    TableTuple tuple(table.get());
    while (iterator.next(tuple)) {
        if (tuple.get(1).getBigInt() != 0) {
            EXPECT_EQ(true, temp_table->deleteTuple(tuple));
        }
    }

    iterator = this->table->iterator();
    while (iterator.next(tuple)) {
        EXPECT_EQ(false, tuple.get(1).getBigInt() != 0);
    }
}
*/

/*TEST_F(EscrowTableTest, TupleUpdateXact) {
    this->init(true);
    //
    // Loop through and randomly update values
    // We will keep track of multiple columns to make sure our updates
    // are properly applied to the tuples. We will test two things:
    //
    //      (1) Updating a tuple sets the values correctly
    //      (2) Updating a tuple without changing the values doesn't do anything
    //
    TableTuple *tuple;
    TableTuple *temp_tuple;

    vector<int64_t> totals;
    totals.reserve(NUM_OF_COLUMNS);
    for (int col_ctr = 0; col_ctr < NUM_OF_COLUMNS; col_ctr++) {
        totals[col_ctr] = 0;
    }

    //
    // Interweave the transactions. Only keep the total
    //
    //int xact_ctr;
    int xact_cnt = 6;
    vector<boost::shared_ptr<UndoLog> > undos;
    for (int xact_ctr = 0; xact_ctr < xact_cnt; xact_ctr++) {
        TransactionId xact_id = xact_ctr;
        undos.push_back(boost::shared_ptr<UndoLog>(new UndoLog(xact_id)));
    }

    TableIterator iterator = this->table->iterator();
    while ((tuple = iterator.next()) != NULL) {
        //printf("BEFORE: %s\n", tuple->debug(this->table.get()).c_str());
        int xact_ctr = (rand() % xact_cnt);
        bool update = (rand() % 3 != 0);
        //printf("xact_ctr:%d\n", xact_ctr);
        //if (update) printf("update!\n");
        temp_tuple = table->tempTuple(tuple);
        for (int col_ctr = 0; col_ctr < NUM_OF_COLUMNS; col_ctr++) {
            //
            // Only check for numeric columns
            //
            if (valueutil::isNumeric(COLUMN_TYPES[col_ctr])) {
                //
                // Update Column
                //
                if (update) {
                    Value new_value = valueutil::getRandomValue(COLUMN_TYPES[col_ctr]);
                    temp_tuple->set(col_ctr, new_value);

                    //
                    // We make a distinction between the updates that we will
                    // commit and those that we will rollback
                    //
                    totals[col_ctr] += xact_ctr % 2 == 0 ? new_value.castAsBigInt() : tuple->get(col_ctr).castAsBigInt();
                } else {
                    totals[col_ctr] += tuple->get(col_ctr).castAsBigInt();
                }
            }
        }
        if (update) {
            //printf("BEFORE?: %s\n", tuple->debug(this->table.get()).c_str());
            //persistent_table->setUndoLog(undos[xact_ctr]);
            EXPECT_EQ(true, persistent_table->updateTuple(temp_tuple, tuple, true));
            //printf("UNDO: %s\n", undos[xact_ctr]->debug().c_str());
        }
        //printf("AFTER: %s\n", temp_tuple->debug(this->table.get()).c_str());
    }

    for (xact_ctr = 0; xact_ctr < xact_cnt; xact_ctr++) {
        if (xact_ctr % 2 == 0) {
            undos[xact_ctr]->commit();
        } else {
            undos[xact_ctr]->rollback();
        }
    }

    //iterator = this->table->iterator();
    //while ((tuple = iterator.next()) != NULL) {
    //    printf("TUPLE: %s\n", tuple->debug(this->table.get()).c_str());
    //}

    //
    // Check to make sure our column totals are correct
    //
    for (int col_ctr = 0; col_ctr < NUM_OF_COLUMNS; col_ctr++) {
        if (valueutil::isNumeric(COLUMN_TYPES[col_ctr])) {
            int64_t new_total = 0;
            iterator = this->table->iterator();
            while ((tuple = iterator.next()) != NULL) {
                //fprintf(stderr, "TUPLE: %s\n", tuple->debug(this->table).c_str());
                new_total += tuple->get(col_ctr).castAsBigInt();
            }
            //printf("\nCOLUMN: %s\n\tEXPECTED: %d\n\tRETURNED: %d\n", this->table->getColumn(col_ctr)->getName().c_str(), totals[col_ctr], new_total);
            EXPECT_EQ(totals[col_ctr], new_total);
        }
    }
}*/

/*TEST_F(EscrowTableTest, TupleDeleteXact) {
    this->init(true);
    //
    // Interweave the transactions. Only keep the total
    //
    //int xact_ctr;
    int xact_cnt = 6;

    //
    // Loop through the tuples and delete half of them in interleaving transactions
    //
    TableIterator iterator = this->table->iterator();
    TableTuple *tuple;
    int64_t total = 0;
    while ((tuple = iterator.next()) != NULL) {
        int xact_ctr = (rand() % xact_cnt);
        //
        // Keep it and store the value before deleting
        // NOTE: Since we are testing whether the deletes work, we only
        //       want to store the values for the tuples where the delete is
        //       going to get rolled back!
        //
        if (xact_ctr % 2 != 0) total += 1;//tuple->get(0).castAsBigInt();
        VOLT_DEBUG("total: %d", (int)total);
        //persistent_table->setUndoLog(undos[xact_ctr]);
        EXPECT_EQ(true, persistent_table->deleteTuple(tuple));
    }

    //
    // Now make sure all of the values add up to our total
    //
    int64_t new_total = 0;
    iterator = this->table->iterator();
    while ((tuple = iterator.next()) != NULL) {
        EXPECT_EQ(true, tuple->isActive());
        new_total += 1;//tuple->get(0).getBigInt();
        VOLT_DEBUG("total2: %d", (int)total);
    }

    //printf("TOTAL = %d\tNEW_TOTAL = %d\n", total, new_total);
    EXPECT_EQ(total, new_total);
}*/

int main() {
    return TestSuite::globalInstance()->runAll();
}
