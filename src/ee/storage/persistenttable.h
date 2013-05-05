/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
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

#ifndef HSTOREPERSISTENTTABLE_H
#define HSTOREPERSISTENTTABLE_H

#include <string>
#include <vector>
#include <cassert>
#include "boost/shared_ptr.hpp"
#include "boost/scoped_ptr.hpp"

// GWW
#include "boost/unordered_map.hpp"

#include "common/ids.h"
#include "common/valuevector.h"
#include "common/tabletuple.h"
#include "storage/table.h"
#include "storage/TupleStreamWrapper.h"
#include "storage/TableStats.h"
#include "storage/PersistentTableStats.h"
#include "storage/CopyOnWriteContext.h"
#include "storage/RecoveryContext.h"
#include "common/UndoQuantumReleaseInterest.h"
#include "common/ThreadLocalPool.h"


#include "expressions/abstractexpression.h"

namespace voltdb {

class TableColumn;
class TableIndex;
class TableIterator;
class TableFactory;
class TupleSerializer;
class SerializeInput;
class Topend;
class ReferenceSerializeOutput;
class ExecutorContext;
class MaterializedViewMetadata;
class RecoveryProtoMsg;
class PersistentTableUndoDeleteAction;

/**
 * Represents a non-temporary table which permanently resides in
 * storage and also registered to Catalog (see other documents for
 * details of Catalog). PersistentTable has several additional
 * features to Table.  It has indexes, constraints to check NULL and
 * uniqueness as well as undo logs to revert changes.
 *
 * PersistentTable can have one or more Indexes, one of which must be
 * Primary Key Index. Primary Key Index is same as other Indexes except
 * that it's used for deletion and updates. Our Execution Engine collects
 * Primary Key values of deleted/updated tuples and uses it for specifying
 * tuples, assuming every PersistentTable has a Primary Key index.
 *
 * Currently, constraints are not-null constraint and unique
 * constraint.  Not-null constraint is just a flag of TableColumn and
 * checked against insertion and update. Unique constraint is also
 * just a flag of TableIndex and checked against insertion and
 * update. There's no rule constraint or foreign key constraint so far
 * because our focus is performance and simplicity.
 *
 * To revert changes after execution, PersistentTable holds UndoLog.
 * PersistentTable does eager update which immediately changes the
 * value in data and adds an entry to UndoLog. We chose eager update
 * policy because we expect reverting rarely occurs.
 */

// GWW
enum NonEscrowType {
	NonEscrow_INSERT = 1,			// INSERT or INSERT + UPDATE*
	NonEscrow_DELETE = 2,			// DELETE or INSERT/UPDATE + DELETE
	NonEscrow_UPDATE = 3,			// UPDATE only
};

#define MAXCOL	100

struct NonEscrowChanger {
	int64_t txnid;
	int	type;	// INSERT or DELETE or non-Escrow Update
	char* beforeTuple;
	int operationCount;		// to track how many non-escrow operations have been done on this tuple
	bool updatedCols[MAXCOL];
};

struct EscrowJournal {
	EscrowJournal()
	{
		txnid = 0;
		delta.setNull();
		jNext = NULL;
		undoActionIdx = -1;
	}
	int64_t		txnid;
	NValue		delta;
	EscrowJournal	*jNext;
	int undoActionIdx;	// belongs to which undo action
};

struct EscrowData {
	EscrowData(int colId): columnIdx(colId)
	{
		inf.setNull();
		sup.setNull();
		jHead = NULL;
	}
	EscrowData()
	{
		columnIdx = -1;
		inf.setNull();
		sup.setNull();
		jHead = NULL;
	}

	int		columnIdx;		// must be Escrow Column
	NValue	inf;
	NValue	sup;
	EscrowJournal	*jHead;	// head of escrow journal list
};

struct TupleJournal{

	TupleJournal(char* dataPtr, TupleSchema *sch, NonEscrowChanger *nonEscrowChanger)
	: currentTuple(dataPtr), schema(sch), changer(nonEscrowChanger),
	  currentPreparingEscrowTxnId(-1), undoActionCountForCurrentPreparingEscrowTxn(-1){}

	TupleJournal(char* dataPtr, TupleSchema *sch)
	: currentTuple(dataPtr), schema(sch), changer(NULL),
	  currentPreparingEscrowTxnId(-1), undoActionCountForCurrentPreparingEscrowTxn(-1){}

	char* currentTuple;
	TupleSchema *schema;
	NonEscrowChanger* changer;	// non-escrow op changing the row
	std::map<int, EscrowData> escrowData;	// EscrowData per Escrow column
	// for correct release/undo handling for escrow update
	// keep tracking the latest escrow txn on this tuple
	int64_t	currentPreparingEscrowTxnId;
	int undoActionCountForCurrentPreparingEscrowTxn;
};

class PersistentTable : public Table, public UndoQuantumReleaseInterest {
    friend class CopyOnWriteContext;
    friend class CopyOnWriteIterator;
    friend class TableFactory;
    friend class TableTuple;
    friend class TableIndex;
    friend class TableIterator;
    friend class PersistentTableStats;
    friend class PersistentTableUndoDeleteAction;
    friend class ::CopyOnWriteTest_CopyOnWriteIterator;
    friend class ::CompactionTest_BasicCompaction;
    friend class ::CompactionTest_CompactionWithCopyOnWrite;
  private:
    // no default ctor, no copy, no assignment
    PersistentTable();
    PersistentTable(PersistentTable const&);
    PersistentTable operator=(PersistentTable const&);

    // default iterator
    TableIterator m_iter;

  public:
    virtual ~PersistentTable();

    void notifyQuantumRelease() {
        if (compactionPredicate()) {
            doForcedCompaction();
        }
    }

    // Return a table iterator by reference
    TableIterator& iterator() {
        m_iter.reset(m_data.begin());
        return m_iter;
    }

    TableIterator* makeIterator() {
        return new TableIterator(this, m_data.begin());
    }

    // ------------------------------------------------------------------
    // OPERATIONS
    // ------------------------------------------------------------------
    void deleteAllTuples(bool freeAllocatedStrings);
    bool insertTuple(TableTuple &source);

    /*
     * Inserts a Tuple without performing an allocation for the
     * uninlined strings.
     */
    void insertTupleForUndo(char *tuple);

    // GWW: insert before-image for non-escrow update
    bool insertBeforeTuple(TableTuple &beforeTupleSource);
    // GWW: delete before-image for non-escrow update
    void deleteBeforeTuple(TableTuple &beforeTuple, TupleJournal *journal);

    /*
     * Note that inside update tuple the order of sourceTuple and
     * targetTuple is swapped when making calls on the indexes. This
     * is just an inconsistency in the argument ordering.
     */
    bool updateTuple(TableTuple &sourceTuple, TableTuple &targetTuple,
                     bool updatesIndexes);

    //GWW
    bool escrowUpdateTuple(TableTuple &sourceTuple, TableTuple &target,
    				bool updatedCols[MAXCOL]);
    bool updateTuple(TableTuple &sourceTuple, TableTuple &target,
    				bool updatedCols[MAXCOL], bool updatesIndexes);

    /*
     * Identical to regular updateTuple except no memory management
     * for unlined columns is performed because that will be handled
     * by the UndoAction.
     */
    void updateTupleForUndo(TableTuple &sourceTuple, TableTuple &targetTuple,
                            bool revertIndexes, TupleJournal *tjournal);

    /*
     * Delete a tuple by looking it up via table scan or a primary key
     * index lookup.
     */
    bool deleteTuple(TableTuple &tuple, bool freeAllocatedStrings);
    void deleteTupleForUndo(voltdb::TableTuple &tupleCopy);

    /*
     * Lookup the address of the tuple that is identical to the specified tuple.
     * Does a primary key lookup or table scan if necessary.
     */
    voltdb::TableTuple lookupTuple(TableTuple tuple);

    // GWW: not in use anymore
//    void finishNonEscrowUpdateTuple(TableTuple &newTuple);
//    void finishNonEscrowUpdateTuple(TupleJournal *tJournal);

    void finishEscrowUpdate(TupleJournal *tjournal, bool committed);



	enum Status_Per_Expression {
		BLOCK = 1,
		IGNORE = 2,
		REPORT = 3,
	};
    int getDispositionPerColumn(char *dataPtr, TupleJournal *journal, AbstractExpression *expr, int colIndex);
    int64_t getBlockerTxnIdForBlockedRead(TupleJournal *journal);
//    void cleanTupleJournal(char* dataPtr);
    void cleanTupleJournal(TupleJournal* journalPtr);
    void testEscrowTupleJournal();
    void printJournalInfo(std::string header);
    void printTupleJournal(std::string header, TupleJournal *tj);



    // ------------------------------------------------------------------
    // INDEXES
    // ------------------------------------------------------------------
    virtual int indexCount() const { return m_indexCount; }
    virtual int uniqueIndexCount() const { return m_uniqueIndexCount; }
    virtual std::vector<TableIndex*> allIndexes() const;
    virtual TableIndex *index(std::string name);
    virtual TableIndex *primaryKeyIndex() { return m_pkeyIndex; }
    virtual const TableIndex *primaryKeyIndex() const { return m_pkeyIndex; }

    // ------------------------------------------------------------------
    // UTILITY
    // ------------------------------------------------------------------
    std::string tableType() const;
    virtual std::string debug();

    int partitionColumn() { return m_partitionColumn; }
    /** inlined here because it can't be inlined in base Table, as it
     *  uses Tuple.copy.
     */
    TableTuple& getTempTupleInlined(TableTuple &source);

    /** Add a view to this table */
    void addMaterializedView(MaterializedViewMetadata *view);

    /**
     * Switch the table to copy on write mode. Returns true if the table was already in copy on write mode.
     */
    bool activateCopyOnWrite(TupleSerializer *serializer, int32_t partitionId);

    /**
     * Create a recovery stream for this table. Returns true if the table already has an active recovery stream
     */
    bool activateRecoveryStream(int32_t tableId);

    /**
     * Serialize the next message in the stream of recovery messages. Returns true if there are
     * more messages and false otherwise.
     */
    void nextRecoveryMessage(ReferenceSerializeOutput *out);

    /**
     * Process the updates from a recovery message
     */
    void processRecoveryMessage(RecoveryProtoMsg* message, Pool *pool);

    /**
     * Attempt to serialize more tuples from the table to the provided
     * output stream.  Returns true if there are more tuples and false
     * if there are no more tuples waiting to be serialized.
     */
    bool serializeMore(ReferenceSerializeOutput *out);

    /**
     * Create a tree index on the primary key and then iterate it and hash
     * the tuple data.
     */
    size_t hashCode();

    size_t getBlocksNotPendingSnapshotCount() {
        return m_blocksNotPendingSnapshot.size();
    }

    void doIdleCompaction();
    void printBucketInfo();

    void increaseStringMemCount(size_t bytes)
    {
        m_nonInlinedMemorySize += bytes;
    }
    void decreaseStringMemCount(size_t bytes)
    {
        m_nonInlinedMemorySize -= bytes;
    }

    // GWW
    EscrowData *findEscrowData(TupleJournal *tjournal, int colIdx) {
    	if(tjournal->escrowData.find(colIdx) != tjournal->escrowData.end())
    		return &tjournal->escrowData.find(colIdx)->second;
    	else
    		return NULL;
    }

    EscrowData *insertEscrowData(TupleJournal *tJournal, int colIdx) {
    	EscrowData eData = EscrowData(colIdx);
    	tJournal->escrowData.insert( std::pair<int, EscrowData> (colIdx, eData));
    	return &tJournal->escrowData.find(colIdx)->second;
    }

    void insertEscrowJournal(TupleJournal *tjournal, int colIdx, EscrowJournal *eJournal) {
    	EscrowData *eData = dynamic_cast<EscrowData*>(&tjournal->escrowData.find(colIdx)->second);
    	if(eData->jHead == NULL)
    		eData->jHead = eJournal;
    	else {
    		eJournal->jNext = eData->jHead;
    		eData->jHead = eJournal;
    	}
    }

    void deleteEscrowData(char *dataPtr, int colIdx) {
    	if(dataPtr != NULL && findTupleJournal(dataPtr) != NULL)
    		m_tupleJournals.find(dataPtr)->second->escrowData.erase(colIdx);
    }

    void deleteEscrowData(TupleJournal *tjournal, int colIdx) {
    	assert(tjournal != NULL);
    	assert(tjournal->escrowData.find(colIdx) != tjournal->escrowData.end());
   		tjournal->escrowData.erase(colIdx);
    }

    boost::unordered_map<char*, TupleJournal*> getTupleJournals() {
    	return m_tupleJournals;
    }

    boost::unordered_map<char*, TupleJournal> getTupleJournalStorage() {
    	return m_tupleJournalStorage;
    }

    TupleJournal* insertTupleJournalStorage(char* dataPtr, TupleSchema *schema, NonEscrowChanger *changer) {
    	TupleJournal tjournal(dataPtr, schema, changer);
    	return &(m_tupleJournalStorage.insert(std::pair<char*, TupleJournal>(dataPtr, tjournal)).first->second);
    }

    TupleJournal* insertTupleJournalStorage(char* dataPtr, TupleSchema *schema) {
    	TupleJournal tjournal(dataPtr, schema);
    	return &(m_tupleJournalStorage.insert(std::pair<char*, TupleJournal>(dataPtr, tjournal)).first->second);
    }

    void insertTupleJournalMapping(char* dataPtr, TupleJournal* journal ) {
    	m_tupleJournals.insert( std::pair<char*, TupleJournal*> (dataPtr, journal) );
    }
    
    TupleJournal *findTupleJournal(char* dataPtr) {
    	if(m_tupleJournals.find(dataPtr) != m_tupleJournals.end())
    		return m_tupleJournals.find(dataPtr)->second;
    	else
    		return NULL;
    }

    void deleteTupleJournalStorage(char* dataPtr) {
    	m_tupleJournalStorage.erase(dataPtr);
    }

    void deleteTupleJournalMapping(char* dataPtr) {
    	if(dataPtr != NULL) {
   			m_tupleJournals.erase(dataPtr);
    	}
    }



protected:

    size_t allocatedBlockCount() const {
        return m_data.size();
    }

    void snapshotFinishedScanningBlock(TBPtr finishedBlock, TBPtr nextBlock) {
        if (nextBlock != NULL) {
            assert(m_blocksPendingSnapshot.find(nextBlock) != m_blocksPendingSnapshot.end());
            m_blocksPendingSnapshot.erase(nextBlock);
            nextBlock->swapToBucket(TBBucketPtr());
        }
        if (finishedBlock != NULL && !finishedBlock->isEmpty()) {
            m_blocksNotPendingSnapshot.insert(finishedBlock);
            int bucketIndex = finishedBlock->calculateBucketIndex();
            if (bucketIndex != -1) {
                finishedBlock->swapToBucket(m_blocksNotPendingSnapshotLoad[bucketIndex]);
            }
        }
    }

    void nextFreeTuple(TableTuple *tuple);
    bool doCompactionWithinSubset(TBBucketMap *bucketMap);
    void doForcedCompaction();

    // ------------------------------------------------------------------
    // FROM PIMPL
    // ------------------------------------------------------------------
    void insertIntoAllIndexes(TableTuple *tuple);
    void deleteFromAllIndexes(TableTuple *tuple);
    void updateFromAllIndexes(TableTuple &targetTuple, const TableTuple &sourceTuple);
    void updateWithSameKeyFromAllIndexes(TableTuple &targetTuple, const TableTuple &sourceTuple);

    bool tryInsertOnAllIndexes(TableTuple *tuple);
    bool tryUpdateOnAllIndexes(TableTuple &targetTuple, const TableTuple &sourceTuple);
    // GWW
    bool tryInsertOnNonUniqueIndexes(TableTuple *tuple);
    void deleteFromAllNonUniqueIndexes(TableTuple *tuple);
    void checkForIndexEntryPhantom(const TableTuple *targetTuple, TableIndex *index);

    bool checkNulls(TableTuple &tuple) const;

    PersistentTable(ExecutorContext *ctx, bool exportEnabled);
    void onSetColumns();

    void notifyBlockWasCompactedAway(TBPtr block);
    void swapTuples(TableTuple sourceTuple, TableTuple destinationTuple);

    /**
     * Normally this will return the tuple storage to the free list.
     * In the memcheck build it will return the storage to the heap.
     */
    void deleteTupleStorage(TableTuple &tuple, TBPtr block = TBPtr(NULL));

    // helper for deleteTupleStorage
    TBPtr findBlock(char *tuple);

    /*
     * Implemented by persistent table and called by Table::loadTuplesFrom
     * to do additional processing for views and Export
     */
    virtual void processLoadedTuple(TableTuple &tuple);

    TBPtr allocateNextBlock();

    // pointer to current transaction id and other "global" state.
    // abstract this out of VoltDBEngine to avoid creating dependendencies
    // between the engine and the storage layers - which complicate test.
    ExecutorContext *m_executorContext;

    // CONSTRAINTS
    TableIndex** m_uniqueIndexes;
    int m_uniqueIndexCount;
    bool* m_allowNulls;

    // INDEXES
    TableIndex** m_indexes;
    int m_indexCount;
    TableIndex *m_pkeyIndex;

    // partition key
    int m_partitionColumn;

    // list of materialized views that are sourced from this table
    std::vector<MaterializedViewMetadata *> m_views;

    // STATS
    voltdb::PersistentTableStats stats_;
    voltdb::TableStats* getTableStats();

    // is Export enabled
    bool m_exportEnabled;

    // Snapshot stuff
    boost::scoped_ptr<CopyOnWriteContext> m_COWContext;

    //Recovery stuff
    boost::scoped_ptr<RecoveryContext> m_recoveryContext;



    // STORAGE TRACKING

    // Map from load to the blocks with level of load
    TBBucketMap m_blocksNotPendingSnapshotLoad;
    TBBucketMap m_blocksPendingSnapshotLoad;

    // Map containing blocks that aren't pending snapshot
    boost::unordered_set<TBPtr> m_blocksNotPendingSnapshot;

    // Map containing blocks that are pending snapshot
    boost::unordered_set<TBPtr> m_blocksPendingSnapshot;

    // Set of blocks with non-empty free lists or available tuples
    // that have never been allocated
    stx::btree_set<TBPtr > m_blocksWithSpace;

    // GWW
//    std::map<char*, TupleJournal*> m_tupleJournals;
//    std::map<char*, TupleJournal> m_tupleJournalStorage;
    boost::unordered_map<char*, TupleJournal*> m_tupleJournals;
    boost::unordered_map<char*, TupleJournal> m_tupleJournalStorage;

  private:
    // pointers to chunks of data. Specific to table impl. Don't leak this type.
    TBMap m_data;
    int m_failedCompactionCount;
};

inline TableTuple& PersistentTable::getTempTupleInlined(TableTuple &source) {
    assert (m_tempTuple.m_data);
    m_tempTuple.copy(source);
    return m_tempTuple;
}


inline void PersistentTable::deleteTupleStorage(TableTuple &tuple, TBPtr block) {
    tuple.setActiveFalse(); // does NOT free strings

    // add to the free list
    m_tupleCount--;
    //m_tuplesPendingDelete--;

    if (block.get() == NULL) {
       block = findBlock(tuple.address());
    }

    bool transitioningToBlockWithSpace = !block->hasFreeTuples();

    int retval = block->freeTuple(tuple.address());
    if (retval != -1) {
        //Check if if the block is currently pending snapshot
        if (m_blocksNotPendingSnapshot.find(block) != m_blocksNotPendingSnapshot.end()) {
            //std::cout << "Swapping block " << static_cast<void*>(block.get()) << " to bucket " << retval << std::endl;
            block->swapToBucket(m_blocksNotPendingSnapshotLoad[retval]);
        //Check if the block goes into the pending snapshot set of buckets
        } else if (m_blocksPendingSnapshot.find(block) != m_blocksPendingSnapshot.end()) {
            block->swapToBucket(m_blocksPendingSnapshotLoad[retval]);
        } else {
            //In this case the block is actively being snapshotted and isn't eligible for merge operations at all
            //do nothing, once the block is finished by the iterator, the iterator will return it
        }
    }

    if (block->isEmpty()) {
        m_data.erase(block->address());
        m_blocksWithSpace.erase(block);
        m_blocksNotPendingSnapshot.erase(block);
        assert(m_blocksPendingSnapshot.find(block) == m_blocksPendingSnapshot.end());
        //Eliminates circular reference
        block->swapToBucket(TBBucketPtr());
    } else if (transitioningToBlockWithSpace) {
        m_blocksWithSpace.insert(block);
    }
}

inline TBPtr PersistentTable::findBlock(char *tuple) {
    TBMapI i = m_data.lower_bound(tuple);
    if (i == m_data.end() && m_data.empty()) {
        throwFatalException("Tried to find a tuple block for a tuple but couldn't find one");
    }
    if (i == m_data.end()) {
        i--;
        if (i.key() + m_tableAllocationSize < tuple) {
            throwFatalException("Tried to find a tuple block for a tuple but couldn't find one");
        }
    } else {
        if (i.key() != tuple) {
            i--;
            if (i.key() + m_tableAllocationSize < tuple) {
                throwFatalException("Tried to find a tuple block for a tuple but couldn't find one");
            }
        }
    }
    return i.data();
}

inline TBPtr PersistentTable::allocateNextBlock() {
    TBPtr block(new (ThreadLocalPool::getExact(sizeof(TupleBlock))->malloc()) TupleBlock(this, m_blocksNotPendingSnapshotLoad[0]));
    m_data.insert( block->address(), block);
    m_blocksNotPendingSnapshot.insert(block);
    return block;
}


}



#endif
