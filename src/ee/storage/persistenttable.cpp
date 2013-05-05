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

#include <sstream>
#include <cassert>
#include <cstdio>

#include "boost/scoped_ptr.hpp"
#include "storage/persistenttable.h"

#include "common/debuglog.h"
#include "common/serializeio.h"
#include "common/FailureInjection.h"
#include "common/tabletuple.h"
#include "common/UndoQuantum.h"
#include "common/executorcontext.hpp"
#include "common/FatalException.hpp"
#include "common/types.h"
#include "common/RecoveryProtoMessage.h"
#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/TupleStreamWrapper.h"
#include "storage/TableStats.h"
#include "storage/PersistentTableStats.h"
#include "storage/PersistentTableUndoInsertAction.h"
#include "storage/PersistentTableUndoDeleteAction.h"
#include "storage/PersistentTableUndoUpdateAction.h"
#include "storage/PersistentTableUndoEscrowUpdateAction.h"
#include "storage/ConstraintFailureException.h"
#include "storage/MaterializedViewMetadata.h"
#include "storage/CopyOnWriteContext.h"
#include "storage/tableiterator.h"
#include "storage/DataConflictException.h"
#include "storage/EscrowOutOfBoundException.h"

using namespace voltdb;

void* keyTupleStorage = NULL;
TableTuple keyTuple;

#define TABLE_BLOCKSIZE 2097152

PersistentTable::PersistentTable(ExecutorContext *ctx, bool exportEnabled) :
    Table(TABLE_BLOCKSIZE),
    m_iter(this, m_data.begin()),
    m_executorContext(ctx),
    m_uniqueIndexes(NULL), m_uniqueIndexCount(0), m_allowNulls(NULL),
    m_indexes(NULL), m_indexCount(0), m_pkeyIndex(NULL),
    stats_(this),
    m_COWContext(NULL),
    m_failedCompactionCount(0)
{
    for (int ii = 0; ii < TUPLE_BLOCK_NUM_BUCKETS; ii++) {
        m_blocksNotPendingSnapshotLoad.push_back(TBBucketPtr(new TBBucket()));
        m_blocksPendingSnapshotLoad.push_back(TBBucketPtr(new TBBucket()));
    }
}

PersistentTable::~PersistentTable() {

    for (int ii = 0; ii < TUPLE_BLOCK_NUM_BUCKETS; ii++) {
        m_blocksNotPendingSnapshotLoad[ii]->clear();
        m_blocksPendingSnapshotLoad[ii]->clear();
    }

    // delete all tuples to free strings
    TableIterator ti(this, m_data.begin());
    TableTuple tuple(m_schema);

    while (ti.next(tuple)) {
        // indexes aren't released as they don't have ownership of strings
        tuple.freeObjectColumns();
        tuple.setActiveFalse();
    }

    for (int i = 0; i < m_indexCount; ++i) {
        TableIndex *index = m_indexes[i];
        if (index != m_pkeyIndex) {
            delete index;
        }
    }
    if (m_pkeyIndex) delete m_pkeyIndex;
    if (m_uniqueIndexes) delete[] m_uniqueIndexes;
    if (m_allowNulls) delete[] m_allowNulls;
    if (m_indexes) delete[] m_indexes;

    // note this class has ownership of the views, even if they
    // were allocated by VoltDBEngine
    for (int i = 0; i < m_views.size(); i++) {
        delete m_views[i];
    }

}

// ------------------------------------------------------------------
// OPERATIONS
// ------------------------------------------------------------------
void PersistentTable::nextFreeTuple(TableTuple *tuple) {
    // First check whether we have any in our list
    // In the memcheck it uses the heap instead of a free list to help Valgrind.
    if (!m_blocksWithSpace.empty()) {
        VOLT_TRACE("GRABBED FREE TUPLE!\n");
        stx::btree_set<TBPtr >::iterator begin = m_blocksWithSpace.begin();
        TBPtr block = (*begin);
        std::pair<char*, int> retval = block->nextFreeTuple();

        /**
         * Check to see if the block needs to move to a new bucket
         */
        if (retval.second != -1) {
            //Check if if the block is currently pending snapshot
            if (m_blocksNotPendingSnapshot.find(block) != m_blocksNotPendingSnapshot.end()) {
                block->swapToBucket(m_blocksNotPendingSnapshotLoad[retval.second]);
            //Check if the block goes into the pending snapshot set of buckets
            } else if (m_blocksPendingSnapshot.find(block) != m_blocksPendingSnapshot.end()) {
                block->swapToBucket(m_blocksPendingSnapshotLoad[retval.second]);
            } else {
                //In this case the block is actively being snapshotted and isn't eligible for merge operations at all
                //do nothing, once the block is finished by the iterator, the iterator will return it
            }
        }

        tuple->move(retval.first);
        if (!block->hasFreeTuples()) {
            m_blocksWithSpace.erase(block);
        }
        assert (m_columnCount == tuple->sizeInValues());
        return;
    }

    // if there are no tuples free, we need to grab another chunk of memory
    // Allocate a new set of tuples
    TBPtr block = allocateNextBlock();

    // get free tuple
    assert (m_columnCount == tuple->sizeInValues());

    std::pair<char*, int> retval = block->nextFreeTuple();

    /**
     * Check to see if the block needs to move to a new bucket
     */
    if (retval.second != -1) {
        //Check if the block goes into the pending snapshot set of buckets
        if (m_blocksPendingSnapshot.find(block) != m_blocksPendingSnapshot.end()) {
            //std::cout << "Swapping block to nonsnapshot bucket " << static_cast<void*>(block.get()) << " to bucket " << retval.second << std::endl;
            block->swapToBucket(m_blocksPendingSnapshotLoad[retval.second]);
        //Now check if it goes in with the others
        } else if (m_blocksNotPendingSnapshot.find(block) != m_blocksNotPendingSnapshot.end()) {
            //std::cout << "Swapping block to snapshot bucket " << static_cast<void*>(block.get()) << " to bucket " << retval.second << std::endl;
            block->swapToBucket(m_blocksNotPendingSnapshotLoad[retval.second]);
        } else {
            //In this case the block is actively being snapshotted and isn't eligible for merge operations at all
            //do nothing, once the block is finished by the iterator, the iterator will return it
        }
    }

    tuple->move(retval.first);
    //cout << "table::nextFreeTuple(" << reinterpret_cast<const void *>(this) << ") m_usedTuples == " << m_usedTuples << endl;

    if (block->hasFreeTuples()) {
        m_blocksWithSpace.insert(block);
    }
}

void PersistentTable::deleteAllTuples(bool freeAllocatedStrings) {
    // nothing interesting
    TableIterator ti(this, m_data.begin());
    TableTuple tuple(m_schema);
    while (ti.next(tuple)) {
        deleteTuple(tuple, true);
    }
}

void setSearchKeyFromTuple(TableTuple &source) {
    keyTuple.setNValue(0, source.getNValue(1));
    keyTuple.setNValue(1, source.getNValue(2));
}

/*
 * Regular tuple insertion that does an allocation and copy for
 * uninlined strings and creates and registers an UndoAction.
 */
bool PersistentTable::insertTuple(TableTuple &source) {

    // not null checks at first
    FAIL_IF(!checkNulls(source)) {
        throw ConstraintFailureException(this, source, TableTuple(),
                                         CONSTRAINT_TYPE_NOT_NULL);
    }

    //
    // First get the next free tuple
    // This will either give us one from the free slot list, or
    // grab a tuple at the end of our chunk of memory
    //
    nextFreeTuple(&m_tmpTarget1);
    m_tupleCount++;
    m_usedTupleCount++;

    //
    // Then copy the source into the target
    //
    m_tmpTarget1.copyForPersistentInsert(source); // tuple in freelist must be already cleared
    m_tmpTarget1.setActiveTrue();
    m_tmpTarget1.setPendingDeleteFalse();
    m_tmpTarget1.setPendingDeleteOnUndoReleaseFalse();

    /**
     * Inserts never "dirty" a tuple since the tuple is new, but...  The
     * COWIterator may still be scanning and if the tuple came from the free
     * list then it may need to be marked as dirty so it will be skipped. If COW
     * is on have it decide. COW should always set the dirty to false unless the
     * tuple is in a to be scanned area.
     */
    if (m_COWContext.get() != NULL) {
        m_COWContext->markTupleDirty(m_tmpTarget1, true);
    } else {
        m_tmpTarget1.setDirtyFalse();
    }
    m_tmpTarget1.isDirty();

    if (!tryInsertOnAllIndexes(&m_tmpTarget1)) {
        // Careful to delete allocated objects
        m_tmpTarget1.freeObjectColumns();
        deleteTupleStorage(m_tmpTarget1);
        // We do not allow a DELETE followed by an INSERT by the same txn
        // this is handled later 
        // TODO: for DELETE then INSERT operation operated by different txn's
        // the second txn should wait, not a ConstraintFailureException
        throw ConstraintFailureException(this, source, TableTuple(),
                                         CONSTRAINT_TYPE_UNIQUE);
    }

    if (m_schema->getUninlinedObjectColumnCount() != 0)
    {
        increaseStringMemCount(m_tmpTarget1.getNonInlinedMemorySize());
    }
    /*
     * Create and register an undo action.
     */
    UndoQuantum *undoQuantum = m_executorContext->getCurrentUndoQuantum();
    assert(undoQuantum);
    Pool *pool = undoQuantum->getDataPool();
    assert(pool);
    PersistentTableUndoInsertAction *ptuia =
      new (pool->allocate(sizeof(PersistentTableUndoInsertAction)))
      PersistentTableUndoInsertAction(m_tmpTarget1, this, pool);
    undoQuantum->registerUndoAction(ptuia);

    // handle any materialized views
    for (int i = 0; i < m_views.size(); i++) {
        m_views[i]->processTupleInsert(source);
    }

    // GWW: add non-escrow journal for new added tuple
    if (m_executorContext->isCVoltDBExecution()) {
		// no previous journal is allowed at this point if it passed Constraint check,
		// otherwise it means we did not erase the journal in earlier work.
		assert(findTupleJournal(m_tmpTarget1.address()) == NULL);

		// construct journal

		NonEscrowChanger *nonEscrow = (NonEscrowChanger*)pool->allocate(sizeof(NonEscrowChanger));
		nonEscrow->txnid = m_executorContext->currentTxnId();
		nonEscrow->type = NonEscrow_INSERT;
		nonEscrow->beforeTuple = NULL;
		nonEscrow->operationCount = 1;
		// initialize updatedCols
		for(int i = 0; i < (int)((m_tmpTarget1.getSchema())->columnCount()); i++)
			nonEscrow->updatedCols[i] = true;

		TupleJournal *journal = insertTupleJournalStorage(m_tmpTarget1.address(), const_cast<TupleSchema*>(m_tmpTarget1.getSchema()), nonEscrow);

		insertTupleJournalMapping( m_tmpTarget1.address(), journal);

		ptuia->setJournalPtr(journal);
    }
    return true;
}

/*
 * Insert a tuple but don't allocate a new copy of the uninlineable
 * strings or create an UndoAction or update a materialized view.
 */
void PersistentTable::insertTupleForUndo(char *tuple) {

    m_tmpTarget1.move(tuple);
    m_tmpTarget1.setPendingDeleteOnUndoReleaseFalse();
    m_tuplesPinnedByUndo--;
    m_usedTupleCount++;

    /*
     * The only thing to do is reinsert the tuple into the indexes. It was never moved,
     * just marked as deleted.
     */
    if (!tryInsertOnAllIndexes(&m_tmpTarget1)) {
        deleteTupleStorage(m_tmpTarget1);
        throwFatalException("Failed to insert tuple into table %s for undo:"
                            " unique constraint violation\n%s\n", m_name.c_str(),
                            m_tmpTarget1.debugNoHeader().c_str());
    }
}

// GWW: insert before-image for non-escrow update
bool PersistentTable::insertBeforeTuple(TableTuple &beforeTupleSource) {

	assert(m_executorContext->isCVoltDBExecution());

    // not null checks at first
    FAIL_IF(!checkNulls(beforeTupleSource)) {
        throw ConstraintFailureException(this, beforeTupleSource, TableTuple(),
                                         CONSTRAINT_TYPE_NOT_NULL);
    }

    //
    // First get the next free tuple
    // This will either give us one from the free slot list, or
    // grab a tuple at the end of our chunk of memory
    //
    nextFreeTuple(&m_tmpTarget1);
    m_tupleCount++;
    m_usedTupleCount++;

    //
    // Then copy the source into the target
    //
    m_tmpTarget1.copyForPersistentInsert(beforeTupleSource); // tuple in freelist must be already cleared
    m_tmpTarget1.setActiveTrue();
    m_tmpTarget1.setPendingDeleteFalse();
    m_tmpTarget1.setPendingDeleteOnUndoReleaseFalse();

    /**
     * Inserts never "dirty" a tuple since the tuple is new, but...  The
     * COWIterator may still be scanning and if the tuple came from the free
     * list then it may need to be marked as dirty so it will be skipped. If COW
     * is on have it decide. COW should always set the dirty to false unless the
     * tuple is in a to be scanned area.
     */
    if (m_COWContext.get() != NULL) {
        m_COWContext->markTupleDirty(m_tmpTarget1, true);
    } else {
        m_tmpTarget1.setDirtyFalse();
    }
    m_tmpTarget1.isDirty();

    if (!tryInsertOnNonUniqueIndexes(&m_tmpTarget1)) {
        // Careful to delete allocated objects
        m_tmpTarget1.freeObjectColumns();
        deleteTupleStorage(m_tmpTarget1);
        // if could not insert on non-unique index, serious error happened
        throwFatalException("Failed to insert %s on a non-unique index!",
        		m_tmpTarget1.debugNoHeader().c_str());
        return false;
    }

    // no undo actions registered for this operation
    // and do not care materialized view for now

    // GWW: add journal if no journal exists or add NonEscrowChanger to
    // existing journal beforeTuple and afterTuple will point to the
    // same journal so the type of this journal should be UPDATE

	assert(findTupleJournal(m_tmpTarget1.address()) == NULL);

	// generate NonEscrowChanger
	NonEscrowChanger *nonEscrow = (NonEscrowChanger*)m_executorContext->getCurrentUndoQuantum()
			->getDataPool()->allocate(sizeof(NonEscrowChanger));
	nonEscrow->txnid = m_executorContext->currentTxnId();
	nonEscrow->type = NonEscrow_UPDATE;
	nonEscrow->beforeTuple = m_tmpTarget1.address();
	nonEscrow->operationCount = 1;
	for(int i = 0; i < (int)((m_tmpTarget1.getSchema())->columnCount()); i++)
		nonEscrow->updatedCols[i] = true;

	TupleJournal *journal = findTupleJournal(beforeTupleSource.address());

	if(journal == NULL) {
		// no TupleJournal, construct journal here
		journal = insertTupleJournalStorage(beforeTupleSource.address(), const_cast<TupleSchema*>(m_tmpTarget1.getSchema()), nonEscrow);

		// add journal
		insertTupleJournalMapping( m_tmpTarget1.address(), journal);
		insertTupleJournalMapping( beforeTupleSource.address(), journal);
	} else {
		// has TupleJournal with only EscrowUpdate, add NonEscrowChanger here
		// and add the mapping for newly inserted beforeTuple
		assert(journal->changer == NULL);
		assert(!journal->escrowData.empty());
		journal->changer = nonEscrow;
		insertTupleJournalMapping( m_tmpTarget1.address(), journal );
	}

    return true;
}

bool PersistentTable::tryInsertOnNonUniqueIndexes(TableTuple *tuple) {
    for (int i = m_indexCount - 1; i >= 0; --i) {
        if(!m_indexes[i]->isUniqueIndex()) {
            FAIL_IF(!m_indexes[i]->addEntry(tuple)) {
                VOLT_DEBUG("Failed to insert into index %s,%s",
                           m_indexes[i]->getTypeName().c_str(),
                           m_indexes[i]->getName().c_str());
                for (int j = i + 1; j < m_indexCount; ++j) {
                	if(!m_indexes[j]->isUniqueIndex())
                		m_indexes[j]->deleteEntry(tuple);
                }
                return false;
            }
        }
    }
    return true;
}

void PersistentTable::deleteBeforeTuple(TableTuple &target, TupleJournal *journal) {
    // delete before tuple but keep journal
    if (target.isNullTuple()) {
        throwFatalException("Failed to delete tuple from table %s:"
                            " tuple does not exist\n\n", m_name.c_str());
    }
    else {
        // Make sure that they are not trying to delete the same tuple twice
        assert(target.isActive());

        // Also make sure they are not trying to delete our m_tempTuple
        assert(&target != &m_tempTuple);

        // Just like insert, we want to remove this tuple from all of our indexes
        deleteFromAllNonUniqueIndexes(&target);

        if (m_schema->getUninlinedObjectColumnCount() != 0)
        {
            decreaseStringMemCount(target.getNonInlinedMemorySize());
        }

        // Delete the strings/objects
        target.freeObjectColumns();
        deleteTupleStorage(target);
        m_usedTupleCount--;

        // Delete the tuple journal associated with it
        assert(journal != NULL);
        journal->changer->beforeTuple = NULL;
        deleteTupleJournalMapping(target.address());
    }
}

// GWW: if one txn generated multiple undoEscrowUpdateActions, only the first
// one executed will do real work here
void PersistentTable::finishEscrowUpdate(TupleJournal *tjournal, bool committed) {
	//TupleJournal *tjournal = findTupleJournal(tuple);
	VOLT_TRACE("finishEscrowUpdate [%s] txnid %jd\n", (committed ? "commit" : "abort"), (intmax_t)m_executorContext->m_releasingTxnId);

	printJournalInfo("starting finishEscrowUopdate");

	int colCount = this->columnCount();
	NValue totalDelta[colCount];

	for(int i = 0; i < colCount; i++)
		totalDelta[i].setNull();

	std::map<int, EscrowData>::iterator itr;
	bool partialUndo = false;

	// we treat it as a partial undo, if the txn we are undoing is the last prepared one
	// or the preparing one
	if(!committed && m_executorContext->m_releasingTxnId == m_executorContext->currentTxnId()) {
		assert(m_executorContext->currentTxnId() == tjournal->currentPreparingEscrowTxnId);
		partialUndo = true;
	}

	for(itr = tjournal->escrowData.begin(); itr != tjournal->escrowData.end(); itr++) {

		EscrowData *e = dynamic_cast<EscrowData*>(&itr->second);

		EscrowJournal *prev = NULL;
		EscrowJournal *ejournal = e->jHead;

		while(ejournal != NULL) {
			if(ejournal->txnid == m_executorContext->m_releasingTxnId &&
					(!partialUndo || ejournal->undoActionIdx == tjournal->undoActionCountForCurrentPreparingEscrowTxn)) {
				// process this ejournal if:
				//	1. belongs to current txn
				//	2a. not partialUndo
				//		or
				//	2b. partialUndo, and belongs to last undo action
				VOLT_TRACE("finishEscrowUpdate for txnid %jd : found own journal\n", (intmax_t)ejournal->txnid);
				if(committed) {
					if(ejournal->delta.isNegative()) {
						e->sup = e->sup.op_add(ejournal->delta);
					} else {
						e->inf = e->inf.op_add(ejournal->delta);
					}

					if(totalDelta[e->columnIdx].isNull())
						totalDelta[e->columnIdx] = ejournal->delta;
					else
						totalDelta[e->columnIdx] = totalDelta[e->columnIdx].op_add(ejournal->delta);

				} else {
					if(ejournal->delta.isNegative()) {
						e->inf = e->inf.op_subtract(ejournal->delta);
					} else {
						e->sup = e->sup.op_subtract(ejournal->delta);
					}
				}

				// removed eJournal will be deallocated automatically
				if(prev == NULL)
					e->jHead = ejournal->jNext;
				else
					prev->jNext = ejournal->jNext;
				ejournal = ejournal->jNext;

			} else {
				// skip this ejournal and find the next one
				if(prev == NULL)
					prev = e->jHead;
				else
					prev = ejournal;
				ejournal = ejournal->jNext;
			}
		}
	}

	// update row_val
	if(committed) {
		TableTuple t(tjournal->currentTuple, m_schema);

		VOLT_TRACE("before - update target tuple: %s\n", t.debugNoHeader().c_str());

		for(int i = 0; i < colCount; i++) {
			if(!totalDelta[i].isNull())
				t.setNValue(i, t.getNValue(i).op_add(totalDelta[i]));
		}

		VOLT_TRACE("updated target tuple: %s\n", t.debugNoHeader().c_str());

		printJournalInfo("after finishEscrowUpdate");

		if (m_COWContext.get() != NULL) {
			m_COWContext->markTupleDirty(t, false);
		}
	}

	if(m_executorContext->m_releasingTxnId == tjournal->currentPreparingEscrowTxnId) {
		tjournal->undoActionCountForCurrentPreparingEscrowTxn--;

		// only clean up tuple mappings and storage here if this is the last undo action
		// for the last prepared escrow txn and no non-escrow changer exists
		if(tjournal->undoActionCountForCurrentPreparingEscrowTxn == 0 &&
				tjournal->changer == NULL){
			// should have processed all escrow journals
			assert(	tjournal->escrowData.size() == 0 );
			VOLT_TRACE("escrow update only and no escrow data left, deleting the journal...\n");
			deleteTupleJournalMapping(tjournal->currentTuple);
			deleteTupleJournalStorage(tjournal->currentTuple);
		}
	}

	VOLT_TRACE("finish current escrow update, txn %lu\n",m_executorContext->m_releasingTxnId);
}


bool PersistentTable::escrowUpdateTuple(TableTuple &source, TableTuple &target, bool updatedCols[]) {

	TupleJournal *tjournal = findTupleJournal(target.address());

	printJournalInfo("before EscrowUpdate");

	if(tjournal == NULL) {
		tjournal = insertTupleJournalStorage(target.address(), const_cast<TupleSchema*>(target.getSchema()));
		insertTupleJournalMapping(target.address(), tjournal);
		assert(tjournal->changer == NULL);
		printJournalInfo("escrowUpdateTuple: added the empty journal");
	}

	assert(tjournal->currentPreparingEscrowTxnId <= m_executorContext->currentTxnId());
	if(tjournal->currentPreparingEscrowTxnId < m_executorContext->currentTxnId()) {
		tjournal->currentPreparingEscrowTxnId = m_executorContext->currentTxnId();
		tjournal->undoActionCountForCurrentPreparingEscrowTxn = 1;
	} else {
		tjournal->undoActionCountForCurrentPreparingEscrowTxn++;
	}

	for(int i = 0; i < this->columnCount(); i++) {
		if(updatedCols[i] && m_schema->isEscrowColumn(i)) {

			EscrowData *eData = findEscrowData(tjournal, i);

			EscrowJournal *ejournal = (EscrowJournal*)m_executorContext->getCurrentUndoQuantum()
							->getDataPool()->allocate(sizeof(EscrowJournal));
			// NValue delta = source.getNValue(i).op_subtract(target.getNValue(i));
			// updated syntax for escrow update: update T set C = ? where ? will be the delta value
			NValue delta = source.getNValue(i);
			ejournal->delta = delta;
			ejournal->txnid = m_executorContext->currentTxnId();
			ejournal->jNext = NULL;
			ejournal->undoActionIdx = tjournal->undoActionCountForCurrentPreparingEscrowTxn;
			VOLT_TRACE("new Ejournal: txn %jd, table %s, %s\n", (intmax_t)ejournal->txnid,
			 					m_name.c_str(), ejournal->delta.debug().c_str());

			if(m_executorContext->isCheckEscrowOutOfBound()){
				NValue inf = (eData == NULL) ? target.getNValue(i) : eData->inf;
				NValue sup = (eData == NULL) ? target.getNValue(i) : eData->sup;

				// test upper bound first, if below 0, throw an out-of-bound constraint failure exception
				if(sup.op_add(delta).isNegative()) {
					// clean up journals and throw the exception
					if(tjournal->changer == NULL && tjournal->escrowData.empty()) {
						deleteTupleJournalMapping(target.address());
						deleteTupleJournalStorage(target.address());
					}

					throw ConstraintFailureException(this, target, TableTuple(), CONTRAINT_TYPE_ESCROW_OUT_OF_BOUND);
				} else if (inf.op_add(delta).isNegative()) {
					// if only potential lower bound < 0, txn should wait and re-try
					// leave TupleJournal here, it will be eventually cleaned up by
					// this txn or later txn
					if(eData != NULL)
						throw DataConflictException(this, target, source,
							eData->jHead->txnid, m_executorContext->currentTxnId());
				}
			}

			// insert new EscrowData
			if(eData == NULL) {
				eData = insertEscrowData(tjournal, i);

				eData->columnIdx = i;
				eData->inf = eData->sup = target.getNValue(i);
			}


			insertEscrowJournal(tjournal, i, ejournal);
			printJournalInfo("inserted eJournal");

			// update inf / sup
			if(delta.isNegative()) {
				eData->inf = eData->inf.op_add(delta);
			} else {
				eData->sup = eData->sup.op_add(delta);
			}
		} // end is-escrow col
	} // end loop over cols

    UndoQuantum *undoQuantum = m_executorContext->getCurrentUndoQuantum();
    assert(undoQuantum);
    Pool *pool = undoQuantum->getDataPool();
    assert(pool);
    PersistentTableUndoEscrowUpdateAction *ptueua =
      new (pool->allocate(sizeof(PersistentTableUndoEscrowUpdateAction)))
      	  PersistentTableUndoEscrowUpdateAction(this, tjournal);

	if(!undoQuantum->isDummy())
		undoQuantum->registerUndoAction(ptueua);

	printJournalInfo("after EscrowUpdate");

	return true;
}


/*
 * Regular tuple update function that does a copy and allocation for
 * updated strings and creates an UndoAction.
 */
bool PersistentTable::updateTuple(TableTuple &source, TableTuple &target, bool updatesIndexes) {
    /*
     * Create and register an undo action and then use the copy of
     * the target (old value with no updates)
     */
     UndoQuantum *undoQuantum = m_executorContext->getCurrentUndoQuantum();
     assert(undoQuantum);
     Pool *pool = undoQuantum->getDataPool();
     assert(pool);
     PersistentTableUndoUpdateAction *ptuua =
       new (pool->allocate(sizeof(PersistentTableUndoUpdateAction)))
       PersistentTableUndoUpdateAction(target, this, pool);

     if(m_executorContext->isCVoltDBExecution())
    	 ptuua->setJournalPtr(findTupleJournal(target.address()));

     if (m_COWContext.get() != NULL) {
         m_COWContext->markTupleDirty(target, false);
     }

    if (m_schema->getUninlinedObjectColumnCount() != 0)
    {
        decreaseStringMemCount(target.getNonInlinedMemorySize());
        increaseStringMemCount(source.getNonInlinedMemorySize());
    }

     source.setActiveTrue();
     //Copy the dirty status that was set by markTupleDirty.
     if (target.isDirty()) {
         source.setDirtyTrue();
     } else {
         source.setDirtyFalse();
     }
     target.copyForPersistentUpdate(source);

     ptuua->setNewTuple(target, pool);

     if (!undoQuantum->isDummy()) {
         //DummyUndoQuantum calls destructor upon register.
         undoQuantum->registerUndoAction(ptuua);
     }

    // the planner should determine if this update can affect indexes.
    // if so, update the indexes here
    if (updatesIndexes) {
        if (!tryUpdateOnAllIndexes(ptuua->getOldTuple(), target)) {
        	// GWW: original VoltDB will assume user will abort the txn, however,
        	// it's possible the exception is caught by the procedure but
        	// user decides to continue, then database state becomes inconsistent
        	// thus, we need to clean up before we throw the exception
        	ptuua->undo();
        	ptuua->ignoreUndoAction();

            throw ConstraintFailureException(this, ptuua->getOldTuple(),
                                             target,
                                             CONSTRAINT_TYPE_UNIQUE);
        }


        // TODO: this special handling no longer needed
        // If the CFE is thrown the Undo action should not attempt
        // to revert the indexes
        ptuua->needToRevertIndexes();
        updateFromAllIndexes(ptuua->getOldTuple(), target);
    }

    // handle any materialized views
    for (int i = 0; i < m_views.size(); i++) {
        m_views[i]->processTupleUpdate(ptuua->getOldTuple(), target);
    }

    /**
     * Check for nulls after the update has been performed because the source tuple may have garbage in
     * some columns
     */
    FAIL_IF(!checkNulls(target)) {
    	// GWW
    	ptuua->undo();
    	ptuua->ignoreUndoAction();

        throw ConstraintFailureException(this, ptuua->getOldTuple(),
                                         target,
                                         CONSTRAINT_TYPE_NOT_NULL);
    }

    if (undoQuantum->isDummy()) {
        //DummyUndoQuantum calls destructor upon register so it can't be called
        //earlier
        undoQuantum->registerUndoAction(ptuua);
    }

    return true;
}

// GWW: main entrance for TupleUpdate: handle regular Update, Non-Escrow Update and Escrow Update
// called from UpdateExecutor
bool PersistentTable::updateTuple(TableTuple &source, TableTuple &target, bool updatedCols[], bool updatesIndexes) {

    TupleJournal *tjournal = findTupleJournal(target.address());

    VOLT_TRACE("WGWG: source: %s, target: %s, updatesIndexes: %d\n", source.debugNoHeader().c_str(), target.debugNoHeader().c_str(), updatesIndexes);

    if(!m_executorContext->isCVoltDBExecution()) {
        // Case1: Regular Update without CVoltDB Algorithm
        return updateTuple(source, target, updatesIndexes);
    } else {
        // either EscrowUpdate or NonEscrowUpdate
    	// if update both escrow cols and non-escrow cols, treat it as NonEscrowUpdate
    	bool escrowUpdate = false;
    	bool nonEscrowUpdate = false;

    	assert(source.m_schema->equals(target.m_schema));

    	for(int i = 0; i < target.m_schema->columnCount(); i++) {
    		if(updatedCols[i]) {
    			if(target.m_schema->isEscrowColumn(i))
    				escrowUpdate = true;
    			else
    				nonEscrowUpdate = true;
    		}
    	}

    	// we do not allow an update operates on both escrow-col and non-escrow-col for now
    	// system throws an exception on this case, TODO: find a name for this type
    	// TODO: support escrow and nonescrow update in one operation?
    	if(escrowUpdate && nonEscrowUpdate) {
    		throw SQLException(SQLException::volt_operation_not_supported,
    				"Can not update on both Escrow columns and non-Escrow columns in Escrow-compatible mode.");
    	}

    	assert(!(escrowUpdate && nonEscrowUpdate));

     	if(escrowUpdate){
     		// Case 2: Escrow Update
     		escrowUpdateTuple(source, target, updatedCols);
     		return true;
    	}


		// Case 3: NonEscrowUpdate

		if(tjournal != NULL && tjournal->changer != NULL) {
			// GWW: if earlier non-escrow change exists which was made by another txn
			// we should block this one.
			if(tjournal->changer->txnid != m_executorContext->currentTxnId()) {
				TableTuple t(tjournal->changer->beforeTuple, tjournal->schema);
				throw DataConflictException(this, t, source,
						tjournal->changer->txnid, m_executorContext->currentTxnId());
			}

			// the tuple is invisible if there was a DELETE performed before
			assert(tjournal->changer->type != NonEscrow_DELETE);

			// for previous INSERT/UPDATE, first update updatedCols
			// type remains the same, and target data will be changed later
			for(int i = 0; i < target.m_schema->columnCount(); i++) {
					tjournal->changer->updatedCols[i] |= updatedCols[i];
			}
			tjournal->changer->operationCount++;
		}
		else {
			// GWW: either no TupleJournal or no NonEscrowChanger (only EscrowUpdate)
			// fresh new non-escrow update, arrange both tuples in the table
			// and create one tuple journal if none exists, and add two entries, letting
			// currentTuple and beforeTuple both point to the same tuple journal
			insertBeforeTuple(target);
			tjournal = findTupleJournal(target.address());
			assert(tjournal != NULL);
			for(int i = 0; i < target.m_schema->columnCount(); i++) {
				tjournal->changer->updatedCols[i] |= updatedCols[i];
			}
		}

		return updateTuple(source, target, updatesIndexes);
	}

}

/*
 * Source contains the tuple before the update and target is a
 * reference to the updated tuple including the actual table
 * storage. First backup the target to a temp tuple so it will be
 * available for updating indexes. Then revert the tuple to the
 * original preupdate values by copying the source to the target. Then
 * update the indexes to use the new key value (if the key has
 * changed). The backup is necessary because the indexes expect the
 * data ptr that will be used as the value in the index.
 */
void PersistentTable::updateTupleForUndo(TableTuple &source, TableTuple &target,
                                         bool revertIndexes, TupleJournal *tjournal) {
    if (m_schema->getUninlinedObjectColumnCount() != 0)
    {
        decreaseStringMemCount(target.getNonInlinedMemorySize());
        increaseStringMemCount(source.getNonInlinedMemorySize());
    }

    //Need to back up the updated version of the tuple to provide to
    //the indexes when updating The indexes expect source's data Ptr
    //to point into the table so it is necessary to copy source to
    //target. Without this backup the target would be lost and it
    //there would be nothing to provide to the index to lookup. In
    //regular updateTuple this storage is provided by the undo
    //quantum.
    TableTuple targetBackup = tempTuple();
    targetBackup.copy(target);

    bool dirty = target.isDirty();

    // GWW: values of escrow columns are not updated in source tuple
    // need to copy them from target first.
    if(ExecutorContext::getExecutorContext()->isCVoltDBExecution()){
		assert(tjournal != NULL);
		// copy escrow data if needed
		if(!tjournal->escrowData.empty()){
			std::map<int, EscrowData>::iterator itr;
			for(itr = tjournal->escrowData.begin(); itr != tjournal->escrowData.end(); itr++) {
				int colIdx = itr->first;
				source.setNValue(colIdx, target.getNValue(colIdx));
			}
		}
    }

    // this is the actual in-place revert to the old version
    target.copy(source);
    if (dirty) {
        target.setDirtyTrue();
    } else {
        target.setDirtyFalse();
    }
    target.isDirty();

    // GWW: finish nonEscrow update -- clean journals
    if(ExecutorContext::getExecutorContext()->isCVoltDBExecution())
    	cleanTupleJournal(tjournal);

    //If the indexes were never updated there is no need to revert them.
    if (revertIndexes) {
        if (!tryUpdateOnAllIndexes(targetBackup, target)) {
            // TODO: this might be too strict. see insertTuple()
            throwFatalException("Failed to update tuple in table %s for undo:"
                                " unique constraint violation\n%s\n%s\n", m_name.c_str(),
                                targetBackup.debugNoHeader().c_str(),
                                target.debugNoHeader().c_str());
        }
        updateFromAllIndexes(targetBackup, target);
    }
}


bool PersistentTable::deleteTuple(TableTuple &target, bool deleteAllocatedStrings) {

	// GWW: it is a bug if one txn tried to delete the same tuple more than once
	// but if two Escrow-compatible txns tried to delete the same tuple, we just
	// block the second one.
    // May not delete an already deleted tuple.
    // assert(target.isActive());

    // The tempTuple is forever!
    assert(&target != &m_tempTuple);

    // GWW: processing steps for deletetuple if escrow enabled
    //			|		Unmodified VoltDB	|			CVoltDB				|
    // ----------------------------------------------------------------------
    // Prepare	| delete entries from all 	| insert delete tuple journal	|
    //			| indexes, set status bits,	| set status bits, generate		|
    //			| generate undoQuantum		| undoQuantum					|
    // ----------------------------------------------------------------------
    // Commit	| set status bits, free 	| delete entry from all indexes	|
    //			| allocated strings		 	| set status bits, free allocated|
    //			| 							| strings, and	clean up tuple
    //			|							| journal						|
    // ----------------------------------------------------------------------
    // Abort	| insert tuple to all indexes| clean up tuple journal		|
    // ----------------------------------------------------------------------
    if(m_executorContext->isCVoltDBExecution()) {
    	TupleJournal *journal = findTupleJournal(target.address());
    	// if any txn has already changed this tuple
		if(journal != NULL) {
			// make sure only the same txn can access its changed tuple without blocking
			if(journal->changer->txnid != m_executorContext->currentTxnId()) {
		        throw DataConflictException(this, target, TableTuple(),
		        		journal->changer->txnid, m_executorContext->currentTxnId());
			}
			assert(target.isActive());
			// can not delete an already "deleted" tuple by same txn
			assert((journal->changer->type & NonEscrow_DELETE) == 0);

			// beforetuple is invisible to the txn that updated it
			// change the type to DELETE, because OPERATION + DELETE is
			// handled as DELETE
			journal->changer->type = NonEscrow_DELETE;
			assert(journal->currentTuple == target.address());
			journal->changer->operationCount++;
		} else {
			// case of no previous edit to this row

			Pool *datapool = m_executorContext->getCurrentUndoQuantum()->getDataPool();
			assert(datapool);
			NonEscrowChanger *nonEscrow = (NonEscrowChanger*)datapool->allocate(sizeof(NonEscrowChanger));

			nonEscrow->txnid = m_executorContext->currentTxnId();
			nonEscrow->type = NonEscrow_DELETE;
			nonEscrow->operationCount = 1;
			nonEscrow->beforeTuple = NULL;
			// no need to initialize updateCols for DELETEd tuple

			TupleJournal *journal = insertTupleJournalStorage(target.address(), const_cast<TupleSchema*>(target.getSchema()), nonEscrow);
			assert(journal != NULL);

			insertTupleJournalMapping(target.address(), journal);
		}
    } else{
    	assert(target.isActive());
		// Just like insert, we want to remove this tuple from all of our indexes
		deleteFromAllIndexes(&target);
    }

    target.setPendingDeleteOnUndoReleaseTrue();
    m_tuplesPinnedByUndo++;
    m_usedTupleCount--;

    /*
     * Create and register an undo action.
     */
    UndoQuantum *undoQuantum = m_executorContext->getCurrentUndoQuantum();
    assert(undoQuantum);
    Pool *pool = undoQuantum->getDataPool();
    assert(pool);
    PersistentTableUndoDeleteAction *ptuda =
            new (pool->allocate(sizeof(PersistentTableUndoDeleteAction))) PersistentTableUndoDeleteAction( target.address(), this);

    if(m_executorContext->isCVoltDBExecution())
    	ptuda->setJournalPtr(findTupleJournal(target.address()));

    // handle any materialized views
    for (int i = 0; i < m_views.size(); i++) {
        m_views[i]->processTupleDelete(target);
    }

    undoQuantum->registerUndoAction(ptuda, this);
    return true;
}

/*
 * Delete a tuple by looking it up via table scan or a primary key
 * index lookup. An undo initiated delete like deleteTupleForUndo
 * is in response to the insertion of a new tuple by insertTuple
 * and that by definition is a tuple that is of no interest to
 * the COWContext. The COWContext set the tuple to have the
 * correct dirty setting when the tuple was originally inserted.
 * TODO remove duplication with regular delete. Also no view updates.
 */
void PersistentTable::deleteTupleForUndo(TableTuple &tupleCopy) {
    TableTuple target = lookupTuple(tupleCopy);
    if (target.isNullTuple()) {
        throwFatalException("Failed to delete tuple from table %s:"
                            " tuple does not exist\n%s\n", m_name.c_str(),
                            tupleCopy.debugNoHeader().c_str());
    }
    else {
        // Make sure that they are not trying to delete the same tuple twice
        assert(target.isActive());

        // Also make sure they are not trying to delete our m_tempTuple
        assert(&target != &m_tempTuple);

        // Just like insert, we want to remove this tuple from all of our indexes
        deleteFromAllIndexes(&target);

        if (m_schema->getUninlinedObjectColumnCount() != 0)
        {
            decreaseStringMemCount(tupleCopy.getNonInlinedMemorySize());
        }

        // Delete the strings/objects
        target.freeObjectColumns();
        deleteTupleStorage(target);
        m_usedTupleCount--;
    }
}

TableTuple PersistentTable::lookupTuple(TableTuple tuple) {
    TableTuple nullTuple(m_schema);

    TableIndex *pkeyIndex = primaryKeyIndex();
    if (pkeyIndex == NULL) {
        /*
         * Do a table scan.
         */
        TableTuple tableTuple(m_schema);
        TableIterator ti(this, m_data.begin());
        while (ti.hasNext()) {
            ti.next(tableTuple);
            if (tableTuple.equalsNoSchemaCheck(tuple)) {
                return tableTuple;
            }
        }
        return nullTuple;
    }

    bool foundTuple = pkeyIndex->moveToTuple(&tuple);
    if (!foundTuple) {
        return nullTuple;
    }

    return pkeyIndex->nextValueAtKey();
}

void PersistentTable::insertIntoAllIndexes(TableTuple *tuple) {
    for (int i = m_indexCount - 1; i >= 0;--i) {
        if (!m_indexes[i]->addEntry(tuple)) {
            throwFatalException(
                    "Failed to insert tuple in Table: %s Index %s", m_name.c_str(), m_indexes[i]->getName().c_str());
        }
    }
}

// GWW
void PersistentTable::checkForIndexEntryPhantom(const TableTuple *targetTuple, TableIndex *index){
	bool ret;
	ret = index->moveToTuple(targetTuple);
	assert(ret == true);
	TableTuple tuple = index->nextValueAtKey();
	TupleJournal *journal = findTupleJournal(tuple.address());
	if(journal != NULL && journal->changer->txnid != m_executorContext->currentTxnId()) {
		throw DataConflictException(this, *targetTuple, TableTuple(m_schema),
				journal->changer->txnid, m_executorContext->currentTxnId());
	}
}


// GWW
void PersistentTable::deleteFromAllNonUniqueIndexes(TableTuple *tuple) {
    for (int i = m_indexCount - 1; i >= 0;--i) {
    	if(!m_indexes[i]->isUniqueIndex()) {
			if (!m_indexes[i]->deleteEntry(tuple)) {
				throwFatalException(
						"Failed to delete tuple in Table: %s Index %s", m_name.c_str(), m_indexes[i]->getName().c_str());
			}
    	}
    }
}

void PersistentTable::deleteFromAllIndexes(TableTuple *tuple) {
    for (int i = m_indexCount - 1; i >= 0;--i) {
        if (!m_indexes[i]->deleteEntry(tuple)) {
            throwFatalException(
                    "Failed to delete tuple in Table: %s Index %s", m_name.c_str(), m_indexes[i]->getName().c_str());
        }
    }
}

void PersistentTable::updateFromAllIndexes(TableTuple &targetTuple, const TableTuple &sourceTuple) {
    for (int i = m_indexCount - 1; i >= 0;--i) {
        if (!m_indexes[i]->replaceEntry(&targetTuple, &sourceTuple)) {
            throwFatalException(
                    "Failed to update tuple in Table: %s Index %s", m_name.c_str(), m_indexes[i]->getName().c_str());
        }
    }
}

void PersistentTable::updateWithSameKeyFromAllIndexes(TableTuple &targetTuple, const TableTuple &sourceTuple) {
    for (int i = m_indexCount - 1; i >= 0;--i) {
        if (!m_indexes[i]->replaceEntryNoKeyChange(&targetTuple, &sourceTuple)) {
            throwFatalException(
                    "Failed to update tuple in Table: %s Index %s", m_name.c_str(), m_indexes[i]->getName().c_str());
        }
    }
}

bool PersistentTable::tryInsertOnAllIndexes(TableTuple *tuple) {
    for (int i = m_indexCount - 1; i >= 0; --i) {
        FAIL_IF(!m_indexes[i]->addEntry(tuple)) {
        	// GWW: check whether failure is due to an existing phantom tuple
        	// so this case only happens for unique indexes
        	if(m_executorContext->isCVoltDBExecution() && m_indexes[i]->isUniqueIndex())
        		checkForIndexEntryPhantom(const_cast<TableTuple*>(tuple), m_indexes[i]);

            VOLT_DEBUG("Failed to insert into index %s,%s",
                       m_indexes[i]->getTypeName().c_str(),
                       m_indexes[i]->getName().c_str());
            for (int j = i + 1; j < m_indexCount; ++j) {
                m_indexes[j]->deleteEntry(tuple);
            }
            return false;
        }
    }
    return true;
}

bool PersistentTable::tryUpdateOnAllIndexes(TableTuple &targetTuple, const TableTuple &sourceTuple) {
    for (int i = m_uniqueIndexCount - 1; i >= 0;--i) {
        if (m_uniqueIndexes[i]->checkForIndexChange(&targetTuple, &sourceTuple) == false)
            continue; // no update is needed for this index

        // if there is a change, the new_key has to be checked
        FAIL_IF (m_uniqueIndexes[i]->exists(&sourceTuple)) {
        	// GWW
        	if(m_executorContext->isCVoltDBExecution())
        		checkForIndexEntryPhantom(&sourceTuple, m_uniqueIndexes[i]);

            VOLT_WARN("Unique Index '%s' complained to the update",
                      m_uniqueIndexes[i]->debug().c_str());
            return false; // cannot insert the new value
        }
    }
    return true;
}

// GWW: 1. We use operationCount to track how many nonEscrow operations
// have been performed on this tuple; at commit/abort time, just decrease
// operationCount if it is over 1, deferring real cleanup until it's equal
// equal to 1.
// 2. We must keep TupleJournal until NonEscrowChanger is reset back to NULL
// and no EscrowData exists anymore, i.e. no active txn updating on this tuple
// 3. TupleJournal and EscrowJournal for Escrow-Update is self-managed by
// escrowUpdateTuple() and finishEscrowUpdate(), this function only handles
// tuple clean-up for non-escrow operations
// 4. For NonEscrow, this function is called from UndoAction.release/undo
// 5. Memory management: non escrow changers are allocated inside finishing txn's
// undoQuantum pool so they are automatically freed after this txn
// commits/aborts; other structures are freed by map management
//void PersistentTable::cleanTupleJournal(char* dataPtr) {
void PersistentTable::cleanTupleJournal(TupleJournal* journal) {

	assert(journal != NULL);
	assert(journal->changer != NULL);

	// delete the journal if this is the first operation or
	// the type of the whole sequence is only NonEscrowUpdate
	if(journal->changer->operationCount == 1){

		// delete beforeTuple if it exists
		if(journal->changer->beforeTuple != NULL){
			TableTuple t = TableTuple(journal->changer->beforeTuple, this->schema());
			deleteBeforeTuple(t, journal);
		}

		// journal for before tuple should have been deleted
		assert(findTupleJournal(journal->changer->beforeTuple) == NULL);

		// delete tuple journal storage only when no nonEscrow journal and no Escrow journal
		if(journal->escrowData.empty()){
			deleteTupleJournalMapping(journal->currentTuple);
			deleteTupleJournalStorage(journal->currentTuple);
		} else
			journal->changer = NULL;
	}
	else
		journal->changer->operationCount--;
}

void PersistentTable::testEscrowTupleJournal() {
	boost::unordered_map<char*, TupleJournal*>::iterator itr;
	for(itr = m_tupleJournals.begin(); itr != m_tupleJournals.end(); itr++) {
		TupleJournal *tj = dynamic_cast<TupleJournal*>(itr->second);
		if(tj->changer != NULL){
			if(tj->changer->beforeTuple != NULL) {
				assert(tj->changer->type != NonEscrow_INSERT);
				assert(findTupleJournal(tj->changer->beforeTuple) != NULL);
				if(tj->changer->operationCount == 1)
					assert(tj->changer->type == NonEscrow_UPDATE);
			}
#if defined(DEBUG) || defined(_DEBUG) || defined(_DEBUG_)
			std::deque<UndoQuantum*> undoQuantums = m_executorContext->getTxnUndoQuantums(tj->changer->txnid);

			for(int i = 0; i < undoQuantums.size(); i++) {
				UndoQuantum *undoQuantum = undoQuantums[i];

				for (std::vector<UndoAction*>::iterator itr = undoQuantum->m_undoActions.begin();
						 itr != undoQuantum->m_undoActions.end(); itr++) {
					UndoAction *p = dynamic_cast<UndoAction*>(*itr);
					PersistentTableUndoInsertAction *ptuia = dynamic_cast<PersistentTableUndoInsertAction*>(p);
					PersistentTableUndoDeleteAction *ptuda = dynamic_cast<PersistentTableUndoDeleteAction*>(p);

					// if type is NonEscrow_INSERT, first undo action is insert and no delete undo action
					if(tj->changer->type == NonEscrow_INSERT){
						if(i == 0 && itr == undoQuantum->m_undoActions.begin())
							assert(ptuia != NULL);
						else
							assert(ptuia == NULL);
						assert(ptuda == NULL);
					}
					// if type is NonEscrow_DELETE, assert only have 1 delete undo action, and no insert undo action
					if(tj->changer->type == NonEscrow_UPDATE){
						if(i == undoQuantums.size() && itr == undoQuantum->m_undoActions.end())
							assert(ptuda != NULL);
						else
							assert(ptuda == NULL);
						assert(ptuia);
					}
					// if type is NonEscrow_UPDATE, assert no insert / delete undo actions
					if(tj->changer->type == NonEscrow_DELETE){
						assert(ptuda == NULL);
						assert(ptuia);
					}
				}
			}
#endif
		}

		if(!tj->escrowData.empty()) {
			std::map<int, EscrowData>::iterator eItr;
			for(eItr = tj->escrowData.begin(); eItr != tj->escrowData.end(); eItr++) {
				EscrowData *ed = dynamic_cast<EscrowData*>(&eItr->second);
				assert(ed->jHead != NULL);
				NValue delta = ed->sup.op_subtract(ed->inf);
				EscrowJournal *ej = ed->jHead;
				NValue total_delta = ej->delta;
				while(ej->jNext) {
					ej = ej->jNext;
					total_delta = total_delta.op_add(ej->delta);
				}
				assert(delta.op_equals(total_delta).isZero());
			}
		}
	}
}
// GWW: no expr passed here, just do a full scan for escrow datas if it is block
// by escrow updates
int64_t PersistentTable::getBlockerTxnIdForBlockedRead(TupleJournal *journal) {

	if(journal->changer != NULL && journal->changer->txnid != ExecutorContext::getExecutorContext()->currentTxnId()){
		VOLT_TRACE(" getBlockerTxnId,called by txn %lu found changer\n",m_executorContext->currentTxnId());
		return journal->changer->txnid;
	}
	else {
		// must be blocked by escrow updates
		VOLT_TRACE(" getBlockerTxnId,called by txn %lu, escrow...\n",m_executorContext->currentTxnId());
		assert(!journal->escrowData.empty());
		std::map<int, EscrowData>::iterator itr;
		for(itr = journal->escrowData.begin(); itr != journal->escrowData.end(); itr++) {
			EscrowData *ed = dynamic_cast<EscrowData*>(&itr->second);
			EscrowJournal *ej = ed->jHead;
			while(ej != NULL) {
				if(ej->txnid != ExecutorContext::getExecutorContext()->currentTxnId()){
					VOLT_TRACE(" getBlockerTxnId, caller txn %lu, found blocker txn %lu\n",
								m_executorContext->currentTxnId(), ej->txnid);
					return ej->txnid;
				}
				ej = ej->jNext;
			}
		}

		return -1;
	}
}


// GWW: used in seqscanexecutor and indexscanexecutor
// Return REPORT if this expression returns only unchanged data which can be
// sent back to the caller without waiting.
// Return BLOCK if the journal for current tuple records a non-escrow change
// by a different txn and this txn wants to READ the changed column through
// the argued expression ( the caller will have to WAIT)
// Return IGNORE if the journal for the current tuple records a non-escrow
// change by the same txn that indicates that this tuple is invisible because
// it's a before tuple or a deleted tuple.
// Right now, we only support EXPRESSION_TYPE_VALUE_TUPLE type expression,
// other expression are treated as a full row READs
int PersistentTable::getDispositionPerColumn(char *dataPtr, TupleJournal *journal,
											AbstractExpression *expr, int colIndex) {
	assert(journal != NULL);
	VOLT_TRACE(" getDisposition, txn %lu",m_executorContext->currentTxnId());
	printTupleJournal("getDisposition", journal);
	if(journal->changer != NULL && journal->changer->txnid == ExecutorContext::getExecutorContext()->currentTxnId()
			&& (dataPtr == journal->changer->beforeTuple || journal->changer->type == NonEscrow_DELETE))
				return IGNORE;

	if(expr != NULL) {
		if (expr->getExpressionType() == EXPRESSION_TYPE_VALUE_PARAMETER ||
			expr->getExpressionType() == EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS)
			return REPORT;
		if(expr->getExpressionType() == EXPRESSION_TYPE_VALUE_TUPLE) {
			assert(colIndex == -1);
			colIndex = expr->getTupleColIdx();
		}
	}

	if(colIndex == -1) {
		// full row READs

		// for NonEscrowUpdate, if chagner's txnid is different from
		// the current one, return BLOCK
		if(journal->changer != NULL &&
				journal->changer->txnid != ExecutorContext::getExecutorContext()->currentTxnId())
			return BLOCK;

		// for EscrowUpdate, if any Escrow Updater's txnid is different
		// from the current one, return BLOCK
		if(!journal->escrowData.empty()) {
			std::map<int, EscrowData>::iterator itr;
			for(itr = journal->escrowData.begin(); itr != journal->escrowData.end(); itr++) {
				EscrowData *ed = dynamic_cast<EscrowData*>(&itr->second);
				EscrowJournal *ej = ed->jHead;
				while(ej != NULL) {
					if(ej->txnid != ExecutorContext::getExecutorContext()->currentTxnId())
						return BLOCK;
					ej = ej->jNext;
				}
			}
		}

		// otherwise, return REPORT
		return REPORT;

	} else {

		if(m_schema->isEscrowColumn(colIndex)) {
			EscrowData *ed = findEscrowData(journal, colIndex);
			if(ed != NULL) {
				EscrowJournal *ej = ed->jHead;
				while(ej != NULL) {
					if(ej->txnid != ExecutorContext::getExecutorContext()->currentTxnId())
						return BLOCK;
					ej = ej->jNext;
				}
			}

			return REPORT;

		} else {
			if(journal->changer != NULL) {
				if(journal->changer->txnid != ExecutorContext::getExecutorContext()->currentTxnId()
						&& journal->changer->updatedCols[colIndex])
					return BLOCK;
				else {
					if(dataPtr == journal->changer->beforeTuple || journal->changer->type == NonEscrow_DELETE)
						return IGNORE;
					else
						return REPORT;
				}
			} else
				 return REPORT;
		}
	}
}


void PersistentTable::printJournalInfo(std::string header) {
	VOLT_TRACE("%s - %jd journals\n", header.c_str(), (intmax_t)m_tupleJournals.size());
	boost::unordered_map<char*, TupleJournal*>::iterator itr;
	for(itr = this->m_tupleJournals.begin(); itr != m_tupleJournals.end(); itr++) {
		TupleJournal *tj = dynamic_cast<TupleJournal*>(itr->second);
		printTupleJournal(header, tj);
		assert(tj);
	}
}

void PersistentTable::printTupleJournal(std::string header, TupleJournal *tj) {
	VOLT_TRACE("%s - TupleJournal for Table: %s\n", header.c_str(), m_name.c_str());
	assert(tj);
	VOLT_TRACE("%s - Preparer: %ld, Releaser: %ld\n", 
			header.c_str(), m_executorContext->currentTxnId(), m_executorContext->m_releasingTxnId);
	VOLT_TRACE("%s - tj = %ld\n", header.c_str(), (long)tj);
	VOLT_TRACE("%s - changer = %ld\n", header.c_str(), (long)tj->changer);
	if(tj->changer != NULL) {
		VOLT_TRACE("\t%s - op: %d, txnid: %jd, table %s, type:%d\n", header.c_str(),
					tj->changer->operationCount, (intmax_t)(tj->changer->txnid),  m_name.c_str(), tj->changer->type);
	}
	if(!tj->escrowData.empty()) {
		std::map<int, EscrowData>::iterator eItr;
    	for(eItr = tj->escrowData.begin(); eItr != tj->escrowData.end(); eItr++) {
    		EscrowData *ed = dynamic_cast<EscrowData*>(&eItr->second);
    		VOLT_TRACE("\t%s - col[%d] - inf: %s, sup: %s\n", header.c_str(), ed->columnIdx, ed->inf.debug().c_str(), ed->sup.debug().c_str());
    		EscrowJournal *ej = ed->jHead;
    		VOLT_TRACE("\t\t%s - first EscrowJournal: txnId - %jd, table %s, delta - %s \n",
    				header.c_str(), (intmax_t)ej->txnid, m_name.c_str(), ej->delta.debug().c_str());
			if(ej){
				while(ej->jNext){
					ej = ej->jNext;
					VOLT_TRACE("\t\t%s - additional EscrowJournal: txnId - %jd, delta - %s \n", header.c_str(), (intmax_t)ej->txnid, ej->delta.debug().c_str());
				}
			}
		}
	}

	VOLT_TRACE("printTupleJournal ending\n");
}


bool PersistentTable::checkNulls(TableTuple &tuple) const {
    assert (m_columnCount == tuple.sizeInValues());
    for (int i = m_columnCount - 1; i >= 0; --i) {
        if (tuple.isNull(i) && !m_allowNulls[i]) {
            VOLT_TRACE ("%d th attribute was NULL. It is non-nillable attribute.", i);
            return false;
        }
    }
    return true;
}

/*
 * claim ownership of a view. table is responsible for this view*
 */
void PersistentTable::addMaterializedView(MaterializedViewMetadata *view) {
    m_views.push_back(view);
}

// ------------------------------------------------------------------
// UTILITY
// ------------------------------------------------------------------
std::string PersistentTable::tableType() const {
    return "PersistentTable";
}

std::string PersistentTable::debug() {
    std::ostringstream buffer;
    buffer << Table::debug();
    buffer << "\tINDEXES: " << m_indexCount << "\n";

    // Indexes
    buffer << "===========================================================\n";
    for (int index_ctr = 0; index_ctr < m_indexCount; ++index_ctr) {
        if (m_indexes[index_ctr]) {
            buffer << "\t[" << index_ctr << "] " << m_indexes[index_ctr]->debug();
            //
            // Primary Key
            //
            if (m_pkeyIndex != NULL && m_pkeyIndex->getName().compare(m_indexes[index_ctr]->getName()) == 0) {
                buffer << " [PRIMARY KEY]";
            }
            buffer << "\n";
        }
    }

    return buffer.str();
}

// ------------------------------------------------------------------
// Accessors
// ------------------------------------------------------------------
// Index
TableIndex *PersistentTable::index(std::string name) {
    for (int i = 0; i < m_indexCount; ++i) {
        TableIndex *index = m_indexes[i];
        if (index->getName().compare(name) == 0) {
            return index;
        }
    }
    std::stringstream errorString;
    errorString << "Could not find Index with name " << name << std::endl;
    for (int i = 0; i < m_indexCount; ++i) {
        TableIndex *index = m_indexes[i];
        errorString << index->getName() << std::endl;
    }
    throwFatalException( "%s", errorString.str().c_str());
}

std::vector<TableIndex*> PersistentTable::allIndexes() const {
    std::vector<TableIndex*> retval;
    for (int i = 0; i < m_indexCount; i++)
        retval.push_back(m_indexes[i]);

    return retval;
}

void PersistentTable::onSetColumns() {
    if (m_allowNulls != NULL) delete[] m_allowNulls;
    m_allowNulls = new bool[m_columnCount];
    for (int i = m_columnCount - 1; i >= 0; --i) {
        m_allowNulls[i] = m_schema->columnAllowNull(i);
    }

    // Also clear some used block state. this structure doesn't have
    // an block ownership semantics - it's just a cache. I think.
    m_blocksWithSpace.clear();

    // note that any allocated memory in m_data is left alone
    // as is m_allocatedTuples
    m_data.clear();
}

/*
 * Implemented by persistent table and called by Table::loadTuplesFrom
 * to do additional processing for views and Export and non-inline
 * memory tracking
 */
void PersistentTable::processLoadedTuple(TableTuple &tuple) {

	// not null checks at first
    FAIL_IF(!checkNulls(tuple)) {
        throw ConstraintFailureException(this, tuple, TableTuple(),
                                         CONSTRAINT_TYPE_NOT_NULL);
    }

    if (!tryInsertOnAllIndexes(&tuple)) {
        throw ConstraintFailureException(this, tuple, TableTuple(),
                                         CONSTRAINT_TYPE_UNIQUE);
    }

    // handle any materialized views
    for (int i = 0; i < m_views.size(); i++) {
        m_views[i]->processTupleInsert(m_tmpTarget1);
    }

    // Account for non-inlined memory allocated via bulk load or recovery
    if (m_schema->getUninlinedObjectColumnCount() != 0)
    {
        increaseStringMemCount(tuple.getNonInlinedMemorySize());
    }
}

TableStats* PersistentTable::getTableStats() {
    return &stats_;
}

/**
 * Switch the table to copy on write mode. Returns true if the table was already in copy on write mode.
 */
bool PersistentTable::activateCopyOnWrite(TupleSerializer *serializer, int32_t partitionId) {
    if (m_COWContext != NULL) {
        return true;
    }
    if (m_tupleCount == 0) {
        return false;
    }

    //All blocks are now pending snapshot
    m_blocksPendingSnapshot.swap(m_blocksNotPendingSnapshot);
    m_blocksPendingSnapshotLoad.swap(m_blocksNotPendingSnapshotLoad);
    assert(m_blocksNotPendingSnapshot.empty());
    for (int ii = 0; ii < m_blocksNotPendingSnapshotLoad.size(); ii++) {
        assert(m_blocksNotPendingSnapshotLoad[ii]->empty());
    }

    m_COWContext.reset(new CopyOnWriteContext( this, serializer, partitionId));
    return false;
}

/**
 * Attempt to serialize more tuples from the table to the provided output stream.
 * Returns true if there are more tuples and false if there are no more tuples waiting to be
 * serialized.
 */
bool PersistentTable::serializeMore(ReferenceSerializeOutput *out) {
    if (m_COWContext == NULL) {
        return false;
    }

    const bool hasMore = m_COWContext->serializeMore(out);
    if (!hasMore) {
        m_COWContext.reset(NULL);
    }

    return hasMore;
}

/**
 * Create a recovery stream for this table. Returns true if the table already has an active recovery stream
 */
bool PersistentTable::activateRecoveryStream(int32_t tableId) {
    if (m_recoveryContext != NULL) {
        return true;
    }
    m_recoveryContext.reset(new RecoveryContext( this, tableId ));
    return false;
}

/**
 * Serialize the next message in the stream of recovery messages. Returns true if there are
 * more messages and false otherwise.
 */
void PersistentTable::nextRecoveryMessage(ReferenceSerializeOutput *out) {
    if (m_recoveryContext == NULL) {
        return;
    }

    const bool hasMore = m_recoveryContext->nextMessage(out);
    if (!hasMore) {
        m_recoveryContext.reset(NULL);
    }
}

/**
 * Process the updates from a recovery message
 */
void PersistentTable::processRecoveryMessage(RecoveryProtoMsg* message, Pool *pool) {
    switch (message->msgType()) {
    case RECOVERY_MSG_TYPE_SCAN_TUPLES: {
        if (activeTupleCount() == 0) {
            uint32_t tupleCount = message->totalTupleCount();
            for (int i = 0; i < m_indexCount; i++) {
                m_indexes[i]->ensureCapacity(tupleCount);
            }
        }
        loadTuplesFromNoHeader(*message->stream(), pool);
        break;
    }
    default:
        throwFatalException("Attempted to process a recovery message of unknown type %d", message->msgType());
    }
}

/**
 * Create a tree index on the primary key and then iterate it and hash
 * the tuple data.
 */
size_t PersistentTable::hashCode() {
    TableIndexScheme sourceScheme = m_pkeyIndex->getScheme();
    sourceScheme.setTree();
    boost::scoped_ptr<TableIndex> pkeyIndex(TableIndexFactory::getInstance(sourceScheme));
    TableIterator iter(this, m_data.begin());
    TableTuple tuple(schema());
    while (iter.next(tuple)) {
        pkeyIndex->addEntry(&tuple);
    }

    pkeyIndex->moveToEnd(true);

    size_t hashCode = 0;
    while (true) {
         tuple = pkeyIndex->nextValue();
         if (tuple.isNullTuple()) {
             break;
         }
         tuple.hashCode(hashCode);
    }
    return hashCode;
}

void PersistentTable::notifyBlockWasCompactedAway(TBPtr block) {
    if (m_blocksNotPendingSnapshot.find(block) != m_blocksNotPendingSnapshot.end()) {
        assert(m_blocksPendingSnapshot.find(block) == m_blocksPendingSnapshot.end());
    } else {
        assert(m_COWContext != NULL);
        assert(m_blocksPendingSnapshot.find(block) != m_blocksPendingSnapshot.end());
        m_COWContext->notifyBlockWasCompactedAway(block);
    }

}

void PersistentTable::swapTuples(TableTuple sourceTuple, TableTuple destinationTuple) {
    ::memcpy(destinationTuple.address(), sourceTuple.address(), m_tupleLength);
    sourceTuple.setActiveFalse();
    assert(!sourceTuple.isPendingDeleteOnUndoRelease());
    if (!sourceTuple.isPendingDelete()) {
        updateWithSameKeyFromAllIndexes(sourceTuple, destinationTuple);
    }

    // GWW: update data pointers in TupleJournalMap
    TupleJournal *tjournal = findTupleJournal(sourceTuple.address());
    if(tjournal != NULL) {
    	if(tjournal->currentTuple == sourceTuple.address()) {
    		// main case, update the address of currentTuple in journal
    		// and delete and re-insert the journal mapping for this tuple
    		// and may need to update mapping for its beforeTuple too
    		tjournal->currentTuple = destinationTuple.address();
			deleteTupleJournalMapping(sourceTuple.address());
			insertTupleJournalMapping(destinationTuple.address(), tjournal);
    	}

    	if(tjournal->changer != NULL &&
    			tjournal->changer->beforeTuple == sourceTuple.address()) {
    		// possiblly the tuple is one beforeTuple
    		// just delete and re-insert the journal mapping for this beforeTuple
    		// we do not need to worry about the beforeTuple pointer in the journal
    		// of the currentTuple, because they point to the same one
    		tjournal->changer->beforeTuple = destinationTuple.address();
    		deleteTupleJournalMapping(sourceTuple.address());
    		insertTupleJournalMapping(destinationTuple.address(), tjournal);
    	}
    }
}

bool PersistentTable::doCompactionWithinSubset(TBBucketMap *bucketMap) {

	VOLT_TRACE("!!!COMPACTION!!!");
    /**
     * First find the two best candidate blocks
     */
    TBPtr fullest;
    TBBucketI fullestIterator;
    bool foundFullest = false;
    for (int ii = (TUPLE_BLOCK_NUM_BUCKETS - 1); ii >= 0; ii--) {
        fullestIterator = (*bucketMap)[ii]->begin();
        if (fullestIterator != (*bucketMap)[ii]->end()) {
            foundFullest = true;
            fullest = *fullestIterator;
            break;
        }
    }
    if (!foundFullest) {
        //std::cout << "Could not find a fullest block for compaction" << std::endl;
        return false;
    }

    int fullestBucketChange = -1;
    while (fullest->hasFreeTuples()) {
        TBPtr lightest;
        TBBucketI lightestIterator;
        bool foundLightest = false;

        for (int ii = 0; ii < TUPLE_BLOCK_NUM_BUCKETS; ii++) {
            lightestIterator = (*bucketMap)[ii]->begin();
            if (lightestIterator != (*bucketMap)[ii]->end()) {
                lightest = *lightestIterator;
                if (lightest != fullest) {
                    foundLightest = true;
                    break;
                } else {
                    lightestIterator++;
                    if (lightestIterator != (*bucketMap)[ii]->end()) {
                        lightest = *lightestIterator;
                        foundLightest = true;
                        break;
                    }
                }
            }
        }
        if (!foundLightest) {
//            TBMapI iter = m_data.begin();
//            while (iter != m_data.end()) {
//                std::cout << "Block " << static_cast<void*>(iter.data().get()) << " has " <<
//                        iter.data()->activeTuples() << " active tuples and " << iter.data()->lastCompactionOffset()
//                        << " last compaction offset and is in bucket " <<
//                        static_cast<void*>(iter.data()->currentBucket().get()) <<
//                        std::endl;
//                iter++;
//            }
//
//            for (int ii = 0; ii < TUPLE_BLOCK_NUM_BUCKETS; ii++) {
//                std::cout << "Bucket " << ii << "(" << static_cast<void*>((*bucketMap)[ii].get()) << ") has size " << (*bucketMap)[ii]->size() << std::endl;
//                if (!(*bucketMap)[ii]->empty()) {
//                    TBBucketI bucketIter = (*bucketMap)[ii]->begin();
//                    while (bucketIter != (*bucketMap)[ii]->end()) {
//                        std::cout << "\t" << static_cast<void*>(bucketIter->get()) << std::endl;
//                        bucketIter++;
//                    }
//                }
//            }
//
//            std::cout << "Could not find a lightest block for compaction" << std::endl;
            return false;
        }

        std::pair<int, int> bucketChanges = fullest->merge(this, lightest);
        int tempFullestBucketChange = bucketChanges.first;
        if (tempFullestBucketChange != -1) {
            fullestBucketChange = tempFullestBucketChange;
        }

        if (lightest->isEmpty()) {
            notifyBlockWasCompactedAway(lightest);
            m_data.erase(lightest->address());
            m_blocksWithSpace.erase(lightest);
            m_blocksNotPendingSnapshot.erase(lightest);
            m_blocksPendingSnapshot.erase(lightest);
            lightest->swapToBucket(TBBucketPtr());
        } else {
            int lightestBucketChange = bucketChanges.second;
            if (lightestBucketChange != -1) {
                lightest->swapToBucket((*bucketMap)[lightestBucketChange]);
            }
        }
    }

    if (fullestBucketChange != -1) {
        fullest->swapToBucket((*bucketMap)[fullestBucketChange]);
    }
    if (!fullest->hasFreeTuples()) {
        m_blocksWithSpace.erase(fullest);
    }
    return true;
}

void PersistentTable::doIdleCompaction() {
    if (!m_blocksNotPendingSnapshot.empty()) {
        doCompactionWithinSubset(&m_blocksNotPendingSnapshotLoad);
    }
    if (!m_blocksPendingSnapshot.empty()) {
        doCompactionWithinSubset(&m_blocksPendingSnapshotLoad);
    }
}

void PersistentTable::doForcedCompaction() {
    if (m_recoveryContext != NULL)
    {
        std::cout << "Deferring compaction until recovery is complete..." << std::endl;
        return;
    }
    bool hadWork1 = true;
    bool hadWork2 = true;
    std::cout << "Doing forced compaction with allocated tuple count " << allocatedTupleCount() << std::endl;
    int failedCompactionCountBefore = m_failedCompactionCount;
    while (compactionPredicate()) {
        assert(hadWork1 || hadWork2);
        if (!hadWork1 && !hadWork2) {
            /*
             * If this code is reached it means that the compaction predicate
             * thinks that it should be possible to merge some blocks,
             * but there were no blocks found in the load buckets that were
             * eligible to be merged. This is a bug in either the predicate
             * or more likely the code that moves blocks from bucket to bucket.
             * This isn't fatal because the list of blocks with free space
             * and deletion of empty blocks is handled independently of
             * the book keeping for load buckets and merging. As the load
             * of the missing (missing from the load buckets)
             * blocks changes they should end up being inserted
             * into the bucketing system again and will be
             * compacted if necessary or deleted when empty.
             * This is a work around for ENG-939
             */
            if (m_failedCompactionCount % 5000 == 0) {
                std::cerr << "Compaction predicate said there should be " <<
                             "blocks to compact but no blocks were found " <<
                             "to be eligible for compaction. This has " <<
                             "occured " << m_failedCompactionCount <<
                             " times." << std::endl;
            }
            if (m_failedCompactionCount == 0) {
                printBucketInfo();
            }
            m_failedCompactionCount++;
            break;
        }
        if (!m_blocksNotPendingSnapshot.empty() && hadWork1) {
            //std::cout << "Compacting blocks not pending snapshot " << m_blocksNotPendingSnapshot.size() << std::endl;
            hadWork1 = doCompactionWithinSubset(&m_blocksNotPendingSnapshotLoad);
        }
        if (!m_blocksPendingSnapshot.empty() && hadWork2) {
            //std::cout << "Compacting blocks pending snapshot " << m_blocksPendingSnapshot.size() << std::endl;
            hadWork2 = doCompactionWithinSubset(&m_blocksPendingSnapshotLoad);
        }
    }
    //If compactions have been failing lately, but it didn't fail this time
    //then compaction progressed until the predicate was satisfied
    if (failedCompactionCountBefore > 0 && failedCompactionCountBefore == m_failedCompactionCount) {
        std::cerr << "Recovered from a failed compaction scenario and compacted to the point that the compaction predicate was satisfied after " << failedCompactionCountBefore << " failed attempts" << std::endl;
        m_failedCompactionCount = 0;
    }
    assert(!compactionPredicate());
    std::cout << "Finished forced compaction with allocated tuple count " << allocatedTupleCount() << std::endl;
}

void PersistentTable::printBucketInfo() {
    std::cout << std::endl;
    TBMapI iter = m_data.begin();
    while (iter != m_data.end()) {
        std::cout << "Block " << static_cast<void*>(iter.data()->address()) << " has " <<
                iter.data()->activeTuples() << " active tuples and " << iter.data()->lastCompactionOffset()
                << " last compaction offset and is in bucket " <<
                static_cast<void*>(iter.data()->currentBucket().get()) <<
                std::endl;
        iter++;
    }

    boost::unordered_set<TBPtr>::iterator blocksNotPendingSnapshot = m_blocksNotPendingSnapshot.begin();
    std::cout << "Blocks not pending snapshot: ";
    while (blocksNotPendingSnapshot != m_blocksNotPendingSnapshot.end()) {
        std::cout << static_cast<void*>((*blocksNotPendingSnapshot)->address()) << ",";
        blocksNotPendingSnapshot++;
    }
    std::cout << std::endl;
    for (int ii = 0; ii < m_blocksNotPendingSnapshotLoad.size(); ii++) {
        if (m_blocksNotPendingSnapshotLoad[ii]->empty()) {
            continue;
        }
        std::cout << "Bucket " << ii << "(" << static_cast<void*>(m_blocksNotPendingSnapshotLoad[ii].get()) << ") has size " << m_blocksNotPendingSnapshotLoad[ii]->size() << std::endl;
        TBBucketI bucketIter = m_blocksNotPendingSnapshotLoad[ii]->begin();
        while (bucketIter != m_blocksNotPendingSnapshotLoad[ii]->end()) {
            std::cout << "\t" << static_cast<void*>((*bucketIter)->address()) << std::endl;
            bucketIter++;
        }
    }

    boost::unordered_set<TBPtr>::iterator blocksPendingSnapshot = m_blocksPendingSnapshot.begin();
    std::cout << "Blocks pending snapshot: ";
    while (blocksPendingSnapshot != m_blocksPendingSnapshot.end()) {
        std::cout << static_cast<void*>((*blocksPendingSnapshot)->address()) << ",";
        blocksPendingSnapshot++;
    }
    std::cout << std::endl;
    for (int ii = 0; ii < m_blocksPendingSnapshotLoad.size(); ii++) {
        if (m_blocksPendingSnapshotLoad[ii]->empty()) {
            continue;
        }
        std::cout << "Bucket " << ii << "(" << static_cast<void*>(m_blocksPendingSnapshotLoad[ii].get()) << ") has size " << m_blocksPendingSnapshotLoad[ii]->size() << std::endl;
        TBBucketI bucketIter = m_blocksPendingSnapshotLoad[ii]->begin();
        while (bucketIter != m_blocksPendingSnapshotLoad[ii]->end()) {
            std::cout << "\t" << static_cast<void*>((*bucketIter)->address()) << std::endl;
            bucketIter++;
        }
    }
    std::cout << std::endl;
}
