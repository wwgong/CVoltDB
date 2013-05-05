/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

#ifndef UNDOLOG_H_
#define UNDOLOG_H_
#include "common/Pool.hpp"
#include "common/UndoQuantum.h"
#include "boost/pool/object_pool.hpp"

#include "common/executorcontext.hpp"
#include "common/debuglog.h"

#include <vector>
#include <deque>
#include <stdint.h>
#include <iostream>
#include <cassert>


namespace voltdb
{
    class UndoLog
    {
    public:
        UndoLog();
        virtual ~UndoLog();
        /**
         * Clean up all outstanding state in the UndoLog.  Essentially
         * contains all the work that should be performed by the
         * destructor.  Needed to work around a memory-free ordering
         * issue in VoltDBEngine's destructor.
         */
        void clear();

        inline UndoQuantum* generateUndoQuantum(int64_t nextUndoToken)
        {
            //std::cout << "Generating token " << nextUndoToken
            //          << " lastUndo: " << m_lastUndoToken
            //          << " lastRelease: " << m_lastReleaseToken << std::endl;
            // Since ExecutionSite is using monotonically increasing
            // token values, every new quanta we're asked to generate should be
            // larger than any token value we've seen before
            assert(nextUndoToken > m_lastUndoToken);
            assert(nextUndoToken > m_lastReleaseToken);
            m_lastUndoToken = nextUndoToken;
            Pool *pool = NULL;
            if (m_undoDataPools.size() == 0) {
                pool = new Pool();
            } else {
                pool = m_undoDataPools.back();
                m_undoDataPools.pop_back();
            }
            assert(pool);
            UndoQuantum *undoQuantum =
                new (pool->allocate(sizeof(UndoQuantum)))
                UndoQuantum(nextUndoToken, pool);
           m_undoQuantums.push_back(undoQuantum);

            return undoQuantum;
        }

        inline UndoQuantum* generate2UndoQuantum(int64_t txnId, int64_t nextUndoToken)
        {
            //std::cout << "Generating token " << nextUndoToken
            //          << " lastUndo: " << m_lastUndoToken
            //          << " lastRelease: " << m_lastReleaseToken << std::endl;
            // Since ExecutionSite is using monotonically increasing
            // token values, every new quanta we're asked to generate should be
            // larger than any token value we've seen before
            assert(nextUndoToken >= m_lastUndoToken);
            assert(nextUndoToken > m_lastReleaseToken);
            assert(m_undoQuantums.empty() || nextUndoToken >= m_undoQuantums.back()->getUndoToken());

            m_lastUndoToken = nextUndoToken;
            Pool *pool = NULL;
            if (m_undoDataPools.size() == 0) {
                pool = new Pool();
            } else {
                pool = m_undoDataPools.back();
                m_undoDataPools.pop_back();
            }
            assert(pool);
            UndoQuantum *undoQuantum =
                new (pool->allocate(sizeof(UndoQuantum)))
                UndoQuantum(nextUndoToken, pool);

            undoQuantum->setTxnId(txnId);
            m_undoQuantums.push_back(undoQuantum);

#if defined(DEBUG) || defined(_DEBUG) || defined(_DEBUG_)
			// GWW
            boost::unordered_map<int64_t, std::deque<UndoQuantum*> >::iterator itr =
            		ExecutorContext::getExecutorContext()->m_txnUndoQuantumsMap.find(txnId);
            if(itr != ExecutorContext::getExecutorContext()->m_txnUndoQuantumsMap.end()){
            	itr->second.push_back(undoQuantum);
            } else{
            	std::deque<UndoQuantum*> undoQuantums;
            	undoQuantums.push_back(undoQuantum);
            	ExecutorContext::getExecutorContext()->m_txnUndoQuantumsMap.insert(
            			std::pair< int64_t, std::deque<UndoQuantum*> >(txnId, undoQuantums));
            }
#endif
            return undoQuantum;
        }

        /*
         * Undo all undoable actions from the latest undo quantum back
         * until the undo quantum with the specified undo token.
         */
        inline void undo(const int64_t undoToken) {
            //std::cout << "Undoing token " << undoToken
            //          << " lastUndo: " << m_lastUndoToken
            //          << " lastRelease: " << m_lastReleaseToken << std::endl;
            // This ensures that undo is only ever called after
            // generateUndoToken has been called
            assert(m_lastReleaseToken < m_lastUndoToken);
            // This ensures that we don't attempt to undo something in
            // the distant past.  In some cases ExecutionSite may hand
            // us the largest token value that definitely doesn't
            // exist; this will just result in all undo quanta being undone.
            assert(undoToken >= m_lastReleaseToken);

            if (undoToken > m_lastUndoToken) {
                // a procedure may abort before it sends work to the EE
                // (informing the EE of its undo token. For example, it
                // may have invalid parameter values or, possibly, aborts
                // in user java code before executing any SQL.  Just
                // return. There is no work to do here.
                return;
            }

            m_lastUndoToken = undoToken - 1;
            while (m_undoQuantums.size() > 0) {
                UndoQuantum *undoQuantum = m_undoQuantums.back();
                const int64_t undoQuantumToken = undoQuantum->getUndoToken();
                if (undoQuantumToken < undoToken) {
                    return;
                }

                m_undoQuantums.pop_back();
                Pool *pool = undoQuantum->getDataPool();
                undoQuantum->undo();
                pool->purge();
                m_undoDataPools.push_back(pool);

                if(undoQuantumToken == undoToken) {
                    return;
                }
            }
        }

        inline void undo2(const int64_t txnId, const int64_t beginUndoToken, const int64_t endUndoToken) {
            assert(m_lastReleaseToken < m_lastUndoToken);
            assert(beginUndoToken >= m_lastReleaseToken);

            assert(txnId > ExecutorContext::getExecutorContext()->lastCommittedTxnId());

            ExecutorContext::getExecutorContext()->m_releasingTxnId = txnId;

			VOLT_TRACE("GWW - undo2, begin = %jd, end = %jd, txnid = %jd, # of undoQuantums = %jd, last release token = %jd m_lastUndoToken = %jd\n",
						(intmax_t)beginUndoToken, (intmax_t)endUndoToken,
						(intmax_t)txnId, (intmax_t)m_undoQuantums.size(),
						m_lastReleaseToken, m_lastUndoToken);

            if (beginUndoToken > m_lastUndoToken) {
                return;
            }

            // GWW: only reuse undo token for undo during prepare
            if(ExecutorContext::getExecutorContext()->currentTxnId() == txnId)
            	m_lastUndoToken = beginUndoToken - 1;

			for(int i = (int)m_undoQuantums.size(); i > 0; i--) {
            	UndoQuantum *undoQuantum = m_undoQuantums.at(i - 1);
            	const int64_t undoQuantumToken = undoQuantum->getUndoToken();

            	if(undoQuantumToken > endUndoToken) {
            		continue;
            	}

            	if(undoQuantumToken < beginUndoToken) {
            		return;
            	}

            	m_undoQuantums.erase(m_undoQuantums.begin() + i - 1);

            	assert(undoQuantum->txnId() == txnId);

                Pool *pool = undoQuantum->getDataPool();
                undoQuantum->undo();
                pool->purge();
                m_undoDataPools.push_back(pool);

                if(undoQuantumToken == beginUndoToken) {
                    return;
                }
            }

#if defined(DEBUG) || defined(_DEBUG) || defined(_DEBUG_)
            ExecutorContext::getExecutorContext()->m_txnUndoQuantumsMap.erase(txnId);
#endif
        }

        /*
         * Release memory held by all undo quantums up to and
         * including the quantum with the specified token. It will be
         * impossible to undo these actions in the future.
         */
        inline void release(const int64_t undoToken) {
            //std::cout << "Releasing token " << undoToken
            //          << " lastUndo: " << m_lastUndoToken
            //          << " lastRelease: " << m_lastReleaseToken << std::endl;
            assert(m_lastReleaseToken < undoToken);
            m_lastReleaseToken = undoToken;
            while (m_undoQuantums.size() > 0) {
                UndoQuantum *undoQuantum = m_undoQuantums.front();
                const int64_t undoQuantumToken = undoQuantum->getUndoToken();
                if (undoQuantumToken > undoToken) {
                    return;
                }

                m_undoQuantums.pop_front();
                Pool *pool = undoQuantum->getDataPool();
                undoQuantum->release();
                pool->purge();
                m_undoDataPools.push_back(pool);
                if(undoQuantumToken == undoToken) {
                    return;
                }
            }
        }

        inline void release2(const int64_t txnId, const int64_t beginUndoToken, const int64_t endUndoToken) {

            assert(m_lastReleaseToken < endUndoToken);

            assert(txnId > ExecutorContext::getExecutorContext()->lastCommittedTxnId());

            ExecutorContext::getExecutorContext()->m_releasingTxnId = txnId;

            VOLT_TRACE("GWW - release2, begin = %jd, end = %jd, txnid = %jd, # of undoQuantums in deque = %jd\n",
            		(intmax_t)beginUndoToken, (intmax_t)endUndoToken,
            		(intmax_t)txnId, (intmax_t)m_undoQuantums.size());

            m_lastReleaseToken = endUndoToken;

            while (m_undoQuantums.size() > 0) {
                UndoQuantum *undoQuantum = m_undoQuantums.front();
                const int64_t undoQuantumToken = undoQuantum->getUndoToken();
                if (undoQuantumToken > endUndoToken) {
                    return;
                }

                m_undoQuantums.pop_front();

                assert(undoQuantumToken >= beginUndoToken);
                assert(undoQuantum->txnId() == txnId);

                Pool *pool = undoQuantum->getDataPool();
                undoQuantum->release();
                pool->purge();
                m_undoDataPools.push_back(pool);
                if(undoQuantumToken == endUndoToken) {
                    return;
                }
            }
#if defined(DEBUG) || defined(_DEBUG) || defined(_DEBUG_)
            ExecutorContext::getExecutorContext()->m_txnUndoQuantumsMap.erase(txnId);
#endif
            VOLT_TRACE("done release for txn %jd\n", txnId);
        }

        int64_t getSize() const
        {
            int64_t total = 0;
            for (int i = 0; i < m_undoDataPools.size(); i++)
            {
                total += m_undoDataPools[i]->getAllocatedMemory();
            }
            for (int i = 0; i < m_undoQuantums.size(); i++)
            {
                total += m_undoQuantums[i]->getAllocatedMemory();
            }
            return total;
        }

        void debug() {
        	std::string s = "UndoQuantums list in UndoLog:\n";
        	std::deque<UndoQuantum*>::iterator itr;
        	for(itr = m_undoQuantums.begin(); itr != m_undoQuantums.end(); itr++) {
        		s.append("\tUndoQuantum: txnid %lu, # undoAction %jd", (*itr)->m_txnId, (intmax_t)((*itr)->size()));
        	}
        }

    private:
        // These two values serve no real purpose except to provide
        // the capability to assert various properties about the undo tokens
        // handed to the UndoLog.  Currently, this makes the following
        // assumptions about how the Java side is managing undo tokens:
        //
        // 1. Java is generating monotonically increasing undo tokens.
        // There may be gaps, but every new token to generateUndoQuantum is
        // larger than every other token previously seen by generateUndoQuantum
        //
        // 2. Right now, the ExecutionSite _always_ releases the
        // largest token generated during a transaction at the end of the
        // transaction, even if the entire transaction was rolled back.  This
        // means that release may get called even if there are no undo quanta
        // present.

        // m_lastUndoToken is the largest token that could possibly be called
        // for real undo; any larger token is either undone or has
        // never existed
        int64_t m_lastUndoToken;

        // m_lastReleaseToken is the largest token that definitely
        // doesn't exist; any smaller value has already been released,
        // any larger value might exist (gaps are possible)
        int64_t m_lastReleaseToken;

        std::vector<Pool*> m_undoDataPools;
        std::deque<UndoQuantum*> m_undoQuantums;
    };
}
#endif /* UNDOLOG_H_ */
