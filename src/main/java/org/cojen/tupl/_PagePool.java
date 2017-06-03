/*
 *  Copyright (C) 2011-2017 Cojen.org
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.LatchCondition;

/**
 * Pool of spare page buffers not currently in use by nodes.
 *
 * @author Generated by PageAccessTransformer from PagePool.java
 */
final class _PagePool extends Latch {
    private final transient LatchCondition mQueue;
    private final long[] mPool;
    private int mPos;

    _PagePool(int pageSize, int poolSize) {
        mQueue = new LatchCondition();
        long[] pool = DirectPageOps.p_allocArray(poolSize);
        for (int i=0; i<poolSize; i++) {
            pool[i] = DirectPageOps.p_calloc(pageSize);
        }
        mPool = pool;
        mPos = poolSize;
    }

    /**
     * Remove a page from the pool, waiting for one to become available if necessary.
     */
    long remove() {
        acquireExclusive();
        try {
            int pos;
            while ((pos = mPos) == 0) {
                mQueue.await(this, -1, 0);
            }
            return mPool[mPos = pos - 1];
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Add a previously removed page back into the pool.
     */
    void add(long page) {
        acquireExclusive();
        try {
            int pos = mPos;
            mPool[pos] = page;
            // Adjust pos after assignment to prevent harm if an array bounds exception was thrown.
            mPos = pos + 1;
            mQueue.signal();
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Must be called when object is no longer referenced.
     */
    void delete() {
        acquireExclusive();
        try {
            for (int i=0; i<mPos; i++) {
                long page = mPool[i];
                mPool[i] = DirectPageOps.p_null();
                DirectPageOps.p_delete(page);
            }
        } finally {
            releaseExclusive();
        }
    }
}
