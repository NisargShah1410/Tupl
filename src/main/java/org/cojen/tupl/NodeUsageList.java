/*
 *  Copyright 2014-2015 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl;

import java.io.IOException;

import org.cojen.tupl.util.Latch;

import static org.cojen.tupl.Node.*;
import static org.cojen.tupl.PageOps.*;

/**
 * List of Nodes, ordered from least to most recently used.
 *
 * @author Brian S O'Neill
 */
@SuppressWarnings("serial")
final class NodeUsageList extends Latch {
    // Allocate an unevictable node.
    static final int MODE_UNEVICTABLE = 1;

    // Don't evict a node when trying to allocate another.
    static final int MODE_NO_EVICT = 2;

    final transient LocalDatabase mDatabase;
    private final int mPageSize;
    private int mMaxSize;
    private int mSize;
    private Node mMostRecentlyUsed;
    private Node mLeastRecentlyUsed;

    NodeUsageList(LocalDatabase db, int maxSize) {
        mDatabase = db;
        mPageSize = db.pageSize();
        acquireExclusive();
        mMaxSize = maxSize;
        releaseExclusive();
    }

    int pageSize() {
        return mPageSize;
    }

    /**
     * Initialize and preallocate a minimum amount of nodes.
     *
     * @param arena optional
     */
    void initialize(Object arena, int min) throws DatabaseException, OutOfMemoryError {
        while (--min >= 0) {
            acquireExclusive();
            if (mSize >= mMaxSize) {
                releaseExclusive();
                break;
            }
            doAllocLatchedNode(arena, 0).releaseExclusive();
        }
    }

    int size() {
        acquireShared();
        int size = mSize;
        releaseShared();
        return size;
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively, with an undefined id and a
     * clean state.
     *
     * @param trial pass 1 for less aggressive recycle attempt
     * @param mode MODE_UNEVICTABLE | MODE_NO_EVICT
     * @return null if no nodes can be recycled or created
     */
    Node tryAllocLatchedNode(int trial, int mode) throws IOException {
        acquireExclusive();

        int limit = mSize;
        do {
            Node node = mLeastRecentlyUsed;
            Node moreUsed;
            if (node == null || (moreUsed = node.mMoreUsed) == null) {
                // Grow the cache if possible.
                if (mSize < mMaxSize) {
                    return doAllocLatchedNode(null, mode);
                } else if (node == null) {
                    break;
                }
            } else {
                // Move node to the most recently used position.
                mLeastRecentlyUsed = moreUsed;
                moreUsed.mLessUsed = null;
                node.mMoreUsed = null;
                (node.mLessUsed = mMostRecentlyUsed).mMoreUsed = node;
                mMostRecentlyUsed = node;
            }

            if (!node.tryAcquireExclusive()) {
                continue;
            }

            if (trial == 1) {
                if (node.mCachedState != CACHED_CLEAN) {
                    if (mSize < mMaxSize) {
                        // Grow the cache instead of evicting.
                        node.releaseExclusive();
                        return doAllocLatchedNode(null, mode);
                    } else if ((mode & MODE_NO_EVICT) != 0) {
                        node.releaseExclusive();
                        break;
                    }
                }

                // For first attempt, release the latch early to prevent blocking other
                // allocations while node is evicted. Subsequent attempts retain the latch,
                // preventing potential allocation starvation.

                releaseExclusive();

                if (node.evict(mDatabase)) {
                    if ((mode & MODE_UNEVICTABLE) != 0) {
                        node.mUsageList.makeUnevictable(node);
                    }
                    // Return with node latch still held.
                    return node;
                }

                acquireExclusive();
            } else if ((mode & MODE_NO_EVICT) != 0) {
                if (node.mCachedState != CACHED_CLEAN) {
                    // MODE_NO_EVICT is only used by non-durable database. It ensures that
                    // all clean nodes are least recently used, so no need to keep looking.
                    node.releaseExclusive();
                    break;
                }
            } else {
                try {
                    if (node.evict(mDatabase)) {
                        if ((mode & MODE_UNEVICTABLE) != 0) {
                            NodeUsageList usageList = node.mUsageList;
                            if (usageList == this) {
                                doMakeUnevictable(node);
                            } else {
                                releaseExclusive();
                                usageList.makeUnevictable(node);
                                // Return with node latch still held.
                                return node;
                            }
                        }
                        releaseExclusive();
                        // Return with node latch still held.
                        return node;
                    }
                } catch (Throwable e) {
                    releaseExclusive();
                    throw e;
                }
            }
        } while (--limit > 0);

        releaseExclusive();

        return null;
    }

    /**
     * Caller must acquire latch, which is released by this method.
     *
     * @param arena optional
     * @param mode MODE_UNEVICTABLE
     */
    private Node doAllocLatchedNode(Object arena, int mode) throws DatabaseException {
        try {
            mDatabase.checkClosed();

            /*P*/ byte[] page;
            /*P*/ // [
            page = p_calloc(arena, mPageSize);
            /*P*/ // |
            /*P*/ // page = mDatabase.mFullyMapped ? p_nonTreePage() : p_calloc(arena, mPageSize);
            /*P*/ // ]

            Node node = new Node(this, page);
            node.acquireExclusive();
            mSize++;

            if ((mode & MODE_UNEVICTABLE) == 0) {
                if ((node.mLessUsed = mMostRecentlyUsed) == null) {
                    mLeastRecentlyUsed = node;
                } else {
                    mMostRecentlyUsed.mMoreUsed = node;
                }
                mMostRecentlyUsed = node;
            }

            // Return with node latch still held.
            return node;
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Indicate that a non-root node is most recently used. Root node is not managed in usage
     * list and cannot be evicted. Caller must hold any latch on node. Latch is never released
     * by this method, even if an exception is thrown.
     */
    void used(final Node node) {
        // Because this method can be a bottleneck, don't wait for exclusive latch. If node is
        // popular, it will get more chances to be identified as most recently used. This
        // strategy works well enough because cache eviction is always a best-guess approach.
        if (tryAcquireExclusive()) {
            Node moreUsed = node.mMoreUsed;
            if (moreUsed != null) {
                Node lessUsed = node.mLessUsed;
                if ((moreUsed.mLessUsed = lessUsed) == null) {
                    mLeastRecentlyUsed = moreUsed;
                } else {
                    lessUsed.mMoreUsed = moreUsed;
                }
                node.mMoreUsed = null;
                (node.mLessUsed = mMostRecentlyUsed).mMoreUsed = node;
                mMostRecentlyUsed = node;
            }
            releaseExclusive();
        }
    }

    /**
     * Indicate that node is least recently used, allowing it to be recycled immediately
     * without evicting another node. Node must be latched by caller, which is always released
     * by this method.
     */
    void unused(final Node node) {
        // Node latch is held to ensure that it isn't used for new allocations too soon. In
        // particular, it might be used for an unevictable allocation. This method would end up
        // erroneously moving the node back into the usage list. 

        try {
            acquireExclusive();
        } catch (Throwable e) {
            node.releaseExclusive();
            throw e;
        }

        try {
            if (mMaxSize == 0) {
                // Closed.
                return;
            }
            Node lessUsed = node.mLessUsed;
            if (lessUsed == null) {
                // Node might already be least...
                if (node.mMoreUsed != null) {
                    // ...confirmed.
                    return;
                }
                // ...Node isn't in the usage list at all.
            } else {
                Node moreUsed = node.mMoreUsed;
                if ((lessUsed.mMoreUsed = moreUsed) == null) {
                    mMostRecentlyUsed = lessUsed;
                } else {
                    moreUsed.mLessUsed = lessUsed;
                }
                node.mLessUsed = null;
            }
            Node least = mLeastRecentlyUsed;
            node.mMoreUsed = least;
            if (least != null) {
                least.mLessUsed = node;
            }
            mLeastRecentlyUsed = node;
        } finally {
            // The node latch must be released before releasing the usage list latch, to
            // prevent the node from being immediately promoted to the most recently used by
            // tryAllocLatchedNode. The caller would acquire the usage list latch, fail to
            // acquire the node latch, and then the node gets falsely promoted.
            node.releaseExclusive();
            releaseExclusive();
        }
    }

    /**
     * Allow a Node which was allocated as unevictable to be evictable, starting off as the
     * most recently used.
     */
    void makeEvictable(final Node node) {
        acquireExclusive();
        try {
            // Only insert if not closed and if not already in the list. The node latch doesn't
            // need to be held, and so a concurrent call to the unused method might insert the
            // node sooner.
            if (mMaxSize != 0 && node.mMoreUsed == null && node.mLessUsed == null) {
                Node most = mMostRecentlyUsed;
                node.mLessUsed = most;
                if (most == null) {
                    mLeastRecentlyUsed = node;
                } else {
                    most.mMoreUsed = node;
                }
                mMostRecentlyUsed = node;
            }
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Allow a Node which was allocated as unevictable to be evictable, as the least recently
     * used.
     */
    void makeEvictableNow(final Node node) {
        acquireExclusive();
        try {
            // See comment in the makeEvictable method.
            if (mMaxSize != 0 && node.mMoreUsed == null && node.mLessUsed == null) {
                Node least = mLeastRecentlyUsed;
                node.mMoreUsed = least;
                if (least == null) {
                    mMostRecentlyUsed = node;
                } else {
                    least.mLessUsed = node;
                }
                mLeastRecentlyUsed = node;
            }
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Allow a Node which was allocated as evictable to be unevictable.
     */
    void makeUnevictable(final Node node) {
        acquireExclusive();
        try {
            if (mMaxSize != 0) {
                doMakeUnevictable(node);
            }
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Caller must hold latch.
     */
    private void doMakeUnevictable(final Node node) {
        final Node lessUsed = node.mLessUsed;
        final Node moreUsed = node.mMoreUsed;
        if (lessUsed == null) {
            mLeastRecentlyUsed = moreUsed;
            if (moreUsed != null) {
                node.mMoreUsed = null;
                moreUsed.mLessUsed = null;
            } else if (node == mMostRecentlyUsed) {
                mMostRecentlyUsed = null;
            }
        } else {
            node.mLessUsed = null;
            if (moreUsed != null) {
                node.mMoreUsed = null;
                lessUsed.mMoreUsed = moreUsed;
                moreUsed.mLessUsed = lessUsed;
            } else {
                mMostRecentlyUsed = lessUsed;
                lessUsed.mMoreUsed = null;
            }
        }
    }

    /**
     * Must be called when object is no longer referenced.
     */
    void delete() {
        acquireExclusive();
        try {
            // Prevent new allocations.
            mMaxSize = 0;

            Node node = mLeastRecentlyUsed;
            mLeastRecentlyUsed = null;
            mMostRecentlyUsed = null;

            while (node != null) {
                Node next = node.mMoreUsed;
                node.mLessUsed = null;
                node.mMoreUsed = null;

                // Free memory and make node appear to be evicted.
                node.delete(mDatabase);

                node = next;
            }
        } finally {
            releaseExclusive();
        }
    }
}
