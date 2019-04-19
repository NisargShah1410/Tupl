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

import java.util.Arrays;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.LatchCondition;

import static org.cojen.tupl.LockResult.*;

/**
 * Partially reentrant shared/upgradable/exclusive lock, with fair acquisition
 * methods. Locks are owned by LockOwners, not Threads. Implementation relies on
 * latching for mutual exclusion, but condition variable logic is used for
 * transferring ownership between LockOwners.
 *
 * @author Generated by PageAccessTransformer from Lock.java
 * @see _LockManager
 */
/*P*/
final class _Lock {
    long mIndexId;
    byte[] mKey;
    int mHashCode;

    // Next entry in _LockManager hash collision chain.
    _Lock mLockManagerNext;

    // 0xxx...  shared locks held (up to (2^31)-2)
    // 1xxx...  upgradable and shared locks held (up to (2^31)-2)
    // 1111...  exclusive lock held (~0)
    int mLockCount;

    // Exclusive or upgradable locker.
    _LockOwner mOwner;

    // _LockOwner instance if one shared locker, or else a hashtable for more. Field is re-used
    // to indicate when an exclusive lock has ghosted an entry, which should be deleted when
    // the transaction commits. A C-style union type would be handy. Object is a _GhostFrame if
    // entry is ghosted.
    private Object mSharedLockOwnersObj;

    // Waiters for upgradable lock. Contains only regular waiters.
    LatchCondition mQueueU;

    // Waiters for shared and exclusive locks. Contains regular and shared waiters.
    LatchCondition mQueueSX;

    /**
     * @param locker optional locker
     */
    boolean isAvailable(_LockOwner locker) {
        return mLockCount >= 0 || mOwner == locker;
    }

    /**
     * Called with any latch held, which is retained.
     *
     * @return UNOWNED, OWNED_SHARED, OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     */
    LockResult check(_LockOwner locker) {
        int count = mLockCount;
        return mOwner == locker
            ? (count == ~0 ? OWNED_EXCLUSIVE : OWNED_UPGRADABLE)
            : ((count != 0 && isSharedLockOwner(locker)) ? OWNED_SHARED : UNOWNED);
    }

    /**
     * Called with exclusive latch held, which is retained. If return value is TIMED_OUT_LOCK,
     * the locker's mWaitingFor field is set to this _Lock as a side-effect.
     *
     * @return INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED, OWNED_SHARED,
     * OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     * @throws IllegalStateException if too many shared locks
     */
    LockResult tryLockShared(Latch latch, _Locker locker, long nanosTimeout) {
        if (mOwner == locker) {
            return mLockCount == ~0 ? OWNED_EXCLUSIVE : OWNED_UPGRADABLE;
        }

        LatchCondition queueSX = mQueueSX;
        if (queueSX != null) {
            if (isSharedLockOwner(locker)) {
                return OWNED_SHARED;
            }
            if (nanosTimeout == 0) {
                locker.mWaitingFor = this;
                return TIMED_OUT_LOCK;
            }
        } else {
            int count = mLockCount;
            if (count == ~0) {
                if (nanosTimeout == 0) {
                    locker.mWaitingFor = this;
                    return TIMED_OUT_LOCK;
                }
                mQueueSX = queueSX = new LatchCondition();
            } else if (count != 0 && isSharedLockOwner(locker)) {
                return OWNED_SHARED;
            } else {
                if ((count & 0x7fffffff) >= 0x7ffffffe) {
                    throw new IllegalStateException("Too many shared locks held");
                }
                addSharedLockOwner(count, locker);
                return ACQUIRED;
            }
        }

        locker.mWaitingFor = this;
        long nanosEnd = nanosTimeout < 0 ? 0 : (System.nanoTime() + nanosTimeout);

        // Await for shared lock.
        int w = queueSX.awaitShared(latch, nanosTimeout, nanosEnd);
        queueSX = mQueueSX;

        if (queueSX == null) {
            // Assume _LockManager was closed.
            locker.mWaitingFor = null;
            return INTERRUPTED;
        }

        // After consuming one signal, next shared waiter must be signaled, and so on.
        if (!queueSX.signalNextShared()) {
            // Indicate that last signal has been consumed, and also free memory.
            mQueueSX = null;
        }

        if (w >= 1) {
            int count = mLockCount;
            if ((count & 0x7fffffff) >= 0x7ffffffe) {
                throw new IllegalStateException("Too many shared locks held");
            }
            addSharedLockOwner(count, locker);
            locker.mWaitingFor = null;
            return ACQUIRED;
        } else if (w == 0) {
            return TIMED_OUT_LOCK;
        } else {
            locker.mWaitingFor = null;
            return INTERRUPTED;
        }
    }

    /**
     * Called with exclusive latch held, which is retained. If return value is TIMED_OUT_LOCK,
     * the locker's mWaitingFor field is set to this _Lock as a side-effect.
     *
     * @return ILLEGAL, INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED,
     * OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     */
    LockResult tryLockUpgradable(Latch latch, _Locker locker, long nanosTimeout) {
        if (mOwner == locker) {
            return mLockCount == ~0 ? OWNED_EXCLUSIVE : OWNED_UPGRADABLE;
        }

        int count = mLockCount;
        if (count != 0 && isSharedLockOwner(locker)) {
            if (!locker.canAttemptUpgrade(count)) {
                return ILLEGAL;
            }
            if (count > 0) {
                // Give the impression that lock was always held upgradable. This prevents
                // pushing the lock into the locker twice.
                mLockCount = (count - 1) | 0x80000000;
                mOwner = locker;
                return OWNED_UPGRADABLE;
            }
        }

        LatchCondition queueU = mQueueU;
        if (queueU != null) {
            if (nanosTimeout == 0) {
                locker.mWaitingFor = this;
                return TIMED_OUT_LOCK;
            }
        } else {
            if (count >= 0) {
                mLockCount = count | 0x80000000;
                mOwner = locker;
                return ACQUIRED;
            }
            if (nanosTimeout == 0) {
                locker.mWaitingFor = this;
                return TIMED_OUT_LOCK;
            }
            mQueueU = queueU = new LatchCondition();
        }

        locker.mWaitingFor = this;
        long nanosEnd = nanosTimeout < 0 ? 0 : (System.nanoTime() + nanosTimeout);

        // Await for upgradable lock.
        int w = queueU.await(latch, nanosTimeout, nanosEnd);
        queueU = mQueueU;

        if (queueU == null) {
            // Assume _LockManager was closed.
            locker.mWaitingFor = null;
            return INTERRUPTED;
        }

        if (queueU.isEmpty()) {
            // Indicate that last signal has been consumed, and also free memory.
            mQueueU = null;
        }

        if (w >= 1) {
            mLockCount |= 0x80000000;
            mOwner = locker;
            locker.mWaitingFor = null;
            return ACQUIRED;
        } else if (w == 0) {
            return TIMED_OUT_LOCK;
        } else {
            locker.mWaitingFor = null;
            return INTERRUPTED;
        }
    }

    /**
     * Called with exclusive latch held, which is retained. If return value is TIMED_OUT_LOCK,
     * the locker's mWaitingFor field is set to this _Lock as a side-effect.
     *
     * @return ILLEGAL, INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED, UPGRADED, or
     * OWNED_EXCLUSIVE
     */
    LockResult tryLockExclusive(Latch latch, _Locker locker, long nanosTimeout) {
        final LockResult ur = tryLockUpgradable(latch, locker, nanosTimeout);
        if (!ur.isHeld() || ur == OWNED_EXCLUSIVE) {
            return ur;
        }

        LatchCondition queueSX = mQueueSX;
        quick: {
            if (queueSX == null) {
                if (mLockCount == 0x80000000) {
                    mLockCount = ~0;
                    return ur == OWNED_UPGRADABLE ? UPGRADED : ACQUIRED;
                } else if (nanosTimeout != 0) {
                    mQueueSX = queueSX = new LatchCondition();
                    break quick;
                }
            } else if (nanosTimeout != 0) {
                break quick;
            }
            if (ur == ACQUIRED) {
                unlockUpgradable();
            }
            locker.mWaitingFor = this;
            return TIMED_OUT_LOCK;
        }

        locker.mWaitingFor = this;
        long nanosEnd = nanosTimeout < 0 ? 0 : (System.nanoTime() + nanosTimeout);

        // Await for exclusive lock.
        int w = queueSX.await(latch, nanosTimeout, nanosEnd);
        queueSX = mQueueSX;

        if (queueSX == null) {
            // Assume _LockManager was closed.
            locker.mWaitingFor = null;
            return INTERRUPTED;
        }

        if (queueSX.isEmpty()) {
            // Indicate that last signal has been consumed, and also free memory.
            mQueueSX = null;
        }

        if (w >= 1) {
            mLockCount = ~0;
            locker.mWaitingFor = null;
            return ur == OWNED_UPGRADABLE ? UPGRADED : ACQUIRED;
        } else {
            if (ur == ACQUIRED) {
                unlockUpgradable();
            }
            if (w == 0) {
                return TIMED_OUT_LOCK;
            } else {
                locker.mWaitingFor = null;
                return INTERRUPTED;
            }
        }
    }

    /**
     * Called internally to unlock an upgradable lock which was just
     * acquired. Implementation is a just a smaller version of the regular
     * unlock method. It doesn't have to deal with ghosts.
     */
    private void unlockUpgradable() {
        mOwner = null;
        LatchCondition queueU = mQueueU;
        if (queueU != null) {
            // Signal at most one upgradable lock waiter.
            queueU.signal();
        }
        mLockCount &= 0x7fffffff;
    }

    /**
     * Called with exclusive latch held, which is released unless an exception is thrown.
     *
     * @param ht briefly released and re-acquired for deleting a ghost
     * @throws IllegalStateException if lock not held
     */
    void unlock(_LockOwner locker, _LockManager.LockHT ht) {
        if (mOwner == locker) {
            deleteGhost(ht);

            mOwner = null;
            LatchCondition queueU = mQueueU;
            int count = mLockCount;

            if (count != ~0) {
                // Unlocking an upgradable lock.
                if ((mLockCount = count & 0x7fffffff) == 0 && queueU == null && mQueueSX == null) {
                    // _Lock is now completely unused.
                    ht.remove(this);
                } else if (queueU != null) {
                    // Signal at most one upgradable lock waiter.
                    queueU.signalRelease(ht);
                    return;
                }
            } else {
                // Unlocking an exclusive lock.
                mLockCount = 0;
                LatchCondition queueSX = mQueueSX;
                if (queueSX == null) {
                    if (queueU == null) {
                        // _Lock is now completely unused.
                        ht.remove(this);
                    } else {
                        // Signal at most one upgradable lock waiter.
                        queueU.signalRelease(ht);
                        return;
                    }
                } else {
                    if (queueU != null) {
                        // Signal at most one upgradable lock waiter, and keep the latch.
                        queueU.signal();
                    }
                    // Signal first shared lock waiter. Queue doesn't contain any exclusive
                    // lock waiters, because they would need to acquire upgradable lock first,
                    // which was held.
                    queueSX.signalRelease(ht);
                    return;
                }
            }
        } else {
            int count = mLockCount;

            unlock: {
                if ((count & 0x7fffffff) != 0) {
                    Object sharedObj = mSharedLockOwnersObj;
                    if (sharedObj == locker) {
                        mSharedLockOwnersObj = null;
                        break unlock;
                    } else if (sharedObj instanceof LockOwnerHTEntry[]) {
                        LockOwnerHTEntry[] entries = (LockOwnerHTEntry[]) sharedObj;
                        if (lockerHTremove(entries, locker)) {
                            if (count == 2) {
                                mSharedLockOwnersObj = lockerHTgetOne(entries);
                            }
                            break unlock;
                        }
                    }
                }

                if (isClosed(locker)) {
                    ht.releaseExclusive();
                    return;
                }

                throw new IllegalStateException("Lock not held");
            }

            mLockCount = --count;

            LatchCondition queueSX = mQueueSX;
            if (count == 0x80000000) {
                if (queueSX != null) {
                    // Signal any exclusive lock waiter. Queue shouldn't contain any shared
                    // lock waiters, because no exclusive lock is held. In case there are any,
                    // signal them instead.
                    queueSX.signalRelease(ht);
                    return;
                }
            } else if (count == 0 && queueSX == null && mQueueU == null) {
                // _Lock is now completely unused.
                ht.remove(this);
            }
        }

        ht.releaseExclusive();
    }

    /**
     * Called with exclusive latch held, which is released unless an exception is thrown.
     *
     * @param latch briefly released and re-acquired for deleting a ghost
     * @throws IllegalStateException if lock not held or too many shared locks
     */
    void unlockToShared(_LockOwner locker, Latch latch) {
        if (mOwner == locker) {
            deleteGhost(latch);

            mOwner = null;
            LatchCondition queueU = mQueueU;
            int count = mLockCount;

            if (count != ~0) {
                // Unlocking upgradable lock into shared.
                if ((count &= 0x7fffffff) >= 0x7ffffffe) {
                    // Retain upgradable lock when this happens.
                    // mLockCount = count;
                    throw new IllegalStateException("Too many shared locks held");
                }
                addSharedLockOwner(count, locker);
            } else {
                // Unlocking exclusive lock into shared.
                addSharedLockOwner(0, locker);
                LatchCondition queueSX = mQueueSX;
                if (queueSX != null) {
                    if (queueU != null) {
                        // Signal at most one upgradable lock waiter, and keep the latch.
                        queueU.signal();
                    }
                    // Signal the first shared lock waiter. Queue doesn't contain any exclusive
                    // lock waiters, because they would need to acquire upgradable lock first,
                    // which was held.
                    queueSX.signalRelease(latch);
                    return;
                }
            }

            // Signal at most one upgradable lock waiter.
            if (queueU != null) {
                queueU.signalRelease(latch);
                return;
            }
        } else if ((mLockCount == 0 || !isSharedLockOwner(locker)) && !isClosed(locker)) {
            throw new IllegalStateException("Lock not held");
        }

        latch.releaseExclusive();
    }

    /**
     * Called with exclusive latch held, which is released unless an exception is thrown.
     *
     * @param latch briefly released and re-acquired for deleting a ghost
     * @throws IllegalStateException if lock not held
     */
    void unlockToUpgradable(_LockOwner locker, Latch latch) {
        if (mOwner != locker) {
            if (isClosed(locker)) {
                latch.releaseExclusive();
                return;
            }
            String message = "Exclusive or upgradable lock not held";
            if (mLockCount == 0 || !isSharedLockOwner(locker)) {
                message = "Lock not held";
            }
            throw new IllegalStateException(message);
        }
        if (mLockCount != ~0) {
            // Already upgradable.
            latch.releaseExclusive();
            return;
        }
        deleteGhost(latch);
        mLockCount = 0x80000000;
        LatchCondition queueSX = mQueueSX;
        if (queueSX == null) {
            latch.releaseExclusive();
        } else {
            queueSX.signalSharedRelease(latch);
        }
    }

    private static boolean isClosed(_LockOwner locker) {
        _LocalDatabase db = locker.getDatabase();
        return db != null && db.isClosed();
    }

    /**
     * @param latch might be briefly released and re-acquired
     */
    void deleteGhost(Latch latch) {
        // TODO: Unlock due to rollback can be optimized. It never needs to actually delete
        // ghosts, because the undo actions replaced them.

        Object obj = mSharedLockOwnersObj;
        if (!(obj instanceof _GhostFrame)) {
            return;
        }

        final _GhostFrame frame = (_GhostFrame) obj;
        mSharedLockOwnersObj = null;

        final _LocalDatabase db = mOwner.getDatabase();
        if (db == null) {
            // Database was closed.
            return;
        }

        frame.action(db, latch, this);
    }

    /**
     * _Lock must be held by given locker, which is either released or transferred into a lock
     * set. Exclusive locks are transferred, and any other type is released. Method must be
     * called by _LockManager with exclusive latch held, which is released unless an exception
     * is thrown.
     *
     * @param ht used to remove this lock if not exclusively held and is no longer used; must
     * be exclusively held
     * @param pending lock set to add into; can be null initially
     * @return new or original lock set
     * @throws IllegalStateException if lock not held
     */
    _PendingTxn transferExclusive(_LockOwner locker, _LockManager.LockHT ht, _PendingTxn pending) {
        if (mLockCount == ~0) {
            // Held exclusively. Must double check expected owner because _Locker tracks _Lock
            // instance multiple times for handling upgrades. Without this check, _Lock can be
            // added to pending set multiple times.
            if (mOwner == locker) {
                if (pending == null) {
                    pending = new _PendingTxn(this);
                } else {
                    pending.add(this);
                }
                mOwner = pending;
            }
            ht.releaseExclusive();
        } else {
            // Unlock upgradable or shared lock. Note that ht is passed along, to allow the
            // latch to be released. This also permits it to delete a ghost, but this shouldn't
            // be possible. An exclusive lock would have been held and detected above.
            unlock(locker, ht);
        }
        return pending;
    }

    boolean matches(long indexId, byte[] key, int hash) {
        return mHashCode == hash && mIndexId == indexId && Arrays.equals(mKey, key);
    }

    /**
     * Must hold exclusive lock to be valid.
     */
    void setGhostFrame(_GhostFrame frame) {
        mSharedLockOwnersObj = frame;
    }

    void setSharedLockOwner(_LockOwner owner) {
        mSharedLockOwnersObj = owner;
    }

    /**
     * Is null, a _LockOwner, a LockOwnerHTEntry[], or a _GhostFrame.
     */
    Object getSharedLockOwner() {
        return mSharedLockOwnersObj;
    }

    /**
     * @param lockType TYPE_SHARED, TYPE_UPGRADABLE, or TYPE_EXCLUSIVE
     */
    void detectDeadlock(_Locker locker, int lockType, long nanosTimeout)
        throws DeadlockException
    {
        _DeadlockDetector detector = new _DeadlockDetector(locker);
        if (detector.scan()) {
            Object att = findOwnerAttachment(locker, lockType);
            throw new DeadlockException(nanosTimeout, att,
                                        detector.mGuilty,
                                        detector.newDeadlockSet(lockType));
        }
    }

    /**
     * Find an exclusive owner attachment, or the first found shared owner attachment. Might
     * acquire and release a shared latch to access the shared owner attachment.
     *
     * @param locker pass null if already latched
     * @param lockType TYPE_SHARED, TYPE_UPGRADABLE, or TYPE_EXCLUSIVE
     */
    Object findOwnerAttachment(_Locker locker, int lockType) {
        // See note in _DeadlockDetector regarding unlatched access to this _Lock.

        _LockOwner owner = mOwner;
        if (owner != null) {
            Object att = owner.attachment();
            if (att != null) {
                return att;
            }
        }

        if (lockType != _LockManager.TYPE_EXCLUSIVE) {
            // Only an exclusive lock request can be blocked by shared locks.
            return null;
        }

        Object sharedObj = mSharedLockOwnersObj;
        if (sharedObj == null) {
            return null;
        }

        if (sharedObj instanceof _LockOwner) {
            return ((_LockOwner) sharedObj).attachment();
        }

        if (sharedObj instanceof LockOwnerHTEntry[]) {
            if (locker != null) {
                // Need a latch to safely check the shared lock owner hashtable.
                _LockManager manager = locker.mManager;
                if (manager != null) {
                    _LockManager.LockHT ht = manager.getLockHT(mHashCode);
                    ht.acquireShared();
                    try {
                        return findOwnerAttachment(null, lockType);
                    } finally {
                        ht.releaseShared();
                    }
                }
            } else {
                LockOwnerHTEntry[] entries = (LockOwnerHTEntry[]) sharedObj;

                for (int i=entries.length; --i>=0; ) {
                    for (LockOwnerHTEntry e = entries[i]; e != null; e = e.mNext) {
                        owner = e.mOwner;
                        if (owner != null) {
                            Object att = owner.attachment();
                            if (att != null) {
                                return att;
                            }
                        }
                    }
                }
            }
        }

        return null;
    }

    /**
     * Note: Caller can short-circuit this test by checking the lock count first. If non-zero,
     * then this method should return false. If the caller has already determined that mQueueSX
     * is non-null, then the short-circuit test is redundant and isn't useful.
     */
    private boolean isSharedLockOwner(_LockOwner locker) {
        Object sharedObj = mSharedLockOwnersObj;
        if (sharedObj == locker) {
            return true;
        }
        if (sharedObj instanceof LockOwnerHTEntry[]) {
            return lockerHTcontains((LockOwnerHTEntry[]) sharedObj, locker);
        }
        return false;
    }

    private void addSharedLockOwner(int count, _LockOwner locker) {
        count++;
        Object sharedObj = mSharedLockOwnersObj;
        if (sharedObj == null) {
            mSharedLockOwnersObj = locker;
        } else if (sharedObj instanceof LockOwnerHTEntry[]) {
            LockOwnerHTEntry[] entries = (LockOwnerHTEntry[]) sharedObj;
            lockerHTadd(entries, count & 0x7fffffff, locker);
        } else {
            // Initial capacity of must be a power of 2.
            LockOwnerHTEntry[] entries = new LockOwnerHTEntry[8];
            lockerHTadd(entries, (_LockOwner) sharedObj);
            lockerHTadd(entries, locker);
            mSharedLockOwnersObj = entries;
        }
        mLockCount = count;
    }

    private static boolean lockerHTcontains(LockOwnerHTEntry[] entries, _LockOwner locker) {
        int hash = locker.hashCode();
        for (LockOwnerHTEntry e = entries[hash & (entries.length - 1)]; e != null; e = e.mNext) {
            if (e.mOwner == locker) {
                return true;
            }
        }
        return false;
    }

    private void lockerHTadd(LockOwnerHTEntry[] entries, int newSize, _LockOwner locker) {
        if (newSize > (entries.length >> 1)) {
            int capacity = entries.length << 1;
            LockOwnerHTEntry[] newEntries = new LockOwnerHTEntry[capacity];
            int newMask = capacity - 1;

            for (int i=entries.length; --i>=0; ) {
                for (LockOwnerHTEntry e = entries[i]; e != null; ) {
                    LockOwnerHTEntry next = e.mNext;
                    int ix = e.mOwner.hashCode() & newMask;
                    e.mNext = newEntries[ix];
                    newEntries[ix] = e;
                    e = next;
                }
            }

            mSharedLockOwnersObj = entries = newEntries;
        }

        lockerHTadd(entries, locker);
    }

    private static void lockerHTadd(LockOwnerHTEntry[] entries, _LockOwner locker) {
        int index = locker.hashCode() & (entries.length - 1);
        LockOwnerHTEntry e = new LockOwnerHTEntry();
        e.mOwner = locker;
        e.mNext = entries[index];
        entries[index] = e;
    }

    private static boolean lockerHTremove(LockOwnerHTEntry[] entries, _LockOwner locker) {
        int index = locker.hashCode() & (entries.length - 1);
        for (LockOwnerHTEntry e = entries[index], prev = null; e != null; e = e.mNext) {
            if (e.mOwner == locker) {
                if (prev == null) {
                    entries[index] = e.mNext;
                } else {
                    prev.mNext = e.mNext;
                }
                return true;
            } else {
                prev = e;
            }
        }
        return false;
    }

    private static _LockOwner lockerHTgetOne(LockOwnerHTEntry[] entries) {
        for (LockOwnerHTEntry e : entries) {
            if (e != null) {
                return e.mOwner;
            }
        }
        throw new AssertionError("No lockers in hashtable");
    }

    /**
     * Entry for simple hashtable of LockOwners.
     */
    static final class LockOwnerHTEntry {
        _LockOwner mOwner;
        LockOwnerHTEntry mNext;
    }
}
