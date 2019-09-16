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

package org.cojen.tupl.core;

import java.io.IOException;

import java.util.concurrent.TimeUnit;

import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.InvalidTransactionException;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnmodifiableReplicaException;

import static org.cojen.tupl.core.RedoOps.*;
import static org.cojen.tupl.core.Utils.*;

/**
 * Standard transaction implementation. The name "LocalTransaction" is used to imply that the
 * transaction is local to the current machine and not remotely accessed, although no remote
 * database layer exists. This class could just as well have been named "TransactionImpl".
 *
 * @author Brian S O'Neill
 */
public final class LocalTransaction extends Locker implements Transaction {
    public static final LocalTransaction BOGUS = new LocalTransaction();

    static final int
        HAS_SCOPE   = 1, // When set, scope has been entered but not logged.
        HAS_COMMIT  = 2, // When set, transaction has committable changes.
        HAS_TRASH   = 4, /* When set, fragmented values are in the trash and must be
                            fully deleted after committing the top-level scope. */
        HAS_PREPARE = 8; // When set, transaction is prepared for two-phase commit.

    final LocalDatabase mDatabase;
    final TransactionContext mContext;
    RedoWriter mRedo;
    DurabilityMode mDurabilityMode;

    LockMode mLockMode;
    long mLockTimeoutNanos;
    private int mHasState;
    private long mSavepoint;
    long mTxnId;

    private UndoLog mUndoLog;

    private Object mAttachment;

    // Is an exception if transaction is borked, BOGUS if bogus.
    private Object mBorked;

    LocalTransaction(LocalDatabase db, RedoWriter redo, DurabilityMode durabilityMode,
                     LockMode lockMode, long timeoutNanos)
    {
        super(db.mLockManager);
        mDatabase = db;
        mContext = db.selectTransactionContext(this);
        mRedo = redo;
        mDurabilityMode = durabilityMode;
        mLockMode = lockMode;
        mLockTimeoutNanos = timeoutNanos;
    }

    // Constructor for redo recovery.
    LocalTransaction(LocalDatabase db, long txnId, LockMode lockMode, long timeoutNanos) {
        this(db, null, DurabilityMode.NO_REDO, lockMode, timeoutNanos);
        mTxnId = txnId;
    }

    // Constructor for undo recovery.
    LocalTransaction(LocalDatabase db, long txnId, int hasState) {
        this(db, null, DurabilityMode.NO_REDO, LockMode.UPGRADABLE_READ, 0);
        mTxnId = txnId;
        mHasState = hasState;
    }

    // Constructor for BOGUS transaction.
    private LocalTransaction() {
        super(null);
        mDatabase = null;
        mContext = null;
        mRedo = null;
        mDurabilityMode = DurabilityMode.NO_REDO;
        mLockMode = LockMode.UNSAFE;
        mBorked = this;
    }

    // Used by recovery.
    final void recoveredScope(long savepoint, int hasState) {
        ParentScope parentScope = super.scopeEnter();
        parentScope.mLockMode = mLockMode;
        parentScope.mLockTimeoutNanos = mLockTimeoutNanos;
        parentScope.mHasState = mHasState;
        parentScope.mSavepoint = mSavepoint;
        mSavepoint = savepoint;
        mHasState = hasState;
    }

    // Used by recovery.
    final void recoveredUndoLog(UndoLog undo) {
        mContext.register(undo);
        mUndoLog = undo;
    }

    // Used by recovery, before passing transaction to a recovery handler.
    final void recoverPrepared(RedoWriter redo, DurabilityMode durabilityMode,
                               LockMode lockMode, long timeoutNanos)
    {
        // When recovered from the undo log only, the commit state won't be set. Set it
        // explicitly now, in order for commit and rollback operations to actually work.
        mHasState |= HAS_COMMIT;

        mRedo = redo;
        mLockMode = lockMode;
        mDurabilityMode = durabilityMode;
        mLockTimeoutNanos = timeoutNanos;
    }

    @Override
    public LocalDatabase getDatabase() {
        return mDatabase;
    }

    @Override
    public void attach(Object obj) {
        mAttachment = obj;
    }

    @Override
    public Object attachment() {
        return mAttachment;
    }

    @Override
    public final void lockMode(LockMode mode) {
        if (mode == null) {
            throw new IllegalArgumentException("Lock mode is null");
        } else {
            bogusCheck();
            mLockMode = mode;
        }
    }

    @Override
    public final LockMode lockMode() {
        return mLockMode;
    }

    @Override
    public final void lockTimeout(long timeout, TimeUnit unit) {
        bogusCheck();
        mLockTimeoutNanos = Utils.toNanos(timeout, unit);
    }

    @Override
    public final long lockTimeout(TimeUnit unit) {
        return unit.convert(mLockTimeoutNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public final void durabilityMode(DurabilityMode mode) {
        if (mode == null) {
            throw new IllegalArgumentException("Durability mode is null");
        } else {
            bogusCheck();
            mDurabilityMode = mode;
        }
    }

    @Override
    public final DurabilityMode durabilityMode() {
        return mDurabilityMode;
    }

    @Override
    public final void check() throws DatabaseException {
        Object borked = mBorked;
        if (borked != null) {
            check(borked);
        }
    }

    private void check(Object borked) throws DatabaseException {
        if (borked == BOGUS) {
            throw new IllegalStateException("Transaction is bogus");
        } else if (borked instanceof Throwable) {
            throw new InvalidTransactionException((Throwable) borked);
        } else {
            throw new InvalidTransactionException(String.valueOf(borked));
        }
    }

    private void bogusCheck() {
        if (mBorked == BOGUS) {
            throw new IllegalStateException("Transaction is bogus");
        }
    }

    @Override
    public final void commit() throws IOException {
        Object borked = mBorked;
        if (borked != null) {
            if (borked == BOGUS) {
                return;
            }
            check(borked);
        }

        try {
            ParentScope parentScope = mParentScope;
            if (parentScope == null) {
                UndoLog undo = mUndoLog;
                if (undo == null) {
                    int hasState = mHasState;
                    if ((hasState & HAS_COMMIT) != 0) {
                        long commitPos = mContext.redoCommitFinal(mRedo, mTxnId, mDurabilityMode);
                        mHasState = hasState & ~(HAS_SCOPE | HAS_COMMIT);
                        if (commitPos != 0) {
                            if (mDurabilityMode == DurabilityMode.SYNC) {
                                mRedo.txnCommitSync(this, commitPos);
                            } else {
                                commitPending(commitPos, null);
                                return;
                            }
                        }
                    }
                    super.scopeUnlockAll();
                } else {
                    // Holding the shared commit lock ensures that the redo log
                    // doesn't disappear before the undo log. Lingering undo
                    // logs with no corresponding redo log are treated as
                    // aborted. Recovery would erroneously rollback committed
                    // transactions.
                    final CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
                    long commitPos;
                    try {
                        if ((commitPos = (mHasState & HAS_COMMIT)) != 0) {
                            commitPos = mContext.redoCommitFinal(mRedo, mTxnId, mDurabilityMode);
                            mHasState &= ~(HAS_SCOPE | HAS_COMMIT);
                        }
                        // Indicates that undo log should be truncated instead
                        // of rolled back during recovery. Commit lock can now
                        // be released safely. See recoveryCleanup.
                        undo.commit();
                    } finally {
                        shared.release();
                    }

                    if (commitPos != 0) {
                        // Durably sync the redo log after releasing the commit lock,
                        // preventing additional blocking.
                        if (mDurabilityMode == DurabilityMode.SYNC) {
                            mRedo.txnCommitSync(this, commitPos);
                        } else {
                            commitPending(commitPos, undo);
                            return;
                        }
                    }

                    // Calling this deletes any ghosts too.
                    super.scopeUnlockAll();

                    // Truncate obsolete log entries after releasing locks. Recovery might need
                    // to re-delete ghosts, which is only possible with a complete undo log.
                    // Truncate operation can be interrupted by a checkpoint, allowing a
                    // partial undo log to be seen by the recovery.
                    undo.truncate();

                    mContext.unregister(undo);
                    mUndoLog = null;

                    int hasState = mHasState;
                    if ((hasState & HAS_TRASH) != 0) {
                        FragmentedTrash.emptyTrash(mDatabase.fragmentedTrash(), mTxnId);
                        mHasState = hasState & ~HAS_TRASH;
                    }
                }

                mTxnId = 0;
            } else {
                int hasState = mHasState;
                if ((hasState & HAS_COMMIT) != 0) {
                    mContext.redoCommit(mRedo, mTxnId);
                    mHasState = hasState & ~(HAS_SCOPE | HAS_COMMIT);
                    parentScope.mHasState |= HAS_COMMIT;
                }

                super.promote();

                UndoLog undo = mUndoLog;
                if (undo != null) {
                    mSavepoint = undo.scopeCommit();
                }
            }
        } catch (Throwable e) {
            borked(e, true, true); // rollback = true, rethrow = true
        }
    }

    private void commitPending(long commitPos, UndoLog undo) throws IOException {
        PendingTxn pending = transferExclusive();
        pending.mContext = mContext;
        pending.mTxnId = mTxnId;
        pending.mCommitPos = commitPos;
        pending.mUndoLog = undo;
        pending.mHasState = mHasState;
        pending.attach(mAttachment);

        mUndoLog = null;
        mHasState = 0;
        mTxnId = 0;

        mRedo.txnCommitPending(pending);
    }

    /**
     * Commit combined with a store operation.
     *
     * @param undoTxn pass this if undo logging is required, BOGUS otherwise
     */
    final void storeCommit(LocalTransaction undoTxn, BTreeCursor cursor, byte[] value)
        throws IOException
    {
        if (mRedo == null) {
            cursor.storeNoRedo(this, value);
            commit();
            return;
        }

        check();

        // Implementation consists of redoStore and commit logic, without extraneous checks.

        long txnId = mTxnId;

        final CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
        try {
            if (txnId == 0) {
                txnId = assignTransactionId();
            }
        } catch (Throwable e) {
            shared.release();
            throw e;
        }

        try {
            int hasState = mHasState;
            byte[] key = cursor.mKey;

            ParentScope parentScope = mParentScope;
            if (parentScope == null) {
                long commitPos;
                try {
                    cursor.storeNoRedo(undoTxn, value);

                    if ((hasState & HAS_SCOPE) == 0) {
                        mContext.redoEnter(mRedo, txnId);
                        mHasState = hasState | HAS_SCOPE;
                    }

                    long cursorId = cursor.mCursorId;
                    if (cursorId == 0) {
                        long indexId = cursor.mTree.mId;
                        if (value == null) {
                            commitPos = mContext.redoDeleteCommitFinal
                                (mRedo, txnId, indexId, key, mDurabilityMode);
                        } else {
                            commitPos = mContext.redoStoreCommitFinal
                                (mRedo, txnId, indexId, key, value, mDurabilityMode);
                        }
                    } else {
                        if (value == null) {
                            mContext.redoCursorDelete(mRedo, cursorId, txnId, key);
                        } else {
                            mContext.redoCursorStore(mRedo, cursorId, txnId, key, value);
                        }
                        commitPos = mContext.redoCommitFinal(mRedo, txnId, mDurabilityMode);
                    }
                } catch (Throwable e) {
                    shared.release();
                    throw e;
                }

                mHasState = hasState & ~(HAS_SCOPE | HAS_COMMIT);

                UndoLog undo = mUndoLog;
                if (undo == null) {
                    shared.release();
                    if (commitPos != 0) {
                        if (mDurabilityMode == DurabilityMode.SYNC) {
                            mRedo.txnCommitSync(this, commitPos);
                        } else {
                            commitPending(commitPos, null);
                            return;
                        }
                    }
                    super.scopeUnlockAll();
                } else {
                    try {
                        undo.commit();
                    } finally {
                        shared.release();
                    }

                    if (commitPos != 0) {
                        if (mDurabilityMode == DurabilityMode.SYNC) {
                            mRedo.txnCommitSync(this, commitPos);
                        } else {
                            commitPending(commitPos, undo);
                            return;
                        }
                    }

                    super.scopeUnlockAll();

                    undo.truncate();

                    mContext.unregister(undo);
                    mUndoLog = null;

                    if ((hasState & HAS_TRASH) != 0) {
                        FragmentedTrash.emptyTrash(mDatabase.fragmentedTrash(), mTxnId);
                        mHasState = hasState & ~HAS_TRASH;
                    }
                }

                mTxnId = 0;
            } else {
                try {
                    // Always undo when inside a scope.
                    cursor.storeNoRedo(this, value);

                    long cursorId = cursor.mCursorId;
                    if (cursorId == 0) {
                        long indexId = cursor.mTree.mId;
                        if ((hasState & HAS_SCOPE) == 0) {
                            setScopeState(parentScope);
                            if (value == null) {
                                mContext.redoDelete
                                    (mRedo, OP_TXN_DELETE, txnId, indexId, key);
                            } else {
                                mContext.redoStore
                                    (mRedo, OP_TXN_STORE, txnId, indexId, key, value);
                            }
                        } else {
                            if (value == null) {
                                mContext.redoDelete
                                    (mRedo, OP_TXN_DELETE_COMMIT, txnId, indexId, key);
                            } else {
                                mContext.redoStore
                                    (mRedo, OP_TXN_STORE_COMMIT, txnId, indexId, key, value);
                            }
                        }
                    } else {
                        if ((hasState & HAS_SCOPE) == 0) {
                            setScopeState(parentScope);
                            if (value == null) {
                                mContext.redoCursorDelete(mRedo, cursorId, txnId, key);
                            } else {
                                mContext.redoCursorStore(mRedo, cursorId, txnId, key, value);
                            }
                        } else {
                            if (value == null) {
                                mContext.redoCursorDelete(mRedo, cursorId, txnId, key);
                            } else {
                                mContext.redoCursorStore(mRedo, cursorId, txnId, key, value);
                            }
                            mContext.redoCommit(mRedo, txnId);
                        }
                    }
                } finally {
                    shared.release();
                }

                mHasState = hasState & ~(HAS_SCOPE | HAS_COMMIT);
                parentScope.mHasState |= HAS_COMMIT;

                super.promote();

                UndoLog undo = mUndoLog;
                if (undo != null) {
                    mSavepoint = undo.scopeCommit();
                }
            }
        } catch (Throwable e) {
            borked(e, true, true); // rollback = true, rethrow = true
        }
    }

    @Override
    public final void commitAll() throws IOException {
        while (true) {
            commit();
            if (mParentScope == null) {
                break;
            }
            exit();
        }
    }

    @Override
    public final void enter() throws IOException {
        check();

        try {
            ParentScope parentScope = super.scopeEnter();
            parentScope.mLockMode = mLockMode;
            parentScope.mLockTimeoutNanos = mLockTimeoutNanos;
            parentScope.mHasState = mHasState;

            UndoLog undo = mUndoLog;
            if (undo != null) {
                parentScope.mSavepoint = mSavepoint;
                mSavepoint = undo.scopeEnter();
            }

            // Scope and commit states are set upon first actual use of this scope.
            mHasState &= ~(HAS_SCOPE | HAS_COMMIT);
        } catch (Throwable e) {
            borked(e, true, true); // rollback = true, rethrow = true
        }
    }

    @Override
    public final void exit() {
        if (mBorked != null) {
            super.scopeExit();
            return;
        }

        try {
            ParentScope parentScope = mParentScope;
            if (parentScope == null) {
                try {
                    int hasState = mHasState;
                    if ((hasState & HAS_SCOPE) != 0) {
                        mContext.redoRollbackFinal(mRedo, mTxnId);
                    }
                    mHasState = 0;
                } catch (UnmodifiableReplicaException e) {
                    // Suppress and let undo proceed.
                }

                UndoLog undo = mUndoLog;
                if (undo != null) {
                    undo.rollback();
                }

                // Exit and release all locks obtained in this scope.
                super.scopeExit();

                mSavepoint = 0;
                if (undo != null) {
                    mContext.unregister(undo);
                    mUndoLog = null;
                }

                mTxnId = 0;
            } else {
                try {
                    int hasState = mHasState;
                    if ((hasState & HAS_SCOPE) != 0) {
                        mContext.redoRollback(mRedo, mTxnId);
                        mHasState = hasState & ~(HAS_SCOPE | HAS_COMMIT);
                    }
                } catch (UnmodifiableReplicaException e) {
                    // Suppress and let undo proceed.
                }

                UndoLog undo = mUndoLog;
                if (undo != null) {
                    undo.scopeRollback(mSavepoint);
                }

                // Exit and release all locks obtained in this scope.
                super.scopeExit();

                mLockMode = parentScope.mLockMode;
                mLockTimeoutNanos = parentScope.mLockTimeoutNanos;
                // Use 'or' assignment to keep HAS_TRASH and HAS_PREPARE state.
                mHasState |= parentScope.mHasState;
                mSavepoint = parentScope.mSavepoint;
            }
        } catch (Throwable e) {
            borked(e, true, false); // rollback = true, rethrow = false
        }
    }

    @Override
    public final void reset() {
        if (mBorked == null) {
            try {
                rollback();
            } catch (Throwable e) {
                borked(e, true, false); // rollback = true, rethrow = false
            }
        } else {
            super.scopeExitAll();
        }
    }

    @Override
    public final void reset(Throwable cause) {
        try {
            if (cause == null) {
                reset();
            } else {
                borked(cause, true, false); // rollback = true, rethrow = false
            }
        } catch (Throwable e) {
            // Ignore. Transaction is borked as a side-effect.
        }
    }

    private void rollback() throws IOException {
        int hasState = mHasState;
        ParentScope parentScope = mParentScope;
        while (parentScope != null) {
            hasState |= parentScope.mHasState;
            parentScope = parentScope.mParentScope;
        }

        try {
            if ((hasState & (HAS_SCOPE | HAS_COMMIT)) != 0) {
                mContext.redoRollbackFinal(mRedo, mTxnId);
            }
            mHasState = 0;
        } catch (UnmodifiableReplicaException e) {
            // Suppress and let undo proceed.
        }

        UndoLog undo = mUndoLog;
        if (undo != null) {
            undo.rollback();
        }

        // Exit and release all locks.
        super.scopeExitAll();

        mSavepoint = 0;
        if (undo != null) {
            mContext.unregister(undo);
            mUndoLog = null;
        }

        mTxnId = 0;
    }

    @Override
    public void flush() throws IOException {
        if (mTxnId != 0) {
            mContext.flush();
        }
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder(Transaction.class.getName());

        if (this == BOGUS) {
            return b.append('.').append("BOGUS").toString();
        }

        b.append('@').append(Integer.toHexString(hashCode()));

        b.append(" {");
        b.append("id").append(": ").append(mTxnId);
        b.append(", ");
        b.append("durabilityMode").append(": ").append(mDurabilityMode);
        b.append(", ");
        b.append("lockMode").append(": ").append(mLockMode);
        b.append(", ");
        b.append("lockTimeout").append(": ");
        TimeUnit unit = Utils.inferUnit(TimeUnit.NANOSECONDS, mLockTimeoutNanos);
        Utils.appendTimeout(b, lockTimeout(unit), unit);

        Object att = mAttachment;
        if (att != null) {
            b.append(", ");
            b.append("attachment").append(": ").append(att);
        }

        Object borked = mBorked;
        if (borked != null) {
            b.append(", ");
            b.append("invalid").append(": ").append(borked);
        }

        return b.append('}').toString();
    }

    @Override
    public final LockResult lockShared(long indexId, byte[] key) throws LockFailureException {
        return super.lockShared(indexId, key, mLockTimeoutNanos);
    }

    final LockResult lockShared(long indexId, byte[] key, int hash) throws LockFailureException {
        return super.lockShared(indexId, key, hash, mLockTimeoutNanos);
    }

    @Override
    public final LockResult lockUpgradable(long indexId, byte[] key) throws LockFailureException {
        return super.lockUpgradable(indexId, key, mLockTimeoutNanos);
    }

    @Override
    public final LockResult lockExclusive(long indexId, byte[] key) throws LockFailureException {
        return super.lockExclusive(indexId, key, mLockTimeoutNanos);
    }

    final LockResult lockExclusive(long indexId, byte[] key, int hash)
        throws LockFailureException
    {
        return super.lockExclusive(indexId, key, hash, mLockTimeoutNanos);
    }

    public final void customRedo(int handlerId, byte[] message, long indexId, byte[] key)
        throws IOException
    {
        check();

        if (mRedo == null) {
            return;
        }

        long txnId = mTxnId;

        if (txnId == 0) {
            final CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
            try {
                txnId = assignTransactionId();
            } finally {
                shared.release();
            }
        }

        int hasState = mHasState;
        if ((hasState & HAS_SCOPE) == 0) {
            ParentScope parentScope = mParentScope;
            if (parentScope != null) {
                setScopeState(parentScope);
            }
            mContext.redoEnter(mRedo, txnId);
        }

        mHasState = hasState | (HAS_SCOPE | HAS_COMMIT);

        if (indexId == 0) {
            if (key != null) {
                throw new IllegalArgumentException("Key cannot be used if indexId is zero");
            }
            mContext.redoCustom(mRedo, txnId, handlerId, message);
        } else {
            LockResult result = lockCheck(indexId, key);
            if (result != LockResult.OWNED_EXCLUSIVE) {
                throw new IllegalStateException("Lock isn't owned exclusively: " + result);
            }
            mContext.redoCustomLock(mRedo, txnId, handlerId, message, indexId, key);
        }
    }

    final void customUndo(int handlerId, byte[] message) throws IOException {
        check();

        final CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
        try {
            undoLog().pushCustom(handlerId, message);
        } catch (Throwable e) {
            borked(e, true, true); // rollback = true, rethrow = true
        } finally {
            shared.release();
        }
    }

    @Override
    public final long getId() {
        long txnId = mTxnId;

        if (txnId == 0 && mRedo != null) {
            final CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
            try {
                txnId = assignTransactionId();
            } finally {
                shared.release();
            }
        }

        return txnId < 0 ? 0 : txnId;
    }

    @Override
    public final void prepare() throws IOException {
        check();

        if (mDatabase.mRecoveryHandler == null) {
            throw new IllegalStateException("Transaction recovery handler is not installed");
        }

        if (mRedo == null || mDurabilityMode == DurabilityMode.NO_REDO) {
            throw new IllegalStateException("Cannot prepare a no-redo transaction");
        }

        try {
            if ((mHasState & HAS_PREPARE) == 0) {
                pushUndoPrepare();
            }

            // Although it might seem like a good idea to not bother writing the redo message
            // if the HAS_PREPARE flag is already set, writing it each time makes the prepare
            // method also useful for performing checked flush operations.

            long commitPos = mContext.redoPrepare(mRedo, txnId(), mDurabilityMode);
            if (commitPos != 0 && mDurabilityMode == DurabilityMode.SYNC) {
                mRedo.txnCommitSync(this, commitPos);
            }
        } catch (Throwable e) {
            borked(e, true, true); // rollback = true, rethrow = true
        }

        mHasState |= HAS_PREPARE;
    }

    /**
     * To be called during redo recovery.
     */
    final void prepareNoRedo() throws IOException {
        check();

        try {
            if ((mHasState & HAS_PREPARE) == 0) {
                pushUndoPrepare();
                mHasState |= HAS_PREPARE;
            }
        } catch (Throwable e) {
            borked(e, true, true); // rollback = true, rethrow = true
        }
    }

    private void pushUndoPrepare() throws IOException {
        final CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
        try {
            undoLog().pushPrepare();
        } finally {
            shared.release();
        }
    }

    /**
     * Recovery cleanup always resets committed transactions, or those with negative
     * identifiers. When the finish parameter is false, unfinished transactions aren't reset.
     * When the finish parameter is true, all non-2PC transactions are reset.
     *
     * @return true if was reset
     */
    final boolean recoveryCleanup(boolean finish) throws IOException {
        finish = mTxnId < 0 | (finish & (mHasState & HAS_PREPARE) == 0);

        UndoLog undo = mUndoLog;
        if (undo != null) {
            finish |= undo.recoveryCleanup();
        }

        if (finish) {
            reset();
        }

        return finish;
    }

    /**
     * Caller must hold commit lock.
     *
     * @param value pass null for redo delete
     */
    final void redoStore(long indexId, byte[] key, byte[] value) throws IOException {
        check();

        if (mRedo == null) {
            return;
        }

        long txnId = mTxnId;

        if (txnId == 0) {
            txnId = assignTransactionId();
        }

        try {
            int hasState = mHasState;

            // Set early in case an exception is thrown. Caller is permitted to write redo
            // entry after making any changes, and setting the commit state ensures that
            // undo log is not prematurely truncated when commit is called.
            mHasState = hasState | HAS_COMMIT;

            if ((hasState & HAS_SCOPE) == 0) {
                ParentScope parentScope = mParentScope;
                if (parentScope != null) {
                    setScopeState(parentScope);
                }
                if (value == null) {
                    mContext.redoDelete(mRedo, OP_TXN_ENTER_DELETE, txnId, indexId, key);
                } else {
                    mContext.redoStore(mRedo, OP_TXN_ENTER_STORE, txnId, indexId, key, value);
                }
                mHasState = hasState | (HAS_SCOPE | HAS_COMMIT);
            } else {
                if (value == null) {
                    mContext.redoDelete(mRedo, OP_TXN_DELETE, txnId, indexId, key);
                } else {
                    mContext.redoStore(mRedo, OP_TXN_STORE, txnId, indexId, key, value);
                }
            }
        } catch (Throwable e) {
            borked(e, false, true); // rollback = false, rethrow = true
        }
    }

    /**
     * Caller must hold commit lock.
     *
     * @param value pass null for redo delete
     */
    final void redoCursorStore(long cursorId, byte[] key, byte[] value) throws IOException {
        check();

        long txnId = mTxnId;

        if (txnId == 0) {
            txnId = assignTransactionId();
        }

        try {
            int hasState = mHasState;

            // Set early in case an exception is thrown. Caller is permitted to write redo
            // entry after making any changes, and setting the commit state ensures that
            // undo log is not prematurely truncated when commit is called.
            mHasState = hasState | HAS_COMMIT;

            if ((hasState & HAS_SCOPE) == 0) {
                ParentScope parentScope = mParentScope;
                if (parentScope != null) {
                    setScopeState(parentScope);
                }
                mContext.redoEnter(mRedo, txnId);
            }

            if (value == null) {
                mContext.redoCursorDelete(mRedo, cursorId, txnId, key);
            } else {
                mContext.redoCursorStore(mRedo, cursorId, txnId, key, value);
            }

            mHasState = hasState | (HAS_SCOPE | HAS_COMMIT);
        } catch (Throwable e) {
            borked(e, false, true); // rollback = false, rethrow = true
        }
    }

    /**
     * Caller must hold commit lock.
     *
     * @param value pass null for redo delete
     * @return non-zero position if caller should call txnCommitSync after releasing commit lock
     */
    final long redoStoreNoLock(long indexId, byte[] key, byte[] value) throws IOException {
        check();

        if (mRedo != null) try {
            return mContext.redoStoreNoLockAutoCommit(mRedo, indexId, key, value, mDurabilityMode);
        } catch (Throwable e) {
            borked(e, false, true); // rollback = false, rethrow = true
        }

        return 0;
    }

    /**
     * Transaction id must be assigned.
     */
    private void setScopeState(ParentScope scope) throws IOException {
        int hasState = scope.mHasState;
        if ((hasState & HAS_SCOPE) == 0) {
            ParentScope parentScope = scope.mParentScope;
            if (parentScope != null) {
                setScopeState(parentScope);
            }

            mContext.redoEnter(mRedo, mTxnId);
            scope.mHasState = hasState | HAS_SCOPE;
        }
    }

    /**
     * Caller must hold commit lock if transaction id has not been assigned yet.
     */
    final long txnId() {
        long txnId = mTxnId;
        if (txnId == 0) {
            txnId = mContext.nextTransactionId();
            if (mRedo != null) {
                // Replicas set the high bit to ensure no identifier conflict with the leader.
                txnId = mRedo.adjustTransactionId(txnId);
            }
            mTxnId = txnId;
        }
        return txnId;
    }

    /**
     * Caller must hold commit lock and have verified that current transaction id is 0.
     */
    private long assignTransactionId() {
        long txnId = mContext.nextTransactionId();
        // Replicas set the high bit to ensure no identifier conflict with the leader.
        txnId = mRedo.adjustTransactionId(txnId);
        mTxnId = txnId;
        return txnId;
    }

    /**
     * Attempt to generate an identifier for a cursor to perform direct redo operations.
     * Caller must hold commit lock.
     */
    final boolean tryRedoCursorRegister(BTreeCursor cursor) throws IOException {
        if (mRedo == null || (mTxnId <= 0 && mRedo.adjustTransactionId(1) <= 0)) {
            return false;
        } else {
            doRedoCursorRegister(cursor);
            return true;
        }
    }

    private long doRedoCursorRegister(BTreeCursor cursor) throws IOException {
        long cursorId = mContext.nextTransactionId();
        try {
            mContext.redoCursorRegister(mRedo, cursorId, cursor.mTree.mId);
        } catch (Throwable e) {
            borked(e, false, true); // rollback = false, rethrow = true
        }
        BTree cursorRegistry = mDatabase.cursorRegistry();
        cursor.mCursorId = cursorId;
        mDatabase.registerCursor(cursorRegistry, cursor);
        return cursorId;
    }

    /**
     * Caller must hold commit lock.
     *
     * @param op OP_SET_LENGTH, OP_WRITE, or OP_CLEAR
     * @param buf pass EMPTY_BYTES for OP_SET_LENGTH or OP_CLEAR
     */
    final void redoCursorValueModify(BTreeCursor cursor, int op,
                                     long pos, byte[] buf, int off, long len)
        throws IOException
    {
        check();

        if (mRedo == null) {
            return;
        }

        long txnId = mTxnId;

        if (txnId == 0) {
            txnId = assignTransactionId();
        }

        try {
            int hasState = mHasState;
            if ((hasState & HAS_SCOPE) == 0) {
                ParentScope parentScope = mParentScope;
                if (parentScope != null) {
                    setScopeState(parentScope);
                }
                mContext.redoEnter(mRedo, txnId);
            }

            mHasState = hasState | (HAS_SCOPE | HAS_COMMIT);

            long cursorId = cursor.mCursorId;

            if (cursorId < 0) {
                // High bit set indicates that a redo op was written which positioned the cursor.
                cursorId &= ~(1L << 63);
            } else {
                if (cursorId == 0) {
                    cursorId = doRedoCursorRegister(cursor);
                }
                mContext.redoCursorFind(mRedo, cursorId, txnId, cursor.mKey);
                cursor.mCursorId = cursorId | (1L << 63);
            }

            if (op == BTreeValue.OP_SET_LENGTH) {
                mContext.redoCursorValueSetLength(mRedo, cursorId, txnId, pos);
            } else if (op == BTreeValue.OP_WRITE) {
                mContext.redoCursorValueWrite(mRedo, cursorId, txnId, pos, buf, off, (int) len);
            } else {
                mContext.redoCursorValueClear(mRedo, cursorId, txnId, pos, len);
            }
        } catch (Throwable e) {
            borked(e, false, true); // rollback = false, rethrow = true
        }
    }

    final void setHasTrash() {
        mHasState |= HAS_TRASH;
    }

    /**
     * Caller must hold commit lock.
     *
     * @param op OP_UNUPDATE or OP_UNDELETE
     * @param payload page with Node-encoded key/value entry
     */
    final void pushUndoStore(long indexId, byte op, /*P*/ byte[] payload, int off, int len)
        throws IOException
    {
        check();
        try {
            undoLog().pushNodeEncoded(indexId, op, payload, off, len);
        } catch (Throwable e) {
            borked(e, false, true); // rollback = false, rethrow = true
        }
    }

    /**
     * Caller must hold commit lock.
     */
    final void pushUninsert(long indexId, byte[] key) throws IOException {
        check();
        try {
            undoLog().pushUninsert(indexId, key);
        } catch (Throwable e) {
            borked(e, false, true); // rollback = false, rethrow = true
        }
    }

    /**
     * Caller must hold commit lock.
     *
     * @param payload Node-encoded key followed by trash id
     */
    final void pushUndeleteFragmented(long indexId, byte[] payload, int off, int len)
        throws IOException
    {
        check();
        try {
            undoLog().pushNodeEncoded(indexId, UndoLog.OP_UNDELETE_FRAGMENTED, payload, off, len);
        } catch (Throwable e) {
            borked(e, false, true); // rollback = false, rethrow = true
        }
    }

    /**
     * Caller must hold commit lock.
     */
    final void pushUncreate(long indexId, byte[] key) throws IOException {
        check();
        try {
            undoLog().pushUncreate(indexId, key);
        } catch (Throwable e) {
            borked(e, false, true); // rollback = false, rethrow = true
        }
    }

    /**
     * Caller must hold commit lock.
     */
    final void pushUnextend(long indexId, byte[] key, long length) throws IOException {
        check();
        try {
            undoLog().pushUnextend(mSavepoint, indexId, key, length);
        } catch (Throwable e) {
            borked(e, false, true); // rollback = false, rethrow = true
        }
    }

    /**
     * Caller must hold commit lock.
     */
    final void pushUnalloc(long indexId, byte[] key, long pos, long length) throws IOException {
        check();
        try {
            undoLog().pushUnalloc(indexId, key, pos, length);
        } catch (Throwable e) {
            borked(e, false, true); // rollback = false, rethrow = true
        }
    }

    /**
     * Caller must hold commit lock.
     */
    final void pushUnwrite(long indexId, byte[] key, long pos, /*P*/ byte[] b, int off, int len)
        throws IOException
    {
        check();
        try {
            undoLog().pushUnwrite(indexId, key, pos, b, off, len);
        } catch (Throwable e) {
            borked(e, false, true); // rollback = false, rethrow = true
        }
    }

    /**
     * Caller must hold commit lock.
     */
    private UndoLog undoLog() throws IOException {
        UndoLog undo = mUndoLog;
        if (undo == null) {
            undo = new UndoLog(mDatabase, txnId());

            ParentScope parentScope = mParentScope;
            while (parentScope != null) {
                undo.doScopeEnter();
                parentScope = parentScope.mParentScope;
            }

            mContext.register(undo);
            mUndoLog = undo;
        }
        return undo;
    }

    /**
     * Rethrows the given exception or a replacement, unless the database is closed.
     *
     * @param rollback rollback should only be performed by operations which don't hold tree
     * node latches; otherwise a latch deadlock can occur as the undo rollback attempts to
     * apply compensating actions against the tree nodes.
     * @param rethrow true to always throw an exception; false to suppress rethrowing if
     * database is known to be closed
     */
    final void borked(Throwable borked, boolean rollback, boolean rethrow) {
        // Note: The mBorked field is set only if the database is closed or if some action in
        // this method altered the state of the transaction. Leaving the field alone in all
        // other cases permits an application to fully rollback later when reset or exit is
        // called. Any action which releases locks must only do so after it has issued a
        // rollback operation to the undo log.

        boolean closed = mDatabase == null ? false : mDatabase.isClosed();

        if (mBorked == null) {
            if (closed) {
                Utils.initCause(borked, mDatabase.closedCause());
                mBorked = borked;
            } else if (rollback) {
                // Attempt to rollback the mess and release the locks.
                try {
                    rollback();
                } catch (Throwable rollbackFailed) {
                    if (isRecoverable(borked) && (rethrow || !closed)) {
                        // Allow application to try again later.
                        Utils.rethrow(borked);
                    }

                    // Rollback failed. Locks cannot be released, ensuring other transactions
                    // cannot see the partial changes made by this transaction. A restart is
                    // required, which then performs a clean rollback.

                    Utils.suppress(borked, rollbackFailed);

                    // Also panic the database if not done so already.
                    try {
                        Utils.closeOnFailure(mDatabase, borked);
                    } catch (Throwable e) {
                        // Ignore.
                    }

                    // Discard all of the locks, making it impossible for them to be released
                    // even if the application later calls reset.
                    discardAllLocks();
                }

                // Setting this field permits future operations like reset to simply release
                // any newly acquired locks, and not attempt to issue an undo log rollback.
                mBorked = borked;

                // Force application to check again if transaction is borked.
                mUndoLog = null;
            }
        }

        if (rethrow || !closed) {
            Utils.rethrow(borked);
        }
    }
}