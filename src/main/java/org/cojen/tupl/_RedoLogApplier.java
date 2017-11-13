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

import java.io.IOException;

import org.cojen.tupl.ext.TransactionHandler;

/**
 * 
 *
 * @author Generated by PageAccessTransformer from RedoLogApplier.java
 * @see RedoLogRecovery
 */
/*P*/
final class _RedoLogApplier implements RedoVisitor {
    private final _LocalDatabase mDatabase;
    private final LHashTable.Obj<_LocalTransaction> mTransactions;
    private final LHashTable.Obj<Index> mIndexes;
    private final LHashTable.Obj<Cursor> mCursors;

    long mHighestTxnId;

    _RedoLogApplier(_LocalDatabase db, LHashTable.Obj<_LocalTransaction> txns) {
        mDatabase = db;
        mTransactions = txns;
        mIndexes = new LHashTable.Obj<>(16);
        mCursors = new LHashTable.Obj<>(4);
    }

    void resetCursors() {
        mCursors.traverse(entry -> {
            entry.value.close();
            return false;
        });
    }

    @Override
    public boolean timestamp(long timestamp) {
        return true;
    }

    @Override
    public boolean shutdown(long timestamp) {
        return true;
    }

    @Override
    public boolean close(long timestamp) {
        return true;
    }

    @Override
    public boolean endFile(long timestamp) {
        return true;
    }

    @Override
    public boolean control(byte[] message) {
        return true;
    }

    @Override
    public boolean reset() {
        return true;
    }

    @Override
    public boolean store(long indexId, byte[] key, byte[] value) throws IOException {
        // No need to actually acquire a lock for log based recovery.
        return storeNoLock(indexId, key, value);
    }

    @Override
    public boolean storeNoLock(long indexId, byte[] key, byte[] value) throws IOException {
        Index ix = openIndex(indexId);
        if (ix != null) {
            ix.store(Transaction.BOGUS, key, value);
        }
        return true;
    }

    @Override
    public boolean renameIndex(long txnId, long indexId, byte[] newName) throws IOException {
        checkHighest(txnId);
        Index ix = openIndex(indexId);
        if (ix != null) {
            mDatabase.renameIndex(ix, newName, txnId);
        }
        return true;
    }

    @Override
    public boolean deleteIndex(long txnId, long indexId) throws IOException {
        _LocalTransaction txn = txn(txnId);

        // Close the index for now. After recovery is complete, trashed indexes are deleted in
        // a separate thread.

        Index ix;
        {
            LHashTable.ObjEntry<Index> entry = mIndexes.remove(indexId);
            if (entry == null) {
                ix = mDatabase.anyIndexById(txn, indexId);
            } else {
                ix = entry.value;
            }
        }

        if (ix != null) {
            ix.close();
        }

        return true;
    }

    @Override
    public boolean txnEnter(long txnId) throws IOException {
        _LocalTransaction txn = txn(txnId);
        if (txn == null) {
            txn = new _LocalTransaction(mDatabase, txnId, LockMode.UPGRADABLE_READ, 0L);
            mTransactions.insert(txnId).value = txn;
        } else {
            txn.enter();
        }
        return true;
    }

    @Override
    public boolean txnRollback(long txnId) throws IOException {
        Transaction txn = txn(txnId);
        if (txn != null) {
            txn.exit();
        }
        return true;
    }

    @Override
    public boolean txnRollbackFinal(long txnId) throws IOException {
        checkHighest(txnId);
        Transaction txn = mTransactions.removeValue(txnId);
        if (txn != null) {
            txn.reset();
        }
        return true;
    }

    @Override
    public boolean txnCommit(long txnId) throws IOException {
        Transaction txn = txn(txnId);
        if (txn != null) {
            txn.commit();
            txn.exit();
        }
        return true;
    }

    @Override
    public boolean txnCommitFinal(long txnId) throws IOException {
        checkHighest(txnId);
        _LocalTransaction txn = mTransactions.removeValue(txnId);
        if (txn != null) {
            txn.commitAll();
        }
        return true;
    }

    @Override
    public boolean txnEnterStore(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        txnEnter(txnId);
        return txnStore(txnId, indexId, key, value);
    }

    @Override
    public boolean txnStore(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        Transaction txn = txn(txnId);
        if (txn != null) {
            Index ix = openIndex(indexId);
            if (ix != null) {
                ix.store(txn, key, value);
            }
        }
        return true;
    }

    @Override
    public boolean txnStoreCommit(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        txnStore(txnId, indexId, key, value);
        return txnCommit(txnId);
    }

    @Override
    public boolean txnStoreCommitFinal(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        txnStore(txnId, indexId, key, value);
        return txnCommitFinal(txnId);
    }

    @Override
    public boolean cursorRegister(long cursorId, long indexId) throws IOException {
        Index ix = openIndex(indexId);
        if (ix != null) {
            Cursor c = ix.newCursor(null);
            mCursors.insert(cursorId).value = c;
        }
        return true;
    }

    @Override
    public boolean cursorUnregister(long cursorId) {
        LHashTable.ObjEntry<Cursor> entry = mCursors.remove(cursorId);
        if (entry != null) {
            entry.value.reset();
        }
        return true;
    }

    @Override
    public boolean cursorFind(long cursorId, long txnId, byte[] key) throws IOException {
        LHashTable.ObjEntry<Cursor> entry = mCursors.get(cursorId);
        if (entry != null) {
            Cursor c = entry.value;
            c.link(txn(txnId));
            c.findNearby(key);
        }
        return true;
    }

    @Override
    public boolean cursorValueSetLength(long cursorId, long txnId, long length)
        throws IOException
    {
        LHashTable.ObjEntry<Cursor> entry = mCursors.get(cursorId);
        if (entry != null) {
            Cursor c = entry.value;
            c.link(txn(txnId));
            c.setValueLength(length);
        }
        return true;
    }

    @Override
    public boolean cursorValueWrite(long cursorId, long txnId,
                                    long pos, byte[] buf, int off, int len)
        throws IOException
    {
        LHashTable.ObjEntry<Cursor> entry = mCursors.get(cursorId);
        if (entry != null) {
            Cursor c = entry.value;
            c.link(txn(txnId));
            c.valueWrite(pos, buf, off, len);
        }
        return true;
    }

    @Override
    public boolean cursorEnterStore(long cursorId, long txnId, byte[] key, byte[] value)
        throws IOException
    {
        txnEnter(txnId);
        return cursorStore(cursorId, txnId, key, value);
    }

    @Override
    public boolean cursorStore(long cursorId, long txnId, byte[] key, byte[] value)
        throws IOException
    {
        LHashTable.ObjEntry<Cursor> entry = mCursors.get(cursorId);
        if (entry != null) {
            Cursor c = entry.value;
            c.link(txn(txnId));
            c.findNearby(key);
            c.store(value);
        }
        return true;
    }

    @Override
    public boolean cursorStoreCommit(long cursorId, long txnId, byte[] key, byte[] value)
        throws IOException
    {
        cursorStore(cursorId, txnId, key, value);
        return txnCommit(txnId);
    }

    @Override
    public boolean cursorStoreCommitFinal(long cursorId, long txnId, byte[] key, byte[] value)
        throws IOException
    {
        cursorStore(cursorId, txnId, key, value);
        return txnCommitFinal(txnId);
    }

    @Override
    public boolean txnLockShared(long txnId, long indexId, byte[] key) throws IOException {
        Transaction txn = txn(txnId);
        if (txn != null) {
            txn.lockShared(indexId, key);
        }
        return true;
    }

    @Override
    public boolean txnLockUpgradable(long txnId, long indexId, byte[] key) throws IOException {
        Transaction txn = txn(txnId);
        if (txn != null) {
            txn.lockUpgradable(indexId, key);
        }
        return true;
    }

    @Override
    public boolean txnLockExclusive(long txnId, long indexId, byte[] key) throws IOException {
        Transaction txn = txn(txnId);
        if (txn != null) {
            txn.lockExclusive(indexId, key);
        }
        return true;
    }

    @Override
    public boolean txnCustom(long txnId, byte[] message) throws IOException {
        Transaction txn = txn(txnId);
        if (txn != null) {
            _LocalDatabase db = mDatabase;
            TransactionHandler handler = db.mCustomTxnHandler;
            if (handler == null) {
                throw new DatabaseException("Custom transaction handler is not installed");
            }
            handler.redo(db, txn, message);
        }
        return true;
    }

    @Override
    public boolean txnCustomLock(long txnId, byte[] message, long indexId, byte[] key)
        throws IOException
    {
        Transaction txn = txn(txnId);
        if (txn != null) {
            _LocalDatabase db = mDatabase;
            TransactionHandler handler = db.mCustomTxnHandler;
            if (handler == null) {
                throw new DatabaseException("Custom transaction handler is not installed");
            }
            txn.lockExclusive(indexId, key);
            handler.redo(db, txn, message, indexId, key);
        }
        return true;
    }

    private _LocalTransaction txn(long txnId) {
        checkHighest(txnId);
        return mTransactions.getValue(txnId);
    }

    private void checkHighest(long txnId) {
        if (txnId > mHighestTxnId) {
            mHighestTxnId = txnId;
        }
    }

    private Index openIndex(long indexId) throws IOException {
        LHashTable.ObjEntry<Index> entry = mIndexes.get(indexId);
        if (entry != null) {
            return entry.value;
        }
        Index ix = mDatabase.anyIndexById(indexId);
        if (ix != null) {
            // Maintain a strong reference to the index.
            mIndexes.insert(indexId).value = ix;
        }
        return ix;
    }
}
