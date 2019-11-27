/*
 *  Copyright (C) 2011-2018 Cojen.org
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

import org.cojen.tupl.LockMode;

import org.cojen.tupl.ext.ReplicationManager;

/**
 * Used to apply recovered transactions from the redo log, when database isn't replictated.
 * This class extends ReplEngine because it applies transactions using multiple threads,
 * but it's replication features aren't used.
 *
 * @author Brian S O'Neill
 */
/*P*/
final class RedoLogApplier extends ReplEngine implements ReplicationManager {
    private long mHighestTxnId;

    /**
     * @param maxThreads pass zero to use all processors; see DatabaseConfig.maxReplicaThreads
     */
    RedoLogApplier(int maxThreads, LocalDatabase db, LHashTable.Obj<LocalTransaction> txns,
                   LHashTable.Obj<BTreeCursor> cursors)
        throws IOException
    {
        // Passing null for manager implies that this class is the manager. Passing 'this' is
        // rejected by the compiler.
        super(null, maxThreads, db, txns, cursors);
    }

    /**
     * Return the highest observed transaction id.
     *
     * @param txnId transaction id recovered from the database header
     */
    public long highestTxnId(long txnId) {
        if (mHighestTxnId != 0) {
            // Subtract for modulo comparison.
            if (txnId == 0 || (mHighestTxnId - txnId) > 0) {
                txnId = mHighestTxnId;
            }
        }
        return txnId;
    }

    @Override
    public Thread newThread(Runnable r) {
        var t = new Thread(r);
        t.setDaemon(true);
        t.setName("Recovery-" + Long.toUnsignedString(t.getId()));
        t.setUncaughtExceptionHandler((thread, ex) -> Utils.closeQuietly(mDatabase, ex));
        return t;
    }

    @Override
    public boolean reset() throws IOException {
        // Ignore resets until the very end.
        return true;
    }

    @Override
    protected LocalTransaction newTransaction(long txnId) {
        if (txnId > mHighestTxnId) {
            mHighestTxnId = txnId;
        }

        var txn = new LocalTransaction
            (mDatabase, txnId, LockMode.UPGRADABLE_READ, INFINITE_TIMEOUT);

        txn.attach(attachment());

        return txn;
    }

    @Override
    protected Object attachment() {
        return "recovery";
    }

    // Implement all of the abstract ReplicationManager methods instead of passing null for the
    // ReplicationManager instance to the ReplEngine. It would instead need to have checks
    // for a null ReplicationManager. These methods aren't expected to be called anyhow.

    @Override
    public long encoding() { return 0; }

    @Override
    public void start(long position) { }

    @Override
    public long readPosition() { return 0; }

    @Override
    public int read(byte[] b, int off, int len) { return -1; }

    @Override
    public Writer writer() { return null; }

    @Override
    public void sync() { }

    @Override
    public void syncConfirm(long position, long timeoutNanos) { }

    @Override
    public boolean failover() { return true; }

    @Override
    public void checkpointed(long position) { }

    @Override
    public void close() { }
}
