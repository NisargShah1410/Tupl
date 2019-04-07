/*
 *  Copyright (C) 2017 Cojen.org
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

package org.cojen.tupl.repl;

import java.io.File;
import java.io.IOException;

import java.net.InetSocketAddress;
import java.net.ServerSocket;

import java.util.Arrays;
import java.util.Random;

import java.util.concurrent.TransferQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import java.util.function.Supplier;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.EventPrinter;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnmodifiableReplicaException;

import org.cojen.tupl.ext.RecoveryHandler;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.TestUtils;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class DatabaseReplicatorTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(DatabaseReplicatorTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
    }

    @After
    public void teardown() throws Exception {
        if (mDatabases != null) {
            for (Database db : mDatabases) {
                if (db != null) {
                    db.close();
                }
            }
        }

        TestUtils.deleteTempFiles(getClass());
    }

    private ServerSocket[] mSockets;
    private File[] mReplBaseFiles;
    private int[] mReplPorts;
    private ReplicatorConfig[] mReplConfigs;
    private DatabaseReplicator[] mReplicators;
    private DatabaseConfig[] mDbConfigs;
    private Database[] mDatabases;

    /**
     * @return first is the leader
     */
    private Database[] startGroup(int members) throws Exception {
        return startGroup(members, Role.NORMAL, null);
    }

    /**
     * @return first is the leader
     */
    private Database[] startGroup(int members, Role replicaRole,
                                  Supplier<RecoveryHandler> handlerSupplier)
        throws Exception
    {
        if (members < 1) {
            throw new IllegalArgumentException();
        }

        mSockets = new ServerSocket[members];

        for (int i=0; i<members; i++) {
            mSockets[i] = new ServerSocket(0);
        }

        mReplBaseFiles = new File[members];
        mReplPorts = new int[members];
        mReplConfigs = new ReplicatorConfig[members];
        mReplicators = new DatabaseReplicator[members];
        mDbConfigs = new DatabaseConfig[members];
        mDatabases = new Database[members];

        for (int i=0; i<members; i++) {
            mReplBaseFiles[i] = TestUtils.newTempBaseFile(getClass()); 
            mReplPorts[i] = mSockets[i].getLocalPort();

            mReplConfigs[i] = new ReplicatorConfig()
                .groupToken(1)
                .localSocket(mSockets[i])
                .baseFile(mReplBaseFiles[i]);

            if (i > 0) {
                mReplConfigs[i].addSeed(mSockets[0].getLocalSocketAddress());
                mReplConfigs[i].localRole(replicaRole);
            }

            mReplicators[i] = DatabaseReplicator.open(mReplConfigs[i]);

            mDbConfigs[i] = new DatabaseConfig()
                .baseFile(mReplBaseFiles[i])
                .replicate(mReplicators[i])
                //.eventListener(new EventPrinter())
                .lockTimeout(5, TimeUnit.SECONDS)
                .directPageAccess(false);

            if (handlerSupplier != null) {
                mDbConfigs[i].recoveryHandler(handlerSupplier.get());
            }

            Database db = Database.open(mDbConfigs[i]);
            mDatabases[i] = db;

            readyCheck: {
                for (int trial=0; trial<100; trial++) {
                    Thread.sleep(100);

                    if (i == 0) {
                        try {
                            db.openIndex("control");
                            // Ensure that replicas obtain the index in the snapshot.
                            db.checkpoint();
                            break readyCheck;
                        } catch (UnmodifiableReplicaException e) {
                            // Not leader yet.
                        }
                    } else {
                        assertNotNull(db.openIndex("control"));
                        break readyCheck;
                    }
                }

                throw new AssertionError(i == 0 ? "No leader" : "Not joined");
            }
        }

        return mDatabases;
    }

    @Test
    public void basicTestOneMember() throws Exception {
        basicTest(1);
    }

    @Test
    public void basicTestThreeMembers() throws Exception {
        for (int i=3; --i>=0; ) {
            try {
                basicTest(3);
                break;
            } catch (UnmodifiableReplicaException e) {
                // Test is load sensitive and leadership is sometimes lost.
                // https://github.com/cojen/Tupl/issues/70
                if (i <= 0) {
                    throw e;
                }
                teardown();
            }
        }
    }

    private void basicTest(int memberCount) throws Exception {
        Database[] dbs = startGroup(memberCount);

        Index ix0 = dbs[0].openIndex("test");

        for (int t=0; t<10; t++) {

            byte[] key = ("hello-" + t).getBytes();
            byte[] value = ("world-" + t).getBytes();
            ix0.store(null, key, value);

            TestUtils.fastAssertArrayEquals(value, ix0.load(null, key));

            for (int i=0; i<dbs.length; i++) {
                for (int q=100; --q>=0; ) {
                    byte[] actual = null;

                    try {
                        Index ix = dbs[i].openIndex("test");
                        actual = ix.load(null, key);
                        TestUtils.fastAssertArrayEquals(value, actual);
                        break;
                    } catch (UnmodifiableReplicaException e) {
                        // Index doesn't exist yet due to replication delay.
                        if (q == 0) {
                            throw e;
                        }
                    } catch (AssertionError e) {
                        // Value doesn't exist yet due to replication delay.
                        if (q == 0 || actual != null) {
                            throw e;
                        }
                    }

                    TestUtils.sleep(100);
                }
            }
        }
    }

    @Test
    public void txnPrepare() throws Exception {
        for (int i=3; --i>=0; ) {
            try {
                doTxnPrepare();
                break;
            } catch (UnmodifiableReplicaException e) {
                // Test is load sensitive and leadership is sometimes lost.
                // https://github.com/cojen/Tupl/issues/70
                if (i <= 0) {
                    throw e;
                }
                teardown();
            }
        }
    }

    private void doTxnPrepare() throws Exception {
        // Test that unfinished prepared transactions are passed to the new leader.

        TransferQueue<Database> recovered = new LinkedTransferQueue<>();

        Supplier<RecoveryHandler> supplier = () -> new RecoveryHandler() {
            private Database mDb;

            @Override
            public void init(Database db) {
                mDb = db;
            }

            @Override
            public void recover(Transaction txn) throws IOException {
                try {
                    recovered.transfer(mDb);

                    // Wait for the signal...
                    recovered.take();

                    // Modify the value before committing.
                    Index ix = mDb.openIndex("test");
                    Cursor c = ix.newCursor(txn);
                    c.find("hello".getBytes());
                    byte[] newValue = Arrays.copyOfRange(c.value(), 0, c.value().length + 1);
                    newValue[newValue.length - 1] = '!';
                    c.store(newValue);
                    c.reset();

                    txn.commit();
                } catch (Exception e) {
                    throw Utils.rethrow(e);
                }
            }
        };

        final int memberCount = 3;
        Database[] dbs = startGroup(memberCount, Role.NORMAL, supplier);

        Index ix0 = dbs[0].openIndex("test");

        // Wait for all members to be electable.
        allElectable: {
            int count = 0;
            for (int i=0; i<100; i++) {
                count = 0;
                for (DatabaseReplicator repl : mReplicators) {
                    if (repl.getLocalRole() == Role.NORMAL) {
                        count++;
                    }
                }
                if (count >= mReplicators.length) {
                    break allElectable;
                }
                Thread.sleep(100);
            }

            fail("Not all members are electable: " + count);
        }

        Transaction txn = dbs[0].newTransaction();
        byte[] key = "hello".getBytes();
        ix0.store(txn, key, "world".getBytes());
        txn.prepare();

        // Close the leader and verify handoff.
        dbs[0].close();

        Database db = recovered.take();
        assertNotEquals(dbs[0], db);

        // Still locked.
        Index ix = db.openIndex("test");
        txn = db.newTransaction();
        assertEquals(LockResult.TIMED_OUT_LOCK, ix.tryLockShared(txn, key, 0));
        txn.reset();

        // Signal that the handler can finish the transaction.
        recovered.add(db);

        assertArrayEquals("world!".getBytes(), ix.load(null, key));

        // Verify replication.
        Database remaining = dbs[1];
        if (remaining == db) {
            remaining = dbs[2];
        }
        ix = remaining.openIndex("test");
        for (int i=10; --i>=0; ) {
            try {
                assertArrayEquals("world!".getBytes(), ix.load(null, key));
                break;
            } catch (Throwable e) {
                if (i <= 0) {
                    throw e;
                }
                Thread.sleep(100);
            }
        }
    }

    @Test
    public void largeWrite() throws Exception {
        Database[] dbs = startGroup(1);
        Database db = dbs[0];
        Index ix = db.openIndex("test");

        byte[] value = new byte[100_000];
        Arrays.fill(value, 0, value.length, (byte) 0x7f); // illegal redo op

        byte[] key = "hello".getBytes();

        Transaction txn = db.newTransaction();
        Cursor c = ix.newCursor(txn);
        c.find(key);
        // This used to hang due to a bug. The commit index was too high, and so it wouldn't be
        // confirmed.
        c.commit(value);

        db.checkpoint();

        db = closeAndReopen(0);

        ix = db.openIndex("test");
        TestUtils.fastAssertArrayEquals(value, ix.load(null, key));

        db.close();
    }

    @Test
    public void valueWriteRecover() throws Exception {
        // Verifies that a checkpoint in the middle of a value write on the replica can still
        // properly recover the registered cursor.

        Database[] dbs = startGroup(2, Role.OBSERVER, null);
        Database leaderDb = dbs[0];
        Database replicaDb = dbs[1];

        Index leaderIx = leaderDb.openIndex("test");

        Random rnd = new Random();

        byte[] part1 = new byte[1000];
        rnd.nextBytes(part1);

        byte[] part2 = new byte[1000];
        rnd.nextBytes(part2);

        Transaction txn = leaderDb.newTransaction();
        byte[] key = "key1".getBytes();
        Cursor c = leaderIx.newAccessor(txn, key);
        c.valueWrite(0, part1, 0, part1.length);
        txn.flush();

        // Wait for replica to catch up.
        fence(leaderDb, replicaDb);

        Index replicaIx = replicaDb.openIndex("test");
        assertTrue(replicaIx.exists(Transaction.BOGUS, key));
        replicaDb.checkpoint();

        replicaDb = closeAndReopen(1);

        replicaIx = replicaDb.openIndex("test");
        assertTrue(replicaIx.exists(Transaction.BOGUS, key));

        // Finish writing and wait for replica to catch up.
        c.valueWrite(part1.length, part2, 0, part2.length);
        c.close();
        txn.commit();
        fence(leaderDb, replicaDb);

        byte[] expect = new byte[part1.length + part2.length];
        System.arraycopy(part1, 0, expect, 0, part1.length);
        System.arraycopy(part2, 0, expect, part1.length, part2.length);

        TestUtils.fastAssertArrayEquals(expect, leaderIx.load(null, key));
        TestUtils.fastAssertArrayEquals(expect, replicaIx.load(null, key));

        replicaDb.close();
    }

    /**
     * Writes a message to the "control" index, and block the replica until it's received.
     */
    private void fence(Database leaderDb, Database replicaDb) throws Exception {
        Index leaderIx = leaderDb.openIndex("control");
        Index replicaIx = replicaDb.openIndex("control");

        byte[] key = (System.nanoTime() + ":" + ThreadLocalRandom.current().nextLong()).getBytes();

        leaderIx.store(null, key, key);

        for (int i=0; i<1000; i++) {
            byte[] value = replicaIx.load(null, key);
            if (value != null) {
                if (Arrays.equals(key, value)) {
                    return;
                }
                fail("Mismatched fence");
            }
            Thread.sleep(10);
        }

        fail("No fence received");
    }

    private Database closeAndReopen(int member) throws Exception {
        mDatabases[member].close();

        // Replace closed socket.
        int port = mSockets[member].getLocalPort();
        ServerSocket ss = new ServerSocket();
        ss.setReuseAddress(true);
        ss.bind(new InetSocketAddress(port));

        mReplConfigs[member].localSocket(ss);
        mReplicators[member] = DatabaseReplicator.open(mReplConfigs[member]);
        mDbConfigs[member].replicate(mReplicators[member]);

        return Database.open(mDbConfigs[member]);
    }
}
