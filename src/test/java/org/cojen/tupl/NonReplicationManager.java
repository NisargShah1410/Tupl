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

import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.cojen.tupl.ext.ReplicationManager;

/**
 * Implementation fails to write anything.
 *
 * @author Brian S O'Neill
 */
class NonReplicationManager implements ReplicationManager {
    private static final int REPLICA = 0, LEADER = 1, CLOSED = 2;

    private int mState;
    private NonWriter mWriter;

    synchronized void asReplica() throws InterruptedException {
        mState = REPLICA;
        if (mWriter != null) {
            mWriter.close();
            while (mWriter != null) {
                wait();
            }
        }
        notifyAll();
    }

    synchronized void asLeader() {
        mState = LEADER;
        notifyAll();
    }

    @Override
    public long encoding() {
        return 1;
    }

    @Override
    public void start(long position) {
    }

    @Override
    public void recover(EventListener listener) {
    }

    @Override
    public long readPosition() {
        return 0;
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) throws IOException {
        try {
            while (mState == REPLICA) {
                wait();
            }
            return -1;
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }
    }

    @Override
    public synchronized void flip() {
        mWriter = mState == LEADER ? new NonWriter() : null;
        notifyAll();
    }

    @Override
    public synchronized Writer writer() throws IOException {
        return mWriter;
    }

    @Override
    public void sync() {
    }

    @Override
    public void syncConfirm(long position, long timeoutNanos) {
    }

    @Override
    public void checkpointed(long position) {
    }

    @Override
    public synchronized void close() {
        mState = CLOSED;
        notifyAll();
    }

    private class NonWriter implements Writer {
        private final List<Runnable> mCallbacks = new ArrayList<>();

        private boolean mClosed;

        @Override
        public long position() {
            return 0;
        }

        @Override
        public synchronized boolean leaderNotify(Runnable callback) {
            if (mClosed) {
                return false;
            } else {
                mCallbacks.add(callback);
                return true;
            }
        }

        @Override
        public synchronized boolean write(byte[] b, int off, int len, long commitPos) {
            return !mClosed;
        }

        @Override
        public synchronized boolean confirm(long position, long timeoutNanos) {
            return !mClosed;
        }

        synchronized void close() {
            mClosed = true;
            for (Runnable r : mCallbacks) {
                r.run();
            }
        }
    }
}
