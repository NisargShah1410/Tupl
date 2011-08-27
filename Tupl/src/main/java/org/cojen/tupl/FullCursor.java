/*
 *  Copyright 2011 Brian S O'Neill
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

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class FullCursor implements Cursor {
    private final TreeCursor mCursor;

    FullCursor(TreeCursor cursor) {
        mCursor = cursor;
    }

    @Override
    public synchronized byte[] getKey() throws IOException {
        return mCursor.get(Entry.GET_KEY);
    }

    @Override
    public synchronized byte[] getValue() throws IOException {
        return mCursor.get(Entry.GET_VALUE);
    }

    @Override
    public synchronized boolean getEntry(Entry entry) throws IOException {
        return mCursor.get(entry) != null;
    }

    @Override
    public Entry getEntry() throws IOException {
        Entry entry = new Entry();
        synchronized (this) {
            mCursor.get(entry);
        }
        return entry;
    }

    @Override
    public synchronized boolean first() throws IOException {
        return mCursor.first();
    }

    @Override
    public synchronized boolean last() throws IOException {
        return mCursor.last();
    }

    @Override
    public synchronized long move(long amount) throws IOException {
        return mCursor.move(amount);
    }

    @Override
    public synchronized boolean next() throws IOException {
        return mCursor.next();
    }

    @Override
    public synchronized boolean previous() throws IOException {
        return mCursor.previous();
    }

    @Override
    public synchronized boolean find(byte[] key) throws IOException {
        return mCursor.find(key);
    }

    @Override
    public synchronized boolean findGe(byte[] key) throws IOException {
        return mCursor.findGe(key);
    }

    @Override
    public synchronized boolean findGt(byte[] key) throws IOException {
        return mCursor.findGt(key);
    }

    @Override
    public synchronized boolean findLe(byte[] key) throws IOException {
        return mCursor.findLe(key);
    }

    @Override
    public synchronized boolean findLt(byte[] key) throws IOException {
        return mCursor.findLt(key);
    }

    @Override
    public synchronized boolean findNearby(byte[] key) throws IOException {
        return mCursor.findNearby(key);
    }

    @Override
    public synchronized void store(byte[] value) throws IOException {
        mCursor.store(value);
    }

    @Override
    public synchronized Cursor copy() {
        return new FullCursor(mCursor.copy());
    }

    @Override
    public synchronized boolean isPositioned() {
        return mCursor.isPositioned();
    }

    @Override
    public synchronized void reset() {
        mCursor.reset();
    }
}
