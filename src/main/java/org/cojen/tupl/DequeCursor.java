/*
 *  Copyright 2013 Brian S O'Neill
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
 * Returned by {@link DequeIndex}.
 *
 * @author Brian S O'Neill
 */
final class DequeCursor extends WrappedCursor<TreeCursor> {
    private final DequeIndex mIndex;

    DequeCursor(DequeIndex index, TreeCursor source) {
        super(source);
        mIndex = index;
    }

    @Override
    public void store(byte[] value) throws IOException {
        mIndex.storeInto(mSource, value);
    }

    @Override
    public Stream newStream() {
        // FIXME
        throw null;
    }

    @Override
    public Cursor copy() {
        return new DequeCursor(mIndex, mSource.copy());
    }
}
