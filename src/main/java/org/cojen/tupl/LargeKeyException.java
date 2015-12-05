/*
 *  Copyright 2012-2015 Cojen.org
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

/**
 * Thrown when attempting to load a key which cannot fit into memory.
 *
 * @author Brian S O'Neill
 */
public class LargeKeyException extends DatabaseException {
    private static final long serialVersionUID = 1L;

    private final long mLength;

    public LargeKeyException(long length) {
        super(createMessage(length));
        mLength = length;
    }

    public LargeKeyException(long length, Throwable cause) {
        super(createMessage(length), cause);
        mLength = length;
    }

    public long getLength() {
        return mLength;
    }

    @Override
    boolean isRecoverable() {
        return true;
    }

    private static String createMessage(long length) {
        return "Key is too large: " + Utils.toUnsignedString(length);
    }
}
