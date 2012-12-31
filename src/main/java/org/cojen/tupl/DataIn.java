/*
 *  Copyright 2011-2012 Brian S O'Neill
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

import java.io.EOFException;
import java.io.InputStream;
import java.io.IOException;

/**
 * Simple buffered input stream.
 *
 * @author Brian S O'Neill
 */
class DataIn extends InputStream {
    private final InputStream mIn;
    private final byte[] mBuffer;

    private int mStart;
    private int mEnd;

    DataIn(InputStream in) {
        this(in, 4096);
    }

    DataIn(InputStream in, int bufferSize) {
        mIn = in;
        mBuffer = new byte[bufferSize];
    }

    @Override
    public int read() throws IOException {
        int start = mStart;
        if (mEnd - start > 0) {
            mStart = start + 1;
            return mBuffer[start] & 0xff;
        } else {
            int amt = mIn.read(mBuffer);
            if (amt <= 0) {
                return -1;
            } else {
                mStart = 1;
                mEnd = amt;
                return mBuffer[0] & 0xff;
            }
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int start = mStart;
        int avail = mEnd - start;
        if (avail >= len) {
            System.arraycopy(mBuffer, start, b, off, len);
            mStart = start + len;
            return len;
        } else {
            System.arraycopy(mBuffer, start, b, off, avail);
            mStart = 0;
            mEnd = 0;
            off += avail;
            len -= avail;

            if (avail > 0) {
                return avail;
            } else if (len >= mBuffer.length) {
                return mIn.read(b, off, len);
            } else {
                int amt = mIn.read(mBuffer, 0, mBuffer.length);
                if (amt <= 0) {
                    return amt;
                } else {
                    int fill = Math.min(amt, len);
                    System.arraycopy(mBuffer, 0, b, off, fill);
                    mStart = fill;
                    mEnd = amt;
                    return fill;
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        mIn.close();
    }

    public int readIntBE() throws IOException {
        int start = require(4);
        int v = Utils.readIntBE(mBuffer, start);
        mStart = start + 4;
        return v;
    }

    public int readIntLE() throws IOException {
        int start = require(4);
        int v = Utils.readIntLE(mBuffer, start);
        mStart = start + 4;
        return v;
    }

    public long readLongBE() throws IOException {
        int start = require(8);
        long v = Utils.readLongBE(mBuffer, start);
        mStart = start + 8;
        return v;
    }

    public long readLongLE() throws IOException {
        int start = require(8);
        long v = Utils.readLongLE(mBuffer, start);
        mStart = start + 8;
        return v;
    }

    public int readUnsignedVarInt() throws IOException {
        int start = require(1);
        byte[] b = mBuffer;
        int v = b[start++];

        if (v < 0) {
            switch ((v >> 4) & 0x07) {
            case 0x00: case 0x01: case 0x02: case 0x03:
                start = require(1);
                v = (1 << 7)
                    + (((v & 0x3f) << 8)
                       | (b[start++] & 0xff));
                break;
            case 0x04: case 0x05:
                start = require(2);
                v = ((1 << 14) + (1 << 7))
                    + (((v & 0x1f) << 16)
                       | ((b[start++] & 0xff) << 8)
                       | (b[start++] & 0xff));
                break;
            case 0x06:
                start = require(3);
                v = ((1 << 21) + (1 << 14) + (1 << 7))
                    + (((v & 0x0f) << 24)
                       | ((b[start++] & 0xff) << 16)
                       | ((b[start++] & 0xff) << 8)
                       | (b[start++] & 0xff));
                break;
            default:
                start = require(4);
                v = ((1 << 28) + (1 << 21) + (1 << 14) + (1 << 7)) 
                    + ((b[start++] << 24)
                       | ((b[start++] & 0xff) << 16)
                       | ((b[start++] & 0xff) << 8)
                       | (b[start++] & 0xff));
                break;
            }
        }

        mStart = start;
        return v;
    }

    public long readUnsignedVarLong() throws IOException {
        int start = require(1);
        byte[] b = mBuffer;
        int d = b[start++];

        long v;
        if (d >= 0) {
            v = (long) d;
        } else {
            switch ((d >> 4) & 0x07) {
            case 0x00: case 0x01: case 0x02: case 0x03:
                start = require(1);
                v = (1L << 7) +
                    (((d & 0x3f) << 8)
                     | (b[start++] & 0xff));
                break;
            case 0x04: case 0x05:
                start = require(2);
                v = ((1L << 14) + (1L << 7))
                    + (((d & 0x1f) << 16)
                       | ((b[start++] & 0xff) << 8)
                       | (b[start++] & 0xff));
                break;
            case 0x06:
                start = require(3);
                v = ((1L << 21) + (1L << 14) + (1L << 7))
                    + (((d & 0x0f) << 24)
                       | ((b[start++] & 0xff) << 16)
                       | ((b[start++] & 0xff) << 8)
                       | (b[start++] & 0xff));
                break;
            default:
                switch (d & 0x0f) {
                default:
                    start = require(4);
                    v = ((1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                        + (((d & 0x07L) << 32)
                           | (((long) (b[start++] & 0xff)) << 24)
                           | (((long) (b[start++] & 0xff)) << 16)
                           | (((long) (b[start++] & 0xff)) << 8)
                           | ((long) (b[start++] & 0xff)));
                    break;
                case 0x08: case 0x09: case 0x0a: case 0x0b:
                    start = require(5);
                    v = ((1L << 35)
                         + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                        + (((d & 0x03L) << 40)
                           | (((long) (b[start++] & 0xff)) << 32)
                           | (((long) (b[start++] & 0xff)) << 24)
                           | (((long) (b[start++] & 0xff)) << 16)
                           | (((long) (b[start++] & 0xff)) << 8)
                           | ((long) (b[start++] & 0xff)));
                    break;
                case 0x0c: case 0x0d:
                    start = require(6);
                    v = ((1L << 42) + (1L << 35)
                         + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                        + (((d & 0x01L) << 48)
                           | (((long) (b[start++] & 0xff)) << 40)
                           | (((long) (b[start++] & 0xff)) << 32)
                           | (((long) (b[start++] & 0xff)) << 24)
                           | (((long) (b[start++] & 0xff)) << 16)
                           | (((long) (b[start++] & 0xff)) << 8)
                           | ((long) (b[start++] & 0xff)));
                    break;
                case 0x0e:
                    start = require(7);
                    v = ((1L << 49) + (1L << 42) + (1L << 35)
                         + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                        + ((((long) (b[start++] & 0xff)) << 48)
                           | (((long) (b[start++] & 0xff)) << 40)
                           | (((long) (b[start++] & 0xff)) << 32)
                           | (((long) (b[start++] & 0xff)) << 24)
                           | (((long) (b[start++] & 0xff)) << 16)
                           | (((long) (b[start++] & 0xff)) << 8)
                           | ((long) (b[start++] & 0xff)));
                    break;
                case 0x0f:
                    start = require(8);
                    v = ((1L << 56) + (1L << 49) + (1L << 42) + (1L << 35)
                         + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                        + ((((long) b[start++]) << 56)
                           | (((long) (b[start++] & 0xff)) << 48)
                           | (((long) (b[start++] & 0xff)) << 40)
                           | (((long) (b[start++] & 0xff)) << 32)
                           | (((long) (b[start++] & 0xff)) << 24)
                           | (((long) (b[start++] & 0xff)) << 16)
                           | (((long) (b[start++] & 0xff)) << 8L)
                           | ((long) (b[start++] & 0xff)));
                    break;
                }
                break;
            }
        }

        mStart = start;
        return v;
    }

    public long readSignedVarLong() throws IOException {
        long v = readUnsignedVarLong();
        return ((v & 1) != 0) ? ((~(v >> 1)) | (1 << 31)) : (v >>> 1);
    }

    public void readFully(byte[] b) throws IOException {
        Utils.readFully(this, b, 0, b.length);
    }

    /**
     * Reads a byte string prefixed with a variable length.
     */
    public byte[] readBytes() throws IOException {
        byte[] bytes = new byte[readUnsignedVarInt()];
        readFully(bytes);
        return bytes;
    }

    /**
     * @return start
     */
    public int require(int amount) throws IOException {
        int start = mStart;
        int avail = mEnd - start;
        if ((amount -= avail) <= 0) {
            return start;
        }

        if (mBuffer.length - mEnd < amount) {
            System.arraycopy(mBuffer, start, mBuffer, 0, avail);
            mStart = start = 0;
            mEnd = avail;
        }

        while (true) {
            int amt = mIn.read(mBuffer, mEnd, mBuffer.length - mEnd);
            if (amt <= 0) {
                throw new EOFException();
            }
            mEnd += amt;
            if ((amount -= amt) <= 0) {
                return start;
            }
        }
    }
}
