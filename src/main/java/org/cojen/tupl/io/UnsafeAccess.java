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

package org.cojen.tupl.io;

import java.lang.reflect.Field;

import com.sun.jna.Native;
import com.sun.jna.Platform;

/**
 * Utility for accessing the unsupported Unsafe class.
 *
 * @author Brian S O'Neill
 */
@SuppressWarnings("rawtypes")
public class UnsafeAccess {
    private static final sun.misc.Unsafe UNSAFE;
    private static final Throwable UNSUPPORTED;
    private static final long ARRAY_OFFSET;

    static {
        sun.misc.Unsafe unsafe = null;
        Throwable unsupported = null;

        try {
            Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            unsafe = (sun.misc.Unsafe) theUnsafe.get(null);
        } catch (Throwable e) {
            unsupported = e;
        }

        UNSAFE = unsafe;
        UNSUPPORTED = unsupported;
        ARRAY_OFFSET = unsafe == null ? 0 : unsafe.arrayBaseOffset(byte[].class);
    }

    private UnsafeAccess() {
    }

    /**
     * @return null if not supported
     */
    public static sun.misc.Unsafe tryObtain() {
        return UNSAFE;
    }

    /**
     * @throws UnsupportedOperationException if not supported
     */
    public static sun.misc.Unsafe obtain() throws UnsupportedOperationException {
        if (UNSAFE == null) {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }
        return UNSAFE;
    }

    /**
     * Allocate native memory.
     */
    public static long alloc(int size) {
        return UNSAFE.allocateMemory(size);
    }

    /**
     * Allocate native memory.
     */
    public static long alloc(int size, boolean aligned) {
        return aligned ? JNA.valloc(size) : UNSAFE.allocateMemory(size);
    }

    /**
     * Allocate native memory, zero filled.
     */
    public static long calloc(int size, boolean aligned) {
        long ptr = alloc(size, aligned);
        UNSAFE.setMemory(ptr, size, (byte) 0);
        return ptr;
    }

    /**
     * Fill a range of native memory.
     */
    public static void fill(long ptr, long len, byte value) {
        UNSAFE.setMemory(ptr, len, value);
    }

    /**
     * Copy native memory.
     */
    public static void copy(long srcPtr, long dstPtr, long len) {
        UNSAFE.copyMemory(srcPtr, dstPtr, len);
    }

    /**
     * Copy to native memory.
     */
    public static void copy(byte[] src, int srcOffset, long dstPtr, long len) {
        UNSAFE.copyMemory(src, ARRAY_OFFSET + srcOffset, null, dstPtr, len);
    }

    /**
     * Copy from native memory.
     */
    public static void copy(long srcPtr, byte[] dst, int dstOffset, long len) {
        UNSAFE.copyMemory(null, srcPtr, dst, ARRAY_OFFSET + dstOffset, len);
    }

    /**
     * Free allocated native memory.
     */
    public static void free(long ptr) {
        UNSAFE.freeMemory(ptr);
    }

    static class JNA {
        static {
            Native.register(Platform.C_LIBRARY_NAME);
        }

        // TODO: Define variant that works on Windows.
        static native long valloc(int size);
    }
}
