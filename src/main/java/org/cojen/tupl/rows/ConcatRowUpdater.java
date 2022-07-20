/*
 *  Copyright (C) 2022 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.rows;

import java.io.IOException;

import org.cojen.tupl.RowUpdater;

/**
 * Concatenates multiple RowUpdaters into a single RowUpdater.
 *
 * @author Brian S O'Neill
 */
abstract class ConcatRowUpdater<R> implements BaseRowScanner<R>, RowUpdater<R> {
    private final int mCharacteristics;

    private RowUpdater<R> mCurrent;

    /**
     * @param dst can be null
     */
    ConcatRowUpdater(int characteristics, final R dst) throws IOException {
        mCharacteristics = characteristics;

        RowUpdater<R> next = next(dst);
        while (true) {
            mCurrent = next;
            if (row() != null || (next = next(dst)) == null) {
                return;
            }
        }
    }

    @Override
    public R row() {
        return mCurrent.row();
    }

    @Override
    public R step() throws IOException {
        R row = mCurrent.step();
        while (true) {
            RowUpdater<R> next;
            if (row != null || (next = next(null)) == null) {
                return row;
            }
            mCurrent = next;
            row = next.row();
        }
    }

    @Override
    public R step(final R dst) throws IOException {
        R row = mCurrent.step(dst);
        while (true) {
            RowUpdater<R> next;
            if (row != null || (next = next(dst)) == null) {
                return row;
            }
            mCurrent = next;
            row = next.row();
        }
    }

    @Override
    public R update() throws IOException {
        R row = mCurrent.update();
        if (row == null) {
            row = step();
        }
        return row;
    }

    @Override
    public R update(final R dst) throws IOException {
        R row = mCurrent.update(dst);
        if (row == null) {
            row = step(dst);
        }
        return row;
    }

    @Override
    public R delete() throws IOException {
        R row = mCurrent.delete();
        if (row == null) {
            row = step();
        }
        return row;
    }

    @Override
    public R delete(final R dst) throws IOException {
        R row = mCurrent.delete(dst);
        if (row == null) {
            row = step(dst);
        }
        return row;
    }

    @Override
    public void flush() throws IOException {
        mCurrent.flush();
    }

    @Override
    public void close() throws IOException {
        mCurrent.close();
    }

    @Override
    public int characteristics() {
        return mCharacteristics;
    }

    /**
     * Returns null if none left, but must always return at least one the first time.
     *
     * @param dst can be null
     */
    protected abstract RowUpdater<R> next(R dst) throws IOException;
}