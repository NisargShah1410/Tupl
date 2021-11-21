/*
 *  Copyright (C) 2021 Cojen.org
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

package org.cojen.tupl.core;

import java.io.IOException;

import org.cojen.tupl.Cursor;

/**
 * Defines an interface intended for locking rows based on a rule that matches on a row.
 *
 * @author Brian S O'Neill
 * @see RowPredicateSet
 */
public interface RowPredicate<R> {
    /**
     * Called by an insert operation, and so all columns of the row are filled in.
     */
    public boolean testRow(R row);

    /**
     * Called by a delete operation, and so only the key columns of the row are filled in.
     * Additional columns must be decoded as necessary.
     *
     * @param value non-null value being deleted
     */
    public boolean testRow(R row, byte[] value);

    /**
     * Called by a delete operation, and so only the key columns of the row are filled in.
     * Additional columns must be decoded as necessary.
     *
     * @param c refers to the row being deleted, but the value hasn't been loaded yet
     */
    public boolean testRow(R row, Cursor c) throws IOException;

    /**
     * Determine if a lock held against the given key matches the row predicate. This variant
     * is called for transactions which were created before the predicate lock.
     */
    public default boolean testKey(byte[] key) {
        // When undecided, always default to true.
        return true;
    }

    /**
     * Returns a predicate that always evaluates to true.
     */
    @SuppressWarnings("unchecked")
    public static <R> RowPredicate<R> all() {
        return All.THE;
    }

    /**
     * Defines a predicate that always evaluates to false.
     */
    public abstract class None<R> implements RowPredicate<R> {
        @Override
        public final boolean testRow(R row) {
            return false;
        }

        @Override
        public final boolean testRow(R row, byte[] value) {
            return false;
        }

        @Override
        public final boolean testRow(R row, Cursor c) {
            return false;
        }
    }

    static final class All implements RowPredicate {
        static final All THE = new All();

        @Override
        public boolean testRow(Object row) {
            return true;
        }

        @Override
        public boolean testRow(Object row, byte[] value) {
            return true;
        }

        @Override
        public boolean testRow(Object row, Cursor c) {
            return true;
        }
    }
}