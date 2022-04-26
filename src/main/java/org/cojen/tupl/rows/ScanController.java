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

package org.cojen.tupl.rows;

import java.io.IOException;

import java.util.Comparator;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

import org.cojen.tupl.core.RowPredicate;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see ScanControllerFactory
 */
public interface ScanController<R> {
    static final byte[] EMPTY = new byte[0];

    /**
     * Returns a predicate which is shared by all scan batches.
     */
    default RowPredicate<R> predicate() {
        return RowPredicate.all();
    }

    /**
     * Returns a new cursor for the current scan batch.
     */
    Cursor newCursor(View view, Transaction txn) throws IOException;

    /**
     * Returns the decoder for the current scan batch.
     */
    RowDecoderEncoder<R> decoder();

    /**
     * Move to the next batch, returning false if none.
     */
    boolean next();
}
