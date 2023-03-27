/*
 *  Copyright (C) 2023 Cojen.org
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

import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnmodifiableViewException;
import org.cojen.tupl.Updater;

import org.cojen.tupl.diag.QueryPlan;

/**
 * Base class for generated join tables.
 *
 * @author Brian S O'Neill
 */
public abstract class JoinTable<J> implements Table<J> {
    @Override
    public Updater<J> newUpdater(Transaction txn) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public Updater<J> newUpdater(Transaction txn, String query, Object... args)
        throws IOException
    {
        throw new UnmodifiableViewException();
    }

    @Override
    public boolean load(Transaction txn, J row) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean exists(Transaction txn, J row) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void store(Transaction txn, J row) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public J exchange(Transaction txn, J row) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public boolean insert(Transaction txn, J row) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public boolean replace(Transaction txn, J row) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public boolean update(Transaction txn, J row) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public boolean merge(Transaction txn, J row) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public boolean delete(Transaction txn, J row) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public QueryPlan updaterPlan(Transaction txn, String query, Object... args) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public void close() throws IOException {
        // Do nothing.
    }

    @Override
    public boolean isClosed() {
        return false;
    }
}
