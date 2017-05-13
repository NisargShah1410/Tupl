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

import java.io.IOException;

import java.util.Arrays;

/**
 * Unnamed tree which prohibits redo durabilty.
 *
 * @author Generated by PageAccessTransformer from TempTree.java
 */
/*P*/
final class _TempTree extends _Tree {
    _TempTree(_LocalDatabase db, long id, byte[] idBytes, _Node root) {
        super(db, id, idBytes, root);
    }

    @Override
    public _TreeCursor newCursor(Transaction txn) {
        return new _TempTreeCursor(this, txn);
    }

    @Override
    public void store(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (txn == null) {
            super.store(Transaction.BOGUS, key, value);
        } else {
            txnStore(txn, key, value);
        }
    }

    private void txnStore(Transaction txn, byte[] key, byte[] value) throws IOException {
        final DurabilityMode dmode = txn.durabilityMode();
        if (dmode == DurabilityMode.NO_REDO) {
            super.store(txn, key, value);
        } else {
            txn.durabilityMode(DurabilityMode.NO_REDO);
            try {
                super.store(txn, key, value);
            } finally {
                txn.durabilityMode(dmode);
            }
        }
    }

    @Override
    public byte[] exchange(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (txn == null) {
            return super.exchange(Transaction.BOGUS, key, value);
        } else {
            return txnExchange(txn, key, value);
        }
    }

    private byte[] txnExchange(Transaction txn, byte[] key, byte[] value) throws IOException {
        final DurabilityMode dmode = txn.durabilityMode();
        if (dmode == DurabilityMode.NO_REDO) {
            return super.exchange(txn, key, value);
        } else {
            txn.durabilityMode(DurabilityMode.NO_REDO);
            try {
                return super.exchange(txn, key, value);
            } finally {
                txn.durabilityMode(dmode);
            }
        }
    }

    @Override
    public boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (txn == null) {
            return super.insert(Transaction.BOGUS, key, value);
        } else {
            return txnInsert(txn, key, value);
        }
    }

    private boolean txnInsert(Transaction txn, byte[] key, byte[] value) throws IOException {
        final DurabilityMode dmode = txn.durabilityMode();
        if (dmode == DurabilityMode.NO_REDO) {
            return super.insert(txn, key, value);
        } else {
            txn.durabilityMode(DurabilityMode.NO_REDO);
            try {
                return super.insert(txn, key, value);
            } finally {
                txn.durabilityMode(dmode);
            }
        }
    }

    @Override
    public boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (txn == null) {
            return super.replace(Transaction.BOGUS, key, value);
        } else {
            return txnReplace(txn, key, value);
        }
    }

    private boolean txnReplace(Transaction txn, byte[] key, byte[] value) throws IOException {
        final DurabilityMode dmode = txn.durabilityMode();
        if (dmode == DurabilityMode.NO_REDO) {
            return super.replace(txn, key, value);
        } else {
            txn.durabilityMode(DurabilityMode.NO_REDO);
            try {
                return super.replace(txn, key, value);
            } finally {
                txn.durabilityMode(dmode);
            }
        }
    }

    @Override
    public boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        if (txn == null) {
            return super.update(Transaction.BOGUS, key, oldValue, newValue);
        } else {
            return txnUpdate(txn, key, oldValue, newValue);
        }
    }

    private boolean txnUpdate(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        final DurabilityMode dmode = txn.durabilityMode();
        if (dmode == DurabilityMode.NO_REDO) {
            return super.update(txn, key, oldValue, newValue);
        } else {
            txn.durabilityMode(DurabilityMode.NO_REDO);
            try {
                return super.update(txn, key, oldValue, newValue);
            } finally {
                txn.durabilityMode(dmode);
            }
        }
    }

    /*
    @Override
    public Stream newStream() {
        _TreeCursor cursor = new _TempTreeCursor(this);
        cursor.autoload(false);
        return new _TreeValueStream(cursor);
    }
    */
}
