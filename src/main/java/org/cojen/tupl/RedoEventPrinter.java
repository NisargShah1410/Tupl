/*
 *  Copyright 2017 Cojen.org
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

import java.nio.charset.StandardCharsets;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class RedoEventPrinter implements RedoVisitor {
    private static final int MAX_VALUE = 1000;

    private final EventListener mListener;
    private final EventType mType;

    RedoEventPrinter(EventListener listener, EventType type) {
        mListener = listener;
        mType = type;
    }

    public boolean reset() {
        mListener.notify(mType, "Redo reset");
        return true;
    }

    public boolean timestamp(long timestamp) {
        mListener.notify(mType, "Redo %1$s: %2$s", "timestamp", toDateTime(timestamp));
        return true;
    }

    public boolean shutdown(long timestamp) {
        mListener.notify(mType, "Redo %1$s: %2$s", "shutdown", toDateTime(timestamp));
        return true;
    }

    public boolean close(long timestamp) {
        mListener.notify(mType, "Redo %1$s: %2$s", "close", toDateTime(timestamp));
        return true;
    }

    public boolean endFile(long timestamp) {
        mListener.notify(mType, "Redo %1$s: %2$s", "endFile", toDateTime(timestamp));
        return true;
    }

    public boolean store(long indexId, byte[] key, byte[] value) {
        mListener.notify(mType, "Redo %1$s: indexId=%2$d, key=%3$s, value=%4$s",
                         "store", indexId, keyStr(key), valueStr(value));
        return true;
    }

    public boolean storeNoLock(long indexId, byte[] key, byte[] value) {
        mListener.notify(mType, "Redo %1$s: indexId=%2$d, key=%3$s, value=%4$s",
                         "storeNoLock", indexId, keyStr(key), valueStr(value));
        return true;
    }

    public boolean renameIndex(long txnId, long indexId, byte[] newName) {
        mListener.notify(mType, "Redo %1$s: txnId=%2$d, indexId=%3$d, name=%4$s",
                         "renameIndex", txnId, indexId, keyStr(newName));
        return true;
    }

    public boolean deleteIndex(long txnId, long indexId) {
        mListener.notify(mType, "Redo %1$s: txnId=%2$d, indexId=%3$d",
                         "deleteIndex", txnId, indexId);
        return true;
    }

    public boolean txnEnter(long txnId) {
        mListener.notify(mType, "Redo %1$s: txnId=%2$d", "txnEnter", txnId);
        return true;
    }

    public boolean txnRollback(long txnId) {
        mListener.notify(mType, "Redo %1$s: txnId=%2$d", "txnRollback", txnId);
        return true;
    }

    public boolean txnRollbackFinal(long txnId) {
        mListener.notify(mType, "Redo %1$s: txnId=%2$d", "txnRollbackFinal", txnId);
        return true;
    }

    public boolean txnCommit(long txnId) {
        mListener.notify(mType, "Redo %1$s: txnId=%2$d", "txnCommit", txnId);
        return true;
    }

    public boolean txnCommitFinal(long txnId) {
        mListener.notify(mType, "Redo %1$s: txnId=%2$d", "txnCommitFinal", txnId);
        return true;
    }

    public boolean txnEnterStore(long txnId, long indexId, byte[] key, byte[] value) {
        mListener.notify(mType, "Redo %1$s: txnId=%2$d, indexId=%3$d, key=%4$s, value=%5$s",
                         "txnEnterStore", txnId, indexId, keyStr(key), valueStr(value));
        return true;
    }

    public boolean txnStore(long txnId, long indexId, byte[] key, byte[] value) {
        mListener.notify(mType, "Redo %1$s: txnId=%2$d, indexId=%3$d, key=%4$s, value=%5$s",
                         "txnStore", txnId, indexId, keyStr(key), valueStr(value));
        return true;
    }

    public boolean txnStoreCommit(long txnId, long indexId, byte[] key, byte[] value) {
        mListener.notify(mType, "Redo %1$s: txnId=%2$d, indexId=%3$d, key=%4$s, value=%5$s",
                         "txnStoreCommit", txnId, indexId, keyStr(key), valueStr(value));
        return true;
    }

    public boolean txnStoreCommitFinal(long txnId, long indexId, byte[] key, byte[] value) {
        mListener.notify(mType, "Redo %1$s: txnId=%2$d, indexId=%3$d, key=%4$s, value=%5$s",
                         "txnStoreCommitFinal", txnId, indexId, keyStr(key), valueStr(value));
        return true;
    }

    public boolean txnLockShared(long txnId, long indexId, byte[] key) {
        mListener.notify(mType, "Redo %1$s: txnId=%2$d, indexId=%3$d, key=%4$s",
                         "txnLockShared", txnId, indexId, keyStr(key));
        return true;
    }

    public boolean txnLockUpgradable(long txnId, long indexId, byte[] key) {
        mListener.notify(mType, "Redo %1$s: txnId=%2$d, indexId=%3$d, key=%4$s",
                         "txnLockUpgradable", txnId, indexId, keyStr(key));
        return true;
    }

    public boolean txnLockExclusive(long txnId, long indexId, byte[] key) {
        mListener.notify(mType, "Redo %1$s: txnId=%2$d, indexId=%3$d, key=%4$s",
                         "txnLockExclusive", txnId, indexId, keyStr(key));
        return true;
    }

    public boolean txnCustom(long txnId, byte[] message) {
        mListener.notify(mType, "Redo %1$s: message=%2$s",
                         "txnCustom", txnId, valueStr(message));
        return true;
    }

    public boolean txnCustomLock(long txnId, byte[] message, long indexId, byte[] key) {
        mListener.notify(mType, "Redo %1$s: message=%2$s, key=%3$s",
                         "txnCustomLock", txnId, valueStr(message), keyStr(key));
        return true;
    }

    private static String keyStr(byte[] key) {
        return "0x" + Utils.toHex(key) + " (" + new String(key, StandardCharsets.UTF_8) + ')';
    }

    private static String valueStr(byte[] value) {
        if (value == null) {
            return "null";
        } else if (value.length <= MAX_VALUE) {
            return "0x" + Utils.toHex(value);
        } else {
            return "0x" + Utils.toHex(value, 0, MAX_VALUE) + "...";
        }
    }

    private static String toDateTime(long timestamp) {
        return java.time.Instant.ofEpochMilli(timestamp).toString();
    }
}
