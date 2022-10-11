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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;

import org.cojen.tupl.Scanner;
import org.cojen.tupl.EntryScanner;
import org.cojen.tupl.Sorter;
import org.cojen.tupl.Transaction;

import static java.util.Spliterator.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class RowSorter<R> extends ScanBatch<R> implements RowConsumer<R> {
    // FIXME: Make configurable and/or "smart".
    private static final int EXTERNAL_THRESHOLD = 1_000_000;

    private ScanBatch<R> mFirstBatch, mLastBatch;

    @SuppressWarnings("unchecked")
    static <R> Scanner<R> sort(BaseTable<R> table, String orderBySpec, Comparator<R> comparator,
                               QueryLauncher<R> launcher, Transaction txn, Object... args)
        throws IOException
    {
        var sorter = new RowSorter<R>();
        // Pass sorter as if it's a row, but it's actually a RowConsumer.
        Scanner<R> source = launcher.newScanner(txn, (R) sorter, args);
        return sorter.sort(table, orderBySpec, comparator, launcher, source);
    }

    @SuppressWarnings("unchecked")
    private Scanner<R> sort(BaseTable<R> table, String orderBySpec, Comparator<R> comparator,
                            QueryLauncher<R> launcher, Scanner source)
        throws IOException
    {
        int numRows = 0;
        for (Object c = source.row(); c != null; c = source.step(c)) {
            if (++numRows >= EXTERNAL_THRESHOLD) {
                return new External(table, orderBySpec, launcher).sort(comparator, source);
            }
        }

        if (numRows == 0) {
            int characteristics = NONNULL | ORDERED | IMMUTABLE | SIZED | SORTED | DISTINCT;
            return new ARS<>(characteristics, comparator);
        }

        var rows = (R[]) new Object[numRows];
        ScanBatch first = mFirstBatch;
        mFirstBatch = null;
        mLastBatch = null;
        first.decodeAllRows(rows, 0);

        Arrays.parallelSort(rows, comparator);

        return new ARS<>(table, rows, sortedCharacteristics(launcher), comparator);
    }

    @Override
    public void beginBatch(RowEvaluator<R> evaluator) {
        ScanBatch<R> batch;
        if (mLastBatch == null) {
            mFirstBatch = batch = this;
        } else {
            batch = new ScanBatch<R>();
            mLastBatch.appendNext(batch);
        }
        mLastBatch = batch;
        batch.mEvaluator = evaluator;
    }

    @Override
    public void accept(byte[] key, byte[] value) throws IOException {
        mLastBatch.addEntry(key, value);
    }

    private static int sortedCharacteristics(QueryLauncher launcher) {
        int characteristics = launcher.characteristics();
        characteristics |= (NONNULL | ORDERED | IMMUTABLE | SIZED | SORTED);
        characteristics &= ~CONCURRENT;
        return characteristics;
    }

    private static class ARS<R> extends ArrayScanner<R> {
        private final int mCharacteristics;
        private final Comparator<R> mComparator;

        ARS(int characteristics, Comparator<R> comparator) {
            mCharacteristics = characteristics;
            mComparator = comparator;
        }

        ARS(BaseTable<R> table, R[] rows, int characteristics, Comparator<R> comparator) {
            super(table, rows);
            mCharacteristics = characteristics;
            mComparator = comparator;
        }

        @Override
        public int characteristics() {
            return mCharacteristics;
        }

        @Override
        public Comparator<R> getComparator() {
            return mComparator;
        }
    }

    private static class SRS<R> extends ScannerScanner<R> {
        private final int mCharacteristics;
        private final Comparator<R> mComparator;

        SRS(EntryScanner scanner, RowDecoder<R> decoder,
            int characteristics, Comparator<R> comparator)
            throws IOException
        {
            super(scanner, decoder);
            mCharacteristics = characteristics;
            mComparator = comparator;
        }

        @Override
        public int characteristics() {
            return mCharacteristics;
        }

        @Override
        public Comparator<R> getComparator() {
            return mComparator;
        }
    }

    private class External implements RowConsumer<R> {
        private final RowStore mRowStore;
        private final Sorter mSorter;
        private final Class<?> mRowType;
        private final SecondaryInfo mSortedInfo;
        private final RowDecoder mDecoder;
        private final int mCharacteristics;

        private Transcoder mTranscoder;

        private byte[][] mBatch;
        private int mBatchSize;

        External(BaseTable<R> table, String orderBySpec, QueryLauncher<R> launcher)
            throws IOException
        {
            mRowStore = table.rowStore();
            mSorter = mRowStore.mDatabase.newSorter();
            mRowType = table.rowType();
            Set<String> projection = launcher.projection();
            mSortedInfo = SortDecoderMaker.findSortedInfo(mRowType, orderBySpec, projection, true);
            mDecoder = SortDecoderMaker.findDecoder(mRowType, mSortedInfo, projection);
            mCharacteristics = sortedCharacteristics(launcher);
        }

        Scanner<R> sort(Comparator<R> comparator, Scanner source) throws IOException {
            try {
                return doSort(comparator, source);
            } catch (Throwable e) {
                try {
                    mSorter.reset();
                } catch (Throwable e2) {
                    RowUtils.suppress(e, e2);
                }
                throw e;
            }
        }

        @SuppressWarnings("unchecked")
        Scanner<R> doSort(Comparator<R> comparator, Scanner source) throws IOException {
            // Transfer all the undecoded rows into the sorter.

            ScanBatch<R> batch = mFirstBatch;

            mFirstBatch = null;
            mLastBatch = null;

            byte[][] kvPairs = null;

            do {
                mTranscoder = transcoder(batch.mEvaluator);
                kvPairs = batch.transcode(mTranscoder, mSorter, kvPairs);
            } while ((batch = batch.detachNext()) != null);

            // Transfer all the remaining undecoded rows into the sorter, passing `this` as the
            // RowConsumer.

            mBatch = new byte[100][];
            while (source.step(this) != null);
            flush();
            mBatch = null;

            return new SRS<>(mSorter.finishScan(), mDecoder, mCharacteristics, comparator);
        }

        private Transcoder transcoder(RowEvaluator<R> evaluator) {
            return mRowStore.findSortTranscoder(mRowType, evaluator, mSortedInfo);
        }

        @Override
        public void beginBatch(RowEvaluator<R> evaluator) throws IOException {
            flush();
            mTranscoder = transcoder(evaluator);
        }

        @Override
        public void accept(byte[] key, byte[] value) throws IOException {
            byte[][] batch = mBatch;
            int size = mBatchSize;
            mTranscoder.transcode(key, value, batch, size);
            size += 2;
            if (size < batch.length) {
                mBatchSize = size;
            } else {
                mSorter.addBatch(batch, 0, size >> 1);
                mBatchSize = 0;
            }
        }

        private void flush() throws IOException {
            if (mBatchSize > 0) {
                mSorter.addBatch(mBatch, 0, mBatchSize >> 1);
                mBatchSize = 0;
            }
        }
    }
}
