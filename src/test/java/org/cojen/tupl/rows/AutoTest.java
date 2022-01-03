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

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class AutoTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(AutoTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mDb = Database.open(new DatabaseConfig());
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
        mDb = null;
    }

    private Database mDb;

    @Test
    public void basicInt() throws Exception {
        Table<TestRow1> table = mDb.openTable(TestRow1.class);

        for (int i=1; i<=1000; i++) {
            var row = table.newRow();
            if ((i & 1) == 0) {
                row.id(i);
            }
            row.val("val-" + i);
            assertTrue(table.insert(null, row));
            if ((i & 1) == 0) {
                assertEquals(i, row.id());
            }
        }

        int count = 0;

        try (var scanner = table.newRowScanner(null)) {
            for (var row = scanner.row(); row != null; row = scanner.step()) {
                count++;
                assertTrue(row.id() >= 1);
            }
        }

        assertEquals(1000, count);
    }

    @PrimaryKey("id")
    public interface TestRow1 {
        @Automatic
        int id();
        void id(int id);

        String val();
        void val(String str);
    }

    @Test
    public void basicLong() throws Exception {
        Table<TestRow2> table = mDb.openTable(TestRow2.class);

        for (int i=1; i<=1000; i++) {
            var row = table.newRow();
            if ((i & 1) == 0) {
                row.id(i);
            }
            row.val("val-" + i);
            assertTrue(table.insert(null, row));
            if ((i & 1) == 0) {
                assertEquals(i, row.id());
            }
        }

        int count = 0;

        try (var scanner = table.newRowScanner(null)) {
            for (var row = scanner.row(); row != null; row = scanner.step()) {
                count++;
                assertTrue(row.id() >= 1);
            }
        }

        assertEquals(1000, count);

        Table<TestRow2> valIx = table.viewSecondaryIndex("val");
        count = 0;

        try (var scanner = valIx.newRowScanner(null)) {
            for (var row = scanner.row(); row != null; row = scanner.step()) {
                count++;
            }
        }

        assertEquals(1000, count);
    }

    @PrimaryKey("id")
    @SecondaryIndex("val")
    public interface TestRow2 {
        @Automatic
        long id();
        void id(long id);

        String val();
        void val(String str);
    }

    @Test
    public void basicUInt() throws Exception {
        Table<TestRow3> table = mDb.openTable(TestRow3.class);

        for (int i=1; i<=1000; i++) {
            var row = table.newRow();
            if ((i & 1) == 0) {
                row.id(i);
            }
            row.val("val-" + i);
            assertTrue(table.insert(null, row));
            if ((i & 1) == 0) {
                assertEquals(i, row.id());
            }
        }

        int count = 0;

        try (var scanner = table.newRowScanner(null)) {
            for (var row = scanner.row(); row != null; row = scanner.step()) {
                count++;
                assertTrue(row.id() >= 1);
            }
        }

        assertEquals(1000, count);

        Table<TestRow3> valIx = table.viewSecondaryIndex("val");
        count = 0;

        try (var scanner = valIx.newRowScanner(null)) {
            for (var row = scanner.row(); row != null; row = scanner.step()) {
                count++;
            }
        }

        assertEquals(1000, count);
    }

    @PrimaryKey("id")
    @SecondaryIndex("val")
    public interface TestRow3 {
        @Automatic @Unsigned
        int id();
        void id(int id);

        String val();
        void val(String str);
    }

    @Test
    public void basicULong() throws Exception {
        Table<TestRow4> table = mDb.openTable(TestRow4.class);

        for (int i=1; i<=1000; i++) {
            var row = table.newRow();
            if ((i & 1) == 0) {
                row.id(i);
            }
            row.val("val-" + i);
            assertTrue(table.insert(null, row));
            if ((i & 1) == 0) {
                assertEquals(i, row.id());
            }
        }

        int count = 0;

        try (var scanner = table.newRowScanner(null)) {
            for (var row = scanner.row(); row != null; row = scanner.step()) {
                count++;
                assertTrue(row.id() >= 1);
            }
        }

        assertEquals(1000, count);
    }

    @PrimaryKey("id")
    public interface TestRow4 {
        @Automatic @Unsigned
        long id();
        void id(long id);

        String val();
        void val(String str);
    }

    @Test
    public void compositeInt() throws Exception {
        Table<TestRow5> table = mDb.openTable(TestRow5.class);

        for (int i=1; i<=1000; i++) {
            var row = table.newRow();
            if ((i & 1) == 0) {
                row.id(i);
            }
            row.val("val-" + i);
            assertTrue(table.insert(null, row));
            if ((i & 1) == 0) {
                assertEquals(i, row.id());
            }
        }

        int count = 0;

        try (var scanner = table.newRowScanner(null)) {
            for (var row = scanner.row(); row != null; row = scanner.step()) {
                count++;
                assertTrue(row.id() >= 1);
            }
        }

        assertEquals(1000, count);
    }

    @PrimaryKey({"val", "id"})
    public interface TestRow5 {
        @Automatic
        int id();
        void id(int id);

        String val();
        void val(String str);
    }

    @Test
    public void compositeLong() throws Exception {
        Table<TestRow6> table = mDb.openTable(TestRow6.class);

        for (int i=1; i<=1000; i++) {
            var row = table.newRow();
            if ((i & 1) == 0) {
                row.id(i);
            }
            row.val("val-" + i);
            assertTrue(table.insert(null, row));
            if ((i & 1) == 0) {
                assertEquals(i, row.id());
            }
        }

        int count = 0;

        try (var scanner = table.newRowScanner(null)) {
            for (var row = scanner.row(); row != null; row = scanner.step()) {
                count++;
                assertTrue(row.id() >= 1);
            }
        }

        assertEquals(1000, count);
    }

    @PrimaryKey({"val", "id"})
    public interface TestRow6 {
        @Automatic
        long id();
        void id(long id);

        String val();
        void val(String str);
    }

    @Test
    public void fullInt() throws Exception {
        Table<TestRow7> table = mDb.openTable(TestRow7.class);

        int i = 0;
        try {
            while (i <= 1000) {
                var row = table.newRow();
                row.val("val-" + i);
                assertTrue(table.insert(null, row));
                i++;
            }
            fail();
        } catch (LockFailureException e) {
        }

        assertTrue("" + i, i > 100 && i <= 200);

        int count = 0;

        try (var scanner = table.newRowScanner(null)) {
            for (var row = scanner.row(); row != null; row = scanner.step()) {
                count++;
                assertNotEquals(0, row.id());
            }
        }

        assertEquals(i, count);
    }

    @PrimaryKey("id")
    public interface TestRow7 {
        @Automatic(min=-100, max=100)
        int id();
        void id(int id);

        String val();
        void val(String str);
    }

    @Test
    public void fullLong() throws Exception {
        Table<TestRow8> table = mDb.openTable(TestRow8.class);

        int i = 0;
        try {
            while (i <= 1000) {
                var row = table.newRow();
                row.val("val-" + i);
                assertTrue(table.insert(null, row));
                i++;
            }
            fail();
        } catch (LockFailureException e) {
        }

        assertTrue("" + i, i > 100 && i <= 200);

        int count = 0;

        try (var scanner = table.newRowScanner(null)) {
            for (var row = scanner.row(); row != null; row = scanner.step()) {
                count++;
                assertNotEquals(0, row.id());
            }
        }

        assertEquals(i, count);
    }

    @PrimaryKey("id")
    public interface TestRow8 {
        @Automatic(min=-100, max=100)
        long id();
        void id(long id);

        String val();
        void val(String str);
    }

    @Test
    public void concurrentInt() throws Exception {
        Table<TestRow1> table = mDb.openTable(TestRow1.class);

        class Task implements Runnable {
            @Override
            public void run() {
                try {
                    for (int i=0; i<100_000; i++) {
                        var row = table.newRow();
                        row.val("val-" + i);
                        assertEquals(null, table.exchange(null, row));
                    }
                } catch (Throwable e) {
                    throw RowUtils.rethrow(e);
                }
            }
        }

        var t1 = TestUtils.startTestTask(new Task());
        var t2 = TestUtils.startTestTask(new Task());

        t1.join();
        t2.join();

        int count = 0;

        try (var scanner = table.newRowScanner(null)) {
            for (var row = scanner.row(); row != null; row = scanner.step()) {
                count++;
                assertNotEquals(0, row.id());
            }
        }

        assertEquals(200_000, count);
    }

    @Test
    public void predicateLock() throws Exception {
        // Test that automatic ids aren't generated in the range of an open scanner.

        Table<TestRow7> table = mDb.openTable(TestRow7.class);

        for (int i=10; i<1010; i++) {
            var row = table.newRow();
            row.id(i);
            row.val("val-" + i);
            table.store(null, row);
        }

        Transaction txn = mDb.newTransaction();
        var scanner = table.newRowScanner(txn, "id >= ?", -10);

        int i = 0;
        try {
            while (i <= 1000) {
                var row = table.newRow();
                row.val("val-" + i);
                assertTrue(table.insert(null, row));
                i++;
            }
            fail();
        } catch (LockFailureException e) {
        }

        assertTrue("" + i, i > 45 && i <= 90);

        int count = 0;

        try (var scanner2 = table.newRowScanner(null)) {
            for (var row = scanner2.row(); row != null; row = scanner2.step()) {
                count++;
                int id = row.id();
                assertTrue("" + id, (-100 <= id && id < -10) || (10 <= id && id < 1010));
            }
        }

        assertEquals(i + 1000, count);

        scanner.close();
        txn.exit();

        // Now more can be filled in.

        try {
            while (i <= 1000) {
                var row = table.newRow();
                row.val("val-" + i);
                assertTrue(table.insert(null, row));
                i++;
            }
            fail();
        } catch (LockFailureException e) {
        }

        assertTrue("" + i, i > 90 && i <= 109);
    }
}