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

import java.math.BigDecimal;

import java.util.Set;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.PrimaryKey;
import org.cojen.tupl.RowScanner;
import org.cojen.tupl.RowView;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class BigDecimalColumnTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(BigDecimalColumnTest.class.getName());
    }

    @Test
    public void equalMatch() throws Exception {
        // Filters must compare BigDecimals by fully decoding them instead of comparing the
        // encoded bytes. The problem is that 0 and 0.0 won't be considered equal. See how
        // CompareUtils handles this case. It calls BigDecimal.compareTo instead of equals.

        Database db = Database.open(new DatabaseConfig());
        RowView<Rec> view = db.openRowView(Rec.class);

        Rec row1 = view.newRow();
        row1.id(new BigDecimal("1.00"));
        row1.value1(new BigDecimal("0.00"));
        row1.value2(new BigDecimal("-1.0"));
        view.store(null, row1);

        Rec row2 = view.newRow();
        row2.id(new BigDecimal("1.000"));
        row2.value1(new BigDecimal("0.0"));
        row2.value2(new BigDecimal("-1.00"));
        view.store(null, row2);

        Rec row3 = view.newRow();
        row3.id(new BigDecimal("1.125"));
        row3.value1(new BigDecimal("0.0000001"));
        row3.value2(new BigDecimal("-1.0000000000000001"));
        view.store(null, row3);

        expect(Set.of(row1, row2), view.newScanner(null, "id == ?", 1));
        expect(Set.of(row1, row2), view.newScanner(null, "value1 == ?", 0.0));
        expect(Set.of(row1, row2), view.newScanner(null, "value2 == ?", -1));

        expect(Set.of(row3), view.newScanner(null, "id == ?", 1.125));
        expect(Set.of(row3), view.newScanner(null, "value1 > ?", new BigDecimal("0.0")));
        expect(Set.of(row1, row2, row3), view.newScanner(null, "value2 <= ?", -1));

        expect(Set.of(row3), view.newScanner(null, "value1 == ?", 0.0000001));

        // Won't find it due to float32 rounding error.
        expect(Set.of(), view.newScanner(null, "value1 == ?", 0.0000001f));

        // Need a range search.
        float low = 0.0000001f;
        low -= Math.ulp(low);
        float high = 0.0000001f;
        high += Math.ulp(high);
        expect(Set.of(row3), view.newScanner(null, "value1 >= ? && value1 <= ?", low, high));
    }

    private static void expect(Set<Rec> set, RowScanner<Rec> scanner) throws Exception {
        Rec last = null;
        int count = 0;
        for (Rec row = scanner.row(); row != null; row = scanner.step()) {
            assertTrue(set.contains(row));
            if (last != null) {
                assertNotEquals(row, last);
            }
            last = row;
            count++;
        }
        assertEquals(set.size(), count);
    }

    @PrimaryKey({"id"})
    public interface Rec {
        BigDecimal id();
        void id(BigDecimal bd);

        BigDecimal value1();
        void value1(BigDecimal bd);

        BigDecimal value2();
        void value2(BigDecimal bd);
    }
}
