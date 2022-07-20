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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.cojen.tupl.rows.ColumnInfo.*;

/**
 * 
 * @see ComparatorMaker
 * @author Brian S O'Neill
 */
public final class OrderBy extends LinkedHashMap<String, OrderBy.Rule> {
    static OrderBy forPrimaryKey(RowInfo rowInfo) {
        var orderBy = new OrderBy(rowInfo.keyColumns.size());
        for (ColumnInfo column : rowInfo.keyColumns.values()) {
            orderBy.put(column.name, new Rule(column, column.typeCode));
        }
        return orderBy;
    }

    static OrderBy forSpec(RowInfo rowInfo, String spec) {
        try {
            OrderBy orderBy = parseSpec(rowInfo.allColumns, spec);
            if (!orderBy.isEmpty()) {
                return orderBy;
            }
        } catch (IndexOutOfBoundsException e) {
        }

        throw new IllegalArgumentException("Malformed ordering specification: " + spec);
    }

    public OrderBy() {
        super();
    }

    private OrderBy(int capacity) {
        super(capacity, 1);
    }

    /**
     * Return an OrderBy which only preserves the first number of rules. If the number is zero,
     * then null is returned.
     */
    OrderBy truncate(int num) {
        if (num <= 0) {
            return null;
        }

        if (num >= size()) {
            return this;
        }

        var ob = new OrderBy(num);

        for (var e : entrySet()) {
            ob.put(e.getKey(), e.getValue());
            if (--num <= 0) {
                break;
            }
        }

        return ob;
    }

    String spec() {
        var b = new StringBuilder();
        for (OrderBy.Rule rule : values()) {
            rule.appendTo(b);
        }
        return b.toString();
    }

    static String[] splitSpec(String spec) {
        int end = nextSubSpec(spec, 0);
        if (end < 0) {
            return new String[] {spec};
        }
        var list = new ArrayList<String>();
        int start = 0;
        while (true) {
            list.add(spec.substring(start, end));
            start = end;
            end = nextSubSpec(spec, end);
            if (end < 0) {
                list.add(spec.substring(start));
                break;
            }
        }
        return list.toArray(String[]::new);
    }

    private static int nextSubSpec(String spec, int pos) {
        int length = spec.length();
        while (++pos < length) {
            char c = spec.charAt(pos);
            if (c == '-' || c == '+') {
                return pos;
            }
        }
        return -1;
    }

    /**
     * @param type only differs from the column typeCode for TYPE_NULL_LOW and TYPE_DESCENDING
     */
    public static record Rule(ColumnInfo column, int type) {
        public ColumnInfo asColumn() {
            ColumnInfo c = column;
            if (c.typeCode != type) {
                c = c.copy();
                c.typeCode = type;
            }
            return c;
        }

        public boolean isDescending() {
            return ColumnInfo.isDescending(type);
        }

        public boolean isNullLow() {
            return ColumnInfo.isNullLow(type);
        }

        public void appendTo(StringBuilder b) {
            b.append(isDescending() ? '-' : '+');
            if (isNullLow()) {
                b.append('!');
            }
            b.append(column.name);
        }
    }

    private static OrderBy parseSpec(Map<String, ColumnInfo> columns, String spec) {
        var orderBy = new OrderBy();

        int length = spec.length();
        for (int pos = 0; pos < length; ) {
            int type = 0;

            int order = spec.charAt(pos++);
            if (order == '-') {
                type |= TYPE_DESCENDING;
            } else if (order != '+') {
                break;
            }

            if (spec.charAt(pos) == '!') {
                type |= TYPE_NULL_LOW;
                pos++;
            }

            int end = pos;
            while (end < length) {
                order = spec.charAt(end);
                if (order == '-' || order == '+') {
                    break;
                }
                end++;
            }

            if (end == pos) {
                break;
            }

            String name = spec.substring(pos, end);
            ColumnInfo column = columns.get(name);
            if (column == null) {
                throw new IllegalStateException
                    ("Unknown column \"" + name + "\" in ordering specification: " + spec);
            }

            pos = end;

            if (column.isPrimitive()) {
                // Can't be null.
                type &= ~TYPE_NULL_LOW;
            }

            if (!orderBy.containsKey(name)) {
                type |= column.typeCode & ~(TYPE_NULL_LOW | TYPE_DESCENDING);
                orderBy.put(name, new Rule(column, type));
            }
        }

        return orderBy;
    }
}