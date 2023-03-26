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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.util.Comparator;
import java.util.Iterator;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * @author Brian S O'Neill
 */
final class JoinComparatorMaker<J> {
    private static final WeakCache<Class<?>, Comparator<?>, Object> cCache;

    static {
        cCache = new WeakCache<>() {
            @Override
            public Comparator<?> newValue(Class<?> joinType, Object unused) {
                return new JoinComparatorMaker<>(joinType).finish();
            }
        };
    }

    /**
     * Returns a new or cached comparator instance.
     */
    @SuppressWarnings("unchecked")
    public static <J> Comparator<J> comparator(Class<J> joinType) {
        return (Comparator<J>) cCache.obtain(joinType, null);
    }

    private final Class<J> mJoinType;
    private final JoinRowInfo mJoinInfo;

    JoinComparatorMaker(Class<J> joinType) {
        mJoinType = joinType;
        mJoinInfo = JoinRowInfo.find(joinType);
    }

    Comparator<J> finish() {
        Class rowClass = JoinRowMaker.find(mJoinType);
        ClassMaker cm = RowGen.beginClassMaker
            (JoinComparatorMaker.class, mJoinType, mJoinInfo.name, null, "comparator")
            .implement(Comparator.class).final_();

        // Keep a singleton instance, in order for a weakly cached reference to the comparator
        // to stick around until the class is unloaded.
        cm.addField(Comparator.class, "THE").private_().static_();

        MethodMaker mm = cm.addConstructor().private_();
        mm.invokeSuperConstructor();
        mm.field("THE").set(mm.this_());

        makeCompare(cm.addMethod(int.class, "compare", rowClass, rowClass).public_());

        // Now implement the bridge methods.

        mm = cm.addMethod(int.class, "compare", mJoinType, mJoinType).public_().bridge();
        mm.return_(mm.invoke("compare", mm.param(0).cast(rowClass), mm.param(1).cast(rowClass)));

        mm = cm.addMethod(int.class, "compare", Object.class, Object.class).public_().bridge();
        mm.return_(mm.invoke("compare", mm.param(0).cast(rowClass), mm.param(1).cast(rowClass)));

        try {
            MethodHandles.Lookup lookup = cm.finishHidden();
            MethodHandle mh = lookup.findConstructor
                (lookup.lookupClass(), MethodType.methodType(void.class));
            return (Comparator<J>) mh.invoke();
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    void makeCompare(MethodMaker mm) {
        var row0 = mm.param(0);
        var row1 = mm.param(1);

        Iterator<JoinColumnInfo> it = mJoinInfo.allColumns.values().iterator();
        while (it.hasNext()) {
            ColumnInfo info = it.next();
            Label nextLabel = mm.label();

            Variable field0 = row0.field(info.name).get();
            Variable field1 = row1.field(info.name).get();

            // Nulls are ordered high.
            Label cont = mm.label();
            field0.ifNe(null, cont);
            field1.ifEq(null, nextLabel);
            mm.return_(1);
            cont.here();
            cont = mm.label();
            field1.ifNe(null, cont);
            mm.return_(-1);
            cont.here();

            Class<?> fieldType = field0.classType();

            Variable resultVar;
            if (Comparable.class.isAssignableFrom(fieldType)) {
                resultVar = field0.invoke("compareTo", field1);
            } else {
                String spec = OrderBy.forPrimaryKey(RowInfo.find(fieldType)).spec();
                Comparator<?> cmp = ComparatorMaker.comparator(fieldType, spec);
                var cmpVar = mm.var(Comparator.class).setExact(cmp);
                resultVar = cmpVar.invoke("compare", field0, field1);
            }

            if (it.hasNext()) {
                resultVar.ifEq(0, nextLabel);
            }

            mm.return_(resultVar);

            nextLabel.here();
        }

        mm.return_(0);
    }
}
