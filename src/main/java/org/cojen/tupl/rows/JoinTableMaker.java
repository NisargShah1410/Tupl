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

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.cojen.maker.Bootstrap;
import org.cojen.maker.ClassMaker;
import org.cojen.maker.Field;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class JoinTableMaker {
    private static final WeakClassCache<Class<?>> cCache;

    static {
        cCache = new WeakClassCache<>() {
            @Override
            protected Class<?> newValue(Class<?> joinType, Object unused) {
                return new JoinTableMaker(joinType).finish();
            }
        };
    }

    /**
     * @see Table#join
     */
    public static <J> JoinTable<J> join(Class<J> joinType, Table<?>... tables) {
        Class<?> clazz = find(joinType);
        // Note: This method could be sped up a bit by caching references to the MethodHandles
        // instead, but then the generated classes would need a singleton reference to the
        // handle to prevent leaking classes when cache entries are removed. Also, the find
        // method would need to call revealDirect to obtain the underlying class.
        MethodType mt = MethodType.methodType(void.class, Table[].class);
        try {
            return (JoinTable<J>) MethodHandles.lookup().findConstructor(clazz, mt).invoke(tables);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    /**
     * Returns a class which is constructed with sub Table instances (varargs).
     */
    static Class<?> find(Class<?> joinType) {
        return cCache.obtain(joinType, null);
    }

    private final Class<?> mJoinType;
    private final Class<?> mJoinClass;
    private final JoinRowInfo mJoinInfo;
    private final ClassMaker mClassMaker;

    private JoinTableMaker(Class<?> joinType) {
        mJoinType = joinType;
        mJoinClass = JoinRowMaker.find(joinType);
        mJoinInfo = JoinRowInfo.find(joinType);

        mClassMaker = RowGen.beginClassMaker
            (JoinTableMaker.class, joinType, mJoinInfo.name, null, "table")
            .extend(JoinTable.class).public_();
    }

    private Class<?> finish() {
        // Add table fields.
        for (ColumnInfo info : mJoinInfo.allColumns.values()) {
            mClassMaker.addField(Table.class, info.name).private_().final_();
        }

        addConstructor();

        // Add the simple rowType method.
        mClassMaker.addMethod(Class.class, "rowType").public_().return_(mJoinType);

        // Add the newRow method and its bridge.
        {
            MethodMaker mm = mClassMaker.addMethod(mJoinType, "newRow").public_();
            mm.return_(mm.new_(mJoinClass));
            mm = mClassMaker.addMethod(Object.class, "newRow").public_().bridge();
            mm.return_(mm.this_().invoke(mJoinType, "newRow", null));
        }

        // Add the cloneRow method and its bridge.
        {
            MethodMaker mm = mClassMaker.addMethod(mJoinType, "cloneRow", Object.class).public_();
            mm.return_(mm.param(0).cast(mJoinClass).invoke("clone"));
            mm = mClassMaker.addMethod(Object.class, "cloneRow", Object.class).public_().bridge();
            mm.return_(mm.this_().invoke(mJoinType, "cloneRow", null, mm.param(0)));
        }

        // Add the unsetRow method.
        {
            MethodMaker mm = mClassMaker.addMethod(null, "unsetRow", Object.class).public_();
            var rowVar = mm.param(0).cast(mJoinClass);
            for (ColumnInfo info : mJoinInfo.allColumns.values()) {
                rowVar.field(info.name).set(null);
            }
        }

        // Add the copyRow method.
        {
            MethodMaker mm = mClassMaker.addMethod
                (null, "copyRow", Object.class, Object.class).public_();
            var srcRowVar = mm.param(0).cast(mJoinClass);
            var dstRowVar = mm.param(1).cast(mJoinClass);
            for (ColumnInfo info : mJoinInfo.allColumns.values()) {
                dstRowVar.field(info.name).set(srcRowVar.field(info.name));
            }
        }

        addNewScannerMethod();
        addNewScannerWithMethod();
        addNewScannerQueryMethod();
        addIsEmptyMethod();
        addComparatorMethod();
        addPredicateMethod();
        addScannerPlanMethod();

        return mClassMaker.finish();
    }

    private void addConstructor() {
        MethodMaker mm = mClassMaker.addConstructor(Table[].class).varargs().public_();
        mm.invokeSuperConstructor();

        int numTables = mJoinInfo.allColumns.size();
        var tablesVar = mm.param(0);

        Label sizeMatches = mm.label();
        tablesVar.alength().ifEq(numTables, sizeMatches);
        mm.var(JoinTableMaker.class).invoke("mismatch", numTables, tablesVar).throw_();
        sizeMatches.here();

        if (numTables == 0) {
            return;
        }

        // Maps column types to all the columns that use that type.
        var columnMap = new LinkedHashMap<Class<?>, ArrayList<ColumnInfo>>();
        for (ColumnInfo info : mJoinInfo.allColumns.values()) {
            var list = columnMap.get(info.type);
            if (list == null) {
                list = new ArrayList<>();
                columnMap.put(info.type, list);
            }
            list.add(info);
        }

        // Assign the fields by looping over the given tables, which in turn loops over all the
        // fields. Cost is O(n^2), but the number of joined tables isn't expected to be huge.

        var tableIndexVar = mm.var(int.class).set(0);
        Label start = mm.label().here();
        Label end = mm.label();
        tablesVar.alength().ifEq(tableIndexVar, end);

        var tableVar = tablesVar.aget(tableIndexVar);
        var tableTypeVar = tableVar.invoke("rowType");
        Label matched = mm.label();

        for (ArrayList<ColumnInfo> columns : columnMap.values()) {
            ColumnInfo info = columns.get(0);
            Class<?> type = info.type;
            Label nextType = mm.label();
            mm.var(Class.class).set(type).invoke
                ("isAssignableFrom", tableTypeVar).ifFalse(nextType);

            int size = columns.size();

            for (int i=0; i<size; i++) {
                info = columns.get(i);
                if (i >= size - 1) {
                    break;
                }
                Field columnField = mm.field(info.name);
                Label next = mm.label();
                columnField.ifNe(null, next);
                columnField.set(tableVar);
                matched.goto_();
                next.here();
            }

            mm.field(info.name).set(tableVar);
            matched.goto_();

            nextType.here();
        }

        matched.here();

        tableIndexVar.inc(1);
        start.goto_();
        end.here();
    }

    /**
     * Adds the newScanner method which doesn't have query filter and is thus a cross join.
     */
    private void addNewScannerMethod() {
        MethodMaker mm = mClassMaker.addMethod
            (Scanner.class, "newScanner", Transaction.class).public_();
        if (mJoinInfo.allColumns.isEmpty()) {
            mm.return_(mm.var(EmptyScanner.class).invoke("make"));
        } else {
            Bootstrap indy = mm.var(JoinTableMaker.class).indy("indyCrossJoin", mJoinType);
            mm.return_(indy.invoke(Scanner.class, "_", null, mm.this_(), mm.param(0)));
        }
    }

    /**
     * Adds the newScannerWith method which doesn't have query filter and is thus a cross join.
     */
    private void addNewScannerWithMethod() {
        MethodMaker mm = mClassMaker.addMethod
            (Scanner.class, "newScannerWith", Transaction.class, Object.class).public_();
        var joinRowVar = mm.param(1).cast(mJoinClass);
        if (mJoinInfo.allColumns.isEmpty()) {
            mm.return_(mm.var(EmptyScanner.class).invoke("make"));
        } else {
            Bootstrap indy = mm.var(JoinTableMaker.class).indy("indyCrossJoin", mJoinType);
            mm.return_(indy.invoke(Scanner.class, "_", null, mm.this_(), mm.param(0), joinRowVar));
        }
    }

    public static CallSite indyCrossJoin(MethodHandles.Lookup lookup, String name, MethodType mt,
                                         Class<?> joinType)
        throws Throwable
    {
        boolean withVariant = mt.parameterCount() > 2;

        MethodMaker mm = MethodMaker.begin(lookup, name, mt);

        var tableVar = mm.param(0);
        var txnVar = mm.param(1);

        Class<?> joinClass = JoinRowMaker.find(joinType);

        Variable joinRowVar;
        if (!withVariant) {
            joinRowVar = mm.new_(joinClass);
        } else {
            joinRowVar = mm.param(2);
            var notNull = mm.label();
            joinRowVar.ifNe(null, notNull);
            joinRowVar.set(mm.new_(joinClass));
            notNull.here();
        }

        String[] levels = JoinRowInfo.find(joinType).allColumns.keySet().toArray(String[]::new);
        Class<?> scannerClass = JoinScannerMaker.find(joinType, levels);

        var paramTypes = new Class[2 + levels.length];
        paramTypes[0] = Transaction.class;
        paramTypes[1] = joinType;
        paramTypes[2] = Scanner.class;
        for (int i=3; i<paramTypes.length; i++) {
            paramTypes[i] = Table.class;
        }

        MethodHandle ctor = lookup.findConstructor
            (scannerClass, MethodType.methodType(void.class, paramTypes));

        var levelField = tableVar.field(levels[0]);
        Variable firstScannerVar;

        if (!withVariant) {
            firstScannerVar = levelField.invoke("newScanner", txnVar);
        } else {
            var levelRowVar = joinRowVar.field(levels[0]);
            firstScannerVar = levelField.invoke("newScannerWith", txnVar, levelRowVar);
        }

        var params = new Object[2 + levels.length];
        params[0] = txnVar;
        params[1] = joinRowVar;
        params[2] = firstScannerVar;
        for (int i=3; i<params.length; i++) {
            params[i] = tableVar.field(levels[i - 2]);
        }

        mm.return_(mm.invoke(ctor, params));

        return new ConstantCallSite(mm.finish());
    }

    private void addNewScannerQueryMethod() {
        // FIXME: addNewScannerQueryMethod; two variants: newScanner and newScannerWith
    }

    private void addIsEmptyMethod() {
        MethodMaker mm = mClassMaker.addMethod(boolean.class, "isEmpty").public_();

        if (mJoinInfo.allColumns.isEmpty()) {
            mm.return_(true);
            return;
        }

        for (ColumnInfo info : mJoinInfo.allColumns.values()) {
            Label cont = mm.label();
            mm.field(info.name).invoke("isEmpty").ifFalse(cont);
            mm.return_(true);
            cont.here();
        }

        mm.return_(false);
    }

    private void addComparatorMethod() {
        // FIXME: addComparatorMethod
    }

    private void addPredicateMethod() {
        // FIXME: addPredicateMethod
    }

    private void addScannerPlanMethod() {
        MethodMaker mm = mClassMaker.addMethod
            (QueryPlan.class, "scannerPlan", Transaction.class, String.class, Object[].class);
        mm.varargs().public_();

        var txnVar = mm.param(0);
        var queryVar = mm.param(1);
        var argsVar = mm.param(2);

        Label hasQuery = mm.label();
        queryVar.ifNe(null, hasQuery);

        var subPlansVar = mm.new_(QueryPlan[].class, mJoinInfo.allColumns.size());
        int i = 0;
        for (ColumnInfo info : mJoinInfo.allColumns.values()) {
            var subPlanVar = mm.field(info.name).invoke("scannerPlan", txnVar, queryVar, argsVar);
            subPlansVar.aset(i, subPlanVar);
            i++;
        }

        mm.return_(mm.new_(QueryPlan.NestedLoopsJoin.class, subPlansVar));

        hasQuery.here();
        mm.new_(Exception.class, "FIXME: addScannerPlanMethod").throw_();
    }

    // Called by generated code.
    public static IllegalStateException mismatch(int expect, Object[] actual) {
        StringBuilder b = new StringBuilder().append("Expected ").append(expect).append(" table");
        if (expect != 1) {
            b.append('s');
        }
        b.append(", but got ").append(actual.length);
        return new IllegalStateException(b.toString());
    }
}
