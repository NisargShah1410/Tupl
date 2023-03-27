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

import java.util.Arrays;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.FieldMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;

/**
 * Implements a nested loops join.
 *
 * @author Brian S O'Neill
 */
final class JoinScannerMaker {
    private static final WeakCache<Key, Class<?>, Object> cCache;

    static {
        cCache = new WeakCache<>() {
            @Override
            protected Class<?> newValue(Key key, Object unused) {
                return new JoinScannerMaker(key.joinType, key.levels).finish();
            }
        };
    }

    /**
     * Returns a class implementing JoinScanner, which is constructed with these parameters:
     *
     *   (Transaction txn, J joinRow, Scanner first, Table... rest)
     *
     * The scanner parameter corresponds to the outermost join level, and the table parameters
     * correspond to the inner join levels.
     *
     * Protected fields are defined which reference the transaction and the inner levels:
     *
     *   protected final Transaction txn;
     *   protected final Table level1table, level2table, ...
     *
     * Two protected methods are defined for each inner level, which must be overridden by
     * subclasses which perform filtering:
     *
     *   protected Scanner level1(joinRow)
     *   protected Scanner level1(joinRow, levelRow)
     *
     * The columns of the joinRow parameter are valid for all outer levels. For the variant
     * which accepts a levelRow parameter, this should be used as the first row in the returned
     * scanner.
     *
     * @param joinType interface which defines a join row
     * @param levels defines the join order, starting from the outermost level; a level name
     * corresponds to a join row column
     * @see JoinScanner
     */
    static Class<?> find(Class<?> joinType, String... levels) {
        return cCache.obtain(new Key(joinType, levels), null);
    }

    private static final class Key {
        final Class<?> joinType;
        final String[] levels;

        Key(Class<?> joinType, String[] levels) {
            this.joinType = joinType;
            this.levels = levels;
        }

        @Override
        public int hashCode() {
            return joinType.hashCode() ^ Arrays.hashCode(levels);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Key other && joinType == other.joinType
                && Arrays.equals(levels, other.levels);
        }
    }

    private final Class<?> mJoinType;
    private final Class<?> mJoinClass;
    private final JoinRowInfo mJoinInfo;

    private final ColumnInfo[] mLevels;

    private ClassMaker mClassMaker;
    private MethodMaker mCtor;

    private JoinScannerMaker(Class<?> joinType, String[] levels) {
        if (levels.length <= 1) {
            throw new IllegalArgumentException();
        }

        mJoinType = joinType;
        mJoinClass = JoinRowMaker.find(joinType);
        mJoinInfo = JoinRowInfo.find(joinType);

        mLevels = new ColumnInfo[levels.length];

        for (int i=0; i<levels.length; i++) {
            mLevels[i] = mJoinInfo.allColumns.get(levels[i]);
        }
    }

    private Class<?> finish() {
        mClassMaker = RowGen.beginClassMaker
            (JoinScannerMaker.class, mJoinType, mJoinInfo.name, null, "scanner")
            .extend(JoinScanner.class).public_();

        mClassMaker.addField(Transaction.class, "txn").protected_().final_();
        mClassMaker.addField(mJoinType, "row").private_();

        var ctorParams = new Object[2 + mLevels.length];
        ctorParams[0] = Transaction.class;
        ctorParams[1] = mJoinType;
        ctorParams[2] = Scanner.class;
        for (int i=3; i<ctorParams.length; i++) {
            ctorParams[i] = Table.class;
        }

        mCtor = mClassMaker.addConstructor(ctorParams).public_();
        mCtor.invokeSuperConstructor();
        mCtor.field("txn").set(mCtor.param(0));

        for (int n=0; n<mLevels.length; n++) {
            addLevel(n);
        }

        addDoStepMethod(false);
        addDoStepMethod(true);

        addRowMethod();
        addStepMethod();
        addStepWithMethod();

        addCloseMethod();

        Label tryStart = mCtor.label().here();
        mCtor.invoke("step", mCtor.param(1), false);
        mCtor.return_();
        Label tryEnd = mCtor.label().here();

        var exVar = mCtor.catch_(tryStart, tryEnd, Throwable.class);
        mCtor.invoke("close", exVar).throw_();

        return mClassMaker.finish();
    }

    /**
     * Add the public row method and the bridge.
     */
    private void addRowMethod() {
        MethodMaker mm = mClassMaker.addMethod(mJoinType, "row").public_().final_();
        mm.return_(mm.field("row"));

        mm = mClassMaker.addMethod(Object.class, "row").public_().final_().bridge();
        mm.return_(mm.this_().invoke(mJoinType, "row", null));
    }

    /**
     * Add the public step method and the bridge method.
     */
    private void addStepMethod() {
        MethodMaker mm = mClassMaker.addMethod(mJoinType, "step").public_().final_();
        var newJoinRowVar = mm.new_(mJoinClass);
        copyLevelRows(newJoinRowVar);
        mm.return_(mm.invoke("step", newJoinRowVar, true));

        // Add the bridge method.
        mm = mClassMaker.addMethod(Object.class, "step").public_().final_().bridge();
        mm.return_(mm.this_().invoke(mJoinType, "step", null));
    }

    /**
     * Add the public step method which accepts a row and the bridge method.
     */
    private void addStepWithMethod() {
        MethodMaker mm = mClassMaker.addMethod(mJoinType, "step", Object.class);
        mm.public_().final_();
        var newJoinRowVar = mm.param(0).cast(mJoinType);
        var notNull = mm.label();
        newJoinRowVar.ifNe(null, notNull);
        newJoinRowVar.set(mm.new_(mJoinClass));
        notNull.here();
        copyLevelRows(newJoinRowVar);
        mm.return_(mm.invoke("stepWith", newJoinRowVar, true));

        // Add the bridge method.
        mm = mClassMaker.addMethod(Object.class, "step", Object.class).public_().final_().bridge();
        mm.return_(mm.this_().invoke(mJoinType, "step", null, mm.param(0)));
    }

    /**
     * Copies the previous level rows into the new join row, for all but the last. It will be
     * filled in as a side-effect of calling the private step method. By design, the level row
     * instances which didn't need to change are always the same instance as before. The user
     * shouldn't have replaced them, but don't bother adding extra protection. It just adds
     * overhead and it can still be defeated.
     */
    private void copyLevelRows(Variable newJoinRowVar) {
        MethodMaker mm = newJoinRowVar.methodMaker();
        var joinRowVar = mm.field("row").get();
        for (int n = 0; n < mLevels.length - 1; n++) {
            String name = mLevels[n].name;
            newJoinRowVar.invoke(name, joinRowVar.invoke(name));
        }
    }

    private void addCloseMethod() {
        MethodMaker mm = mClassMaker.addMethod(null, "close").public_();
        mm.field("row").set(null);
        for (int n=0; n<mLevels.length; n++) {
            Variable levelVar = mm.field("level" + n);
            if (n == 0) {
                levelVar.invoke("close");
            } else {
                levelVar = levelVar.get();
                Label isNull = mm.label();
                levelVar.ifEq(null, isNull);
                levelVar.invoke("close");
                isNull.here();
            }
        }
    }

    /**
     * Add fields and methods for the given join level.
     */
    private void addLevel(int n) {
        String name = "level" + n;
        FieldMaker fm = mClassMaker.addField(Scanner.class, name).private_();

        if (n == 0) {
            fm.final_();
            mCtor.field(name).set(mCtor.param(2));
            return;
        }

        String tableFieldName = name + "table";
        mClassMaker.addField(Table.class, tableFieldName).protected_().final_();
        mCtor.field(tableFieldName).set(mCtor.param(2 + n));

        MethodMaker mm = mClassMaker.addMethod(Scanner.class, name, mJoinType).protected_();
        mm.return_(mm.field(tableFieldName).invoke("newScanner", mm.field("txn")));

        mm = mClassMaker.addMethod(Scanner.class, name, mJoinType, mLevels[n].type).protected_();
        mm.return_(mm.field(tableFieldName).invoke("newScannerWith", mm.field("txn"), mm.param(1)));
    }

    /**
     * @param with pass true to generate the variant which steps with a provided row object
     */
    private void addDoStepMethod(boolean with) {
        String name = with ? "stepWith" : "step";

        MethodMaker mm = mClassMaker.addMethod
            (mJoinType, name, mJoinType, boolean.class).private_();

        var joinRowVar = mm.param(0);
        var jumpInVar = mm.param(1);

        Label jumpIn = mm.label();
        jumpInVar.ifTrue(jumpIn);

        makeStepLoop(joinRowVar, jumpIn, 0, null, with);

        mm.field("row").set(null);
        mm.return_(null);
    }

    /**
     * @param with pass true to generate the variant which steps with a provided row object
     */
    private void makeStepLoop(Variable joinRowVar, Label jumpIn, int n, String name, boolean with) {
        MethodMaker mm = joinRowVar.methodMaker();

        if (name == null) {
            name = "level" + n;
        }

        var levelField = mm.field(name);
        var levelRowVar = mm.field(name).invoke("row").cast(mLevels[n].type);

        Label start = mm.label().here();
        Label end = mm.label();
        levelRowVar.ifEq(null, end);

        joinRowVar.invoke(mLevels[n].name, levelRowVar);

        n++;

        if (n >= mLevels.length) {
            mm.field("row").set(joinRowVar);
            mm.return_(joinRowVar);
            // Jump into the middle of a loop!
            jumpIn.here();
        } else {
            String nextName = "level" + n;
            Label recurse = null;

            if (with) {
                var firstRowVar = joinRowVar.invoke(mLevels[n].name);
                Label isNull = mm.label();
                firstRowVar.ifEq(null, isNull);
                mm.field(nextName).set(mm.invoke(nextName, joinRowVar, firstRowVar));
                recurse = mm.label().goto_();
                isNull.here();
            }

            mm.field(nextName).set(mm.invoke(nextName, joinRowVar));

            if (recurse != null) {
                recurse.here();
            }

            makeStepLoop(joinRowVar, jumpIn, n, nextName, with);
        }

        n--;

        if (with) {
            levelRowVar.set(joinRowVar.invoke(mLevels[n].name));
            Label isNull = mm.label();
            levelRowVar.ifEq(null, isNull);
            levelRowVar.set(levelField.invoke("step", levelRowVar).cast(mLevels[n].type));
            start.goto_();
            isNull.here();
        }

        levelRowVar.set(levelField.invoke("step").cast(mLevels[n].type));

        start.goto_();
        end.here();
    }
}
