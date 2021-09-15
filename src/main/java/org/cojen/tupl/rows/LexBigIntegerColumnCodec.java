/*
 *  Copyright 2021 Cojen.org
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

package org.cojen.tupl.rows;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Encoding suitable for lexicographically ordered columns which supports nulls.
 *
 * @see RowUtils#encodeBigIntegerLex
 * @author Brian S O'Neill
 */
final class LexBigIntegerColumnCodec extends BigIntegerColumnCodec {
    protected Variable mBytesVar;

    LexBigIntegerColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new LexBigIntegerColumnCodec(mInfo, mm);
    }

    @Override
    int minSize() {
        return 1;
    }

    @Override
    boolean isLast() {
        return false;
    }

    @Override
    void encodePrepare() {
        mBytesVar = mMaker.var(byte[].class);
    }

    @Override
    void encodeSkip() {
        mBytesVar.set(null);
    }

    @Override
    Variable encodeSize(Variable srcVar, Variable totalVar) {
        return encodeSize(srcVar, totalVar, mBytesVar);
    }

    private Variable encodeSize(Variable srcVar, Variable totalVar, Variable bytesVar) {
        if (totalVar == null) {
            totalVar = mMaker.var(int.class).set(0);
        }
        Label notNull = mMaker.label();
        srcVar.ifNe(null, notNull);
        bytesVar.set(null);
        Label cont = mMaker.label();
        mMaker.goto_(cont);
        notNull.here();
        bytesVar.set(srcVar.invoke("toByteArray"));
        var lengthVar = bytesVar.alength();
        totalVar.inc(lengthVar);
        lengthVar.ifLt(0x7f, cont);
        totalVar.inc(4);
        cont.here();
        return totalVar;
    }

    @Override
    void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        String methodName = "encodeBigIntegerLex";
        if (mInfo.isDescending()) {
            methodName += "Desc";
        }
        offsetVar.set(mMaker.var(RowUtils.class).invoke(methodName, dstVar, offsetVar, mBytesVar));
    }

    @Override
    void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        var rowUtils = mMaker.var(RowUtils.class);
        var resultVar = rowUtils.invoke("decodeBigIntegerLexHeader", srcVar, offsetVar);
        offsetVar.set(resultVar.cast(int.class));

        var lengthVar = resultVar.shr(32).cast(int.class);

        if (dstVar == null) {
            offsetVar.inc(lengthVar);
        } else {
            Label notNull = mMaker.label();
            lengthVar.ifNe(0, notNull);
            dstVar.set(null);
            Label cont = mMaker.label();
            mMaker.goto_(cont);
            notNull.here();
            if (mInfo.isDescending()) {
                rowUtils.invoke("flip", srcVar, offsetVar, lengthVar);
                Label tryStart = mMaker.label().here();
                dstVar.set(mMaker.new_(dstVar, srcVar, offsetVar, lengthVar));
                mMaker.finally_(tryStart, () -> {
                    rowUtils.invoke("flip", srcVar, offsetVar, lengthVar);
                });
            } else {
                dstVar.set(mMaker.new_(dstVar, srcVar, offsetVar, lengthVar));
            }
            offsetVar.inc(lengthVar);
            cont.here();
        }
    }

    @Override
    void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        decode(null, srcVar, offsetVar, endVar);
    }

    @Override
    protected Variable filterPrepareBytes(Variable argVar) {
        String methodName = "encodeBigIntegerLex";
        if (mInfo.isDescending()) {
            methodName += "Desc";
        }
        var bytesVar = mMaker.var(byte[].class);
        var lengthVar = encodeSize(argVar, mMaker.var(int.class).set(minSize()), bytesVar);
        var encodedBytesVar = mMaker.new_(byte[].class, lengthVar);
        mMaker.var(RowUtils.class).invoke(methodName, encodedBytesVar, 0, bytesVar);
        return encodedBytesVar;
    }

    @Override
    protected boolean compareBytesUnsigned() {
        return true;
    }

    @Override
    boolean canFilterQuick(ColumnInfo dstInfo) {
        return dstInfo.typeCode == mInfo.typeCode;
    }

    @Override
    Object filterQuickDecode(ColumnInfo dstInfo,
                             Variable srcVar, Variable offsetVar, Variable endVar)
    {
        decodeSkip(srcVar, offsetVar, endVar);
        // Return a stable copy to the end offset.
        return offsetVar.get();
    }

    /**
     * @param decoded the string end offset, unless a String compare should be performed
     */
    @Override
    void filterQuickCompare(ColumnInfo dstInfo, Variable srcVar, Variable offsetVar,
                            int op, Object decoded, Variable argObjVar, int argNum,
                            Label pass, Label fail)
    {
        filterQuickCompareLex(dstInfo, srcVar, offsetVar, (Variable) decoded,
                              op, argObjVar, argNum, pass, fail);
    }

    @Override
    protected void decodeHeader(Variable srcVar, Variable offsetVar, Variable endVar,
                                Variable lengthVar, Variable isNullVar)
    {
        throw new UnsupportedOperationException();
    }
}