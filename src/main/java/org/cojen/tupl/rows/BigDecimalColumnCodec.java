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

import java.math.BigInteger;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class BigDecimalColumnCodec extends ColumnCodec {
    private final ColumnCodec mUnscaledCodec;

    private Variable mScaleVar;
    private Variable mUnscaledVar;

    /**
     * @param unscaledCodec supports BigInteger
     */
    BigDecimalColumnCodec(ColumnInfo info, ColumnCodec unscaledCodec, MethodMaker mm) {
        super(info, mm);
        mUnscaledCodec = unscaledCodec;
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new BigDecimalColumnCodec(mInfo, mUnscaledCodec.bind(mm), mm);
    }

    @Override
    protected final boolean doEquals(Object obj) {
        return ((BigDecimalColumnCodec) obj).mUnscaledCodec.equals(mUnscaledCodec);
    }

    @Override
    public final int doHashCode() {
        return mUnscaledCodec.doHashCode();
    }

    @Override
    int codecFlags() {
        return mUnscaledCodec.codecFlags();
    }

    @Override
    int minSize() {
        return 0;
    }

    @Override
    void encodePrepare() {
        mScaleVar = mMaker.var(int.class);
        mUnscaledVar = mMaker.var(BigInteger.class);
        mUnscaledCodec.encodePrepare();
    }

    @Override
    void encodeSkip() {
        mScaleVar.set(0);
        mUnscaledVar.set(null);
        mUnscaledCodec.encodeSkip();
    }

    @Override
    Variable encodeSize(Variable srcVar, Variable totalVar) {
        boolean newTotal = false;
        if (totalVar == null) {
            totalVar = mMaker.var(int.class);
            newTotal = true;
        }

        Label end = null;

        if (mInfo.isNullable()) {
            end = mMaker.label();
            Label notNull = mMaker.label();
            srcVar.ifNe(null, notNull);
            encodeSkip();
            if (newTotal) {
                totalVar.set(1);
            } else {
                totalVar.inc(1);
            }
            mMaker.goto_(end);
            notNull.here();
        }

        var rowUtils = mMaker.var(RowUtils.class);

        mScaleVar.set(rowUtils.invoke("convertSignedVarInt", srcVar.invoke("scale")));

        var lengthVar = rowUtils.invoke("calcUnsignedVarIntLength", mScaleVar);

        if (newTotal) {
            totalVar.set(lengthVar);
        } else {
            totalVar.inc(lengthVar);
        }

        mUnscaledVar.set(srcVar.invoke("unscaledValue"));
        totalVar = mUnscaledCodec.encodeSize(mUnscaledVar, totalVar);

        if (end != null) {
            end.here();
        }

        return totalVar;
    }

    @Override
    void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        Label end = null;
        if (mInfo.isNullable()) {
            end = mMaker.label();
            encodeNullHeaderIfNull(end, srcVar, dstVar, offsetVar);
        }

        var rowUtils = mMaker.var(RowUtils.class);
        offsetVar.set(rowUtils.invoke("encodeUnsignedVarInt", dstVar, offsetVar, mScaleVar));

        mUnscaledCodec.encode(mUnscaledVar, dstVar, offsetVar);

        if (end != null) {
            end.here();
        }
    }

    @Override
    void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        Label end = null;

        if (mInfo.isNullable()) {
            end = mMaker.label();
            Variable h = srcVar.aget(offsetVar).cast(int.class).and(0xff);
            Label notNull = mMaker.label();
            h.ifLt(0xf8, notNull);
            dstVar.set(null);
            offsetVar.inc(1);
            mMaker.goto_(end);
            notNull.here();
        }

        var rowUtils = mMaker.var(RowUtils.class);
        var decodedVar = rowUtils.invoke("decodeSignedVarInt", srcVar, offsetVar);

        offsetVar.set(decodedVar.shr(32).cast(int.class));
        var scaleVar = decodedVar.cast(int.class);
        var unscaledVar = mMaker.var(BigInteger.class);
        mUnscaledCodec.decode(unscaledVar, srcVar, offsetVar, endVar);
        dstVar.set(mMaker.new_(dstVar, unscaledVar, scaleVar));

        if (end != null) {
            end.here();
        }
    }

    @Override
    void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        if (isLast()) {
            mUnscaledCodec.decodeSkip(srcVar, offsetVar, endVar);
        } else {
            String method = mInfo.isNullable() ? "skipNullableBigDecimal" : "skipBigDecimal";
            offsetVar.set(mMaker.var(RowUtils.class).invoke(method, srcVar, offsetVar));
        }
    }
}
