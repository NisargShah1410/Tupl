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

import java.io.IOException;

import java.lang.invoke.MethodHandles;

import java.util.Comparator;
import java.util.Map;

import java.util.function.Predicate;

import org.cojen.dirmi.Pipe;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Field;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Table;

/**
 * Generates classes which are used by the remote client. Although the class implements the
 * Table interface, only the basic "rowType" methods are implemented.
 *
 * @author Brian S O'Neill
 * @see TableBasicsMaker
 */
public abstract class ClientTableHelper<R> implements Table<R> {
    private static final WeakCache<Class<?>, ClientTableHelper<?>, Object> cCache;

    static {
        cCache = new WeakCache<>() {
            @Override
            protected ClientTableHelper<?> newValue(Class<?> rowType, Object unused) {
                return make(rowType);
            }
        };
    }

    /**
     * Returns a cached singleton instance for the given row type.
     */
    @SuppressWarnings("unchecked")
    public static <R> ClientTableHelper<R> find(Class<R> rowType) {
        return (ClientTableHelper<R>) cCache.obtain(rowType, null);
    }

    /**
     * @param pipe is recycled or closed as a side-effect
     */
    public abstract boolean load(R row, Pipe pipe) throws IOException;

    /**
     * @param pipe is recycled or closed as a side-effect
     */
    public abstract boolean exists(R row, Pipe pipe) throws IOException;

    /**
     * @param pipe is recycled or closed as a side-effect
     */
    public abstract void store(R row, Pipe pipe) throws IOException;

    /**
     * @param pipe is recycled or closed as a side-effect
     */
    public abstract R exchange(R row, Pipe pipe) throws IOException;

    /**
     * @param pipe is recycled or closed as a side-effect
     */
    public abstract boolean insert(R row, Pipe pipe) throws IOException;

    /**
     * @param pipe is recycled or closed as a side-effect
     */
    public abstract boolean replace(R row, Pipe pipe) throws IOException;

    /**
     * @param pipe is recycled or closed as a side-effect
     */
    public abstract boolean update(R row, Pipe pipe) throws IOException;

    /**
     * @param pipe is recycled or closed as a side-effect
     */
    public abstract boolean merge(R row, Pipe pipe) throws IOException;

    /**
     * @param pipe is recycled or closed as a side-effect
     */
    public abstract boolean delete(R row, Pipe pipe) throws IOException;

    @Override
    public Comparator<R> comparator(String spec) {
        // FIXME: comparator
        throw null;
    }

    @Override
    public Predicate<R> predicate(String query, Object... args) {
        // FIXME: predicate
        throw null;
    }

    /**
     * Returns an uncloned row descriptor, which is an encoded RowHeader.
     */
    public abstract byte[] rowDescriptor();

    protected void writeAndRead(Pipe pipe, byte[] bytesVar) throws Throwable {
        pipe.write(bytesVar);
        pipe.flush();

        Object obj = pipe.readThrowable();

        if (obj instanceof Throwable e) {
            try {
                pipe.recycle();
            } catch (Throwable e2) {
                e.addSuppressed(e2);
            }
            throw e;
        }
    }

    protected void success(Pipe pipe) {
        try {
            pipe.recycle();
        } catch (IOException e) {
            // Ignore.
        }
    }

    protected void fail(Pipe pipe, Throwable e) {
        try {
            pipe.close();
        } catch (Throwable e2) {
            e.addSuppressed(e2);
        }
    }

    private static ClientTableHelper<?> make(Class<?> rowType) {
        RowGen rowGen = RowInfo.find(rowType).rowGen();
        ClassMaker cm = rowGen.beginClassMaker(ClientTableHelper.class, rowType, null);

        cm.public_().extend(ClientTableHelper.class);

        // This implements the basic "rowType" methods.
        cm.implement(TableBasicsMaker.find(rowType));

        cm.addConstructor().private_();

        // Keep a singleton instance, in order for a weakly cached reference to the helper to
        // stick around until the class is unloaded.
        cm.addField(ClientTableHelper.class, "THE").private_().static_().final_();

        cm.addField(boolean.class, "assert").private_().static_().final_();

        {
            MethodMaker mm = cm.addClinit();
            mm.field("THE").set(mm.new_(cm));
            mm.field("assert").set(mm.class_().invoke("desiredAssertionStatus"));
        }

        // Add the rowDescriptor method.
        {
            MethodMaker mm = cm.addMethod(byte[].class, "rowDescriptor").public_();
            var descVar = mm.var(byte[].class);
            descVar.setExact(RowHeader.make(rowGen).encode(false));
            mm.return_(descVar);
        }

        Class<?> rowClass = RowMaker.find(rowType);

        addEncodeMethods(cm, rowGen, rowClass);

        addByKeyMethod("load", cm, rowGen, rowClass);
        addByKeyMethod("exists", cm, rowGen, rowClass);
        addByKeyMethod("delete", cm, rowGen, rowClass);

        addStoreMethod("store", null, cm, rowGen, rowClass);
        addStoreMethod("exchange", rowType, cm, rowGen, rowClass);
        addStoreMethod("insert", boolean.class, cm, rowGen, rowClass);
        addStoreMethod("replace", boolean.class, cm, rowGen, rowClass);

        addUpdateMethod("update", cm, rowGen, rowClass);
        addUpdateMethod("merge", cm, rowGen, rowClass);

        // Need to implement a bridge for the exchange method.
        {
            MethodMaker mm = cm.addMethod
                (Object.class, "exchange", Object.class, Pipe.class).public_().bridge();
            mm.return_(mm.this_().invoke(rowType, "exchange", null, mm.param(0), mm.param(1)));
        }

        MethodHandles.Lookup lookup = cm.finishHidden();

        try {
            var clazz = lookup.lookupClass();
            var vh = lookup.findStaticVarHandle(clazz, "THE", ClientTableHelper.class);
            return (ClientTableHelper<?>) vh.get();
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    /**
     * Adds the following methods which encode row states and columns.
     *
     * private static byte[] encodeKeyColumns(R row);   // encode only set/dirty key columns
     * private static byte[] encodeAllColumns(R row);   // encode all set/dirty columns
     * private static byte[] encodeDirtyColumns(R row); // encode set/dirty keys and dirty values
     */
    private static void addEncodeMethods(ClassMaker cm, RowGen rowGen, Class<?> rowClass) {
        MethodMaker mm;

        mm = cm.addMethod(byte[].class, "encodeKeyColumns", rowClass).private_().static_();
        mm.return_(encodeColumns(rowGen, mm.param(0), true, false));

        mm = cm.addMethod(byte[].class, "encodeAllColumns", rowClass).private_().static_();
        mm.return_(encodeColumns(rowGen, mm.param(0), false, false));

        mm = cm.addMethod(byte[].class, "encodeDirtyColumns", rowClass).private_().static_();
        mm.return_(encodeColumns(rowGen, mm.param(0), false, true));
    }

    /**
     * @param variant "load", "exists", or "delete"
     */
    private static void addByKeyMethod(String variant,
                                       ClassMaker cm, RowGen rowGen, Class<?> rowClass)
    {
        MethodMaker mm = cm.addMethod(boolean.class, variant, Object.class, Pipe.class).public_();

        var rowVar = mm.param(0).cast(rowClass);
        var pipeVar = mm.param(1);

        mm.invoke("writeAndRead", pipeVar, mm.invoke("encodeKeyColumns", rowVar));

        Label tryStart = mm.label().here();

        var resultVar = pipeVar.invoke("readByte");

        if (variant == "load") {
            Label notLoaded = mm.label();
            resultVar.ifEq(0, notLoaded);
            decodeValueColumns(rowGen, rowVar, pipeVar);
            TableMaker.markAllClean(rowVar, rowGen, rowGen);
            Label done = mm.label().goto_();
            notLoaded.here();
            rowGen.markNonPrimaryKeyColumnsUnset(rowVar);
            done.here();
        }

        mm.invoke("success", pipeVar);
        mm.return_(resultVar.ne(0));

        mm.catch_(tryStart, Throwable.class, exVar -> {
            mm.invoke("fail", pipeVar, exVar);
            exVar.throw_();
        });
    }

    /**
     * @param variant "store", "exchange", "insert", or "replace"
     */
    private static void addStoreMethod(String variant, Class returnType,
                                       ClassMaker cm, RowGen rowGen, Class<?> rowClass)
    {
        MethodMaker mm = cm.addMethod(returnType, variant, Object.class, Pipe.class).public_();

        var rowVar = mm.param(0).cast(rowClass);
        var pipeVar = mm.param(1);

        mm.invoke("writeAndRead", pipeVar, mm.invoke("encodeAllColumns", rowVar));

        Label tryStart = mm.label().here();

        // FIXME: Support automatic key columns. A different result code indicates that columns
        // must be loaded, which is expected to be just one.
        var resultVar = pipeVar.invoke("readByte");

        if (variant == "store") {
            TableMaker.markAllClean(rowVar, rowGen, rowGen);
            mm.invoke("success", pipeVar);
            mm.return_();
        } else if (variant == "exchange") {
            TableMaker.markAllClean(rowVar, rowGen, rowGen);
            Variable oldRowVar = mm.var(rowClass).set(null);
            Label noOperation = mm.label();
            resultVar.ifEq(0, noOperation);
            oldRowVar.set(mm.invoke("newRow").cast(rowClass));
            TableMaker.copyFields(rowVar, oldRowVar, rowGen.info.keyColumns.values());
            decodeValueColumns(rowGen, oldRowVar, pipeVar);
            TableMaker.markAllClean(oldRowVar, rowGen, rowGen);
            noOperation.here();
            mm.invoke("success", pipeVar);
            mm.return_(oldRowVar);
        } else {
            Label noOperation = mm.label();
            resultVar.ifEq(0, noOperation);
            TableMaker.markAllClean(rowVar, rowGen, rowGen);
            noOperation.here();
            mm.invoke("success", pipeVar);
            mm.return_(resultVar.ne(0));
        }

        mm.catch_(tryStart, Throwable.class, exVar -> {
            mm.invoke("fail", pipeVar, exVar);
            exVar.throw_();
        });
    }

    /**
     * @param variant "update" or "merge"
     */
    private static void addUpdateMethod(String variant,
                                        ClassMaker cm, RowGen rowGen, Class<?> rowClass)
    {
        MethodMaker mm = cm.addMethod(boolean.class, variant, Object.class, Pipe.class).public_();

        var rowVar = mm.param(0).cast(rowClass);
        var pipeVar = mm.param(1);

        mm.invoke("writeAndRead", pipeVar, mm.invoke("encodeDirtyColumns", rowVar));

        Label tryStart = mm.label().here();

        var resultVar = pipeVar.invoke("readByte");

        Label noOperation = mm.label();
        resultVar.ifEq(0, noOperation);

        if (variant == "merge") {
            decodeValueColumns(rowGen, rowVar, pipeVar);
            TableMaker.markAllClean(rowVar, rowGen, rowGen);
        }

        noOperation.here();
        mm.invoke("success", pipeVar);
        mm.return_(resultVar.ne(0));

        mm.catch_(tryStart, Throwable.class, exVar -> {
            mm.invoke("fail", pipeVar, exVar);
            exVar.throw_();
        });
    }

    /**
     * Reads the value columns from a pipe and decodes them into a row.
     */
    private static void decodeValueColumns(RowGen rowGen, Variable rowVar, Variable pipeVar) {
        MethodMaker mm = rowVar.methodMaker();

        var lengthVar = mm.var(RowUtils.class).invoke("decodePrefixPF", pipeVar);
        var bytesVar = mm.new_(byte[].class, lengthVar);

        pipeVar.invoke("readFully", bytesVar);

        ColumnCodec[] valueCodecs = ColumnCodec.bind(rowGen.valueCodecs(), mm);

        var offsetVar = mm.var(int.class).set(0);

        for (ColumnCodec codec : valueCodecs) {
            codec.decode(rowVar.field(codec.mInfo.name), bytesVar, offsetVar, null);
        }
    }

    /**
     * Encodes row state and columns into a byte array.
     *
     * @param keysOnly when true, only encode key columns
     * @param dirtyOnly when true, only dirty value columns are encoded; when false, set/dirty
     * columns are encoded
     * @return a new byte array
     */
    private static Variable encodeColumns(RowGen rowGen, Variable rowVar,
                                          boolean keysOnly, boolean dirtyOnly)
    {
        MethodMaker mm = rowVar.methodMaker();

        // Calculate the key and value encoding length.

        ColumnCodec[] keyCodecs = ColumnCodec.bind(rowGen.keyCodecs(), mm);
        ColumnCodec[] valueCodecs = ColumnCodec.bind(rowGen.valueCodecs(), mm);

        Variable keyLengthVar = calcLength(rowGen, rowVar, keyCodecs, false);

        Variable valueLengthVar;
        if (keysOnly) {
            valueLengthVar = null;
        } else {
            valueLengthVar = calcLength(rowGen, rowVar, valueCodecs, dirtyOnly);
        }

        // Calculate the array size.

        String[] stateFieldNames = rowGen.stateFields();
        var fullLengthVar = mm.var(int.class).set(stateFieldNames.length << 2);
        var utilsVar = mm.var(RowUtils.class);

        // Add in the key encoding length.
        {
            fullLengthVar.inc(keyLengthVar);
            fullLengthVar.inc(utilsVar.invoke("lengthPrefixPF", keyLengthVar));
        }

        // Add in the value encoding length.
        if (valueLengthVar == null) {
            // Need room for the value length prefix, which is zero.
            fullLengthVar.inc(1);
        } else {
            fullLengthVar.inc(valueLengthVar);
            fullLengthVar.inc(utilsVar.invoke("lengthPrefixPF", valueLengthVar));
        }

        // Allocate the array and encode into it.

        var bytesVar = mm.new_(byte[].class, fullLengthVar);
        var offsetVar = mm.var(int.class).set(0);

        // Encode all the column states first.

        if (valueLengthVar != null || valueCodecs.length == 0) {
            if (!dirtyOnly) {
                for (String name : stateFieldNames) {
                    utilsVar.invoke("encodeIntBE", bytesVar, offsetVar, rowVar.field(name));
                    offsetVar.inc(4);
                }
            } else {
                // Value columns which are clean aren't encoded, and so CLEAN states must be
                // converted to UNSET states. Key column states aren't affected.

                final int mask = 0xaaaa_aaaa; // is 0b1010...
                int keysRemaining = keyCodecs.length;

                for (int i=0; i<stateFieldNames.length; i++) {
                    Variable stateVar = rowVar.field(stateFieldNames[i]);

                    if (keysRemaining < 16) {
                        var andMask = mask | ((0b100 << ((keysRemaining - 1) << 1)) - 1);
                        stateVar.set(stateVar.and(andMask).or(stateVar.and(mask).ushr(1)));
                    } else {
                        var maskedStateVar = stateVar.and(mask);
                        stateVar.set(maskedStateVar.or(maskedStateVar.ushr(1)));
                    }

                    utilsVar.invoke("encodeIntBE", bytesVar, offsetVar, stateVar);

                    offsetVar.inc(4);
                    keysRemaining -= 16; // max columns per state field
                }
            }
        } else {
            // The value columns aren't encoded, and so their states are all zero. Although the
            // value column states could be omitted entirely, writing zeros permits future
            // enhancements without breaking compatibility.

            int keysRemaining = keyCodecs.length;

            for (int i=0; i<stateFieldNames.length; i++) {
                if (keysRemaining <= 0) {
                    // No need to explicitly encode a zero state because the array was zero
                    // filled upon allocation. Just increment the offset to skip over the
                    // remaining fields.
                    offsetVar.inc(4 * (stateFieldNames.length - i));
                    break;
                }

                Variable stateVar = rowVar.field(stateFieldNames[i]);

                if (keysRemaining < 16) {
                    int mask = (0b100 << ((keysRemaining - 1) << 1)) - 1;
                    stateVar = stateVar.and(mask);
                }

                utilsVar.invoke("encodeIntBE", bytesVar, offsetVar, stateVar);

                offsetVar.inc(4);
                keysRemaining -= 16; // max columns per state field
            }
        }

        // Encode the key columns.
        {
            offsetVar.set(utilsVar.invoke("encodePrefixPF", bytesVar, offsetVar, keyLengthVar));
            encodeColumns(rowGen, rowVar, bytesVar, offsetVar, keyCodecs, false);
        }

        // Encode the value columns.
        if (valueLengthVar != null) {
            offsetVar.set(utilsVar.invoke("encodePrefixPF", bytesVar, offsetVar, valueLengthVar));
            encodeColumns(rowGen, rowVar, bytesVar, offsetVar, valueCodecs, dirtyOnly);
        }

        Label cont = mm.label();
        mm.field("assert").ifFalse(cont);
        if (valueLengthVar == null) {
            offsetVar.inc(1);
        }
        offsetVar.ifEq(bytesVar.alength(), cont);
        mm.new_(AssertionError.class,
                mm.concat(offsetVar, " != ", bytesVar.alength()), null).throw_();
        cont.here();

        return bytesVar;
    }

    /**
     * Generates code which determines the key or value length, in bytes, when all or a subset
     * of the corresponding columns are known to be set.
     *
     * @param codecs key or value codecs; must be bound to the MethodMaker
     * @return a new int variable with the total length
     */
    private static Variable calcLength(RowGen rowGen, Variable rowVar,
                                       ColumnCodec[] codecs, boolean dirtyOnly)
    {
        MethodMaker mm = rowVar.methodMaker();
        Variable totalVar = mm.var(int.class).set(0);

        if (codecs.length == 0) {
            return totalVar;
        }

        Map<String, Integer> columnNumbers = rowGen.columnNumbers();

        String stateFieldName = null;
        Variable stateField = null;

        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];
            codec.encodePrepare();

            ColumnInfo info = codec.mInfo;
            int num = columnNumbers.get(info.name);

            String sfName = rowGen.stateField(num);
            if (!sfName.equals(stateFieldName)) {
                stateFieldName = sfName;
                stateField = rowVar.field(stateFieldName).get();
            }

            int sfMask = RowGen.stateFieldMask(num);
            Label included = mm.label();
            var masked = stateField.and(sfMask);
            if (dirtyOnly) {
                masked.ifEq(sfMask, included);
            } else {
                masked.ifNe(0, included);
            }

            codec.encodeSkip();
            Label cont = mm.label().goto_();

            included.here();
            int minSize = codec.minSize();
            if (minSize != 0) {
                totalVar.inc(minSize);
            }
            codec.encodeSize(rowVar.field(info.name), totalVar);

            cont.here();
        }

        return totalVar;
    }

    /**
     * Generates code which writes all or a subset of columns. Must call call calcLength first
     * for all the codecs.
     *
     * @param bytesVar destination byte array
     * @param offsetVar offset into byte array; is incremented as a side-effect
     * @param codecs key or value codecs; must be bound to the MethodMaker
     */
    private static void encodeColumns(RowGen rowGen, Variable rowVar,
                                      Variable bytesVar, Variable offsetVar,
                                      ColumnCodec[] codecs, boolean dirtyOnly)
    {
        if (codecs.length == 0) {
            return;
        }

        MethodMaker mm = rowVar.methodMaker();

        Map<String, Integer> columnNumbers = rowGen.columnNumbers();

        String stateFieldName = null;
        Variable stateField = null;

        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];

            ColumnInfo info = codec.mInfo;
            int num = columnNumbers.get(info.name);

            String sfName = rowGen.stateField(num);
            if (!sfName.equals(stateFieldName)) {
                stateFieldName = sfName;
                stateField = rowVar.field(stateFieldName).get();
            }

            int sfMask = RowGen.stateFieldMask(num);
            Label skip = mm.label();
            var masked = stateField.and(sfMask);
            if (dirtyOnly) {
                masked.ifNe(sfMask, skip);
            } else {
                masked.ifEq(0, skip);
            }

            codec.encode(rowVar.field(info.name), bytesVar, offsetVar);

            skip.here();
        }
    }
}