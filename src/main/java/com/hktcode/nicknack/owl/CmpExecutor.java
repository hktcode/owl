/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public abstract class CmpExecutor<T extends CmpOperateEntity>
{
    public static ImmutableList<Duration> sort(Future<Duration>[] futures) //
        throws ExecutionException, InterruptedException
    {
        Duration[] durations = new Duration[futures.length];
        for (int i = 0; i < futures.length; ++i) {
            durations[i] = futures[i].get();
        }
        Arrays.sort(durations, (lhs, rhs)->rhs.compareTo(lhs));
        return ImmutableList.copyOf(durations);
    }

    private static final Logger logger = LoggerFactory.getLogger(CmpExecutor.class);

    protected final T entity;

    protected CmpExecutor(T entity)
    {
        this.entity = entity;
    }

    public abstract long put(ZonedDateTime now, RecordUpdateNormal record) throws InterruptedException;

    public abstract long put(ZonedDateTime now, RecordDeleteNormal record) throws InterruptedException;

    public abstract long put(ZonedDateTime now, RecordInsertNormal record) throws InterruptedException;

    public void executeInternal
        /* */( ZonedDateTime now
        /* */, BlockingQueue<RecordSelect> oldQueue //
        /* */, BlockingQueue<RecordSelect> newQueue //
        /* */) throws InterruptedException //
    {
        RecordSelect oldval = oldQueue.take();
        RecordSelect newval = newQueue.take();
        long updateCounts = 0;
        long updateEffect = 0;
        long deleteCounts = 0;
        long deleteEffect = 0;
        long insertCounts = 0;
        long insertEffect = 0;
        while (!(oldval instanceof RecordFinishNormal && newval instanceof RecordFinishNormal)) {
            if (oldval instanceof RecordFinishThrows || newval instanceof RecordFinishThrows) {
                logger.info("stic failure, fetch a RecordFinishThrows");
                break;
            }
            int c;
            if (oldval instanceof RecordFinishNormal) {
                c = 1;
            }
            else if (newval instanceof RecordFinishNormal) {
                c = -1;
            }
            else {
                c = this.compare((RecordSelectNormal)oldval, (RecordSelectNormal)newval);
            }
            if (c == 0) {
                RecordSelectNormal oldselect = (RecordSelectNormal)oldval;
                RecordSelectNormal newselect = (RecordSelectNormal)newval;
                long change = this.put(now, RecordUpdateNormal.of(oldselect.tupleval, newselect.tupleval));
                if (change != 0) {
                    ++updateCounts;
                    updateEffect += change;
                }
                oldval = oldQueue.take();
                newval = newQueue.take();
            }
            else if (c < 0) {
                RecordSelectNormal oldselect = (RecordSelectNormal)oldval;
                long change = this.put(now, RecordDeleteNormal.of(oldselect.tupleval));
                if (change != 0) {
                    ++deleteCounts;
                    deleteEffect += change;
                }
                oldval = oldQueue.take();
            }
            else {
                RecordSelectNormal newselect = (RecordSelectNormal)newval;
                long change = this.put(now, RecordInsertNormal.of(newselect.tupleval));
                if (change != 0) {
                    ++insertCounts;
                    insertEffect += change;
                }
                newval = newQueue.take();
            }
        }
        logger.info("compare execute finish: " //
                + "\n    insertCounts={}" //
                + "\n    updateCounts={}" //
                + "\n    deleteCounts={}" //
                + "\n    insertEffect={}" //
                + "\n    updateEffect={}" //
                + "\n    deleteEffect={}" //
            , insertCounts, updateCounts, deleteCounts //
            , insertEffect, updateEffect, deleteEffect);
    }

    private int compare(RecordSelectNormal lhs, RecordSelectNormal rhs)
    {
        int compare = 0;
        for (String unkey : this.entity.tuplekey) {
            Object lhsval = lhs.tupleval.get(unkey);
            Object rhsval = rhs.tupleval.get(unkey);
            compare = compare(lhsval, rhsval, unkey);
            if (compare != 0) {
                return compare;
            }
        }
        return compare;
    }

    private static int compare(Object lhsval, Object rhsval, String unkey)
    {
        if (lhsval instanceof Byte && rhsval instanceof Byte) {
            Byte lhs = (Byte)lhsval;
            Byte rhs = (Byte)rhsval;
            return Byte.compare(lhs, rhs);
        }
        else if (lhsval instanceof Short && rhsval instanceof Short) {
            Short lhs = (Short)lhsval;
            Short rhs = (Short)rhsval;
            return Short.compare(lhs, rhs);
        }
        else if (lhsval instanceof Integer && rhsval instanceof Integer) {
            Integer lhs = (Integer)lhsval;
            Integer rhs = (Integer)rhsval;
            return Integer.compare(lhs, rhs);
        }
        else if (lhsval instanceof Long && rhsval instanceof Long) {
            Long lhs = (Long) lhsval;
            Long rhs = (Long) rhsval;
            return Long.compare(lhs, rhs);
        }
        else if (lhsval instanceof Float && rhsval instanceof Float) {
            return Float.compare((Float)lhsval, (Float)rhsval);
        }
        else if (lhsval instanceof Double && rhsval instanceof Double) {
            Double lhs = (Double)lhsval;
            Double rhs = (Double)rhsval;
            return Double.compare(lhs, rhs);
        }
        else if (lhsval instanceof BigInteger && rhsval instanceof BigInteger) {
            return ((BigInteger)lhsval).compareTo((BigInteger)rhsval);
        }
        else if (lhsval instanceof BigDecimal && rhsval instanceof BigDecimal) {
            return ((BigDecimal)lhsval).compareTo((BigDecimal)rhsval);
        }
        else if (lhsval instanceof Number && rhsval instanceof Number) {
            BigDecimal lhs = new BigDecimal(lhsval.toString());
            BigDecimal rhs = new BigDecimal(rhsval.toString());
            return lhs.compareTo(rhs);
        }
        else if (lhsval instanceof java.sql.Date && rhsval instanceof java.sql.Date) {
            java.sql.Date lhs = (java.sql.Date)lhsval;
            java.sql.Date rhs = (java.sql.Date)rhsval;
            return lhs.compareTo(rhs);
        }
        else if (lhsval instanceof Time && rhsval instanceof Time) {
            Time lhs = (Time)lhsval;
            Time rhs = (Time)rhsval;
            return lhs.compareTo(rhs);
        }
        else if (lhsval instanceof Timestamp && rhsval instanceof Timestamp) {
            Timestamp lhs = (Timestamp)lhsval;
            Timestamp rhs = (Timestamp)rhsval;
            return lhs.compareTo(rhs);
        }
        else if (lhsval instanceof Date && rhsval instanceof Date) {
            Date lhs = (Date)lhsval;
            Date rhs = (Date)rhsval;
            return lhs.compareTo(rhs);
        }
        else if (lhsval instanceof String && rhsval instanceof String) {
            ByteBuffer lhs = ByteBuffer.wrap(((String)lhsval).getBytes(StandardCharsets.UTF_8));
            ByteBuffer rhs = ByteBuffer.wrap(((String)rhsval).getBytes(StandardCharsets.UTF_8));
            return lhs.compareTo(rhs);
        }
        else if (lhsval instanceof byte[] && rhsval instanceof byte[]) {
            ByteBuffer lhs = ByteBuffer.wrap((byte[])lhsval);
            ByteBuffer rhs = ByteBuffer.wrap((byte[])rhsval);
            return lhs.compareTo(rhs);
        }
        else if (lhsval instanceof ByteBuffer && rhsval instanceof ByteBuffer) {
            ByteBuffer lhs = (ByteBuffer)lhsval;
            ByteBuffer rhs = (ByteBuffer)rhsval;
            return lhs.compareTo(rhs);
        }
        else if (Objects.equals(lhsval, rhsval)) {
            return 0;
        }
        else {
            logger.error("NOT SUPPORT COLUMN TYPE: lhs={}, rhs={}, lhs.type={}, rhs.type={}, unkey={}" //
                , lhsval, rhsval, lhsval.getClass(), rhsval.getClass(), unkey);
            throw new RuntimeException(); // TODO:
        }
    }

    public static boolean equals(Object lhsval, Object rhsval)
    {
        if (lhsval instanceof byte[] && rhsval instanceof byte[]) {
            ByteBuffer lhs = ByteBuffer.wrap((byte[])lhsval);
            ByteBuffer rhs = ByteBuffer.wrap((byte[])rhsval);
            return lhs.compareTo(rhs) == 0;
        }
        if (lhsval instanceof ByteBuffer && rhsval instanceof ByteBuffer) {
            ByteBuffer lhs = (ByteBuffer)lhsval;
            ByteBuffer rhs = (ByteBuffer)rhsval;
            return lhs.compareTo(rhs) == 0;
        }
        Object lhs = lhsval;
        Object rhs = rhsval;
        if (lhsval instanceof Number && rhsval instanceof Number) {
            lhs = new BigDecimal(lhsval.toString());
            rhs = new BigDecimal(rhsval.toString());
        }
        return Objects.equals(lhs, rhs);
    }
}
