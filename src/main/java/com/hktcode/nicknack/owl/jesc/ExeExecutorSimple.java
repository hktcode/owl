/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.jesc;

import com.google.common.collect.ImmutableMap;
import com.hktcode.lang.exception.ArgumentNullException;
import com.hktcode.nicknack.owl.SqlScriptEntity;
import com.hktcode.nicknack.owl.jdbc.JdbcTemplateBase;
import com.hktcode.nicknack.owl.RecordSelect;
import com.hktcode.nicknack.owl.RecordSelectNormal;
import com.hktcode.nicknack.owl.SelectCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.concurrent.*;

public class ExeExecutorSimple
{
    private static final Logger logger = LoggerFactory.getLogger(ExeExecutorSimple.class);

    private final ExeSimpleEntity entity;

    public ExeExecutorSimple(ExeSimpleEntity entity)
    {
        if (entity == null) {
            throw new ArgumentNullException("entity");
        }
        this.entity = entity;
    }

    public int execute(LocalDate statDate) throws SQLException, InterruptedException
    {
        if (statDate == null) {
            throw new ArgumentNullException("statDate");
        }
        ZonedDateTime starts = ZonedDateTime.now();
        String taskname = entity.taskname;
        logger.info("execute task: statDate={}, taskname={}", statDate, taskname);
        ExecutorService service = Executors.newFixedThreadPool(2);
        ImmutableMap<String, Object> params = ImmutableMap.of("stat_date", statDate.toString());


        SqlScriptEntity prevsql = this.entity.prevcode;
        SqlScriptEntity coresql = this.entity.corecode;
        SqlScriptEntity postsql = this.entity.postcode;

        BlockingQueue<RecordSelect> tqueue = new LinkedBlockingQueue<>(16);

        SelectCallable coreCallable = SelectCallable.of(tqueue, coresql, params);
        Future<Duration> coreFuture = service.submit(coreCallable);
        long fetchMillis = 0;

        final String prevEngine = prevsql.engineid;
        final String prevcode = prevsql.execcode;
        final String postEngine = postsql.engineid;
        final String postcode = postsql.execcode;

        if (!prevEngine.equals("") && !prevcode.equals("") && !prevEngine.equals(postEngine)) {
            JdbcTemplateBase jdbc = JdbcTemplateBase.getJdbcTemplate(prevEngine);
            logger.info("execute prev sql: codename={}", prevsql.codename);
            long rowCount;
            ZonedDateTime prevstarts = ZonedDateTime.now();
            try (Connection cnt = jdbc.getDataSource().getConnection()) {
                rowCount = jdbc.execute(cnt, prevcode, params, prevsql.argument);
            }
            ZonedDateTime prevfinish = ZonedDateTime.now();
            Duration duration = Duration.between(prevstarts, prevfinish);
            logger.info("prev sql success: rowCount={}, duration={}", rowCount, duration);
        }
        if (!postEngine.equals("") && !postcode.equals("")) {
            JdbcTemplateBase jdbc = JdbcTemplateBase.getJdbcTemplate(postEngine);
            try (Connection cnt = jdbc.getDataSource().getConnection()) {
                boolean autoCommit = cnt.getAutoCommit();
                cnt.setAutoCommit(false);
                try {
                    if (prevEngine.equals(postEngine)) {
                        logger.info("execute prev sql: codename={}", prevsql.codename);
                        ZonedDateTime prevstarts = ZonedDateTime.now();
                        long rowCount = jdbc.execute(cnt, prevcode, params, prevsql.argument);
                        ZonedDateTime prevfinish = ZonedDateTime.now();
                        Duration duration = Duration.between(prevstarts, prevfinish);
                        logger.info("prev sql success: rowCount={}, duration={}", rowCount, duration);
                    }
                    fetchMillis += this.callInternal(cnt, tqueue);
                    cnt.commit();
                } catch (Exception ex) {
                    if (!cnt.isClosed()) {
                        cnt.rollback();
                    }
                    logger.error("insert callable throws exception: ", ex);
                    throw ex;
                } finally {
                    if (!cnt.isClosed()) {
                        cnt.setAutoCommit(autoCommit);
                    }
                }
            }
        }
        ZonedDateTime finish = ZonedDateTime.now();
        Duration duration = Duration.between(starts, finish).minusMillis(fetchMillis);
        return 0;
    }

    private RecordSelect fetch(BlockingQueue<RecordSelect> tqueue) //
        throws InterruptedException
    {
        RecordSelect result;
        while ((result = tqueue.poll(1, TimeUnit.MINUTES)) == null) {
            logger.warn("insert callable fetch fail: taskname={}", this.entity.taskname);
        }
        return result;
    }

    private long callInternal(Connection cnt, BlockingQueue<RecordSelect> tqueue)
        throws SQLException, InterruptedException
    {
        long fetchMillis = 0;
        long effectedCount = 0;
        long successNoInfo = 0;
        long executeFailed = 0;
        long executeUnkown = 0;
        long receivedCount = 0;
        logger.info("execute post sql: codename={}", entity.postcode.codename);
        ZonedDateTime starts = ZonedDateTime.now();
        try (PreparedStatement ps = cnt.prepareStatement(entity.postcode.execcode)) {
            long fetchStarts = System.currentTimeMillis();
            RecordSelect record = this.fetch(tqueue);
            long fetchFinish = System.currentTimeMillis();
            fetchMillis += fetchFinish - fetchStarts;
            final int size = entity.postcode.argument.size();
            long batchSize = 0;
            while (record instanceof RecordSelectNormal) {
                RecordSelectNormal r = (RecordSelectNormal)record;
                int index = 0;
                while (index < size) {
                    String field = entity.postcode.argument.get(index);
                    Object value = r.tupleval.get(field);
                    ps.setObject(++index, value);
                }
                ps.addBatch();
                ++batchSize;
                fetchStarts = System.currentTimeMillis();
                record = tqueue.poll();
                fetchFinish = System.currentTimeMillis();
                fetchMillis += fetchFinish - fetchStarts;
                if (batchSize > 10000 || (batchSize > 0 && record == null)) {
                    long[] effectedCounts = ps.executeLargeBatch();
                    for (long ec : effectedCounts) {
                        if (ec >= 0) {
                            effectedCount += ec;
                        }
                        else if (ec == Statement.SUCCESS_NO_INFO){
                            ++successNoInfo;
                        }
                        else if (ec == Statement.EXECUTE_FAILED) {
                            ++executeFailed;
                        }
                        else {
                            ++executeUnkown;
                            logger.warn("unknown execute value: ec={}", ec);
                        }
                    }
                    ps.clearBatch();
                    batchSize = 0;
                }
                if (record == null) {
                    fetchStarts = System.currentTimeMillis();
                    record = this.fetch(tqueue);
                    fetchFinish = System.currentTimeMillis();
                    fetchMillis += fetchFinish - fetchStarts;
                }
            }
        }
        ZonedDateTime finish = ZonedDateTime.now();
        Duration duration = Duration.between(starts, finish);
        logger.info("core sql success: effectedCount={}, duration={}", effectedCount, duration);
        return fetchMillis;
    }
}
