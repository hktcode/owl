/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl;

import com.google.common.collect.ImmutableMap;
import com.hktcode.lang.exception.ArgumentNullException;
import com.hktcode.nicknack.owl.jdbc.JdbcTemplateBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.PreparedStatementCreator;

import java.sql.*;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.hktcode.nicknack.owl.SqlNull.SQL_NULL;

public class SelectCallable implements Callable<Duration>
{
    private static final Logger logger = LoggerFactory.getLogger(SelectCallable.class);

    public static SelectCallable of
        /* */( BlockingQueue<RecordSelect> tqueue
        /* */, SqlScriptEntity select
        /* */, ImmutableMap<String, Object> params
        /* */)
    {
        if (tqueue == null) {
            throw new ArgumentNullException("tqueue");
        }
        if (select == null) {
            throw new ArgumentNullException("select");
        }
        if (params == null) {
            throw new ArgumentNullException("params");
        }
        return new SelectCallable(tqueue, select, params);
    }

    private final BlockingQueue<RecordSelect> tqueue;

    private final SqlScriptEntity select;

    private final ImmutableMap<String, Object> params;

    @Override
    public Duration call() throws InterruptedException, SQLException
    {
        ZonedDateTime starts = ZonedDateTime.now();
        JdbcTemplateBase jdbc //
            = JdbcTemplateBase.getJdbcTemplate(select.engineid);
        long offerMillis = 0;
        try (Connection cnt = jdbc.getDataSource().getConnection()) {
            boolean autoCommit = cnt.getAutoCommit();
            cnt.setAutoCommit(false);
            // TODO: 针对MySQL，可能有优化空间。
            PreparedStatementCreator psc //
                = jdbc.newForwardPreparedStatementCreator(select.execcode);
            try (PreparedStatement ps = psc.createPreparedStatement(cnt)) {
                final int argvsize = select.argument.size();
                for (int i = 0; i < argvsize; ++i) {
                    final String argkey = select.argument.get(i);
                    final Object argval = params.get(argkey);
                    ps.setObject(i + 1, argval);
                }
                ps.execute();
                ResultSet resultSet = ps.getResultSet();
                if (resultSet != null) {
                    try (ResultSet rs = resultSet) {
                        jdbc.setResultSetForward(rs);
                        ResultSetMetaData metaData = rs.getMetaData();
                        while (rs.next()) {
                            ImmutableMap.Builder<String, Object> builder //
                                = ImmutableMap.builder();
                            final int columnCount = metaData.getColumnCount();
                            for (int i = 0; i < columnCount; ++i) {
                                final int index = i + 1;
                                String label = metaData.getColumnLabel(index);
                                Object value = rs.getObject(index);
                                value = (value == null ? SQL_NULL : value);
                                builder.put(label, value);
                            }
                            ImmutableMap<String, Object> t = builder.build();
                            RecordSelectNormal r = RecordSelectNormal.of(t);
                            offerMillis += this.push(r);
                        }
                    }
                }
                offerMillis += this.push(RecordFinishNormal.of());
            } finally {
                if (!cnt.isClosed()) {
                    cnt.setAutoCommit(autoCommit);
                }
            }
        } catch (Exception ex) {
            logger.error("run query fail: codename={}", this.select.codename, ex);
            this.push(RecordFinishThrows.of());
            throw ex;
        }
        ZonedDateTime finish = ZonedDateTime.now();
        return Duration.between(starts, finish).minusMillis(offerMillis);
    }

    private SelectCallable
        /* */( BlockingQueue<RecordSelect> tqueue
        /* */, SqlScriptEntity select
        /* */, ImmutableMap<String, Object> params
        /* */)
    {
        this.tqueue = tqueue;
        this.select = select;
        this.params = params;
    }

    private long push(RecordSelect record) throws InterruptedException
    {
        long starts = System.currentTimeMillis();
        while (!this.tqueue.offer(record, 1, TimeUnit.MINUTES)) {
            logger.warn("query offer fail: name={}", select.codename);
        }
        long finish = System.currentTimeMillis();
        return finish - starts;
    }
}
