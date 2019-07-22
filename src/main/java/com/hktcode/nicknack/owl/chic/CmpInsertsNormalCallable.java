/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.chic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.hktcode.lang.exception.ArgumentNullException;
import com.hktcode.nicknack.owl.jdbc.JdbcTemplateBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class CmpInsertsNormalCallable implements Callable<Duration>
{
    private static final Logger logger = LoggerFactory.getLogger(CmpInsertsNormalCallable.class);

    public static CmpInsertsNormalCallable of //
        /* */( CmpInsertsEntity entity //
        /* */, BlockingQueue<ImmutableMap<String, Object>> tqueue //
        /* */, JdbcTemplateBase source //
        /* */, String action
        /* */)
    {
        if (entity == null) {
            throw new ArgumentNullException("entity");
        }
        if (tqueue == null) {
            throw new ArgumentNullException("tqueue");
        }
        if (source == null) {
            throw new ArgumentNullException("source");
        }
        if (action == null) {
            throw new ArgumentNullException("action");
        }
        return new CmpInsertsNormalCallable(entity, tqueue, source, action);
    }

    private final BlockingQueue<ImmutableMap<String, Object>> tqueue;

    private final JdbcTemplateBase source;

    private final CmpInsertsEntity entity;

    private final String action;

    private CmpInsertsNormalCallable
        /* */( CmpInsertsEntity entity
        /* */, BlockingQueue<ImmutableMap<String, Object>> tqueue
        /* */, JdbcTemplateBase source
        /* */, String action
        /* */)
    {
        this.entity = entity;
        this.tqueue = tqueue;
        this.source = source;
        this.action = action;
    }

    @Override
    public Duration call() throws SQLException, InterruptedException
    {
        ZonedDateTime starts = ZonedDateTime.now();
        long fetchMillis = 0;
        long fetchStarts = System.currentTimeMillis();
        ImmutableMap<String, Object> record = this.fetch();
        long fetchFinish = System.currentTimeMillis();
        fetchMillis += fetchFinish - fetchStarts;
        if (record.isEmpty()) {
            ZonedDateTime finish = ZonedDateTime.now();
            return Duration.between(starts, finish).minusMillis(fetchMillis);
        }
        ImmutableList<String> fields = record.keySet().asList();
        ImmutableList<String> unkeys = this.entity.tuplekey.asList();
        String master = "";
        if (!"".equals(this.entity.metadata)) {
            master = this.buildInsert(this.entity.metadata, unkeys);
        }
        String detail = this.buildInsert(this.entity.relation, fields);

        List<Map<String, Object>> tuples = new ArrayList<>(1000);
        do {
            tuples.add(record);
            if (tuples.size() >= 1000) {
                this.run(tuples, master, unkeys, detail, fields);
            }
            fetchStarts = System.currentTimeMillis();
            record = this.fetch(tuples, master, unkeys, detail, fields);
            fetchFinish = System.currentTimeMillis();
            fetchMillis += fetchFinish - fetchStarts;
        } while (!record.isEmpty());
        if (!tuples.isEmpty()) {
            this.run(tuples, master, unkeys, detail, fields);
        }
        ZonedDateTime finish = ZonedDateTime.now();
        return Duration.between(starts, finish).minusMillis(fetchMillis);
    }

    private ImmutableMap<String, Object> fetch
        /* */( List<Map<String, Object>> tuples
        /* */, String master
        /* */, ImmutableList<String> unkeys
        /* */, String detail
        /* */, ImmutableList<String> fields
        /* */) throws InterruptedException, SQLException
    {
        ImmutableMap<String, Object> result = this.tqueue.poll();
        if (result != null) {
            return result;
        }
        if (!tuples.isEmpty()) {
            this.run(tuples, master, unkeys, detail, fields);
        }
        return this.fetch();
    }

    private ImmutableMap<String, Object> fetch() throws InterruptedException
    {
        ImmutableMap<String, Object> result;
        while ((result = this.tqueue.poll(1, TimeUnit.MINUTES)) == null) {
            logger.warn("insert callable fetch fail: taskname={}", this.entity.taskname);
        }
        return result;
    }

    private void run
        /* */( List<Map<String, Object>> tuples
        /* */, String master
        /* */, ImmutableList<String> unkeys
        /* */, String detail
        /* */, ImmutableList<String> fields
        /* */) throws InterruptedException, SQLException
    {
        try (Connection cnt = this.source.getDataSource().getConnection()) {
            boolean autoCommit = cnt.getAutoCommit();
            cnt.setAutoCommit(false);
            try {
                if ("".equals(master)) {
                    this.executeBatch(cnt, detail, tuples, fields);
                }
                else if ("DELETE".equals(this.action)) {
                    this.executeBatch(cnt, master, tuples, unkeys);
                }
                else {
                    this.executeBatch(cnt, master, tuples, unkeys);
                    this.executeBatch(cnt, detail, tuples, fields);
                }
                cnt.commit();
                tuples.clear();
            } catch (Exception ex) {
                logger.error("insert callable throws exception: taskname={}", this.entity.taskname, ex);
                if (!cnt.isClosed()) {
                    cnt.rollback();
                }
                throw ex;
            } finally {
                if (!cnt.isClosed()) {
                    cnt.setAutoCommit(autoCommit);
                }
            }
        }
    }

    private int[] executeBatch
        /* */( Connection cnt //
        /* */, String statement //
        /* */, List<? extends Map<String, Object>> tuples //
        /* */, List<String> fields //
        /* */) throws SQLException //
    {
        try (PreparedStatement ps = cnt.prepareStatement(statement)) {
            for (Map<String, Object> tuple : tuples) {
                ps.setObject(1, this.action);
                for (int j = 0; j < fields.size(); ++j) {
                    ps.setObject(j + 2, tuple.get(fields.get(j)));
                }
                ps.addBatch();
            }
            return ps.executeBatch();
        }
    }

    private String buildInsert(String relation, List<String> fields)
    {
        StringBuilder builder = new StringBuilder("INSERT INTO ");
        builder.append(relation);
        builder.append("(");
        builder.append(this.entity.dmlfield);
        for (String key : fields) {
            builder.append(",");
            builder.append(key);
        }
        builder.append(") VALUES (?");
        final int size = fields.size();
        for (int i = 0; i < size; ++i) {
            builder.append(", ?");
        }
        builder.append(")");
        return builder.toString();
    }

}
