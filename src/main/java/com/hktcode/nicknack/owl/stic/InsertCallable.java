/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.stic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.hktcode.lang.exception.ArgumentNullException;
import com.hktcode.nicknack.owl.jdbc.JdbcTemplateBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class InsertCallable implements Callable<Duration>
{
    public static InsertCallable of //
        /* */( String relation //
        /* */, BlockingQueue<ImmutableMap<String, Object>> tqueue //
        /* */, JdbcTemplateBase source //
        /* */)
    {
        if (relation == null) {
            throw new ArgumentNullException("relation");
        }
        if (tqueue == null) {
            throw new ArgumentNullException("tqueue");
        }
        if (source == null) {
            throw new ArgumentNullException("source");
        }
        return new InsertCallable(relation, tqueue, source);
    }

    private final BlockingQueue<ImmutableMap<String, Object>> tqueue;

    private final JdbcTemplateBase source;

    private final String relation;

    private InsertCallable
        /* */( String relation
        /* */, BlockingQueue<ImmutableMap<String, Object>> tqueue
        /* */, JdbcTemplateBase source
        /* */)
    {
        this.relation = relation;
        this.tqueue = tqueue;
        this.source = source;
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
        StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
        insertBuilder.append(relation);
        insertBuilder.append("(");
        Iterator<String> iter = fields.iterator();
        insertBuilder.append(iter.next());
        while (iter.hasNext()) {
            insertBuilder.append(",");
            insertBuilder.append(iter.next());
        }
        insertBuilder.append(") VALUES (?");
        for (int i = 1; i < fields.size(); ++i) {
            insertBuilder.append(", ?");
        }
        insertBuilder.append(")");
        String insert = insertBuilder.toString();
        List<Map<String, Object>> tuples = new ArrayList<>(1000);
        do {
            tuples.add(record);
            if (tuples.size() >= 1000) {
                this.run(insert, tuples, fields);
                tuples.clear();
            }
            fetchStarts = System.currentTimeMillis();
            record = this.fetch(insert, tuples, fields);
            fetchFinish = System.currentTimeMillis();
            fetchMillis += fetchFinish - fetchStarts;
        } while (!record.isEmpty());
        if (!tuples.isEmpty()) {
            this.run(insert, tuples, fields);
            tuples.clear();
        }

        ZonedDateTime finish = ZonedDateTime.now();
        return Duration.between(starts, finish).minusMillis(fetchMillis);
    }

    private ImmutableMap<String, Object> fetch
        /* */( String insert
        /* */, List<Map<String, Object>> tuples
        /* */, ImmutableList<String> fields
        /* */) throws InterruptedException, SQLException
    {
        ImmutableMap<String, Object> result = this.tqueue.poll();
        if (result != null) {
            return result;
        }
        if (!tuples.isEmpty()) {
            this.run(insert, tuples, fields);
            tuples.clear();
        }
        return this.fetch();
    }

    private ImmutableMap<String, Object> fetch() throws InterruptedException
    {
        ImmutableMap<String, Object> result;
        while ((result = this.tqueue.poll(1, TimeUnit.MINUTES)) == null) {
            logger.warn("insert callable fetch fail: relation={}", this.relation);
        }
        return result;
    }

    private void run(String insert, List<Map<String, Object>> tuples, ImmutableList<String> fields)
        throws SQLException
    {
        try(Connection cnt = this.source.getDataSource().getConnection()) {
            boolean autoCommit = cnt.getAutoCommit();
            cnt.setAutoCommit(false);
            try {
                this.source.executeBatch(cnt, insert, tuples, fields);
                cnt.commit();
            }
            catch (Exception ex) {
                if (!cnt.isClosed()) {
                    cnt.rollback();
                }
                logger.error("insert callable throws exception: ", ex);
                throw ex;
            }
            finally {
                if (!cnt.isClosed()) {
                    cnt.setAutoCommit(autoCommit);
                }
            }
        }
        catch (Exception ex) {
            logger.error("insert fail: name={}", this.relation, ex);
            throw ex;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(InsertCallable.class);
}
