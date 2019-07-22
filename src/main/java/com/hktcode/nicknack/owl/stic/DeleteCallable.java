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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class DeleteCallable implements Callable<Duration>
{
    public static DeleteCallable of
        /* */( CmpUpdatesEntity entity
        /* */, BlockingQueue<ImmutableMap<String, Object>> tqueue
        /* */, JdbcTemplateBase source
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
        return new DeleteCallable(entity, tqueue, source);
    }

    private final CmpUpdatesEntity entity;

    private final BlockingQueue<ImmutableMap<String, Object>> tqueue;

    private final JdbcTemplateBase source;

    @Override
    public Duration call() throws Exception
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
        String relation = this.entity.relation;
        String heritage = this.entity.heritage;
        ImmutableList<String> pkfields = this.entity.tuplekey.asList();
        StringBuilder deleteBuilder = new StringBuilder("DELETE FROM ");
        deleteBuilder.append(relation);
        deleteBuilder.append(" WHERE 1 = 1 ");
        for (String field : pkfields) {
            deleteBuilder.append(" and ");
            deleteBuilder.append(field);
            deleteBuilder.append(" = ?");
        }
        String delete = deleteBuilder.toString();

        ImmutableList<String> fields = record.keySet().asList();
        StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
        insertBuilder.append(heritage);
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
                this.run(delete, pkfields, insert, fields, tuples);
                tuples.clear();
            }
            fetchStarts = System.currentTimeMillis();
            record = this.fetch(delete, pkfields, insert, fields, tuples);
            fetchFinish = System.currentTimeMillis();
            fetchMillis += fetchFinish - fetchStarts;
        } while (!record.isEmpty());
        if (!tuples.isEmpty()) {
            this.run(delete, pkfields, insert, fields, tuples);
            tuples.clear();
        }

        ZonedDateTime finish = ZonedDateTime.now();
        return Duration.between(starts, finish).minusMillis(fetchMillis);
    }

    private ImmutableMap<String, Object> fetch() throws InterruptedException
    {
        ImmutableMap<String, Object> result;
        while ((result = this.tqueue.poll(1, TimeUnit.MINUTES)) == null) {
            logger.warn("delete callable fetch fail: taskname={}", this.entity.taskname);
        }
        return result;
    }

    private ImmutableMap<String, Object> fetch
        /* */( String delete
        /* */, ImmutableList<String> unkeys
        /* */, String insert
        /* */, ImmutableList<String> fields
        /* */, List<Map<String, Object>> tuples
        /* */) throws InterruptedException, SQLException
    {
        ImmutableMap<String, Object> result = this.tqueue.poll();
        if (result != null) {
            return result;
        }
        if (!tuples.isEmpty()) {
            this.run(delete, unkeys, insert, fields, tuples);
            tuples.clear();
        }
        return this.fetch();
    }

    private static final Logger logger = LoggerFactory.getLogger(DeleteCallable.class);

    private void run
        /* */( String delete
        /* */, ImmutableList<String> unkeys
        /* */, String insert
        /* */, ImmutableList<String> fields
        /* */, List<Map<String, Object>> tuples
        /* */)
        throws SQLException
    {
        try(Connection cnt = this.source.getDataSource().getConnection()) {
            boolean autoCommit = cnt.getAutoCommit();
            cnt.setAutoCommit(false);
            try {
                try (PreparedStatement ps = cnt.prepareStatement(delete)) {
                    for (Map<String, Object> tuple : tuples) {
                        for (int i = 0; i < unkeys.size(); ++i) {
                            ps.setObject(i + 1, tuple.get(unkeys.get(i)));
                        }
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                try (PreparedStatement ps = cnt.prepareStatement(insert)) {
                    for (Map<String, Object> tuple : tuples) {
                        for (int j = 0; j < fields.size(); ++j) {
                            ps.setObject(j + 1, tuple.get(fields.get(j)));
                        }
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                cnt.commit();
            }
            catch (Exception ex) {
                logger.error("delete callable throws exception: ", ex);
                if (!cnt.isClosed()) {
                    cnt.rollback();
                }
                throw ex;
            }
            finally {
                if (!cnt.isClosed()) {
                    cnt.setAutoCommit(autoCommit);
                }
            }
        }

    }

    private DeleteCallable
        /* */( CmpUpdatesEntity entity
        /* */, BlockingQueue<ImmutableMap<String, Object>> tqueue
        /* */, JdbcTemplateBase source
        /* */)
    {
        this.entity = entity;
        this.tqueue = tqueue;
        this.source = source;
    }
}
