/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.stic;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.hktcode.lang.exception.ArgumentNullException;
import com.hktcode.nicknack.owl.jdbc.JdbcTemplateBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class UpdateCallable implements Callable<Duration>
{
    public static UpdateCallable of //
        /* */( CmpUpdatesEntity entity //
        /* */, BlockingQueue<ImmutableList<LoggerRelationUpdateEntity>> tqueue //
        /* */, JdbcTemplateBase source //
        /* */) //
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
        return new UpdateCallable(entity, tqueue, source);
    }

    private final BlockingQueue<ImmutableList<LoggerRelationUpdateEntity>> tqueue;

    private final JdbcTemplateBase source;

    public final CmpUpdatesEntity entity;

    private UpdateCallable //
        /* */( CmpUpdatesEntity entity //
        /* */, BlockingQueue<ImmutableList<LoggerRelationUpdateEntity>> tqueue //
        /* */, JdbcTemplateBase source //
        /* */) //
    {
        this.tqueue = tqueue;
        this.source = source;
        this.entity = entity;
    }

    @Override
    public Duration call() throws SQLException, InterruptedException
    {
        ZonedDateTime starts = ZonedDateTime.now();
        List<ImmutableList<LoggerRelationUpdateEntity>> updatelist = new ArrayList<>();
        long fetchMillis = 0;
        long fetchStarts = System.currentTimeMillis();
        ImmutableList<LoggerRelationUpdateEntity> tmp = this.fetch(updatelist);
        long fetchFinish = System.currentTimeMillis();
        fetchMillis += fetchFinish - fetchStarts;
        while (!tmp.isEmpty()) {
            updatelist.add(tmp);
            if (updatelist.size() >= 128) {
                this.run(updatelist);
                updatelist.clear();
            }
            fetchStarts = System.currentTimeMillis();
            tmp = this.fetch(updatelist);
            fetchFinish = System.currentTimeMillis();
            fetchMillis += fetchFinish - fetchStarts;
        }
        if (!updatelist.isEmpty()) {
            this.run(updatelist);
            updatelist.clear();
        }
        ZonedDateTime finish = ZonedDateTime.now();
        return Duration.between(starts, finish).minusMillis(fetchMillis);
    }

    private ImmutableList<LoggerRelationUpdateEntity> fetch(List<ImmutableList<LoggerRelationUpdateEntity>> updatelist)
        throws InterruptedException, SQLException
    {
        ImmutableList<LoggerRelationUpdateEntity> result = this.tqueue.poll();
        if (result != null) {
            return result;
        }
        if (!updatelist.isEmpty()) {
            this.run(updatelist);
            updatelist.clear();
        }
        while ((result = this.tqueue.poll(1, TimeUnit.MINUTES)) == null) {
            logger.warn("update callable fetch fail"); // TODO:
        }
        return result;
    }

    private static final Logger logger = LoggerFactory.getLogger(UpdateCallable.class);

    private void run(List<ImmutableList<LoggerRelationUpdateEntity>> updatelist) //
        throws SQLException
    {
        try (Connection cnt = this.source.getDataSource().getConnection()) {
            boolean autoCommit = cnt.getAutoCommit();
            try {
                cnt.setAutoCommit(false);
                for (List<LoggerRelationUpdateEntity> updates : updatelist) {
                    StringBuilder sql = new StringBuilder("UPDATE ");
                    LoggerRelationUpdateEntity e = updates.get(0);
                    sql.append(e.relation);
                    sql.append(" SET ");
                    sql.append(e.property);
                    sql.append(" = ? ");
                    for (int i = 1; i < updates.size(); ++i) {
                        e = updates.get(i);
                        sql.append(", ");
                        sql.append(e.property);
                        sql.append(" = ?");
                    }
                    sql.append(" where 1 = 1 ");
                    Map<String, Object> tuplekey = e.tuplekey;
                    for (Map.Entry<String, Object> entry : tuplekey.entrySet()) {
                        sql.append("and ");
                        sql.append(entry.getKey());
                        sql.append(" = ? ");
                    }
                    try (PreparedStatement ps = cnt.prepareStatement(sql.toString())) {
                        int index = 0;
                        while (index < updates.size()) {
                            ps.setObject(index + 1, updates.get(index).newvalue);
                            ++index;
                        }
                        for (Map.Entry<String, Object> entry : tuplekey.entrySet()) {
                            ps.setObject(++index, entry.getValue());
                        }
                        ps.execute();
                    }
                }

                String relogger = this.entity.relogger;
                if (Objects.equals("", relogger)) {
                    return;
                }
                final String logInsertSQL = "" //
                    + "INSERT INTO " //
                    + relogger //
                    + "( updatets " //
                    + ", relation " //
                    + ", tuplekey " //
                    + ", property " //
                    + ", oldvalue " //
                    + ", newvalue " //
                    + ") VALUES (?, ?, ?, ?, ?, ?)";
                try (PreparedStatement ps = cnt.prepareStatement(logInsertSQL)) {
                    for(List<LoggerRelationUpdateEntity> updates : updatelist) {
                        ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
                        for (Map.Entry<String, Object> v : updates.get(0).tuplekey.entrySet()) {
                            node.put(v.getKey(), v.getValue().toString());
                        }
                        String tuplekey = node.toString();
                        for (LoggerRelationUpdateEntity e : updates) {
                            int index = 0;
                            ps.setObject(++index, e.updatets.toLocalDateTime().toString());
                            ps.setObject(++index, e.relation);
                            ps.setObject(++index, tuplekey);
                            ps.setObject(++index, e.property);
                            ps.setObject(++index, e.oldvalue);
                            ps.setObject(++index, e.newvalue);
                            ps.addBatch();
                        }
                    }
                    ps.executeBatch();
                }
                cnt.commit();
            }
            catch (Exception ex) {
                logger.error("update callable throws exception: ", ex);
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
}
