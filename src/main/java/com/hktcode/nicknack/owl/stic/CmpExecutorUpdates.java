/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.stic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.hktcode.lang.exception.ArgumentNullException;
import com.hktcode.nicknack.owl.*;
import com.hktcode.nicknack.owl.jdbc.JdbcTemplateBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

public class CmpExecutorUpdates extends CmpExecutor<CmpUpdatesEntity>
{
    public static CmpExecutorUpdates of(CmpUpdatesEntity entity)
    {
        if (entity == null) {
            throw new ArgumentNullException("entity");
        }
        return new CmpExecutorUpdates(entity);
    }

    private static final Logger logger = LoggerFactory.getLogger(CmpExecutorUpdates.class);

    public final BlockingQueue<ImmutableList<LoggerRelationUpdateEntity>> updates;
    public final BlockingQueue<ImmutableMap<String, Object>> inserts;
    public final BlockingQueue<ImmutableMap<String, Object>> deletes;

    public CmpExecutorUpdates(CmpUpdatesEntity entity)
    {
        super(entity);
        this.updates = new LinkedBlockingQueue<>(16);
        this.inserts = new LinkedBlockingQueue<>(16);
        this.deletes = new LinkedBlockingQueue<>(16);
    }

    public long execute(LocalDate statDate) //
        throws InterruptedException, ExecutionException
    {
        if (statDate == null) {
            throw new ArgumentNullException("statDate");
        }
        ZonedDateTime starts = ZonedDateTime.now();
        final int updateCount = 3;
        final int insertCount = 1;
        final int deleteCount = 3;
        final int totalsCount = updateCount + insertCount + deleteCount;
        ExecutorService service = Executors.newFixedThreadPool(totalsCount + 2);
        ImmutableMap<String, Object> argval //
            = ImmutableMap.of("stat_date", statDate.toString());

        SqlScriptEntity oquery = entity.oldtuple;
        SqlScriptEntity nquery = entity.newtuple;

        BlockingQueue<RecordSelect> oqueue = new ArrayBlockingQueue<>(16);
        BlockingQueue<RecordSelect> nqueue = new ArrayBlockingQueue<>(16);
        SelectCallable oldCallable = SelectCallable.of(oqueue, oquery, argval);
        SelectCallable newCallable = SelectCallable.of(nqueue, nquery, argval);
        Future<Duration> oldFuture = service.submit(oldCallable);
        Future<Duration> newFutrue = service.submit(newCallable);

        JdbcTemplateBase source = JdbcTemplateBase.getJdbcTemplate(oquery.engineid);

        @SuppressWarnings("unchecked")
        Future<Duration>[] updateFuture = new Future[updateCount];
        for (int i = 0; i < updateCount; ++i) {
            UpdateCallable c = UpdateCallable.of(entity, this.updates, source);
            updateFuture[i] = service.submit(c);
        }
        @SuppressWarnings("unchecked")
        Future<Duration>[] insertFuture = new Future[insertCount];
        for (int i = 0; i < insertCount; ++i) {
            InsertCallable c = InsertCallable.of(entity.relation, this.inserts, source);
            insertFuture[i] = service.submit(c);
        }
        @SuppressWarnings("unchecked")
        Future<Duration>[] deleteFuture = new Future[deleteCount];
        for (int i = 0; i < deleteCount; ++i) {
            DeleteCallable c = DeleteCallable.of(entity, this.deletes, source);
            deleteFuture[i] = service.submit(c);
        }

        this.executeInternal(starts, oqueue, nqueue);

        for (int i = 0; i < deleteCount; ++i) {
            this.deletes.put(ImmutableMap.of());
        }
        for (int i = 0; i < insertCount; ++i) {
            this.inserts.put(ImmutableMap.of());
        }
        for (int i = 0; i < updateCount; ++i) {
            this.updates.put(ImmutableList.of());
        }
        service.shutdown();
        while(!service.awaitTermination(1, TimeUnit.MINUTES)) {
            logger.info("await service finish");
        }
        ImmutableList<Duration> deletets = sort(deleteFuture);
        ImmutableList<Duration> insertts = sort(insertFuture);
        ImmutableList<Duration> updatets = sort(updateFuture);
        Duration oldDuration = oldFuture.get();
        Duration newDuration = newFutrue.get();
        ZonedDateTime finish = ZonedDateTime.now();
        Duration duration = Duration.between(starts, finish);
        CompareReport report = CompareReport.of(duration, oldDuration, newDuration, insertts, updatets, deletets);
        logger.info("cmp updates finish: taskname={}\n{}", entity.taskname, report);
        return 0;
    }

    @Override
    public long put(ZonedDateTime now, RecordUpdateNormal record) //
        throws InterruptedException
    {
        ImmutableMap<String, Object> oldtuple = record.oldtuple;
        ImmutableMap<String, Object> newtuple = record.newtuple;
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        for (String unkey : this.entity.tuplekey) {
            builder.put(unkey, newtuple.get(unkey));
        }
        ImmutableMap<String, Object> tuplekey = builder.build();
        ImmutableList.Builder<LoggerRelationUpdateEntity> listBuilder //
            = ImmutableList.builder();

        final String relation = this.entity.relation;
        for (Map.Entry<String, Object> entry : newtuple.entrySet()) {
            String property = entry.getKey();
            if (this.entity.tuplekey.contains(property)) {
                continue;
            }
            Object newvalue = entry.getValue();
            Object oldvalue = oldtuple.get(property);
            if (!equals(oldvalue, newvalue)) {
                LoggerRelationUpdateEntity entity //
                    = LoggerRelationUpdateEntity.of(now, relation, tuplekey, property, oldvalue, newvalue);
                listBuilder.add(entity);
            }
        }
        ImmutableList<LoggerRelationUpdateEntity> updatelist = listBuilder.build();
        if (updatelist.isEmpty()) {
            return 0;
        }
        this.updates.put(updatelist);
        return updatelist.size() + 1L;
    }

    @Override
    public long put(ZonedDateTime now, RecordDeleteNormal record) //
        throws InterruptedException
    {
        ImmutableMap<String, Object> map = record.oldtuple;
        String delfield = this.entity.delfield;
        if (!"".equals(delfield)) {
            Object delvalue = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(now);
            map = ImmutableMap.<String, Object>builder()
                .put(delfield, delvalue) //
                .putAll(map) //
                .build();
        }
        this.deletes.put(map);
        return 2;
    }

    @Override
    public long put(ZonedDateTime now, RecordInsertNormal record) //
        throws InterruptedException
    {
        ImmutableMap<String, Object> map = record.newtuple;
        String putfield = this.entity.putfield;
        if (!"".equals(putfield)) {
            Object putvalue = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(now);
            map = ImmutableMap.<String, Object>builder()
                .put(putfield, putvalue)
                .putAll(map)
                .build();
        }
        this.inserts.put(map);
        return 1;
    }
}
