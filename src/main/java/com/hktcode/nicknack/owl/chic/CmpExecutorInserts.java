/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.chic;

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
import java.util.Map;
import java.util.concurrent.*;

public class CmpExecutorInserts extends CmpExecutor<CmpInsertsEntity>
{
    private static final Logger logger = LoggerFactory.getLogger(CmpExecutorInserts.class);

    public static CmpExecutorInserts of(CmpInsertsEntity entity)
    {
        if (entity == null) {
            throw new ArgumentNullException("entity");
        }
        return new CmpExecutorInserts(entity);
    }

    public final BlockingQueue<ImmutableMap<String, Object>> updates;
    public final BlockingQueue<ImmutableMap<String, Object>> inserts;
    public final BlockingQueue<ImmutableMap<String, Object>> deletes;

    private CmpExecutorInserts(CmpInsertsEntity entity)
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
        // TODO: logger.info("execute task: statDate={}, name={}", statDate, name);
        final int updateCount = 1;
        final int insertCount = 1;
        final int deleteCount = 1;
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
            Callable<Duration> c
                = CmpInsertsNormalCallable.of(this.entity, this.updates, source, "UPDATE");
            updateFuture[i] = service.submit(c);
        }
        @SuppressWarnings("unchecked")
        Future<Duration>[] insertFuture = new Future[insertCount];
        for (int i = 0; i < insertCount; ++i) {
            Callable<Duration> c
                = CmpInsertsNormalCallable.of(this.entity, this.inserts, source, "INSERT");
            insertFuture[i] = service.submit(c);
        }
        @SuppressWarnings("unchecked")
        Future<Duration>[] deleteFuture = new Future[deleteCount];
        for (int i = 0; i < deleteCount; ++i) {
            Callable<Duration> c
                = CmpInsertsNormalCallable.of(this.entity, this.deletes, source, "DELETE");
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
            this.updates.put(ImmutableMap.of());
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

        for (Map.Entry<String, Object> entry : newtuple.entrySet()) {
            String property = entry.getKey();
            if (this.entity.tuplekey.contains(property)) {
                continue;
            }
            Object newvalue = entry.getValue();
            Object oldvalue = oldtuple.get(property);
            if (!equals(oldvalue, newvalue)) {
                this.updates.put(record.newtuple);
                return "".equals(this.entity.metadata) ? 1 : 2;
            }
        }
        return 0;
    }

    @Override
    public long put(ZonedDateTime now, RecordDeleteNormal record) //
        throws InterruptedException
    {
        ImmutableMap<String, Object> map = record.oldtuple;
        this.deletes.put(map);
        return 1;
    }

    @Override
    public long put(ZonedDateTime now, RecordInsertNormal record) //
        throws InterruptedException
    {
        ImmutableMap<String, Object> map = record.newtuple;
        this.inserts.put(map);
        return "".equals(this.entity.metadata) ? 1 : 2;
    }
}
