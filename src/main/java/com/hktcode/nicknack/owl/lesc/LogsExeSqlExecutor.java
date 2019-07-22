/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.lesc;

import com.google.common.collect.ImmutableMap;
import com.hktcode.lang.exception.ArgumentNullException;
import com.hktcode.nicknack.owl.RecordSelect;
import com.hktcode.nicknack.owl.SqlScriptEntity;
import com.hktcode.nicknack.owl.jdbc.JdbcTemplateBase;
import com.hktcode.nicknack.owl.jdbc.WithoutCachePreparedStatementCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class LogsExeSqlExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(LogsExeSqlExecutor.class);

    private final ExeInsertEntity entity;

    public LogsExeSqlExecutor(ExeInsertEntity entity)
    {
        if (entity == null) {
            throw new ArgumentNullException("entity");
        }
        this.entity = entity;
    }

    public int execute(LocalDate statDate) throws SQLException
    {
        if (statDate == null) {
            throw new ArgumentNullException("statDate");
        }
        ExecutorService service = Executors.newFixedThreadPool(1);
        ExeInsertEntity info = this.entity;
        String name = info.taskname;
        logger.info("execute task: statDate={}, taskname={}", statDate, name);
        ImmutableMap<String, Object> params = ImmutableMap.of("stat_date", statDate.toString());

        SqlScriptEntity prev = this.entity.prevcode;
        SqlScriptEntity core = this.entity.corecode;
        SqlScriptEntity post = this.entity.postcode;

        BlockingQueue<RecordSelect> tqueue = new LinkedBlockingQueue<>(16);

        ExeSelectCallable callable = ExeSelectCallable.of(tqueue, prev, core, params);
        Future<Duration> selectFuture = service.submit(callable);

        JdbcTemplateBase jdbc = JdbcTemplateBase.getJdbcTemplate(post.engineid);
        try (Connection cnt = jdbc.getDataSource().getConnection()) {
            boolean autoCommit = cnt.getAutoCommit();
            cnt.setAutoCommit(false);
            try {
                // TODO:
                cnt.commit();
            }
            catch (Exception ex) {
                if (!cnt.isClosed()) {
                    cnt.rollback();
                }
                logger.error("exe insert callable throws exception: ", ex);
            }
            finally {
                if (!cnt.isClosed()) {
                    cnt.setAutoCommit(autoCommit);
                }
            }
        }



        return 0;
    }
}
