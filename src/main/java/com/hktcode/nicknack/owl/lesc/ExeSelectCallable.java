/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.lesc;

import com.google.common.collect.ImmutableMap;
import com.hktcode.lang.exception.ArgumentNullException;
import com.hktcode.nicknack.owl.RecordSelect;
import com.hktcode.nicknack.owl.SelectCallable;
import com.hktcode.nicknack.owl.SqlScriptEntity;
import com.hktcode.nicknack.owl.jdbc.JdbcTemplateBase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

public class ExeSelectCallable implements Callable<Duration>
{

    public static ExeSelectCallable of //
        /* */( BlockingQueue<RecordSelect> tqueue //
        /* */, SqlScriptEntity presql //
        /* */, SqlScriptEntity select //
        /* */, ImmutableMap<String, Object> params //
        /* */)
    {
        if (tqueue == null) {
            throw new ArgumentNullException("tqueue");
        }
        if (presql == null) {
            throw new ArgumentNullException("presql");
        }
        if (select == null) {
            throw new ArgumentNullException("select");
        }
        if (params == null) {
            throw new ArgumentNullException("params");
        }
        return new ExeSelectCallable(tqueue, presql, select, params);
    }

    private final BlockingQueue<RecordSelect> tqueue;

    private final SqlScriptEntity presql;

    private final SqlScriptEntity select;

    private final ImmutableMap<String, Object> params;

    private ExeSelectCallable //
        /* */( BlockingQueue<RecordSelect> tqueue //
        /* */, SqlScriptEntity presql //
        /* */, SqlScriptEntity select //
        /* */, ImmutableMap<String, Object> params //
        /* */)
    {
        this.tqueue = tqueue;
        this.presql = presql;
        this.select = select;
        this.params = params;
    }

    @Override
    public Duration call() throws Exception
    {
        long starts = System.currentTimeMillis();
        JdbcTemplateBase jdbc = JdbcTemplateBase.getJdbcTemplate(presql.engineid);
        ImmutableMap<String, Object> selectParams;
        try (Connection cnt = jdbc.getDataSource().getConnection();
             PreparedStatement ps = cnt.prepareStatement(presql.execcode);) {
            for (int i = 0; i < presql.argument.size(); ++i) {
                ps.setObject(i + 1, params.get(presql.argument.get(i)));
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException();
                }
                HashMap<String, Object> map = new HashMap<>();
                ResultSetMetaData metadata = rs.getMetaData();
                for (int i = 0; i < metadata.getColumnCount(); ++i) {
                    String label = metadata.getColumnLabel(i + 1);
                    Object value = rs.getObject(i + 1);
                    map.put(label, value);
                }
                selectParams = ImmutableMap.copyOf(map);
            }
        }
        long finish = System.currentTimeMillis();
        SelectCallable callable = SelectCallable.of(tqueue, select, selectParams);
        Duration duration = callable.call();
        return duration.plusMillis(finish - starts);
    }
}
