/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.stic;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.hktcode.nicknack.Owl;
import com.hktcode.nicknack.owl.SqlScriptEntity;
import com.hktcode.nicknack.owl.jdbc.JdbcTemplateBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CmpUpdatesCommand extends Owl
{
    private static final Logger logger = LoggerFactory.getLogger(CmpUpdatesCommand.class);

    public static void main(String[] args)
    {
        logger.info("cmp updates command starts: home={}", Owl.Home);
        for (int i = 0; i < args.length; ++i) {
            logger.info("    args[{}] = {}", i, args[i]);
        }
        if (args.length == 0) {
            System.out.println("usage: stic [name]");
            return;
        }
        Path dataSourceEtc = Paths.get(Home, "etc", "datasource.properties");
        JdbcTemplateBase.loadJdbcTemplates(dataSourceEtc);
        JdbcTemplateBase jdbc = JdbcTemplateBase.getJdbcTemplate(Owl.ENGINE);
        String taskname = args[0];
        List<Map<String, Object>> list = jdbc.queryForList(SELECT, taskname);
        logger.info("job list: size={}, taskname={}", list.size(), taskname);
        for (Map<String, Object> map : list) {
            doJob(map);
        }
    }

    private static final String SELECT = "" //
        + "\n SELECT k.taskname as taskname " //
        + "\n      , k.relation as relation " //
        + "\n      , k.oldtuple as oldtuple " //
        + "\n      , k.newtuple as newtuple " //
        + "\n      , k.tuplekey as tuplekey " //
        + "\n      , k.putfield as putfield " //
        + "\n      , k.delfield as delfield " //
        + "\n      , k.document as document " //
        + "\n      , k.isenable as isenable " //
        + "\n      , k.heritage as heritage " //
        + "\n      , k.relogger as relogger " //
        + "\n   FROM hktcode_owl.job_cmp_update k " //
        + "\n  WHERE k.taskname = ? ";

    private static void doJob(Map<String, Object> map)
    {
        String taskname = Objects.toString(map.get("taskname"), "");
        String relation = Objects.toString(map.get("relation"), "");
        String osqlname = Objects.toString(map.get("oldtuple"), "");
        String nsqlname = Objects.toString(map.get("newtuple"), "");
        String unkytext = Objects.toString(map.get("tuplekey"), "");
        String putfield = Objects.toString(map.get("putfield"), "");
        String delfield = Objects.toString(map.get("delfield"), "");
        String document = Objects.toString(map.get("document"), "");
        String isenable = Objects.toString(map.get("isenable"), "");
        String heritage = Objects.toString(map.get("heritage"), "");
        String relogger = Objects.toString(map.get("relogger"), "");
        if (Objects.equals("", relation)) {
            logger.error("relation is empty: taskname={}", taskname);
            return;
        }
        Map<String, SqlScriptEntity> sqls = SqlScriptEntity.Cache;
        SqlScriptEntity oldtuple = sqls.get(osqlname);
        if (oldtuple == null) {
            logger.error("sql not found: taskname={}, codename={}", taskname, osqlname);
            return;
        }
        SqlScriptEntity newtuple = sqls.get(nsqlname);
        if (newtuple == null) {
            logger.error("sql not found: taskname={}, codename={}", taskname, nsqlname);
            return;
        }
        if (!Objects.equals("EXECUTE", isenable)) {
            logger.error("task disable: taskname={}, isenable={}", taskname, isenable);
            return;
        }

        Iterable<String> unkyIter //
            = Splitter.on(',').trimResults().omitEmptyStrings().split(unkytext);
        ImmutableSet<String> tuplekey = ImmutableSet.copyOf(unkyIter);

        ZonedDateTime now = ZonedDateTime.now();
        try {
            CmpUpdatesEntity job = new CmpUpdatesEntity //
                /* */( taskname //
                /* */, relation //
                /* */, oldtuple //
                /* */, newtuple //
                /* */, tuplekey //
                /* */, isenable //
                /* */, document //
                /* */, heritage //
                /* */, putfield //
                /* */, delfield //
                /* */, relogger //
                /* */);
            CmpExecutorUpdates executor = CmpExecutorUpdates.of(job);
            long val = executor.execute(now.toLocalDate());
            logger.info("job success: taskname={}, exitval={}", taskname, val);
        }
        catch (Exception ex) {
            logger.info("job failure: taskname={}", taskname, ex);
        }
    }
}
