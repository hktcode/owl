/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.chic;

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

public class CmpInsertsCommand extends Owl
{
    private static final Logger logger = LoggerFactory.getLogger(CmpInsertsCommand.class);

    public static void main( String[] args )
    {
        logger.info("cmp inserts command starts: home={}", Owl.Home);
        for (int i = 0; i < args.length; ++i) {
            logger.info("    args[{}] = {}", i, args[i]);
        }
        if (args.length == 0) {
            System.out.println("usage: chic [name]");
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
        + "\n      , k.metadata as metadata " //
        + "\n      , k.relation as relation " //
        + "\n      , k.oldtuple as oldtuple " //
        + "\n      , k.newtuple as newtuple " //
        + "\n      , k.tuplekey as tuplekey " //
        + "\n      , k.dmlfield as dmlfield " //
        + "\n      , k.isenable as isenable " //
        + "\n      , k.document as document " //
        + "\n   FROM hktcode_owl.job_cmp_insert k " //
        + "\n  WHERE k.taskname = ? " //
        + "\n";


    private static void doJob(Map<String, Object> map)
    {
        String taskname = Objects.toString(map.get("taskname"), "");
        String metadata = Objects.toString(map.get("metadata"), "");
        String relation = Objects.toString(map.get("relation"), "");
        String osqlname = Objects.toString(map.get("oldtuple"), "");
        String nsqlname = Objects.toString(map.get("newtuple"), "");
        String unkytext = Objects.toString(map.get("tuplekey"), "");
        String document = Objects.toString(map.get("document"), "");
        String isenable = Objects.toString(map.get("isenable"), "");
        String dmlfield = Objects.toString(map.get("dmlfield"), "");
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
        Iterable<String> unkyIterable = Splitter.on(',').trimResults().omitEmptyStrings().split(unkytext);
        ImmutableSet<String> tuplekey = ImmutableSet.copyOf(unkyIterable);

        ZonedDateTime now = ZonedDateTime.now();
        try {
            CmpInsertsEntity job = new CmpInsertsEntity //
                /* */( taskname //
                /* */, relation //
                /* */, oldtuple //
                /* */, newtuple //
                /* */, tuplekey //
                /* */, isenable //
                /* */, document //
                /* */, metadata //
                /* */, dmlfield //
                /* */);
            CmpExecutorInserts executor = CmpExecutorInserts.of(job);
            long val = executor.execute(now.toLocalDate());
            logger.info("job success: taskname={}, exitval={}", taskname, val);
        }
        catch (Exception ex) {
            logger.info("job failure: taskname={}", taskname, ex);
        }
    }
}
