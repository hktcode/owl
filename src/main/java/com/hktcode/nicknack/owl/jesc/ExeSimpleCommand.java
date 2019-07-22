/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.jesc;

import com.hktcode.nicknack.Owl;
import com.hktcode.nicknack.owl.SqlScriptEntity;
import com.hktcode.nicknack.owl.jdbc.JdbcTemplateBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ExeSimpleCommand extends Owl
{
    private static final Logger logger = LoggerFactory.getLogger(ExeSimpleCommand.class);

    public static void main( String[] args )
    {
        logger.info("just exe sql command starts: home={}", Owl.Home);
        for (int i = 0; i < args.length; ++i) {
            logger.info("    args[{}] = {}", i, args[i]);
        }
        if (args.length == 0) {
            System.out.println("usage: jesc [name] [days] [reference-days]");
            return;
        }
        Path dataSourceEtc = Paths.get(Home, "etc", "datasouce.properties");
        JdbcTemplateBase.loadJdbcTemplates(dataSourceEtc);
        long days = 1;
        if (args.length > 1) {
            days = Long.parseLong(args[1]);
        }
        LocalDate refDate = LocalDate.now();
        if (args.length > 2) {
            refDate = LocalDate.parse(args[2]);
        }

        JdbcTemplateBase jdbc = JdbcTemplateBase.getJdbcTemplate(Owl.ENGINE);
        String taskname = args[0];
        List<Map<String, Object>> list = jdbc.queryForList(SELECT, taskname);
        logger.info("job list: size={}, taskname={}", list.size(), taskname);
        for (Map<String, Object> map : list) {
            doJob(map, days, refDate);
        }
    }

    private static final String SELECT = "" //
        + "\n SELECT taskname as taskname " //
        + "\n      , prevcode as prevcode " //
        + "\n      , corecode as corecode " //
        + "\n      , postcode as postcode " //
        + "\n      , isenable as isenable " //
        + "\n   FROM hktcode_owl.job_exe_update k " //
        + "\n  WHERE k.taskname = ? " //
        ;

    private static void doJob( Map<String, Object> map, long days, LocalDate refDate )
    {
        String taskname = Objects.toString(map.get("taskname"), "");
        String prevname = Objects.toString(map.get("prevcode"), "");
        String corename = Objects.toString(map.get("corecode"), "");
        String postname = Objects.toString(map.get("postcode"), "");
        String isenable = Objects.toString(map.get("isenable"), "");

        SqlScriptEntity prevcode //
            = "".equals(prevname) ? SqlScriptEntity.of() : SqlScriptEntity.Cache.get(prevname);
        if (prevcode == null) {
            logger.error("not found prevcode: codename={}", prevname);
            return;
        }
        SqlScriptEntity corecode
            = "".equals(corename) ? SqlScriptEntity.of() : SqlScriptEntity.Cache.get(corename);
        if (corecode == null) {
            logger.error("not found corecode: codename={}", corename);
            return;
        }
        SqlScriptEntity postcode
            = "".equals(postname) ? SqlScriptEntity.of() : SqlScriptEntity.Cache.get(postname);
        if (postcode == null) {
            logger.error("not found postcode: codename={}", postname);
            return;
        }
        if (!Objects.equals("EXECUTE", isenable)) {
            logger.error("task disable: taskname={}, isenable={}", taskname, isenable);
            return;
        }
        ExeSimpleEntity entity = new ExeSimpleEntity(taskname, prevcode, corecode, postcode);

        try {
            for (LocalDate statDate = refDate.minusDays(days) //
                 ; statDate.isBefore(refDate) //
                ; statDate = statDate.plusDays(1)) {
                ExeExecutorSimple executor = new ExeExecutorSimple(entity);
                int exitval = executor.execute(statDate);
                logger.info("job success: taskname={}, statdate={}, exitval={}", taskname, statDate, exitval);

            }
        }
        catch (Exception ex) {
            logger.info("job failure: taskname={}", taskname, ex);
        }
    }
}
