/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.lesc;

import com.hktcode.nicknack.Owl;
import com.hktcode.nicknack.owl.jdbc.JdbcTemplateBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;

public class ExeInsertCommand extends Owl
{
    private static final Logger logger = LoggerFactory.getLogger(ExeInsertCommand.class);

    public static void main( String[] args )
    {
        logger.info("exe insert command starts: home={}", Owl.Home);
        for (int i = 0; i < args.length; ++i) {
            logger.info("    args[{}] = {}", i, args[i]);
        }
        if (args.length == 0) {
            System.out.println("usage: lesc [name] [days] [reference-days]");
            return;
        }
        Path dataSourceEtc = Paths.get(Home, "etc", "datasource.properties");
        JdbcTemplateBase.loadJdbcTemplates(dataSourceEtc);
        String name = args[0];
        long days = 1;
        if (args.length > 1) {
            days = Long.parseLong(args[1]);
        }
        LocalDate refDate = LocalDate.now();
        if (args.length > 2) {
            refDate = LocalDate.parse(args[2]);
        }

        // TODO:
    }
}
