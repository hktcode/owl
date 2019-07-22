/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Owl
{
    public static final String Home;

    public static final String ENGINE = "hktcode.owl";

    static {
        String homeEnvName = "OWL_HOME";
        String home = System.getenv(homeEnvName);
        if (home == null || "".equals(home)) {
            System.err.printf("HOME ENVIROMENT IS UNDEFINED: name=%s%n", homeEnvName);
            System.exit(1);
        }
        Home = home;
    }
    
    private static final Logger logger = LoggerFactory.getLogger(Owl.class);

    public static void main(String[] args)
    {
        logger.info("application begin: home={}, tmpdir={}", Home);
        for (int i = 0; i < args.length; ++i) {
            logger.info("    args[{}]: {}", i, args[i]);
        }
    }
}
