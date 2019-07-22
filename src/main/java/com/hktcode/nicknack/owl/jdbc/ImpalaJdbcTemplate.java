/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.jdbc;

import com.cloudera.impala.jdbc41.DataSource;
import com.hktcode.lang.exception.ArgumentNullException;

public class ImpalaJdbcTemplate extends JdbcTemplateBase
{
    public static ImpalaJdbcTemplate of(DataSource dataSource)
    {
        if (dataSource == null) {
            throw new ArgumentNullException("dataSource");
        }
        return new ImpalaJdbcTemplate(dataSource);
    }

    private ImpalaJdbcTemplate(DataSource dataSource)
    {
        super(dataSource);
    }
}
