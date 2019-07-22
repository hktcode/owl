/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.jdbc;

import com.hktcode.lang.exception.ArgumentNullException;

import javax.sql.DataSource;

public class HiveJdbcTemplate extends JdbcTemplateBase
{
    public static HiveJdbcTemplate of(DataSource dataSource)
    {
        if (dataSource == null) {
            throw new ArgumentNullException("dataSource");
        }
        return new HiveJdbcTemplate(dataSource);
    }

    private HiveJdbcTemplate(DataSource dataSource)
    {
        super(dataSource);
    }
}
