/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.jdbc;

import com.hktcode.lang.exception.ArgumentNullException;

import javax.sql.DataSource;

public class PgSqlJdbcTemplate extends JdbcTemplateBase
{
    public static PgSqlJdbcTemplate of(DataSource dataSource)
    {
        if (dataSource == null) {
            throw new ArgumentNullException("dataSource");
        }
        return new PgSqlJdbcTemplate(dataSource);
    }

    private PgSqlJdbcTemplate(DataSource dataSource)
    {
        super(dataSource);
    }
}
