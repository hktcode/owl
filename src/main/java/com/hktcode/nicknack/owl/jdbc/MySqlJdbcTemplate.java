/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.jdbc;

import com.hktcode.lang.exception.ArgumentNullException;

import javax.sql.DataSource;
import java.sql.ResultSet;

public class MySqlJdbcTemplate extends JdbcTemplateBase
{
    public static MySqlJdbcTemplate of(DataSource dataSource)
    {
        if (dataSource == null) {
            throw new ArgumentNullException("dataSource");
        }
        return new MySqlJdbcTemplate(dataSource);
    }

    private MySqlJdbcTemplate(DataSource dataSource)
    {
        super(dataSource);
    }

    @Override
    public MySqlForwardPreparedStatementCreator //
    newForwardPreparedStatementCreator(String sql)
    {
        if (sql == null) {
            throw new ArgumentNullException("sql");
        }
        return MySqlForwardPreparedStatementCreator.of(sql);
    }

    @Override
    public void setResultSetForward(ResultSet rs)
    {
        if (rs == null) {
            throw new ArgumentNullException("rs");
        }
        // MySQL服务端设置，此处不需要设置.
    }
}
