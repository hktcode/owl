/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.hktcode.lang.exception.ArgumentNullException;
import org.springframework.jdbc.core.PreparedStatementCreator;

public class MySqlForwardPreparedStatementCreator implements PreparedStatementCreator
{
    public static MySqlForwardPreparedStatementCreator of(String statement)
    {
        if (statement == null) {
            throw new ArgumentNullException("statement");
        }
        return new MySqlForwardPreparedStatementCreator(statement);
    }

    public final String statement;
    
    private MySqlForwardPreparedStatementCreator(String statement)
    {
        this.statement = statement;
    }
    
    @Override
    public PreparedStatement createPreparedStatement(Connection con) throws SQLException
    {
        PreparedStatement ps = con.prepareStatement //
                /* */( this.statement //
                /* */, ResultSet.TYPE_FORWARD_ONLY //
                /* */, ResultSet.CONCUR_READ_ONLY //
                /* */);
        ps.setFetchSize(Integer.MIN_VALUE);
        ps.setFetchDirection(ResultSet.FETCH_REVERSE);
        return ps;
    }
}
