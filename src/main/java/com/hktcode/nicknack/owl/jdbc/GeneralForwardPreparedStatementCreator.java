/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.jdbc;

import com.hktcode.lang.exception.ArgumentNullException;
import org.springframework.jdbc.core.PreparedStatementCreator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GeneralForwardPreparedStatementCreator //
    implements PreparedStatementCreator
{
    public static GeneralForwardPreparedStatementCreator of //
        (String statement, int fetchsize)
    {
        if (statement == null) {
            throw new ArgumentNullException("statement");
        }
        // TODO: check fetchsize
        return new GeneralForwardPreparedStatementCreator(statement, fetchsize);
    }

    public final String statement;

    public final int fetchsize;

    private GeneralForwardPreparedStatementCreator //
        (String statement, int fetchsize)
    {
        this.statement = statement;
        this.fetchsize = fetchsize;
    }

    @Override
    public PreparedStatement createPreparedStatement(Connection connection) //
        throws SQLException
    {
        if (connection == null) {
            throw new ArgumentNullException("connection");
        }
        PreparedStatement result = connection.prepareStatement //
                /* */( this.statement //
                /* */, ResultSet.TYPE_FORWARD_ONLY //
                /* */, ResultSet.CONCUR_READ_ONLY //
                /* */);
        result.setFetchDirection(ResultSet.FETCH_FORWARD);
        return connection.prepareStatement(this.statement);
    }
}
