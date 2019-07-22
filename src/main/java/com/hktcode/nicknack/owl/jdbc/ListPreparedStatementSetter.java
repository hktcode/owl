/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.jdbc;

import com.google.common.collect.ImmutableList;
import com.hktcode.lang.exception.ArgumentNullException;
import org.springframework.jdbc.core.PreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static com.hktcode.nicknack.owl.SqlNull.SQL_NULL;

public class ListPreparedStatementSetter implements PreparedStatementSetter
{
    public final ImmutableList<Object> parameters;

    public ListPreparedStatementSetter(ImmutableList<Object> parameters)
    {
        if (parameters == null) {
            throw new ArgumentNullException("parameters");
        }
        this.parameters = parameters;
    }

    @Override
    public void setValues(PreparedStatement ps) throws SQLException
    {
        if (ps == null) {
            throw new ArgumentNullException("ps");
        }
        setValues(ps, this.parameters);
    }

    public static void setValues(PreparedStatement ps, List<Object> argv) //
        throws SQLException
    {
        if (ps == null) {
            throw new ArgumentNullException("ps");
        }
        if (argv == null) {
            throw new ArgumentNullException("argv");
        }
        int index = 0;
        int size = argv.size();
        while (index < size) {
            Object value = argv.get(index);
            value = (value == SQL_NULL ? null : value);
            ps.setObject(++index, value);
        }
    }
}
