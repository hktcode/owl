/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.jdbc;

import com.hktcode.lang.exception.ArgumentNullException;
import org.springframework.jdbc.core.PreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

import static com.hktcode.nicknack.owl.SqlNull.SQL_NULL;

public class ArrayPreparedStatementSetter implements PreparedStatementSetter
{
    private final Object[] parameters;

    public static ArrayPreparedStatementSetter of(Object[] parameters)
    {
        if (parameters == null) {
            throw new ArgumentNullException("parameters");
        }
        return new ArrayPreparedStatementSetter(parameters);
    }

    public static void setValues(PreparedStatement ps, Object[] argv) //
        throws SQLException
    {
        if (ps == null) {
            throw new ArgumentNullException("ps");
        }
        if (argv == null) {
            throw new ArgumentNullException("argv");
        }
        int index = 0;
        int length = argv.length;
        while (index < length) {
            Object value = argv[index];
            value = (value == SQL_NULL ? null : value);
            ps.setObject(++index, value);
        }
    }

    private ArrayPreparedStatementSetter(Object[] parameters)
    {
        this.parameters = Arrays.copyOf(parameters, parameters.length);
    }

    @Override
    public void setValues(PreparedStatement ps) throws SQLException
    {
        if (ps == null) {
            throw new ArgumentNullException("ps");
        }
        setValues(ps, this.parameters);
    }
}
