/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl;

public class SqlNull
{
    public static final SqlNull SQL_NULL = new SqlNull();

    private SqlNull(){}

    @Override
    public boolean equals(Object o)
    {
        return o instanceof SqlNull;
    }

    @Override
    public int hashCode()
    {
        return "NULL".hashCode();
    }

    @Override
    public String toString()
    {
        return "NULL";
    }
}
