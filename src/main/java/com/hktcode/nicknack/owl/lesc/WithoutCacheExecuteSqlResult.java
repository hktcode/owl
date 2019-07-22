/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.lesc;

public class WithoutCacheExecuteSqlResult
{
    private final long mQueryCount;

    private final long mEffectedRowsCount;

    public WithoutCacheExecuteSqlResult(long queryCount, long effectedRowsCount)
    {
        this.mQueryCount = queryCount;
        this.mEffectedRowsCount = effectedRowsCount;
    }

    public long getQueryCount()
    {
        return this.mQueryCount;
    }

    public long getEffectedCRowsCount()
    {
        return this.mEffectedRowsCount;
    }

}
