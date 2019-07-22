/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl;

import com.google.common.collect.ImmutableMap;
import com.hktcode.lang.exception.ArgumentNullException;

public class RecordSelectNormal implements RecordSelect
{
    public static RecordSelectNormal of(ImmutableMap<String, Object> tupleval)
    {
        if (tupleval == null) {
            throw new ArgumentNullException("tupleval");
        }
        return new RecordSelectNormal(tupleval);
    }

    public final ImmutableMap<String, Object> tupleval;

    private RecordSelectNormal(ImmutableMap<String, Object> tupleval)
    {
        this.tupleval = tupleval;
    }
}
