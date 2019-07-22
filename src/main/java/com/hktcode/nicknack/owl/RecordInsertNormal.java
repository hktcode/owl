/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl;

import com.google.common.collect.ImmutableMap;
import com.hktcode.lang.exception.ArgumentNullException;

public class RecordInsertNormal implements RecordInsert
{
    public static RecordInsertNormal of(ImmutableMap<String, Object> newtuple)
    {
        if (newtuple == null) {
            throw new ArgumentNullException("newtuple");
        }
        return new RecordInsertNormal(newtuple);
    }

    public final ImmutableMap<String, Object> newtuple;

    private RecordInsertNormal(ImmutableMap<String, Object> newtuple)
    {
        this.newtuple = newtuple;
    }
}
