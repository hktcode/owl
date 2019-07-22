/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl;

import com.google.common.collect.ImmutableMap;
import com.hktcode.lang.exception.ArgumentNullException;

public class RecordDeleteNormal implements RecordDelete
{
    public static RecordDeleteNormal of(ImmutableMap<String, Object> oldtuple)
    {
        if (oldtuple == null) {
            throw new ArgumentNullException("oldtuple");
        }
        return new RecordDeleteNormal(oldtuple);
    }

    public final ImmutableMap<String, Object> oldtuple;

    private RecordDeleteNormal(ImmutableMap<String, Object> oldtuple)
    {
        this.oldtuple = oldtuple;
    }
}
