/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl;

import com.google.common.collect.ImmutableMap;
import com.hktcode.lang.exception.ArgumentNullException;

public class RecordUpdateNormal implements RecordUpdate
{
    public static RecordUpdateNormal of(ImmutableMap<String, Object> oldtuple, ImmutableMap<String, Object> newtuple)
    {
        if (oldtuple == null) {
            throw new ArgumentNullException("oldtuple");
        }
        if (newtuple == null) {
            throw new ArgumentNullException("newtuple");
        }
        return new RecordUpdateNormal(oldtuple, newtuple);
    }

    public final ImmutableMap<String, Object> oldtuple;

    public final ImmutableMap<String, Object> newtuple;

    private RecordUpdateNormal(ImmutableMap<String, Object> oldtuple, ImmutableMap<String, Object> newtuple)
    {
        this.oldtuple = oldtuple;
        this.newtuple = newtuple;
    }
}
