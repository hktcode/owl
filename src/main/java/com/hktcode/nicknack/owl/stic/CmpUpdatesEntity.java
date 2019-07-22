/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.stic;

import com.google.common.collect.ImmutableSet;
import com.hktcode.nicknack.owl.CmpOperateEntity;
import com.hktcode.nicknack.owl.SqlScriptEntity;

public class CmpUpdatesEntity extends CmpOperateEntity
{
    public CmpUpdatesEntity //
        /* */(String taskname //
        /* */, String relation //
        /* */, SqlScriptEntity oldtuple //
        /* */, SqlScriptEntity newtuple //
        /* */, ImmutableSet<String> tuplekey //
        /* */, String isenable //
        /* */, String document //
        /* */, String heritage //
        /* */, String putfield //
        /* */, String delfield //
        /* */, String relogger //
        /* */)
    {
        super(taskname, relation, oldtuple, newtuple, tuplekey, isenable, document);
        this.heritage = heritage;
        this.putfield = putfield;
        this.delfield = delfield;
        this.relogger = relogger;
    }

    public final String relogger;

    public final String putfield;

    public final String delfield;

    public final String heritage;
}




















































