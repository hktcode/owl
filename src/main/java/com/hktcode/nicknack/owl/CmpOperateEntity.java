/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl;

import com.google.common.collect.ImmutableSet;

public class CmpOperateEntity
{
    public CmpOperateEntity
        /* */( String taskname
        /* */, String relation
        /* */, SqlScriptEntity oldtuple
        /* */, SqlScriptEntity newtuple
        /* */, ImmutableSet<String> tuplekey
        /* */, String isenable
        /* */, String document //
        /* */)
    {
        this.taskname = taskname;
        this.relation = relation;
        this.oldtuple = oldtuple;
        this.newtuple = newtuple;
        this.tuplekey = tuplekey;
        this.isenable = isenable;
        this.document = document;
    }

    public final String taskname;

    public final String relation;

    public final SqlScriptEntity oldtuple;

    public final SqlScriptEntity newtuple;

    public final ImmutableSet<String> tuplekey;

    public final String isenable;

    public final String document;
}




















































