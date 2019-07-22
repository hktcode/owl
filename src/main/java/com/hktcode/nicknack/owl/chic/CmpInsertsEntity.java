/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.chic;

import com.google.common.collect.ImmutableSet;
import com.hktcode.nicknack.owl.CmpOperateEntity;
import com.hktcode.nicknack.owl.SqlScriptEntity;

public class CmpInsertsEntity extends CmpOperateEntity
{
    public CmpInsertsEntity //
        /* */(String taskname //
        /* */, String relation //
        /* */, SqlScriptEntity oldtuple //
        /* */, SqlScriptEntity newtuple //
        /* */, ImmutableSet<String> tuplekey //
        /* */, String isenable //
        /* */, String document //
        /* */, String metadata //
        /* */, String dmlfield //
        /* */)
    {
        super(taskname, relation, oldtuple, newtuple, tuplekey, isenable, document);
        this.metadata = metadata;
        this.dmlfield = dmlfield;
    }
    
    public final String metadata;

    public final String dmlfield;
}

