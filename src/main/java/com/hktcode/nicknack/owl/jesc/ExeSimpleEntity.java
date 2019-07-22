/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.jesc;

import com.hktcode.lang.exception.ArgumentNullException;
import com.hktcode.nicknack.owl.SqlScriptEntity;

public class ExeSimpleEntity
{
    public ExeSimpleEntity //
        /* */( String taskname //
        /* */, SqlScriptEntity prevcode //
        /* */, SqlScriptEntity corecode //
        /* */, SqlScriptEntity postcode //
        /* */)
    {
        if (taskname == null) {
            throw new ArgumentNullException("taskname");
        }
        if (prevcode == null) {
            throw new ArgumentNullException("prevcode");
        }
        if (corecode == null) {
            throw new ArgumentNullException("corecode");
        }
        if (postcode == null) {
            throw new ArgumentNullException("postcode");
        }

        this.taskname = taskname;
        this.prevcode = prevcode;
        this.corecode = corecode;
        this.postcode = postcode;
    }

    public final String taskname;

    public final SqlScriptEntity prevcode;

    public final SqlScriptEntity corecode;

    public final SqlScriptEntity postcode;
}
