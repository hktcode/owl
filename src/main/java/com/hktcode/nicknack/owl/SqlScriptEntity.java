/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.hktcode.lang.exception.ArgumentNullException;
import com.hktcode.nicknack.Owl;
import com.hktcode.nicknack.owl.jdbc.JdbcTemplateBase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SqlScriptEntity
{
    public static final ImmutableMap<String, SqlScriptEntity> Cache;

    static
    {
        final String AllSqlsSelectStatements = "" //
            + "\n SELECT codename " //
            + "\n      , engineid " //
            + "\n      , execcode " //
            + "\n      , argument " //
            + "\n      , document " //
            + "\n   FROM hktcode_owl.scp_sql_script ";
        Map<String, SqlScriptEntity> sqls = new HashMap<>();
        List<Map<String, Object>> sqlMapsList = JdbcTemplateBase.getJdbcTemplate(Owl.ENGINE) //
            .queryForList(AllSqlsSelectStatements);
        for (Map<String, Object> sqlMap : sqlMapsList) {
            String codename = Objects.toString(sqlMap.get("codename"), "");
            String engineid = Objects.toString(sqlMap.get("engineid"), "");
            String execcode = Objects.toString(sqlMap.get("execcode"), "");
            String argument = Objects.toString(sqlMap.get("argument"), "");
            String document = Objects.toString(sqlMap.get("document"), "");
            Iterable<String> parametersIterator //
                = Splitter.on(',').trimResults().omitEmptyStrings().split(argument);
            ImmutableList<String> argulist = ImmutableList.copyOf(parametersIterator);
            SqlScriptEntity statement //
                = SqlScriptEntity.of(codename, engineid, execcode, argulist, document);
            sqls.put(codename, statement);
        }
        Cache = ImmutableMap.copyOf(sqls);
    }

    public static SqlScriptEntity of()
    {
        return new SqlScriptEntity("", "", "", ImmutableList.of(), "");
    }

    public static SqlScriptEntity of //
        /* */( String codename //
        /* */, String engineid //
        /* */, String execcode //
        /* */, ImmutableList<String> argument //
        /* */, String document //
        /* */)
    {
        if (codename == null) {
            throw new ArgumentNullException("codename");
        }
        if (engineid == null) {
            throw new ArgumentNullException("engineid");
        }
        if (execcode == null) {
            throw new ArgumentNullException("execcode");
        }
        if (argument == null) {
            throw new ArgumentNullException("argument");
        }
        if (document == null) {
            throw new ArgumentNullException("document");
        }
        return new SqlScriptEntity(codename, engineid, execcode, argument, document);
    }

    private SqlScriptEntity //
        /* */( String codename //
        /* */, String engineid //
        /* */, String execcode //
        /* */, ImmutableList<String> argument //
        /* */, String document //
        /* */)
    {
        this.codename = codename;
        this.engineid = engineid;
        this.execcode = execcode;
        this.argument = argument;
        this.document = document;
    }

    public final String codename;

    public final String engineid;

    public final String execcode;

    public final ImmutableList<String> argument;

    public final String document;

    @Override
    public String toString()
    {
        ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        node.put("codename", codename);
        node.put("engineid", engineid);
        node.put("execcode", execcode);
        node.put("document", document);
        ArrayNode argulist = node.putArray("argument");
        for (String argu : argument) {
            argulist.add(argu);
        }
        return node.toString();
    }

}
