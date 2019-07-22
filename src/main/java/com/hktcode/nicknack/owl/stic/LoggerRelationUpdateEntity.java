/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.stic;

import com.google.common.collect.ImmutableMap;
import com.hktcode.lang.exception.ArgumentNullException;

import java.time.ZonedDateTime;

/**
 * 静态表修改实体类.
 *
 * 参考数据库中的表{@code logger_relation_update}
 */
public class LoggerRelationUpdateEntity
{
    /**
     * 根据{@link LoggerRelationUpdateEntity}各个属性构造对象.
     *
     * @param updatets 修改的时间.
     * @param relation 修改的关系.
     * @param tuplekey 修改行的键.
     * @param property 修改的属性.
     * @param oldvalue 修改前的值.
     * @param newvalue 修改后的值.
     * @return 根据属性构造的 {@link LoggerRelationUpdateEntity} 对象.
     * @throws ArgumentNullException if one of the parameters is {@code null}.
     */
    public static LoggerRelationUpdateEntity of
        /* */( ZonedDateTime updatets
        /* */, String relation
        /* */, ImmutableMap<String, Object> tuplekey
        /* */, String property
        /* */, Object oldvalue
        /* */, Object newvalue
        /* */)
    {
        if (updatets == null) {
            throw new ArgumentNullException("updatets");
        }
        if (relation == null) {
            throw new ArgumentNullException("relation");
        }
        if (tuplekey == null) {
            throw new ArgumentNullException("tuplekey");
        }
        if (property == null) {
            throw new ArgumentNullException("property");
        }
        if (oldvalue == null) {
            throw new ArgumentNullException("oldvalue");
        }
        if (newvalue == null) {
            throw new ArgumentNullException("newvalue");
        }
        return new LoggerRelationUpdateEntity //
            (updatets, relation, tuplekey, property, oldvalue, newvalue);
    }

    /**
     * 修改时间.
     */
    public final ZonedDateTime updatets;

    /**
     * 被修改的关系名称.
     */
    public final String relation;

    /**
     * 被修改tuple的主键信息.
     */
    public final ImmutableMap<String, Object> tuplekey;

    /**
     * 修改的字段名.
     */
    public final String property;

    /**
     * 修改前的值.
     */
    public final Object oldvalue;

    /**
     * 修改后的值.
     */
    public final Object newvalue;

    /**
     * 构造函数.
     *
     * @param updatets 修改的时间.
     * @param relation 修改的关系.
     * @param tuplekey 修改行的键.
     * @param property 修改的属性.
     * @param oldvalue 修改前的值.
     * @param newvalue 修改后的值.
     */
    private LoggerRelationUpdateEntity
        /* */( ZonedDateTime updatets
        /* */, String relation
        /* */, ImmutableMap<String, Object> tuplekey
        /* */, String property
        /* */, Object oldvalue
        /* */, Object newvalue
        /* */)
    {
        this.updatets = updatets;
        this.relation = relation;
        this.tuplekey = tuplekey;
        this.property = property;
        this.oldvalue = oldvalue;
        this.newvalue = newvalue;
    }
}
