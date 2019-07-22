/*
 * Copyright (c) 2019, Huang Ketian.
 */
package com.hktcode.lang.exception;

/**
 * 不能为{@code null}的参数为{@code null}时抛出的异常.
 */
public class ArgumentNullException extends NullPointerException
{
    /**
     * 参数名称.
     */
    public final String name;

    /**
     * 构造函数.
     *
     * @param name 参数的名称. 该参数不能为{@code null}。
     *
     * @throws ArgumentNullException 当参数{@code name}为{@code null}时抛出.
     */
    public ArgumentNullException(String name)
    {
        super("argument is null: name=" + (name == null ? "null" : name));
        if (name == null) {
            throw new ArgumentNullException("name");
        }
        this.name = name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "ArgumentNullException: name=" + this.name;
    }
}
