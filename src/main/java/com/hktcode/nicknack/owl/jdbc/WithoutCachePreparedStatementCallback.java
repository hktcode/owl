/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.PreparedStatementCallback;

import com.hktcode.nicknack.owl.SqlScriptEntity;
import com.hktcode.nicknack.owl.lesc.WithoutCacheExecuteSqlResult;
import com.google.common.collect.ImmutableMap;

public class WithoutCachePreparedStatementCallback implements PreparedStatementCallback<WithoutCacheExecuteSqlResult>
{
    private static final Logger logger = LoggerFactory.getLogger(WithoutCacheExecuteSqlResult.class);

    private final ImmutableMap<String, String> mParams;

    private final SqlScriptEntity mPostSql;

    private final SqlScriptEntity mCoreSql;

    public WithoutCachePreparedStatementCallback(SqlScriptEntity coreSql, ImmutableMap<String, String> params,
                                                 SqlScriptEntity postSql)
    {
        this.mCoreSql = coreSql;
        this.mParams = params;
        this.mPostSql = postSql;
    }

    @Override
    public WithoutCacheExecuteSqlResult doInPreparedStatement(PreparedStatement ps)
    {
        int coreParamtersSize = this.mCoreSql.argument.size();
        Object[] coreParams = new Object[coreParamtersSize];
        for (int i = 0; i < coreParamtersSize; ++i) {
            coreParams[i] = this.mParams.get(this.mCoreSql.argument.get(i));
        }
        JdbcTemplateBase core = JdbcTemplateBase.getJdbcTemplate(this.mCoreSql.engineid);
        CorePreparedStatementCallback callback = new CorePreparedStatementCallback(ps, coreParams, this.mPostSql);
        return core.executeByForwardPreparedStatement(this.mCoreSql.execcode, callback);
    }

    private static class CorePreparedStatementCallback
            implements PreparedStatementCallback<WithoutCacheExecuteSqlResult>
    {
        private final Object[] mCoreParams;

        private final SqlScriptEntity mPostSql;
        
        private final PreparedStatement mPostStatement;

        public CorePreparedStatementCallback(PreparedStatement preStatement, Object[] coreParams, SqlScriptEntity postSql)
        {
            this.mPostStatement = preStatement;
            this.mCoreParams = coreParams;
            this.mPostSql = postSql;
        }

        @Override
        public WithoutCacheExecuteSqlResult doInPreparedStatement(PreparedStatement ps)
                throws SQLException
        {
            long queryCount = 0;
            long batchCount = 0;
            long effectedRowsCount = 0;
            for (int i = 0; i < this.mCoreParams.length; ++i) {
                ps.setObject(i + 1, this.mCoreParams[i]);
            }

            Connection connection = ps.getConnection();
            connection.setAutoCommit(false);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    for (int i = 0; i < this.mPostSql.argument.size(); ++i) {
                        Object pobj = rs.getObject(this.mPostSql.argument.get(i));
                        String pstr = Objects.toString(pobj, null);
                        this.mPostStatement.setString(i + 1, pstr);
                    }
                    ++queryCount;
                    this.mPostStatement.addBatch();
                    ++batchCount;
                    if (batchCount >= 10000) {
                        int[] effectedCounts = this.mPostStatement.executeBatch();
                        for (int ec : effectedCounts) {
                            effectedRowsCount += ec;
                        }
                        this.mPostStatement.clearBatch();
                        batchCount = 0;
                    }
                }
                if (batchCount > 0) {
                    int[] effectedCounts = this.mPostStatement.executeBatch();
                    for (int ec : effectedCounts) {
                        effectedRowsCount += ec;
                    }
                    this.mPostStatement.clearBatch();
                    batchCount = 0;
                }
            }
            finally {
                if (!connection.isClosed()) {
                    // 确定不是由于连接关闭导致异常从而进入此处。
                    connection.setAutoCommit(true);
                }
            }
            return new WithoutCacheExecuteSqlResult(queryCount, effectedRowsCount);
        }
    }
}

