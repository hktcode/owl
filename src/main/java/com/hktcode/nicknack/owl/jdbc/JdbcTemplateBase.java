/*
 * Copyright (c) 2019, Huang Ketian.
 */

package com.hktcode.nicknack.owl.jdbc;

import com.cloudera.hive.jdbc41.HS2Driver;
import com.hktcode.lang.exception.ArgumentNullException;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreator;

import javax.sql.DataSource;
import java.io.*;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public abstract class JdbcTemplateBase extends JdbcTemplate
{
    private static final Map<String, JdbcTemplateBase> JdbcTemplates = new HashMap<>();

    public static JdbcTemplateBase getJdbcTemplate(String name)
    {
        if (name == null) {
            throw new ArgumentNullException("name");
        }
        JdbcTemplateBase result = JdbcTemplates.get(name);
        if (result == null) {
            throw new RuntimeException(); // TODO
        }
        return result;
    }

    public static void loadJdbcTemplates(Path filepath)
    {
        Properties properties = new Properties();
        File config = filepath.toFile();
        if (!config.exists()) {
            logger.info("datasource config file not exist: filepath={}", filepath.toAbsolutePath());
            return;
        }
        try (InputStream input = new FileInputStream(filepath.toFile())) {
            properties.load(input);
            logger.info("datasource config file load success.");
            @SuppressWarnings("unchecked")
            Enumeration<String> names = (Enumeration<String>) properties.propertyNames();
            while (names.hasMoreElements()) {
                String key = names.nextElement();
                String val = properties.getProperty(key);
                if (val.startsWith("jdbc:mysql")) {
                    BasicDataSource dataSource = new BasicDataSource();
                    dataSource.setDriverClassName(com.mysql.jdbc.Driver.class.getName());
                    dataSource.setUrl(val);
                    setDataSource(dataSource);
                    JdbcTemplates.put(key, MySqlJdbcTemplate.of(dataSource));
                }
                else if (val.startsWith("jdbc:postgresql")) {
                    BasicDataSource dataSource = new BasicDataSource();
                    dataSource.setDriverClassName(org.postgresql.Driver.class.getName());
                    dataSource.setUrl(val);
                    setDataSource(dataSource);
                    JdbcTemplates.put(key, PgSqlJdbcTemplate.of(dataSource));
                }
                else if (val.startsWith("jdbc:impala")) {
                    com.cloudera.impala.jdbc41.DataSource dataSource = new com.cloudera.impala.jdbc41.DataSource();
                    dataSource.setURL(val);
                    JdbcTemplates.put(key, ImpalaJdbcTemplate.of(dataSource));
                }
                else if (val.startsWith("jdbc:hive")) {
                    BasicDataSource dataSource = new BasicDataSource();
                    dataSource.setDriverClassName(HS2Driver.class.getName());
                    dataSource.setUrl(val);
                    setDataSource(dataSource);
                    JdbcTemplates.put(key, HiveJdbcTemplate.of(dataSource));
                }
                else {
                    // TODO
                    throw new RuntimeException();
                }
            }
        }
        catch (IOException ex) {
            logger.error("load datasouce config file fail: filepath={}, message={}" //
                    , filepath.toAbsolutePath(), ex.getMessage(), ex);
            UncheckedIOException newEx = new UncheckedIOException(ex);
            throw newEx;
        }
        logger.info("jdbc template load success: size={}", JdbcTemplates.size());
        for (Map.Entry<String, JdbcTemplateBase> entry : JdbcTemplates.entrySet()) {
            logger.info("    key={}, dataSource={}", entry.getKey(), entry.getValue().getDataSource());
        }
    }

    private final static Logger logger = LoggerFactory.getLogger(JdbcTemplateBase.class);

    protected JdbcTemplateBase(DataSource dataSource)
    {
        super(dataSource);
    }

    public <T> T executeByForwardPreparedStatement(String sql, PreparedStatementCallback<T> callback)
    {
        if (sql == null) {
            throw new ArgumentNullException("sql");
        }
        if (callback == null) {
            throw new ArgumentNullException("callback");
        }
        PreparedStatementCreator psc //
            = this.newForwardPreparedStatementCreator(sql);
        return super.execute(psc, callback);
    }

    public PreparedStatementCreator newForwardPreparedStatementCreator(String sql)
    {
        if (sql == null) {
            throw new ArgumentNullException("sql");
        }
        return GeneralForwardPreparedStatementCreator.of(sql, 10240);
    }

    public long execute
        /* */( Connection cnt //
        /* */, String statement //
        /* */, Map<String, Object> tuples //
        /* */, List<String> fields //
        /* */) //
        throws SQLException
    {
        try(PreparedStatement ps = cnt.prepareStatement(statement)) {
            int index = 0;
            int size = fields.size();
            while (index < size) {
                String field = fields.get(index);
                Object value = tuples.get(field);
                ps.setObject(++index, value);
            }
            return ps.executeLargeUpdate();
        }
    }

    public long[] executeBatch
        /* */( Connection cnt //
        /* */, String statement //
        /* */, List<? extends Map<String, Object>> tuples //
        /* */, List<String> fields //
        /* */) //
        throws SQLException
    {
        try (PreparedStatement ps = cnt.prepareStatement(statement)) {
            for (Map<String, Object> tuple : tuples) {
                int index = 0;
                final int size = fields.size();
                while (index < size) {
                    String field = fields.get(index);
                    Object value = tuple.get(field);
                    ps.setObject(++index, value);
                }
                ps.addBatch();
            }
            return ps.executeLargeBatch();
        }
    }

    public void setResultSetForward(ResultSet rs) throws SQLException
    {
        rs.setFetchDirection(ResultSet.FETCH_FORWARD);
        rs.setFetchSize(1024);
    }

    private static void setDataSource(BasicDataSource dataSource)
    {
        dataSource.setRemoveAbandonedTimeout(120);
        dataSource.setInitialSize(0);
        dataSource.setMaxWaitMillis(60000);
        dataSource.setMinIdle(0);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setTestWhileIdle(true);
        dataSource.setTestOnBorrow(true);
        dataSource.setTestOnReturn(true);
        dataSource.setValidationQuery("SELECT 1");
        dataSource.setRemoveAbandonedOnBorrow(true);
        dataSource.setRemoveAbandonedOnMaintenance(true);
        dataSource.setMaxTotal(-1);
    }
}
