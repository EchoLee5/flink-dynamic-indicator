package org.example.dynamicrules.functions;

import org.example.dynamicrules.Indicator;
import org.example.dynamicrules.jdbc.FieldNamedPreparedStatement;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.example.config.Parameters.*;
import static java.lang.String.format;

/**
 * @author lisen
 * @desc
 * @date Created in 2021/11/19 16:26
 */
@Slf4j
public class LookupFunction extends RichMapFunction<Indicator, Indicator> {
    private transient Cache<String, Map<String, String>> cache;
    private transient JdbcConnectionProvider connectionProvider;
    private transient FieldNamedPreparedStatement preparedStatement;
    private Map<String, String> config;
    private String[] leftJoinKeys;
    private String[] rightJoinKeys;
    private String[] extendFields;

    @Override
    public void open(Configuration parameters) throws Exception {
        config = getRuntimeContext().getExecutionConfig()
                .getGlobalJobParameters().toMap();
        long cacheMaxSize = Long.parseLong(config.get(LOOKUP_CACHE_MAX_SIZE.getName()));
        long cacheExpireMs = Long.parseLong(config.get(LOOKUP_CACHE_EXPIRE_MS.getName()));
        this.cache =
                cacheMaxSize == -1 || cacheExpireMs == -1
                        ? null
                        : CacheBuilder.newBuilder()
                        .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                        .maximumSize(cacheMaxSize)
                        .build();

        connectionProvider = getConnectionProvider(config);

        leftJoinKeys = getJoinKeys(true);
        rightJoinKeys = getJoinKeys(false);

        extendFields = getExtendFields();

        establishStatement();
    }

    @Override
    public Indicator map(Indicator value) throws Exception {
        String[] joinValue = Arrays.stream(leftJoinKeys)
                .map(key -> value.getFields().get(key)).toArray(String[]::new);
        String cacheKey = String.join(",", joinValue);

        if (cache != null) {
            Map<String, String> cacheRow = cache.getIfPresent(cacheKey);
            if (cacheRow != null) {
                for (String extendField : extendFields) {
                    value.getFields().put(extendField, cacheRow.get(extendField));
                }
                return value;
            }
        }

        int index = 0;
        preparedStatement.clearParameters();
        for (String joinKey : leftJoinKeys) {
            preparedStatement.setString(index, value.getFields().get(joinKey));
            index ++;
        }

        try {

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, String> map = new HashMap();
                    for (String extendField : extendFields) {
                        map.put(extendField, resultSet.getString(extendField));

                    }
                    value.getFields().putAll(map);
                    cache.put(cacheKey, map);
                }
            }
        } catch (SQLException e) {
            preparedStatement.close();
            connectionProvider.getConnection().close();
            establishStatement();
        }


        return value;
    }

    private JdbcConnectionProvider getConnectionProvider(Map<String, String> config) throws SQLException, ClassNotFoundException {
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder builder = new JdbcConnectionOptions
                .JdbcConnectionOptionsBuilder();
        String url = String.format("jdbc:mysql://%s:%s/%s", config.get(MYSQL_HOST.getName()),
                config.get(MYSQL_PORT.getName()),
                config.get(LOOKUP_DATABASE_NAME.getName()));
        builder.withUrl(url);
        builder.withUsername(config.get(MYSQL_USERNAME.getName()));
        builder.withPassword(config.get(MYSQL_PASSWORD.getName()));
        builder.withDriverName("com.mysql.jdbc.Driver");
        SimpleJdbcConnectionProvider provider = new SimpleJdbcConnectionProvider(builder.build());

        return provider;
    }

    @Override
    public void close() throws Exception {
        if (cache != null) {
            cache.cleanUp();
            cache = null;
        }
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                log.info("JDBC statement could not be closed: " + e.getMessage());
            } finally {
                preparedStatement = null;
            }
        }

        connectionProvider.getConnection().close();
    }

    private void establishStatement() throws Exception {
        String query = getSelectFromStatement(config.get(LOOKUP_TABLE_NAME.getName()),
                extendFields, rightJoinKeys);
        Connection dbConn = connectionProvider.getConnection();
        preparedStatement = FieldNamedPreparedStatement.prepareStatement(
                dbConn, query, rightJoinKeys);
    }

    private String[] getExtendFields() {
        String keyNamesString = config.get(LOOKUP_TABLE_EXTEND_FIELDS.getName());
        if (!"".equals(keyNamesString)) {
            String[] tokens = keyNamesString.split(",", -1);
            return tokens;
        } else {
            return new String[]{};
        }
    }

    private String[] getJoinKeys(boolean isLeft) {
        Map<String, String> joinKeys = getJoinKeys();

        String[] leftJoinKeys = joinKeys.keySet().toArray(new String[0]);
        if (isLeft) {
            return leftJoinKeys;
        } else {
            return Arrays.stream(leftJoinKeys).map(joinKeys::get).toArray(String[]::new);
        }
    }

    private Map<String, String> getJoinKeys() {
        String keyNamesString = config.get(LOOKUP_TABLE_JOIN_KEYS.getName());

        Map<String, String> joinKeys = new HashMap<>();
        if (!"".equals(keyNamesString)) {
            String[] tokens = keyNamesString.split(",", -1);
            for (String joinKey : tokens) {
                if (!"".equals(joinKey)) {
                    String[] kv = joinKey.split("=", -1);
                    String left = kv[0];
                    String right = kv[1];
                    joinKeys.put(left, right);
                }
            }
        }

        return joinKeys;
    }

    public String getSelectFromStatement(
            String tableName, String[] selectFields, String[] conditionFields) {
        String selectExpressions =
                Arrays.stream(selectFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        return "SELECT "
                + selectExpressions
                + " FROM "
                + quoteIdentifier(tableName)
                + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "");
    }

    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }
}
