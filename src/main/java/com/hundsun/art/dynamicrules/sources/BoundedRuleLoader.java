package com.hundsun.art.dynamicrules.sources;

import com.hundsun.art.dynamicrules.Rule;
import com.hundsun.art.dynamicrules.RuleParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import static com.hundsun.art.config.Parameters.*;

/**
 * @author lisen
 * @desc
 * @date Created in 2021/11/16 15:25
 */
@Slf4j
public class BoundedRuleLoader {
    private Map<String, String> config;

    public BoundedRuleLoader(Map<String, String> config) {
        this.config = config;
    }

    public Map<Integer, Rule> load() {
        Map<Integer, Rule> ruleMap = new HashMap<>();
        try (Connection connection = getConnectionProvider(config).getConnection()) {
            PreparedStatement columnLenPreparedStatement = connection
                    .prepareStatement(String.format("SELECT count(1) from information_schema.COLUMNS " +
                                    "WHERE table_schema='%s' and table_name='%s'",
                            config.get(RULES_DATABASE_NAME.getName()),
                            config.get(RULES_TABLE_NAME.getName())));

            PreparedStatement preparedStatement = connection.prepareStatement(String.format("select * from %s",
                    config.get(RULES_TABLE_NAME.getName())));

            ResultSet columnLenResultSet = columnLenPreparedStatement.executeQuery();
            int columnLen = 0;
            while (columnLenResultSet.next()) {
                columnLen = columnLenResultSet.getInt(1);
            }
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                StringBuilder ruleBuilder = new StringBuilder();
                for (int i = 1; i <= columnLen; i++) {
                    ruleBuilder.append(resultSet.getString(i)).append(",");
                }
                ruleBuilder.deleteCharAt(ruleBuilder.length() - 1);
                Rule rule = null;
                try {
                    rule = new RuleParser().fromString(ruleBuilder.toString());
                } catch (IOException e) {
                    log.warn("Failed parsing rule, dropping it:", e);
                }
                ruleMap.put(rule.getRuleId(), rule);
            }
        } catch (Exception e) {
            log.error("Load bounded rule failed {}", e.getMessage());
        }

        return ruleMap;
    }

    private JdbcConnectionProvider getConnectionProvider(Map<String, String> config) {
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder builder = new JdbcConnectionOptions
                .JdbcConnectionOptionsBuilder();
        String url = String.format("jdbc:mysql://%s:%s/%s", config.get(MYSQL_HOST.getName()),
                config.get(MYSQL_PORT.getName()),
                config.get(RULES_DATABASE_NAME.getName()));
        builder.withUrl(url);
        builder.withUsername(config.get(MYSQL_USERNAME.getName()));
        builder.withPassword(config.get(MYSQL_PASSWORD.getName()));
        builder.withDriverName("com.mysql.jdbc.Driver");
        SimpleJdbcConnectionProvider provider = new SimpleJdbcConnectionProvider(builder.build());

        return provider;
    }
}
