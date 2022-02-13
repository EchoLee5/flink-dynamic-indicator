package com.hundsun.art.dynamicrules.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author lisen
 * @desc 封装的PreparedStatement
 * @date Created in 2021/11/22 13:21
 */
public class FieldNamedPreparedStatementImpl implements FieldNamedPreparedStatement {

    private final PreparedStatement statement;
    private final int[][] indexMapping;

    private FieldNamedPreparedStatementImpl(PreparedStatement statement, int[][] indexMapping) {
        this.statement = statement;
        this.indexMapping = indexMapping;
    }

    @Override
    public void clearParameters() throws SQLException {
        statement.clearParameters();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return statement.executeQuery();
    }

    @Override
    public void addBatch() throws SQLException {
        statement.addBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return statement.executeBatch();
    }

    @Override
    public void setString(int fieldIndex, String x) throws SQLException {
        for (int index : indexMapping[fieldIndex]) {
            statement.setString(index, x);
        }
    }

    @Override
    public void close() throws SQLException {
        statement.close();
    }

    // ----------------------------------------------------------------------------------------

    public static FieldNamedPreparedStatement prepareStatement(
            Connection connection, String sql, String[] fieldNames) throws SQLException {
        checkNotNull(connection, "connection must not be null.");
        checkNotNull(sql, "sql must not be null.");
        checkNotNull(fieldNames, "fieldNames must not be null.");

        if (sql.contains("?")) {
            throw new IllegalArgumentException("SQL statement must not contain ? character.");
        }

        HashMap<String, List<Integer>> parameterMap = new HashMap<>();
        String parsedSQL = parseNamedStatement(sql, parameterMap);
        // currently, the statements must contain all the field parameters
        checkArgument(parameterMap.size() == fieldNames.length);
        int[][] indexMapping = new int[fieldNames.length][];
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = fieldNames[i];
            checkArgument(
                    parameterMap.containsKey(fieldName),
                    fieldName + " doesn't exist in the parameters of SQL statement: " + sql);
            indexMapping[i] = parameterMap.get(fieldName).stream().mapToInt(v -> v).toArray();
        }

        return new FieldNamedPreparedStatementImpl(
                connection.prepareStatement(parsedSQL), indexMapping);
    }

    public static String parseNamedStatement(String sql, Map<String, List<Integer>> paramMap) {
        StringBuilder parsedSql = new StringBuilder();
        int fieldIndex = 1; // SQL statement parameter index starts from 1
        int length = sql.length();
        for (int i = 0; i < length; i++) {
            char c = sql.charAt(i);
            if (':' == c) {
                int j = i + 1;
                while (j < length && Character.isJavaIdentifierPart(sql.charAt(j))) {
                    j++;
                }
                String parameterName = sql.substring(i + 1, j);
                checkArgument(
                        !parameterName.isEmpty(),
                        "Named parameters in SQL statement must not be empty.");
                paramMap.computeIfAbsent(parameterName, n -> new ArrayList<>()).add(fieldIndex);
                fieldIndex++;
                i = j - 1;
                parsedSql.append('?');
            } else {
                parsedSql.append(c);
            }
        }
        return parsedSql.toString();
    }
}
