package org.example.dynamicrules.jdbc;

import org.apache.flink.annotation.PublicEvolving;

import java.sql.*;

@PublicEvolving
public interface FieldNamedPreparedStatement extends AutoCloseable {

    static FieldNamedPreparedStatement prepareStatement(
            Connection connection, String sql, String[] fieldNames) throws SQLException {
        return FieldNamedPreparedStatementImpl.prepareStatement(connection, sql, fieldNames);
    }


    void clearParameters() throws SQLException;


    ResultSet executeQuery() throws SQLException;


    void addBatch() throws SQLException;


    int[] executeBatch() throws SQLException;

    void setString(int fieldIndex, String x) throws SQLException;

    void close() throws SQLException;
}
