package com.spark.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PostgresWriter {
    private final Connection connection;

    public PostgresWriter(String jdbcUrl, String user, String password) throws Exception {
        this.connection = DriverManager.getConnection(jdbcUrl, user, password);
    }

    public void writeData(String id, String name, int age, String createdAt,
                          String street, String city, String state, String email, String phone) throws Exception {
        String sql = "INSERT INTO customer.flattened_user_data (id, name, age, created_at, street, city, state, email, phone) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, age = EXCLUDED.age, " +
                "created_at = EXCLUDED.created_at, street = EXCLUDED.street, city = EXCLUDED.city, " +
                "state = EXCLUDED.state, email = EXCLUDED.email, phone = EXCLUDED.phone";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, id);
            pstmt.setString(2, name);
            pstmt.setInt(3, age);
            pstmt.setTimestamp(4, java.sql.Timestamp.valueOf(createdAt.replace("T", " ").replace("Z", "")));
            pstmt.setString(5, street);
            pstmt.setString(6, city);
            pstmt.setString(7, state);
            pstmt.setString(8, email);
            pstmt.setString(9, phone);

            pstmt.executeUpdate();
        }
    }

    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    public void writeRawJsonData(String rawJson) {
        String insertQuery = "INSERT INTO customer.audit_table (raw_json, created_at) VALUES (?, NOW())";
        try (PreparedStatement statement = connection.prepareStatement(insertQuery)) {
            statement.setString(1, rawJson);
            statement.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error writing to audit table: " + e.getMessage());
        }
    }
}
