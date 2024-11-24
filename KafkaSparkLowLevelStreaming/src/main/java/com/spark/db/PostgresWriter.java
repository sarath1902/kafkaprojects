package com.spark.db;

import com.spark.users.User;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PostgresWriter {
    private final Connection connection;

    public PostgresWriter(String jdbcUrl, String user, String password) throws Exception {
        this.connection = DriverManager.getConnection(jdbcUrl, user, password);
    }

    public void writeData(User user) throws Exception {
        String sql = "INSERT INTO customer.flattened_user_data (id, name, age, created_at, street, city, state, email, phone) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, age = EXCLUDED.age, " +
                "created_at = EXCLUDED.created_at, street = EXCLUDED.street, city = EXCLUDED.city, " +
                "state = EXCLUDED.state, email = EXCLUDED.email, phone = EXCLUDED.phone";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, user.getId());
            statement.setString(2, user.getName());
            statement.setInt(3, user.getAge());
            statement.setTimestamp(4, java.sql.Timestamp.valueOf(user.getCreated_at().replace("T", " ").replace("Z", "")));
            statement.setString(5, user.getStreet());
            statement.setString(6, user.getCity());
            statement.setString(7, user.getState());
            statement.setString(8, user.getEmail());
            statement.setString(9, user.getPhone());

            statement.executeUpdate();
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
