package com.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.github.javafaker.Faker;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App {
    private static final String DB_URL = "jdbc:mysql://localhost:3306/users_db";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "";
    private static final int TOTAL_USERS = 10_000_000;
    private static final int THREAD_COUNT = 10;
    private static final int BATCH_SIZE = 10_000;

    private static HikariDataSource dataSource;

    public static void main(String[] args) {
        setupConnectionPool();

        System.out.println("Starting insertion of " + TOTAL_USERS + " new users...");
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        int batchSizePerThread = TOTAL_USERS / THREAD_COUNT;

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int start = i * batchSizePerThread;
            final int end = (i == THREAD_COUNT - 1) ? TOTAL_USERS : start + batchSizePerThread;
            executor.execute(() -> insertUsers(start, end));
        }
        executor.shutdown();
    }

    private static void setupConnectionPool() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(DB_URL);
        config.setUsername(DB_USER);
        config.setPassword(DB_PASSWORD);
        config.setMaximumPoolSize(THREAD_COUNT);
        dataSource = new HikariDataSource(config);
    }

    private static void insertUsers(int start, int end) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "INSERT INTO users (first_name, last_name, email, address) VALUES (?, ?, ?, ?)")) {

            Faker faker = new Faker();
            for (int i = start; i < end; i++) {
                statement.setString(1, faker.name().firstName());
                statement.setString(2, faker.name().lastName());
                statement.setString(3, faker.internet().emailAddress());
                statement.setString(4, faker.address().fullAddress());
                statement.addBatch();

                if (i % BATCH_SIZE == 0 && i > start) {
                    statement.executeBatch();
                    statement.clearBatch();
                    System.out.println("Thread " + Thread.currentThread().getId() + " inserted " + (i - start) + " records so far...");
                }
            }
            statement.executeBatch();
            System.out.println("Thread " + Thread.currentThread().getId() + " completed insertion of users from " + start + " to " + end);
        } catch (SQLException e) {
            System.err.println("Error inserting data: " + e.getMessage());
        }
    }
}
