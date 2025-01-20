package com.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.*;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExportCSV {
    private static final String DB_URL = "jdbc:mysql://localhost:3306/users_db";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "";
    private static final String CSV_FILE_PATH = "users_backup.csv";
    private static final int THREAD_COUNT = 5;
    private static final int BATCH_SIZE = 200_000;
    private static final int TOTAL_RECORDS = 10_000_000;
    private static final Object lock = new Object();

    private static HikariDataSource dataSource;

    public static void main(String[] args) {
        setupConnectionPool();
        initializeCSVFile();

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int startOffset = i * BATCH_SIZE;
            executor.execute(() -> exportToCSV(startOffset));
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

    private static void initializeCSVFile() {
        File csvFile = new File(CSV_FILE_PATH);
        if (!csvFile.exists()) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile))) {
                writer.write("id,first_name,last_name,email,address\n");
                writer.flush();
                System.out.println("Created new CSV file with headers.");
            } catch (IOException e) {
                System.err.println("Error creating CSV file: " + e.getMessage());
            }
        }
    }

    private static void exportToCSV(int offset) {
        try (Connection connection = dataSource.getConnection();
             BufferedWriter csvWriter = new BufferedWriter(new FileWriter(CSV_FILE_PATH, true))) {

            int exportedCount = 0;

            while (exportedCount < TOTAL_RECORDS) {
                synchronized (lock) {
                    if (exportedCount >= TOTAL_RECORDS) break;
                }

                try (PreparedStatement stmt = connection.prepareStatement(
                        "SELECT * FROM users LIMIT ? OFFSET ?")) {
                    stmt.setInt(1, BATCH_SIZE);
                    stmt.setInt(2, offset);
                    ResultSet resultSet = stmt.executeQuery();

                    StringBuilder dataBuffer = new StringBuilder();
                    int batchCount = 0;

                    while (resultSet.next()) {
                        dataBuffer.append(resultSet.getInt("id")).append(",")
                                .append(resultSet.getString("first_name")).append(",")
                                .append(resultSet.getString("last_name")).append(",")
                                .append(resultSet.getString("email")).append(",")
                                .append(resultSet.getString("address")).append("\n");
                        batchCount++;
                    }

                    if (batchCount == 0) {
                        System.out.println("No more users to export. Exiting...");
                        break;
                    }

                    synchronized (lock) {
                        csvWriter.write(dataBuffer.toString());
                        csvWriter.flush();
                        exportedCount += batchCount;
                        System.out.println("Thread " + Thread.currentThread().getId() +
                                " exported " + batchCount + " users, total exported: " + exportedCount);
                    }

                    offset += BATCH_SIZE;
                }
            }
        } catch (SQLException | IOException e) {
            System.err.println("Error exporting data: " + e.getMessage());
        }
    }
}
