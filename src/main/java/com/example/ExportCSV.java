package com.example;

import java.io.*;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExportCSV {
    private static final String DB_URL = "jdbc:mysql://localhost:3306/users_db";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "";
    private static final String CSV_FILE_PATH = "users_backup.csv";
    private static final int THREAD_COUNT = 5; // Number of threads
    private static final int BATCH_SIZE = 200_000; // Fetch this many records per thread
    private static final int TOTAL_RECORDS = 10_000_000; // Maximum records to export
    private static int recordsExported = 0; // Track exported records
    private static final Object lock = new Object(); // Synchronization lock

    public static void main(String[] args) {
        initializeCSVFile();

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int startOffset = i * BATCH_SIZE;
            executor.execute(() -> exportToCSV(startOffset));
        }

        executor.shutdown();
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
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             BufferedWriter csvWriter = new BufferedWriter(new FileWriter(CSV_FILE_PATH, true))) {

            while (true) {
                int totalUsers = getUserCount();

                synchronized (lock) {
                    if (recordsExported >= TOTAL_RECORDS) {
                        System.out.println("Export completed. Reached 10 million records.");
                        break;
                    }
                }

                if (recordsExported >= totalUsers) {
                    System.out.println("Waiting for more data... (Current: " + recordsExported + " / " + totalUsers + ")");
                    Thread.sleep(5000); // Wait 5 seconds before checking again
                    continue;
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

                        synchronized (lock) {
                            if (recordsExported >= TOTAL_RECORDS) {
                                break;
                            }
                            recordsExported++;
                        }
                    }

                    if (batchCount == 0) {
                        System.out.println("No new users available. Waiting...");
                        Thread.sleep(5000); // Wait and retry
                        continue;
                    }

                    synchronized (lock) {
                        csvWriter.write(dataBuffer.toString());
                        csvWriter.flush();
                    }

                    System.out.println("Thread " + Thread.currentThread().getId() +
                            " exported " + batchCount + " users, total: " + recordsExported);

                    offset += BATCH_SIZE;
                }
            }
        } catch (SQLException | IOException | InterruptedException e) {
            System.err.println("Error exporting data: " + e.getMessage());
        }
    }

    private static int getUserCount() {
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM users")) {
            if (resultSet.next()) {
                return resultSet.getInt(1);
            }
        } catch (SQLException e) {
            System.err.println("Error fetching user count: " + e.getMessage());
        }
        return 0;
    }
}