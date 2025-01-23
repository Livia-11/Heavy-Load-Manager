package com.example;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import com.github.javafaker.Faker;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class InsertData {

    private static final int BATCH_SIZE = 50000;
    private static final long TOTAL_RECORDS = 10_000_000L;
    private static long RECORDS_PER_THREAD;
    private static final AtomicLong totalRecordsInserted = new AtomicLong(0);
    private static long startTime;
    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        startTime = System.currentTimeMillis();
        int userThreadCount = promptForOperationChoice();
        if (userThreadCount < 1) return;

        Properties properties = loadProperties();
        ExecutorService executorService = Executors.newFixedThreadPool(userThreadCount);
        CountDownLatch completionLatch = new CountDownLatch(userThreadCount);
        RECORDS_PER_THREAD = TOTAL_RECORDS / userThreadCount;

        handleDatabaseOperations(executorService, completionLatch, properties, userThreadCount);

        try {
            completionLatch.await(30, TimeUnit.MINUTES);
            executorService.shutdown();
            logProgress(RECORDS_PER_THREAD * userThreadCount);
        } catch ( InterruptedException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private static int promptForOperationChoice() {
        System.out.println("Choose operation:");
        System.out.println("1. Generate and insert data into database");
        System.out.print("Enter your choice (1): ");
        int choice = scanner.nextInt();
        if (choice != 1) {
            System.out.println("Invalid choice. Exiting...");
            return -1;
        }
        System.out.print("Enter number of threads for data insertion (recommended: 10 or more): ");
        int userThreadCount = scanner.nextInt();
        if (userThreadCount < 1) {
            System.out.println("Invalid thread count. Exiting...");
            return -1;
        }
        return userThreadCount;
    }

    private static Properties loadProperties() {
        Properties properties = new Properties();
        try {
            properties.load(InsertData.class.getResourceAsStream("/application.properties"));
        } catch (IOException e) {
            System.out.println("Error loading properties: " + e.getMessage());
            System.exit(1);
        }
        return properties;
    }

    private static void handleDatabaseOperations(ExecutorService executorService, CountDownLatch completionLatch,
                                                 Properties properties, int threadCount) {
        String url = properties.getProperty("DB.URL");
        String user = properties.getProperty("DB.USER");
        String password = properties.getProperty("DB.PASSWORD");

        try {
            for (int i = 0; i < threadCount; i++) {
                Connection conn = DriverManager.getConnection(url, user, password);
                conn.setAutoCommit(false);
                executorService.submit(new DataGenerator(conn, RECORDS_PER_THREAD, BATCH_SIZE, i, totalRecordsInserted, TOTAL_RECORDS, completionLatch));
            }
            System.out.printf("Started %d database threads, %d records per thread%n", threadCount, RECORDS_PER_THREAD);
        } catch (SQLException e) {
            System.out.println("Database connection error: " + e.getMessage());
        }
    }

    private static void logProgress(long count) {
        long currentTime = System.currentTimeMillis();
        double timeInSeconds = (currentTime - startTime) / 1000.0;
        double recordsPerSecond = count / timeInSeconds;
        System.out.printf("Inserted %,d records in %.2f seconds (%.2f records/sec)%n", count, timeInSeconds, recordsPerSecond);
    }
}

class DataGenerator implements Runnable {
    private final Connection connection;
    private final long recordsToGenerate;
    private final int batchSize;
    private final int threadId;
    private final AtomicLong totalRecordsInserted;
    private final long maxRecords;
    private final CountDownLatch completionLatch;

    public DataGenerator(Connection connection, long recordsToGenerate, int batchSize,
                         int threadId, AtomicLong totalRecordsInserted, long maxRecords,
                         CountDownLatch completionLatch) {
        this.connection = connection;
        this.recordsToGenerate = recordsToGenerate;
        this.batchSize = batchSize;
        this.threadId = threadId;
        this.totalRecordsInserted = totalRecordsInserted;
        this.maxRecords = maxRecords;
        this.completionLatch = completionLatch;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("DB-Thread-" + threadId);
        try {
            if (connection == null || connection.isClosed()) {
                System.out.println("Thread " + threadId + ": Invalid database connection");
                return;
            }

            createTableIfNotExists();

            String insertQuery = "INSERT INTO try_tb (first_name, last_name, email) VALUES (?, ?, ?)";
            Faker faker = new Faker();
            long lastCount = 0;

            try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                for (long i = 0; i < recordsToGenerate; i++) {
                    if (totalRecordsInserted.get() >= maxRecords) {
                        System.out.printf("Thread %d: Stopping - Total records limit reached%n", threadId);
                        break;
                    }

                    insertRecord(faker, preparedStatement, i);

                    if ((i + 1) % batchSize == 0) {
                        executeBatchAndCommit(preparedStatement);
                        checkAndLogProgress(i, lastCount);
                    }
                }

                executeBatchAndCommit(preparedStatement);

            }
        } catch (SQLException e) {
            System.out.println("Error in thread " + threadId + ": " + e.getMessage());
        } finally {
            completionLatch.countDown();
        }
    }

    private void createTableIfNotExists() throws SQLException {
        try (var stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS users_tb (" +
                    "id BIGINT AUTO_INCREMENT PRIMARY KEY," +
                    "first_name VARCHAR(100)," +
                    "last_name VARCHAR(100)," +
                    "email VARCHAR(150))");
            connection.commit();
        }
    }

    private void insertRecord(Faker faker, PreparedStatement preparedStatement, long i) throws SQLException {
        preparedStatement.setString(1, faker.name().firstName());
        preparedStatement.setString(2, faker.name().lastName());
        preparedStatement.setString(3, faker.internet().emailAddress());
        preparedStatement.addBatch();
    }

    private void executeBatchAndCommit(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.executeBatch();
        connection.commit();
        totalRecordsInserted.addAndGet(batchSize);
    }

    private void checkAndLogProgress(long i, long lastCount) throws SQLException {
        try (var stmt = connection.createStatement();
             var rs = stmt.executeQuery("SELECT COUNT(*) FROM users_tb")) {
            if (rs.next()) {
                long currentCount = rs.getLong(1);
                long newRecords = currentCount - lastCount;
                System.out.printf("Thread %d: Processed %d records (Verified: %d new records)%n", threadId, i + 1, newRecords);
                lastCount = currentCount;
            }
        }
    }


}
