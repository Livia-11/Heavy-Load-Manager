package com.example;

import com.github.javafaker.Faker;
import java.sql.*;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class InsertData {
    private static final int BATCH_SIZE = 100_000; // Optimal batch size
    private static final long TOTAL_RECORDS = 10_000_000L; // Total records to insert
    private static long RECORDS_PER_THREAD;
    private static final AtomicLong totalRecordsInserted = new AtomicLong(0);
    private static long startTime;
    private static final Scanner scanner = new Scanner(System.in);

    // Database Credentials
    private static final String DB_URL = "jdbc:mysql://localhost:3306/users_db";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "";

    public static void main(String[] args) {
        startTime = System.currentTimeMillis();
        int userThreadCount = getThreadCount();
        if (userThreadCount < 1) return;

        ExecutorService executorService = Executors.newFixedThreadPool(userThreadCount);
        CountDownLatch completionLatch = new CountDownLatch(userThreadCount);
        RECORDS_PER_THREAD = TOTAL_RECORDS / userThreadCount;

        handleDatabaseInsertion(executorService, completionLatch, userThreadCount);

        try {
            completionLatch.await(30, TimeUnit.MINUTES);
            executorService.shutdown();
            logProgress(RECORDS_PER_THREAD * userThreadCount);
        } catch (InterruptedException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private static int getThreadCount() {
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

    private static void handleDatabaseInsertion(ExecutorService executorService, CountDownLatch completionLatch, int threadCount) {
        try {
            for (int i = 0; i < threadCount; i++) {
                Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
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

    static class DataGenerator implements Runnable {
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

                String insertQuery = "INSERT INTO users_try (first_name, last_name, email) VALUES (?, ?, ?)";
                Faker faker = new Faker();
                long lastCount = 0;

                try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                    for (long i = 0; i < recordsToGenerate; i++) {
                        if (totalRecordsInserted.get() >= maxRecords) {
                            System.out.printf("Thread %d: Stopping - Total records limit reached%n", threadId);
                            break;
                        }

                        insertRecord(faker, preparedStatement);

                        if ((i + 1) % batchSize == 0) {
                            executeBatchAndCommit(preparedStatement);
                            checkAndLogProgress(lastCount);
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
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS users_try (" +
                        "id BIGINT AUTO_INCREMENT PRIMARY KEY," +
                        "first_name VARCHAR(100)," +
                        "last_name VARCHAR(100)," +
                        "email VARCHAR(150))");
                connection.commit();
            }
        }

        private void insertRecord(Faker faker, PreparedStatement preparedStatement) throws SQLException {
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

        private void checkAndLogProgress(long lastCount) throws SQLException {
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM users_try")) {
                if (rs.next()) {
                    long currentCount = rs.getLong(1);
                    long newRecords = currentCount - lastCount;
                    System.out.printf("Thread %d: Verified %d new records in database%n", threadId, newRecords);
                }
            }
        }
    }
}