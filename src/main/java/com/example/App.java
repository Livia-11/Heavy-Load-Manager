package com.example;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import com.github.javafaker.Faker;
import java.util.Scanner;

public class App {

    // Database connection details
    private static final String DB_URL = "jdbc:mysql://localhost:3306/users_db";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "";
    private static final int TOTAL_USERS = 10_000_000; // Total users to insert
    private static final int BATCH_SIZE = 500_000; // Optimized batch size for high throughput
    private static final AtomicInteger progressCounter = new AtomicInteger(0);

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Enter the number of threads to use:");
        int threadCount = Integer.parseInt(scanner.nextLine());

        System.out.println("Starting insertion of " + TOTAL_USERS + " users using " + threadCount + " threads...");

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // Submit insertion tasks
        for (int i = 0; i < threadCount; i++) {
            executor.submit(new UserInsertionTask());
        }

        executor.shutdown();

        try {
            if (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
                executor.shutdownNow();
                System.err.println("Executor did not terminate in the specified time.");
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        System.out.println("All threads have completed insertion.");
    }

    static class UserInsertionTask implements Runnable {

        @Override
        public void run() {
            try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                 PreparedStatement statement = connection.prepareStatement(
                         "INSERT INTO users (first_name, last_name, email, address) VALUES (?, ?, ?, ?)")) {
                connection.setAutoCommit(false); // Disable autocommit for performance
                DataGenerator generator = new DataGenerator();

                // Try to insert in large chunks to minimize commits
                while (true) {
                    int start = progressCounter.getAndAdd(BATCH_SIZE);
                    if (start >= TOTAL_USERS) break;

                    int end = Math.min(start + BATCH_SIZE, TOTAL_USERS);
                    List<String[]> batchData = generator.generateBatch(end - start);

                    for (String[] userData : batchData) {
                        statement.setString(1, userData[0]);
                        statement.setString(2, userData[1]);
                        statement.setString(3, userData[2]);
                        statement.setString(4, userData[3]);
                        statement.addBatch();
                    }

                    statement.executeBatch();
                    connection.commit(); // Commit after each batch
                    statement.clearBatch();

                    System.out.println("Thread " + Thread.currentThread().getId() + ": Inserted up to user " + end);
                }

            } catch (SQLException e) {
                System.err.println("Thread " + Thread.currentThread().getId() +
                        " encountered an error: " + e.getMessage());
            }
        }
    }

    static class DataGenerator {
        private final Faker faker;

        public DataGenerator() {
            this.faker = new Faker();
        }

        public List<String[]> generateBatch(int batchSize) {
            List<String[]> batch = new ArrayList<>(batchSize);
            for (int i = 0; i < batchSize; i++) {
                String[] userData = new String[4];
                userData[0] = faker.name().firstName();
                userData[1] = faker.name().lastName();
                userData[2] = faker.internet().emailAddress();
                userData[3] = faker.address().fullAddress();
                batch.add(userData);
            }
            return batch;
        }
    }
}
