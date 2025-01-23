package com.example;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.github.javafaker.Faker;

public class App {

    // Database connection details
    private static final String DB_URL = "jdbc:mysql://localhost:3306/users_db";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "";
    private static final int TOTAL_USERS = 10_000_000; // Total users to insert
    private static final int BATCH_SIZE = 1000; // Batch size for SQL insertion

    public static void main(String[] args) {
        // Number of threads can be customized by user input or default value
        int threadCount = 10; // Default thread count

        if (args.length > 0) {
            try {
                threadCount = Integer.parseInt(args[0]);
                System.out.println("Using custom thread count: " + threadCount);
            } catch (NumberFormatException e) {
                System.err.println("Invalid thread count provided. Using default: " + threadCount);
            }
        }

        System.out.println("Starting insertion of " + TOTAL_USERS + " users...");

        // Executor service to manage threads
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // Calculate the number of users each thread will insert
        int usersPerThread = TOTAL_USERS / threadCount;

        // Submit threads to execute the insertions in parallel
        for (int i = 0; i < threadCount; i++) {
            final int start = i * usersPerThread;
            final int end = (i == threadCount - 1) ? TOTAL_USERS : start + usersPerThread; // Handle last chunk
            System.out.println("Thread " + (i + 1) + " will insert users from " + start + " to " + end);
            executor.execute(() -> insertUsers(start, end));
        }

        // Shutdown the executor once all tasks are submitted
        executor.shutdown();

        // Wait for all threads to complete before printing the final message
        while (!executor.isTerminated()) {
            // Wait for all threads to finish
        }

        System.out.println("All threads have completed insertion.");
    }

    // Method to insert users in batches
    private static void insertUsers(int start, int end) {
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement statement = connection.prepareStatement(
                     "INSERT INTO users (first_name, last_name, email, address) VALUES (?, ?, ?, ?)")
        ) {
            DataGenerator generator = new DataGenerator();
            int recordCount = 0;

            // Loop to insert records in batches
            for (int i = start; i < end; i++) {
                List<String[]> batchData = generator.generateBatch(BATCH_SIZE);
                for (String[] userData : batchData) {
                    statement.setString(1, userData[0]);
                    statement.setString(2, userData[1]);
                    statement.setString(3, userData[2]);
                    statement.setString(4, userData[3]);
                    statement.addBatch();
                }

                // Execute batch when the batch size is reached
                statement.executeBatch();
                statement.clearBatch(); // Clear the batch after execution
                recordCount += batchData.size();
                System.out.println("Thread " + Thread.currentThread().getId() + " inserted " + recordCount + " records so far...");
            }

            System.out.println("Thread " + Thread.currentThread().getId() +
                    " completed insertion of users from " + start + " to " + end);
        } catch (SQLException e) {
            // Handle SQL exceptions
            System.err.println("Thread " + Thread.currentThread().getId() +
                    " encountered an error: " + e.getMessage());
        }
    }

    // DataGenerator class to generate user data
    static class DataGenerator {
        private final Faker faker;

        public DataGenerator() {
            this.faker = new Faker();
        }

        // Generate a batch of user data
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