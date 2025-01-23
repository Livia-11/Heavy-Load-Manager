package com.example;

import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.github.javafaker.Faker;
public class App {

    // Database connection details
    private static final String DB_URL = "jdbc:mysql://localhost:3306/users_db";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "";
    private static final int TOTAL_USERS = 10_000_000; // Total users to insert
    private static final int THREAD_COUNT = 10; // Number of threads to be used
    private static final int BATCH_SIZE = 1000; // Batch size for SQL insertion
    public static void main(String[] args) {


        System.out.println("Starting insertion of " + TOTAL_USERS + " users...");
        // Executor service to manage threads
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        // Calculate the number of users each thread will insert
        int usersPerThread = TOTAL_USERS / THREAD_COUNT;
        // Submit threads to execute the insertions in parallel
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int start = i * usersPerThread;
            final int end = (i == THREAD_COUNT - 1) ? TOTAL_USERS : start + usersPerThread; // Handle last chunk
            System.out.println("Thread " + (i + 1) + " will insert users from " + start + " to " + end);
            executor.execute(() -> insertUsers(start, end)); // Submit task to the executor
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
                     "INSERT INTO users (first_name, last_name, email, address) VALUES (?, ?, ?, ?)")) {
            Faker faker = new Faker();
            int recordCount = 0;
            // Loop to insert records in batches
            for (int i = start; i < end; i++) {
                statement.setString(1, faker.name().firstName());
                statement.setString(2, faker.name().lastName());
                statement.setString(3, faker.internet().emailAddress());
                statement.setString(4, faker.address().fullAddress());
                statement.addBatch(); // Add this record to the batch
                // Execute batch when the batch size is reached
                if (++recordCount % BATCH_SIZE == 0) {
                    statement.executeBatch(); // Execute the current batch
                    statement.clearBatch(); // Clear the batch after execution
                    System.out.println("Thread " + Thread.currentThread().getId() +
                            " inserted " + recordCount + " records so far...");
                }
            }
            // Execute any remaining records that didn't reach the batch size
            statement.executeBatch();
            System.out.println("Thread " + Thread.currentThread().getId() +
                    " completed insertion of users from " + start + " to " + end);
        } catch (SQLException e) {
            // Handle SQL exceptions
            System.err.println("Thread " + Thread.currentThread().getId() +
                    " encountered an error: " + e.getMessage());
        }
    }
}





















