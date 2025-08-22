package com.igot.cb.cassandra.exceptions;


import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;

import com.igot.cb.cassandra.CassandraPropertyReaderException;

class CassandraPropertyReaderExceptionTest {

    @Test
    void testCassandraPropertyReaderExceptionConstructor() {
        String expectedMessage = "Test error message";
        Throwable expectedCause = new IllegalArgumentException("Test cause");
        CassandraPropertyReaderException exception = new CassandraPropertyReaderException(expectedMessage, expectedCause);
        assertEquals("Exception message should match", expectedMessage, exception.getMessage());
        assertEquals("Exception cause should match", expectedCause, exception.getCause());
    }

    @Test
    void testExceptionWithNullMessage() {
        String expectedMessage = null;
        Throwable expectedCause = new RuntimeException("Some cause");
        CassandraPropertyReaderException exception = new CassandraPropertyReaderException(expectedMessage, expectedCause);
        assertEquals("Exception message should be null", expectedMessage, exception.getMessage());
        assertEquals("Exception cause should match", expectedCause, exception.getCause());
    }

    @Test
    void testExceptionWithNullCause() {
        String expectedMessage = "Error reading Cassandra properties";
        Throwable expectedCause = null;
        CassandraPropertyReaderException exception = new CassandraPropertyReaderException(expectedMessage, expectedCause);
        assertEquals("Exception message should match", expectedMessage, exception.getMessage());
        assertEquals("Exception cause should be null", expectedCause, exception.getCause());
    }
}
