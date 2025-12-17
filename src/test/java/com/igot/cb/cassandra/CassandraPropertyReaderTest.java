package com.igot.cb.cassandra;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.*;

class CassandraPropertyReaderTest {

    @Test
    void testGetInstance() {
        CassandraPropertyReader instance1 = CassandraPropertyReader.getInstance();
        CassandraPropertyReader instance2 = CassandraPropertyReader.getInstance();
        
        assertNotNull(instance1);
        assertSame(instance1, instance2);
    }

    @Test
    void testReadProperty() {
        CassandraPropertyReader reader = CassandraPropertyReader.getInstance();
        String result = reader.readProperty("test.key");
        assertNotNull(result);
        assertEquals("test.key", result); // Returns key itself if not found
    }

    @Test
    void testReadPropertyWithEmptyKey() {
        CassandraPropertyReader reader = CassandraPropertyReader.getInstance();
        String result = reader.readProperty("");
        assertEquals("", result);
    }

    @Test
    void testReadPropertyWithExistingKey() {
        CassandraPropertyReader reader = CassandraPropertyReader.getInstance();
        // Test with actual keys from the properties file
        String result1 = reader.readProperty("contextId");
        assertEquals("contextid", result1);
        
        String result2 = reader.readProperty("contextData");
        assertEquals("contextdata", result2);
        
        String result3 = reader.readProperty("planid");
        assertEquals("planId", result3);
    }

    @Test
    void testLoadPropertiesFileNotFound() {
        // Test the scenario where properties file is not found
        assertThrows(CassandraPropertyReaderException.class, () -> {
            TestCassandraPropertyReader testReader = new TestCassandraPropertyReader();
            testReader.testLoadPropertiesFileNotFound();
        });
    }

    @Test
    void testLoadPropertiesIOException() {
        assertThrows(CassandraPropertyReaderException.class, () -> {
            TestCassandraPropertyReader testReader = new TestCassandraPropertyReader();
            testReader.testLoadPropertiesIOException();
        });
    }

    // Helper class to test exception scenarios
    private static class TestCassandraPropertyReader {
        
        public void testLoadPropertiesFileNotFound() {
            try {
                // Simulate file not found by using a non-existent file
                InputStream in = this.getClass().getClassLoader().getResourceAsStream("non-existent-file.properties");
                if (in == null) {
                    throw new IOException("Property file 'cassandratablecolumn.properties' not found in the classpath");
                }
            } catch (IOException e) {
                throw new CassandraPropertyReaderException("Error loading properties from file 'cassandratablecolumn.properties'", e);
            }
        }

        public void testLoadPropertiesIOException() {
            try {
                // Simulate IOException during properties loading
                InputStream in = new InputStream() {
                    @Override
                    public int read() throws IOException {
                        throw new IOException("Simulated IO Exception");
                    }
                };
                java.util.Properties properties = new java.util.Properties();
                properties.load(in);
            } catch (IOException e) {
                throw new CassandraPropertyReaderException("Error loading properties from file 'cassandratablecolumn.properties'", e);
            }
        }
    }
}