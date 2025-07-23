package com.igot.cb.cassandra;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author Mahesh RV
 * @author Ruksana
 */
public class CassandraPropertyReader {

    private static final String FILE_NAME = "cassandratablecolumn.properties";
    private static final Logger logger = LogManager.getLogger(CassandraPropertyReader.class);

    private final Properties properties = new Properties();

    /**
     * Private constructor to prevent instantiation from outside.
     * Loads properties from the configuration file.
     */
    private CassandraPropertyReader() {
        loadProperties();
    }

    /**
     * Loads properties from the configuration file into the properties object.
     */
    private void loadProperties() {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream(FILE_NAME)) {
            if (in != null) {
                properties.load(in);
                logger.info("Loaded properties from {}", FILE_NAME);
            } else {
                throw new IOException("Property file '" + FILE_NAME + "' not found in the classpath");
            }
        } catch (IOException e) {
            logger.error("Error loading properties from file '{}'", FILE_NAME, e);
            throw new CassandraPropertyReaderException("Error loading properties from file '" + FILE_NAME + "'", e);
        }
    }

    /**
     * Retrieves the instance of CassandraPropertyReaderV2.
     * This method follows lazy initialization.
     *
     * @return An instance of CassandraPropertyReaderV2.
     */
    public static CassandraPropertyReader getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * Retrieves the property value for the given key.
     * If the key is not found, the key itself is returned.
     *
     * @param key The key of the property.
     * @return The value of the property, or the key itself if not found.
     */
    public String readProperty(String key) {
        return properties.getProperty(key, key); // Return key itself if property not found
    }

    /**
     * Holder class for lazy initialization of CassandraPropertyReaderV2 instance.
     * This ensures thread-safe and efficient lazy loading.
     */
    private static class Holder {
        private static final CassandraPropertyReader INSTANCE = new CassandraPropertyReader();
    }
}
