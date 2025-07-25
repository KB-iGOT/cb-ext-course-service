package com.igot.cb.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.time.AtomicTimestampGenerator;
import com.igot.cb.cassandra.exceptions.CustomException;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PropertiesCache;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import java.util.stream.Collectors;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author Mahesh RV
 * @author Ruksana
 * <p>
 * Manages Cassandra connections and sessions.
 */
@Component
public class CassandraConnectionManagerImpl implements CassandraConnectionManager {
    private static final Logger logger = LoggerFactory.getLogger(CassandraConnectionManagerImpl.class);
    private static final Map<String, CqlSession> cassandraSessionMap = new ConcurrentHashMap<>(2);
    private static CqlSession session;

    /**
     * Method invoked after bean creation for initialization
     */
    public CassandraConnectionManagerImpl() {
        // Initialize the connection and register shutdown hook
        registerShutdownHook();
        createCassandraConnection();
    }

    /**
     * Retrieves a session for the specified keyspace.
     * If a session for the keyspace already exists, returns it; otherwise, creates a new session.
     *
     * @param keyspaceName The keyspace for which to retrieve the session.
     * @return The session object for the specified keyspace.
     */
    @Override
    public CqlSession getSession(String keyspaceName) {
        // Check if session for keyspace already exists
        CqlSession currentSession = cassandraSessionMap.get(keyspaceName);
        if (currentSession != null&& !currentSession.isClosed()) {
            return currentSession;
        } else {
            // Create new session scoped to keyspace using the USE command
            CqlSession newSession = createCassandraConnectionWithKeySpaces(keyspaceName);
            cassandraSessionMap.put(keyspaceName, newSession);
            return newSession;
        }
    }

    /**
     * Creates a Cassandra connection based on properties
     */
    private CqlSession createCassandraConnectionWithKeySpaces(String keySpaceName) {
        try {
            // Load the properties required for connection
            PropertiesCache cache = PropertiesCache.getInstance();
            String cassandraHost = cache.getProperty(Constants.CASSANDRA_CONFIG_HOST);
            if (StringUtils.isBlank(cassandraHost)) {
                throw new CustomException(
                        Constants.ERROR,
                        "Cassandra host is not configured",
                        HttpStatus.INTERNAL_SERVER_ERROR);
            }
            List<String> hosts = Arrays.asList(cassandraHost.split(","));
            List<InetSocketAddress> contactPoints = hosts.stream()
                    .map(host -> new InetSocketAddress(host.trim(), 9042)) // Assuming default port 9042
                    .collect(Collectors.toList());
            List<String> contactPointsString = hosts.stream()
                    .map(host -> host.trim() + ":9042") // Ensure proper host:port format
                    .collect(Collectors.toList());
            DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                    .withStringList(DefaultDriverOption.CONTACT_POINTS, contactPointsString)
                    .withString(DefaultDriverOption.REQUEST_CONSISTENCY, getConsistencyLevel().name())
                    .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "datacenter1")
                    .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE,
                            Integer.parseInt(cache.getProperty(Constants.CORE_CONNECTIONS_PER_HOST_FOR_LOCAL)))
                    .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE,
                            Integer.parseInt(cache.getProperty(Constants.CORE_CONNECTIONS_PER_HOST_FOR_REMOTE)))
                    .withInt(DefaultDriverOption.HEARTBEAT_INTERVAL,
                            Integer.parseInt(cache.getProperty(Constants.HEARTBEAT_INTERVAL)))
                    .withInt(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, 10000)
                    .withInt(DefaultDriverOption.REQUEST_TIMEOUT, 10000)
                    .withString(DefaultDriverOption.PROTOCOL_VERSION, ProtocolVersion.V4.toString())
                    .withClass(DefaultDriverOption.RETRY_POLICY_CLASS, com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy.class)
                    .withClass(DefaultDriverOption.TIMESTAMP_GENERATOR_CLASS, AtomicTimestampGenerator.class)
                    .build();
            CqlSession sessionWithKeyspaces;
            if (StringUtils.isNotBlank(keySpaceName)) {
                sessionWithKeyspaces = CqlSession.builder()
                        .addContactPoints(contactPoints)
                        .withLocalDatacenter("datacenter1")
                        .withKeyspace(keySpaceName)
                        .withConfigLoader(loader)
                        .build();
            } else {
                sessionWithKeyspaces = CqlSession.builder()
                        .addContactPoints(contactPoints)
                        .withLocalDatacenter("datacenter1")
                        .withConfigLoader(loader)
                        .build();
            }
            logger.info("Connected to the keyspaces: " + keySpaceName);
            // Get metadata and log cluster information
            final Metadata metadata = sessionWithKeyspaces.getMetadata();
            logger.info(String.format("Connected to cluster: %s", metadata.getClusterName()));
            // Log nodes in the cluster
            for (Node host : metadata.getNodes().values()) {
                logger.info(String.format("Datacenter: %s; Host: %s; Rack: %s", host.getDatacenter(), host.getEndPoint(), host.getRack()));
            }
            return sessionWithKeyspaces;
        } catch (Exception e) {
            logger.error("Error while creating Cassandra connection", e);
            throw new CustomException(
                    Constants.ERROR,
                    e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void createCassandraConnection() {
        try {
            session = createCassandraConnectionWithKeySpaces(null);
        } catch (Exception e) {
            logger.error("Error while creating Cassandra connection", e);
            throw new CustomException(
                    Constants.ERROR,
                    e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Retrieves consistency level from properties
     *
     * @return -consistency level from properties
     */
    private static ConsistencyLevel getConsistencyLevel() {
        String consistency = PropertiesCache.getInstance().readProperty(Constants.SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL);
        logger.info("CassandraConnectionManagerImpl:getConsistencyLevel: level = " + consistency);
        if (StringUtils.isBlank(consistency)) return null;

        try {
            return DefaultConsistencyLevel.valueOf(consistency.toUpperCase());
        } catch (IllegalArgumentException exception) {
            logger.info("CassandraConnectionManagerImpl:getConsistencyLevel: Exception occurred with error message = "
                    + exception.getMessage());
        }
        return null;
    }



    /**
     * Registers a shutdown hook to clean-up resources
     */
    public static void registerShutdownHook() {
        Runtime runtime = Runtime.getRuntime();
        runtime.addShutdownHook(new ResourceCleanUp());
        logger.info("Cassandra ShutDownHook registered.");
    }

    /**
     * Cleans up Cassandra resources during shutdown
     */
    static class ResourceCleanUp extends Thread {
        @Override
        public void run() {
            try {
                logger.info("Started resource cleanup for Cassandra.");
                for (Map.Entry<String, CqlSession> entry : cassandraSessionMap.entrySet()) {
                    entry.getValue().close();
                }
                if (session != null) {
                    session.close();
                }
                logger.info("Completed resource cleanup for Cassandra.");
            } catch (Exception ex) {
                logger.error("Error during resource cleanup", ex);
            }
        }
    }

}