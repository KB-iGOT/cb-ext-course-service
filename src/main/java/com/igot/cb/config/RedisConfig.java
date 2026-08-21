package com.igot.cb.config;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.igot.cb.common.ServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PropertiesCache;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Configuration class for Redis connection pool.
 * It sets up the JedisPool with specified configurations and properties.
 */
@Configuration
@EnableCaching
@Slf4j
public class RedisConfig {

    private final PropertiesCache propertiesCache;
    
    @Autowired
    private ServerProperties serverProperties;

    /**
     * Constructor for RedisConfig.
     * Initializes the PropertiesCache instance.
     */
    public RedisConfig() {
        this.propertiesCache = PropertiesCache.getInstance();
    }

    /**
     * Creates a JedisPool bean for Redis connection pooling.
     * It sets the pool configurations and connects to the Redis server using host and port from properties.
     * Authenticates with a password only when one is configured, so the same build works against
     * both password-protected and password-less Redis servers.
     *
     * @return JedisPool instance configured with Redis settings.
     */
    @Bean(name = "jedisPool")
    public JedisPool jedisPool() {
        System.setProperty("org.apache.commons.pool2.registerMbeans", "false");

        return buildJedisPool(propertiesCache.getProperty(Constants.REDIS_HOST),
                Integer.parseInt(propertiesCache.getProperty(Constants.REDIS_PORT)),
                propertiesCache.readProperty(Constants.REDIS_PASSWORD));
    }

    /**
     * Creates a JedisPool bean for Redis data connection pooling.
     * This bean connects to a separate Redis instance for data operations.
     * Authenticates with a password only when one is configured, so the same build works against
     * both password-protected and password-less Redis servers.
     *
     * @return JedisPool instance configured with Redis data settings.
     */
    @Bean(name = "jedisDataPool")
    public JedisPool jedisDataPool() {
        System.setProperty("org.apache.commons.pool2.registerMbeans", "false");

        return buildJedisPool(serverProperties.getRedisDataHost(),
                Integer.parseInt(serverProperties.getRedisDataPort()),
                serverProperties.getRedisDataPassword());
    }

    /**
     * Builds a JedisPool for the given host/port. When {@code password} is blank, the pool
     * connects without AUTH, preserving compatibility with Redis servers that have no password set.
     */
    private JedisPool buildJedisPool(String host, int port, String password) {
        JedisPoolConfig poolConfig = buildPoolConfig();
        DefaultJedisClientConfig.Builder clientConfigBuilder = DefaultJedisClientConfig.builder();
        if (StringUtils.isNotBlank(password)) {
            clientConfigBuilder.password(password);
            log.info("Connecting to Redis server at {}:{} with password authentication enabled", host, port);
        } else {
            log.info("Connecting to Redis server at {}:{} without password authentication", host, port);
        }
        return new JedisPool(poolConfig, new HostAndPort(host, port), clientConfigBuilder.build());
    }

    private JedisPoolConfig buildPoolConfig() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(128);
        poolConfig.setMaxTotal(3000);
        poolConfig.setMinIdle(100);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleTimeMillis(120000);
        poolConfig.setTimeBetweenEvictionRunsMillis(30000);
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);
        return poolConfig;
    }
}
