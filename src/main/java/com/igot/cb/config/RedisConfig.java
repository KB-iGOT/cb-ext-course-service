package com.igot.cb.config;

import org.igot.common.PropertiesCache;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.igot.cb.util.Constants;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

/**
 * Configuration class for Redis connection pool.
 * It sets up the JedisPool with specified configurations and properties.
 */
@Configuration
@EnableCaching
@Slf4j
public class RedisConfig {

    private final PropertiesCache propertiesCache;

    /**
     * Constructor for RedisConfig.
     * Initializes the PropertiesCache instance.
     */
    public RedisConfig(PropertiesCache propertiesCache) {
        this.propertiesCache = propertiesCache;
    }

    /**
     * Creates a JedisPool bean for Redis connection pooling.
     * It sets the pool configurations and connects to the Redis server using host and port from properties.
     *
     * @return JedisPool instance configured with Redis settings.
     */
    @Bean(name = "jedisPool")
    public JedisPool jedisPool() {
        System.setProperty("org.apache.commons.pool2.registerMbeans", "false");

        JedisPoolConfig poolConfig = buildPoolConfig();
        return new JedisPool(poolConfig, propertiesCache.getProperty(Constants.REDIS_HOST),
                Integer.parseInt(propertiesCache.getProperty(Constants.REDIS_PORT)));
    }

    private JedisPoolConfig buildPoolConfig() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(128);
        poolConfig.setMaxTotal(3000);
        poolConfig.setMinIdle(100);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleDuration(Duration.ofMillis(120000));
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofMillis(30000));
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);
        return poolConfig;
    }
}