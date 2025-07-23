package com.igot.cb.config;

import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.igot.cb.util.Constants;
import com.igot.cb.util.PropertiesCache;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Configuration
@EnableCaching
@Slf4j
public class RedisConfig {

    private final PropertiesCache propertiesCache;

    public RedisConfig() {
        this.propertiesCache = PropertiesCache.getInstance();
    }

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
        poolConfig.setMinEvictableIdleTimeMillis(120000);
        poolConfig.setTimeBetweenEvictionRunsMillis(30000);
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);
        return poolConfig;
    }
}
