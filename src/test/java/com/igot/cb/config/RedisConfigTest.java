package com.igot.cb.config;

import com.igot.cb.util.Constants;
import com.igot.cb.util.PropertiesCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import redis.clients.jedis.JedisPool;


import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RedisConfigTest {

    @Mock
    private PropertiesCache propertiesCache;

    private RedisConfig redisConfig;

    @BeforeEach
    void setUp() {
        redisConfig = new RedisConfig();
    }

    @Test
    void testJedisPoolCreation() {
        try (MockedStatic<PropertiesCache> mockedStatic = mockStatic(PropertiesCache.class)) {
            mockedStatic.when(PropertiesCache::getInstance).thenReturn(propertiesCache);
            when(propertiesCache.getProperty(Constants.REDIS_HOST)).thenReturn("localhost");
            when(propertiesCache.getProperty(Constants.REDIS_PORT)).thenReturn("6379");

            RedisConfig config = new RedisConfig();
            JedisPool jedisPool = config.jedisPool();

            assertNotNull(jedisPool);
            verify(propertiesCache).getProperty(Constants.REDIS_HOST);
            verify(propertiesCache).getProperty(Constants.REDIS_PORT);
        }
    }

    @Test
    void testConstructor() {
        RedisConfig config = new RedisConfig();
        assertNotNull(config);
        
        // Verify that propertiesCache is properly initialized
        PropertiesCache cache = (PropertiesCache) ReflectionTestUtils.getField(config, "propertiesCache");
        assertNotNull(cache);
    }

    @Test
    void testJedisPoolWithDifferentPorts() {
        try (MockedStatic<PropertiesCache> mockedStatic = mockStatic(PropertiesCache.class)) {
            mockedStatic.when(PropertiesCache::getInstance).thenReturn(propertiesCache);
            when(propertiesCache.getProperty(Constants.REDIS_HOST)).thenReturn("redis-server");
            when(propertiesCache.getProperty(Constants.REDIS_PORT)).thenReturn("6380");

            RedisConfig config = new RedisConfig();
            JedisPool jedisPool = config.jedisPool();

            assertNotNull(jedisPool);
            verify(propertiesCache).getProperty(Constants.REDIS_HOST);
            verify(propertiesCache).getProperty(Constants.REDIS_PORT);
        }
    }

    @Test
    void testBuildPoolConfigSettings() {
        try (MockedStatic<PropertiesCache> mockedStatic = mockStatic(PropertiesCache.class)) {
            mockedStatic.when(PropertiesCache::getInstance).thenReturn(propertiesCache);
            when(propertiesCache.getProperty(Constants.REDIS_HOST)).thenReturn("localhost");
            when(propertiesCache.getProperty(Constants.REDIS_PORT)).thenReturn("6379");

            RedisConfig config = new RedisConfig();
            JedisPool jedisPool = config.jedisPool();

            assertNotNull(jedisPool);
            
            // Verify that the jedis pool is created successfully
            // Note: JedisPool doesn't expose getPoolConfig() method in newer versions
            // We can only verify that the pool was created without errors
            assertFalse(jedisPool.isClosed());
        }
    }

    @Test
    void testSystemPropertySet() {
        try (MockedStatic<PropertiesCache> mockedStatic = mockStatic(PropertiesCache.class)) {
            mockedStatic.when(PropertiesCache::getInstance).thenReturn(propertiesCache);
            when(propertiesCache.getProperty(Constants.REDIS_HOST)).thenReturn("localhost");
            when(propertiesCache.getProperty(Constants.REDIS_PORT)).thenReturn("6379");

            RedisConfig config = new RedisConfig();
            config.jedisPool();

            // Verify that the system property is set
            assertEquals("false", System.getProperty("org.apache.commons.pool2.registerMbeans"));
        }
    }
}