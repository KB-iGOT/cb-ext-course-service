package com.igot.cb.config;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.igot.cb.common.ServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PropertiesCache;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

class RedisConfigTest {

    private RedisConfig redisConfig;
    private PropertiesCache mockPropertiesCache;
    private ServerProperties mockServerProperties;

    @BeforeEach
    void setUp() throws Exception {
        mockPropertiesCache = mock(PropertiesCache.class);
        mockServerProperties = mock(ServerProperties.class);
        redisConfig = new RedisConfig();

        // Inject mock PropertiesCache using reflection
        Field propertiesCacheField = RedisConfig.class.getDeclaredField("propertiesCache");
        propertiesCacheField.setAccessible(true);
        propertiesCacheField.set(redisConfig, mockPropertiesCache);

        // ServerProperties is @Autowired, so inject it the same way
        Field serverPropertiesField = RedisConfig.class.getDeclaredField("serverProperties");
        serverPropertiesField.setAccessible(true);
        serverPropertiesField.set(redisConfig, mockServerProperties);
    }

    @Test
    void testConstructor() {
        RedisConfig config = new RedisConfig();
        assertNotNull(config);
    }

    @Test
    void testJedisPoolCreation() {
        when(mockPropertiesCache.getProperty(Constants.REDIS_HOST)).thenReturn("localhost");
        when(mockPropertiesCache.getProperty(Constants.REDIS_PORT)).thenReturn("6379");

        JedisPool jedisPool = redisConfig.jedisPool();

        assertNotNull(jedisPool);
        assertEquals("false", System.getProperty("org.apache.commons.pool2.registerMbeans"));

        verify(mockPropertiesCache).getProperty(Constants.REDIS_HOST);
        verify(mockPropertiesCache).getProperty(Constants.REDIS_PORT);
    }

    @Test
    void testBuildPoolConfig() throws Exception {
        Method buildPoolConfigMethod = RedisConfig.class.getDeclaredMethod("buildPoolConfig");
        buildPoolConfigMethod.setAccessible(true);

        JedisPoolConfig poolConfig = (JedisPoolConfig) buildPoolConfigMethod.invoke(redisConfig);

        assertNotNull(poolConfig);
        assertEquals(128, poolConfig.getMaxIdle());
        assertEquals(3000, poolConfig.getMaxTotal());
        assertEquals(100, poolConfig.getMinIdle());
        assertTrue(poolConfig.getTestOnBorrow());
        assertTrue(poolConfig.getTestOnReturn());
        assertTrue(poolConfig.getTestWhileIdle());
        assertEquals(3, poolConfig.getNumTestsPerEvictionRun());
        assertTrue(poolConfig.getBlockWhenExhausted());
    }
}