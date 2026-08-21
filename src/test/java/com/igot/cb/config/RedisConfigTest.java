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

    @BeforeEach
    void setUp() throws Exception {
        mockPropertiesCache = mock(PropertiesCache.class);
        redisConfig = new RedisConfig();

        // Inject mock PropertiesCache using reflection
        Field propertiesCacheField = RedisConfig.class.getDeclaredField("propertiesCache");
        propertiesCacheField.setAccessible(true);
        propertiesCacheField.set(redisConfig, mockPropertiesCache);
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
    void testJedisPoolCreation_noPasswordConfigured() {
        when(mockPropertiesCache.getProperty(Constants.REDIS_HOST)).thenReturn("localhost");
        when(mockPropertiesCache.getProperty(Constants.REDIS_PORT)).thenReturn("6379");
        when(mockPropertiesCache.readProperty(Constants.REDIS_PASSWORD)).thenReturn(null);

        JedisPool jedisPool = redisConfig.jedisPool();

        assertNotNull(jedisPool);
        verify(mockPropertiesCache).readProperty(Constants.REDIS_PASSWORD);
    }

    @Test
    void testJedisPoolCreation_withPasswordConfigured() {
        when(mockPropertiesCache.getProperty(Constants.REDIS_HOST)).thenReturn("localhost");
        when(mockPropertiesCache.getProperty(Constants.REDIS_PORT)).thenReturn("6379");
        when(mockPropertiesCache.readProperty(Constants.REDIS_PASSWORD)).thenReturn("s3cr3t");

        JedisPool jedisPool = redisConfig.jedisPool();

        assertNotNull(jedisPool);
        verify(mockPropertiesCache).readProperty(Constants.REDIS_PASSWORD);
    }

    @Test
    void testJedisDataPoolCreation_noPasswordConfigured() throws Exception {
        ServerProperties serverProperties = new ServerProperties();
        serverProperties.setRedisDataHost("localhost");
        serverProperties.setRedisDataPort("6378");
        serverProperties.setRedisDataPassword("");
        injectServerProperties(serverProperties);
        

        JedisPool jedisDataPool = redisConfig.jedisDataPool();

        assertNotNull(jedisDataPool);
    }

    @Test
    void testJedisDataPoolCreation_withPasswordConfigured() throws Exception {
        ServerProperties serverProperties = new ServerProperties();
        serverProperties.setRedisDataHost("localhost");
        serverProperties.setRedisDataPort("6378");
        serverProperties.setRedisDataPassword("s3cr3t");
        injectServerProperties(serverProperties);

        JedisPool jedisDataPool = redisConfig.jedisDataPool();

        assertNotNull(jedisDataPool);
    }

    private void injectServerProperties(ServerProperties serverProperties) throws Exception {
        Field serverPropertiesField = RedisConfig.class.getDeclaredField("serverProperties");
        serverPropertiesField.setAccessible(true);
        serverPropertiesField.set(redisConfig, serverProperties);
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