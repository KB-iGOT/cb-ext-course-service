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

        when(mockPropertiesCache.getProperty(Constants.REDIS_HOST)).thenReturn("localhost");
        when(mockPropertiesCache.getProperty(Constants.REDIS_PORT)).thenReturn("6379");
        when(mockServerProperties.getRedisDataHost()).thenReturn("localhost");
        when(mockServerProperties.getRedisDataPort()).thenReturn("6378");
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

    /**
     * The reason the keys are split: the two pools point at different Redis servers, so one may
     * run with requirepass while the other does not. The open pool must not read the other's password.
     */
    @Test
    void testPoolsAuthenticateIndependently() {
        when(mockPropertiesCache.readProperty(Constants.REDIS_PASSWORD_REQUIRED)).thenReturn("false");
        when(mockPropertiesCache.readProperty(Constants.REDIS_DATA_PASSWORD_REQUIRED)).thenReturn("true");
        when(mockPropertiesCache.readProperty(Constants.REDIS_DATA_PASSWORD)).thenReturn("data-secret");

        assertNotNull(redisConfig.jedisPool());
        assertNotNull(redisConfig.jedisDataPool());

        verify(mockPropertiesCache, never()).readProperty(Constants.REDIS_PASSWORD);
        verify(mockPropertiesCache).readProperty(Constants.REDIS_DATA_PASSWORD);
    }

    /**
     * A missing password with the flag on must fail at bean creation, not on the first Redis call.
     */
    @Test
    void testPoolFailsWhenPasswordRequiredButMissing() {
        when(mockPropertiesCache.readProperty(Constants.REDIS_PASSWORD_REQUIRED)).thenReturn("true");
        when(mockPropertiesCache.readProperty(Constants.REDIS_PASSWORD)).thenReturn("  ");

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> redisConfig.jedisPool());
        assertTrue(ex.getMessage().contains(Constants.REDIS_PASSWORD));
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
