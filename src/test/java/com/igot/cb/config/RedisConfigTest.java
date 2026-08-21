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
     * The reason the settings are split: the two pools point at different Redis servers, so one may
     * run with requirepass while the other does not. They also read from different places - the
     * data pool's credentials are Spring-bound on ServerProperties, alongside its host and port,
     * while the cache pool's come from PropertiesCache. Neither may reach into the other's source.
     */
    @Test
    void testPoolsAuthenticateIndependently() {
        when(mockPropertiesCache.readProperty(Constants.REDIS_PASSWORD_REQUIRED)).thenReturn("false");
        when(mockServerProperties.isRedisDataPasswordRequired()).thenReturn(true);
        when(mockServerProperties.getRedisDataPassword()).thenReturn("data-secret");

        assertNotNull(redisConfig.jedisPool());
        assertNotNull(redisConfig.jedisDataPool());

        verify(mockServerProperties).getRedisDataPassword();
        // String literals because these keys now exist only as @Value expressions on ServerProperties.
        verify(mockPropertiesCache, never()).readProperty("redis.data.password.required");
        verify(mockPropertiesCache, never()).readProperty("redis.data.password");
    }

    /**
     * A missing password with the flag on must fail at bean creation, not on the first Redis call.
     */
    @Test
    void testPoolFailsWhenPasswordRequiredButMissing() {
        when(mockPropertiesCache.readProperty(Constants.REDIS_PASSWORD_REQUIRED)).thenReturn("true");
        when(mockPropertiesCache.readProperty(Constants.REDIS_PASSWORD)).thenReturn("  ");

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> redisConfig.jedisPool());
        // the message must point at the cache instance (:6379), not the data one
        assertTrue(ex.getMessage().contains("localhost:6379"), ex.getMessage());
    }

    /** The same guard on the data pool, now that its password is bound through ServerProperties. */
    @Test
    void testDataPoolFailsWhenPasswordRequiredButMissing() {
        when(mockServerProperties.isRedisDataPasswordRequired()).thenReturn(true);
        when(mockServerProperties.getRedisDataPassword()).thenReturn("  ");

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> redisConfig.jedisDataPool());
        // and at the data instance (:6378) - the port is what tells the two apart
        assertTrue(ex.getMessage().contains("localhost:6378"), ex.getMessage());
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
