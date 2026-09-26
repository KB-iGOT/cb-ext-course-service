package com.igot.cb.config;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
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
    private final List<JedisPool> pools = new ArrayList<>();

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

    /**
     * buildPoolConfig sets minIdle=100 and a 30s eviction interval, so constructing a pool schedules
     * an evictor that will later try to open 100 connections to a Redis that need not exist on the
     * machine running the build. Nothing here connects at construction time, but an unclosed pool
     * outlives the test. close() cancels the evictor.
     */
    @AfterEach
    void tearDown() {
        pools.forEach(JedisPool::close);
        pools.clear();
    }

    /** Registers a pool for shutdown in tearDown. */
    private JedisPool track(JedisPool pool) {
        pools.add(pool);
        return pool;
    }

    @Test
    void testConstructor() {
        RedisConfig config = new RedisConfig();
        assertNotNull(config);
    }

    @Test
    void testJedisPoolCreation() {
        JedisPool jedisPool = track(redisConfig.jedisPool());

        assertNotNull(jedisPool);
        assertEquals("false", System.getProperty("org.apache.commons.pool2.registerMbeans"));

        verify(mockPropertiesCache).getProperty(Constants.REDIS_HOST);
        verify(mockPropertiesCache).getProperty(Constants.REDIS_PORT);
    }

    /**
     * The reason the settings are split: the two pools point at different Redis servers, so one may
     * run with ACLs while the other does not. They also read from different places - the data pool's
     * credentials are Spring-bound on ServerProperties, alongside its host and port, while the cache
     * pool's come from PropertiesCache. Neither may reach into the other's source.
     */
    @Test
    void testPoolsAuthenticateIndependently() {
        when(mockPropertiesCache.readProperty(Constants.REDIS_PASSWORD_REQUIRED)).thenReturn("false");
        when(mockServerProperties.isRedisDataPasswordRequired()).thenReturn(true);
        when(mockServerProperties.getRedisDataUsername()).thenReturn("data-user");
        when(mockServerProperties.getRedisDataPassword()).thenReturn("data-secret");

        assertNotNull(track(redisConfig.jedisPool()));
        assertNotNull(track(redisConfig.jedisDataPool()));

        verify(mockServerProperties).getRedisDataUsername();
        verify(mockServerProperties).getRedisDataPassword();
        // String literals because these keys now exist only as @Value expressions on ServerProperties.
        verify(mockPropertiesCache, never()).readProperty("redis.data.password.required");
        verify(mockPropertiesCache, never()).readProperty("redis.data.username");
        verify(mockPropertiesCache, never()).readProperty("redis.data.password");
    }

    /**
     * A missing username with the flag on must fail at bean creation, not on the first Redis call.
     * An unset property reads as "" rather than null, and Jedis sends the two-argument AUTH whenever
     * the username is non-null - so without this guard the server answers WRONGPASS on every command
     * and the cache managers swallow it as a miss.
     */
    @Test
    void testPoolFailsWhenUsernameRequiredButMissing() {
        when(mockPropertiesCache.readProperty(Constants.REDIS_PASSWORD_REQUIRED)).thenReturn("true");
        when(mockPropertiesCache.readProperty(Constants.REDIS_USERNAME)).thenReturn("");
        when(mockPropertiesCache.readProperty(Constants.REDIS_PASSWORD)).thenReturn("cache-secret");

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> redisConfig.jedisPool());
        assertTrue(ex.getMessage().contains("username"), ex.getMessage());
        // the message must point at the cache instance (:6379), not the data one
        assertTrue(ex.getMessage().contains("localhost:6379"), ex.getMessage());
    }

    /**
     * A missing password with the flag on must fail the same way.
     */
    @Test
    void testPoolFailsWhenPasswordRequiredButMissing() {
        when(mockPropertiesCache.readProperty(Constants.REDIS_PASSWORD_REQUIRED)).thenReturn("true");
        when(mockPropertiesCache.readProperty(Constants.REDIS_USERNAME)).thenReturn("cache-user");
        when(mockPropertiesCache.readProperty(Constants.REDIS_PASSWORD)).thenReturn("  ");

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> redisConfig.jedisPool());
        assertTrue(ex.getMessage().contains("password"), ex.getMessage());
        assertTrue(ex.getMessage().contains("localhost:6379"), ex.getMessage());
    }

    /**
     * An unset key reads as null rather than "" when it is absent from the file entirely.
     * Both spellings of "not configured" must be rejected.
     */
    @Test
    void testPoolFailsWhenUsernameIsNull() {
        when(mockPropertiesCache.readProperty(Constants.REDIS_PASSWORD_REQUIRED)).thenReturn("true");
        when(mockPropertiesCache.readProperty(Constants.REDIS_USERNAME)).thenReturn(null);
        when(mockPropertiesCache.readProperty(Constants.REDIS_PASSWORD)).thenReturn("cache-secret");

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> redisConfig.jedisPool());
        assertTrue(ex.getMessage().contains("username"), ex.getMessage());
    }

    /** The same two guards on the data pool, whose credentials come from ServerProperties. */
    @Test
    void testDataPoolFailsWhenUsernameRequiredButMissing() {
        when(mockServerProperties.isRedisDataPasswordRequired()).thenReturn(true);
        when(mockServerProperties.getRedisDataUsername()).thenReturn("");
        when(mockServerProperties.getRedisDataPassword()).thenReturn("data-secret");

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> redisConfig.jedisDataPool());
        assertTrue(ex.getMessage().contains("username"), ex.getMessage());
        // and at the data instance (:6378) - the port is what tells the two apart
        assertTrue(ex.getMessage().contains("localhost:6378"), ex.getMessage());
    }

    @Test
    void testDataPoolFailsWhenPasswordRequiredButMissing() {
        when(mockServerProperties.isRedisDataPasswordRequired()).thenReturn(true);
        when(mockServerProperties.getRedisDataUsername()).thenReturn("data-user");
        when(mockServerProperties.getRedisDataPassword()).thenReturn("  ");

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> redisConfig.jedisDataPool());
        assertTrue(ex.getMessage().contains("password"), ex.getMessage());
        assertTrue(ex.getMessage().contains("localhost:6378"), ex.getMessage());
    }

    /** Both pools build when a full username + password pair is supplied, each using its own. */
    @Test
    void testBothPoolsBuildWithCredentials() {
        when(mockPropertiesCache.readProperty(Constants.REDIS_PASSWORD_REQUIRED)).thenReturn("true");
        when(mockPropertiesCache.readProperty(Constants.REDIS_USERNAME)).thenReturn("cache-user");
        when(mockPropertiesCache.readProperty(Constants.REDIS_PASSWORD)).thenReturn("cache-secret");
        when(mockServerProperties.isRedisDataPasswordRequired()).thenReturn(true);
        when(mockServerProperties.getRedisDataUsername()).thenReturn("data-user");
        when(mockServerProperties.getRedisDataPassword()).thenReturn("data-secret");

        assertNotNull(track(redisConfig.jedisPool()));
        assertNotNull(track(redisConfig.jedisDataPool()));

        verify(mockPropertiesCache).readProperty(Constants.REDIS_USERNAME);
        verify(mockServerProperties).getRedisDataUsername();
    }

    /**
     * Backwards compatibility, and the reason the flags exist: with both off and no credentials
     * configured anywhere, both pools build unauthenticated rather than tripping the guard. This is
     * the default in application.properties, so it is what an untouched deployment does.
     */
    @Test
    void testPoolsBuildUnauthenticatedWhenFlagsOff() {
        assertDoesNotThrow(() -> {
            assertNotNull(track(redisConfig.jedisPool()));
            assertNotNull(track(redisConfig.jedisDataPool()));
        });
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
