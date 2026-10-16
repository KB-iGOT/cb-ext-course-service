package com.igot.cb.config;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.test.util.ReflectionTestUtils;

import com.igot.cb.cache.CbExtRedisCacheMgr;
import com.igot.cb.common.ServerProperties;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PropertiesCache;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

class RedisConfigTest {

    private static final String LOCALHOST = "localhost";
    private static final String PORT_6379 = "6379";
    private static final String DATA_HOST = "redis-data";
    private static final String DB_INDEX_FIELD = "dbIndex";
    private static final String JEDIS_POOL_FIELD = "jedisPool";
    private static final int CB_PLAN_DB_INDEX = 1;
    private static final int USER_PROFILE_DB_INDEX = 2;
    private static final int EXT_CONTENT_DB_INDEX = 3;

    private RedisConfig redisConfig;
    private PropertiesCache mockPropertiesCache;
    private ServerProperties serverProperties;
    private CbExtServerProperties cbExtServerProperties;

    @BeforeEach
    void setUp() throws Exception {
        mockPropertiesCache = mock(PropertiesCache.class);
        serverProperties = mock(ServerProperties.class);
        cbExtServerProperties = mock(CbExtServerProperties.class);

        when(cbExtServerProperties.getCbPlanRedisHost()).thenReturn(LOCALHOST);
        when(cbExtServerProperties.getCbPlanRedisPort()).thenReturn(6379);
        when(cbExtServerProperties.getCbPlanRedisDbIndex()).thenReturn(CB_PLAN_DB_INDEX);
        when(cbExtServerProperties.getUserProfileRedisHost()).thenReturn(LOCALHOST);
        when(cbExtServerProperties.getUserProfileRedisPort()).thenReturn(6379);
        when(cbExtServerProperties.getUserProfileRedisDbIndex()).thenReturn(USER_PROFILE_DB_INDEX);
        when(cbExtServerProperties.getExtContentRedisHost()).thenReturn(LOCALHOST);
        when(cbExtServerProperties.getExtContentRedisPort()).thenReturn(6379);
        when(cbExtServerProperties.getExtContentRedisDbIndex()).thenReturn(EXT_CONTENT_DB_INDEX);

        redisConfig = new RedisConfig(serverProperties, cbExtServerProperties);

        Field propertiesCacheField = RedisConfig.class.getDeclaredField("propertiesCache");
        propertiesCacheField.setAccessible(true);
        propertiesCacheField.set(redisConfig, mockPropertiesCache);
    }

    @Test
    void testConstructor() {
        assertNotNull(new RedisConfig(serverProperties, cbExtServerProperties));
    }

    @Test
    void testJedisPoolCreation() {
        when(mockPropertiesCache.getProperty(Constants.REDIS_HOST)).thenReturn(LOCALHOST);
        when(mockPropertiesCache.getProperty(Constants.REDIS_PORT)).thenReturn(PORT_6379);

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

    @Test
    void jedisDataPool_usesRedisDataEndpoint() {
        when(serverProperties.getRedisDataHost()).thenReturn(DATA_HOST);
        when(serverProperties.getRedisDataPort()).thenReturn(PORT_6379);

        assertNotNull(redisConfig.jedisDataPool());

        verify(serverProperties).getRedisDataHost();
        verify(serverProperties).getRedisDataPort();
    }

    @Test
    void jedisPoolCbPlan_usesCbPlanEndpointOnly() {
        assertNotNull(redisConfig.jedisPoolCbPlan());

        verify(cbExtServerProperties, atLeastOnce()).getCbPlanRedisHost();
        verify(cbExtServerProperties, atLeastOnce()).getCbPlanRedisPort();
        verify(cbExtServerProperties, never()).getUserProfileRedisHost();
        verify(cbExtServerProperties, never()).getExtContentRedisHost();
    }

    @Test
    void jedisPoolUserProfile_usesUserProfileEndpointOnly() {
        assertNotNull(redisConfig.jedisPoolUserProfile());

        verify(cbExtServerProperties, atLeastOnce()).getUserProfileRedisHost();
        verify(cbExtServerProperties, atLeastOnce()).getUserProfileRedisPort();
        verify(cbExtServerProperties, never()).getCbPlanRedisHost();
        verify(cbExtServerProperties, never()).getExtContentRedisHost();
    }

    @Test
    void jedisPoolExtContent_usesExtContentEndpointOnly() {
        assertNotNull(redisConfig.jedisPoolExtContent());

        verify(cbExtServerProperties, atLeastOnce()).getExtContentRedisHost();
        verify(cbExtServerProperties, atLeastOnce()).getExtContentRedisPort();
        verify(cbExtServerProperties, never()).getCbPlanRedisHost();
        verify(cbExtServerProperties, never()).getUserProfileRedisHost();
    }

    @Test
    void cbPlanRedisCacheMgr_bindsCbPlanPoolAndDbIndex() {
        CbExtRedisCacheMgr cacheMgr = redisConfig.cbPlanRedisCacheMgr();

        assertNotNull(cacheMgr);
        assertNotNull(ReflectionTestUtils.getField(cacheMgr, JEDIS_POOL_FIELD));
        assertEquals(CB_PLAN_DB_INDEX, ReflectionTestUtils.getField(cacheMgr, DB_INDEX_FIELD));
        verify(cbExtServerProperties, never()).getUserProfileRedisDbIndex();
        verify(cbExtServerProperties, never()).getExtContentRedisDbIndex();
    }

    @Test
    void userProfileRedisCacheMgr_bindsUserProfilePoolAndDbIndex() {
        CbExtRedisCacheMgr cacheMgr = redisConfig.userProfileRedisCacheMgr();

        assertNotNull(cacheMgr);
        assertNotNull(ReflectionTestUtils.getField(cacheMgr, JEDIS_POOL_FIELD));
        assertEquals(USER_PROFILE_DB_INDEX, ReflectionTestUtils.getField(cacheMgr, DB_INDEX_FIELD));
        verify(cbExtServerProperties, never()).getCbPlanRedisDbIndex();
        verify(cbExtServerProperties, never()).getExtContentRedisDbIndex();
    }

    @Test
    void extContentRedisCacheMgr_bindsExtContentPoolAndDbIndex() {
        CbExtRedisCacheMgr cacheMgr = redisConfig.extContentRedisCacheMgr();

        assertNotNull(cacheMgr);
        assertNotNull(ReflectionTestUtils.getField(cacheMgr, JEDIS_POOL_FIELD));
        assertEquals(EXT_CONTENT_DB_INDEX, ReflectionTestUtils.getField(cacheMgr, DB_INDEX_FIELD));
        verify(cbExtServerProperties, never()).getCbPlanRedisDbIndex();
        verify(cbExtServerProperties, never()).getUserProfileRedisDbIndex();
    }
}