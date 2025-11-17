package com.igot.cb.cache;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@ExtendWith(MockitoExtension.class)
class RedisCacheMgrTest {

    @Mock
    private JedisPool jedisPool;

    @Mock
    private Jedis jedis;

    @InjectMocks
    private RedisCacheMgr redisCacheMgr;


    @Test
    void testGetFromCache_success() {
        String key = "content:123";
        String value = "{\"id\":\"content:123\",\"title\":\"Test Content\"}";

        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.get(key)).thenReturn(value);

        String result = redisCacheMgr.getFromCache(key);

        assertEquals(value, result);
        verify(jedisPool).getResource();
        verify(jedis).get(key);
        verify(jedis).close();
    }

    @Test
    void testGetFromCache_exception() {
        String key = "invalid";

        when(jedisPool.getResource()).thenThrow(new RuntimeException("Redis unavailable"));

        String result = redisCacheMgr.getFromCache(key);

        assertNull(result);
        verify(jedisPool).getResource();
    }

    @Test
    void testSetAccessSettingRuleCache_success() {
        String redisKey = "accessRules";
        String fieldKey = "rule1";
        Map<String, Object> fieldData = Map.of("key", "value");

        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.hset(eq(redisKey), eq(fieldKey), anyString())).thenReturn(1L);
        when(jedis.expire(redisKey, 600)).thenReturn(1L);

        boolean result = redisCacheMgr.setAccessSettingRuleCache(redisKey, fieldKey, fieldData);

        assertTrue(result);
        verify(jedis).hset(eq(redisKey), eq(fieldKey), anyString());
        verify(jedis).expire(redisKey, 600);
        verify(jedis).close();
    }

    @Test
    void testSetAccessSettingRuleCache_exception() {
        String redisKey = "accessRules";
        String fieldKey = "rule1";
        Map<String, Object> fieldData = Map.of("key", "value");

        when(jedisPool.getResource()).thenThrow(new RuntimeException("Redis error"));

        boolean result = redisCacheMgr.setAccessSettingRuleCache(redisKey, fieldKey, fieldData);

        assertFalse(result);
        verify(jedisPool).getResource();
    }

    @Test
    void testGetCachedAccessRule_success() {
        String redisKey = "accessRules";
        String contextId = "course123";
        String contextIdType = "Course";
        String fieldKey = "course123|Course";
        String value = "{\"rule\":\"data\"}";

        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.hget(redisKey, fieldKey)).thenReturn(value);

        String result = redisCacheMgr.getCachedAccessRule(redisKey, contextId, contextIdType);

        assertEquals(value, result);
        verify(jedis).hget(redisKey, fieldKey);
        verify(jedis).close();
    }

    @Test
    void testGetCachedAccessRule_exception() {
        String redisKey = "accessRules";
        String contextId = "course123";
        String contextIdType = "Course";

        when(jedisPool.getResource()).thenThrow(new RuntimeException("Redis error"));

        String result = redisCacheMgr.getCachedAccessRule(redisKey, contextId, contextIdType);

        assertNull(result);
        verify(jedisPool).getResource();
    }

    @Test
    void testGetAllCachedAccessRules_success() {
        String redisKey = "accessRules";
        Map<String, String> expectedMap = Map.of(
            "rule1", "{\"data1\":\"value1\"}",
            "rule2", "{\"data2\":\"value2\"}"
        );

        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.hgetAll(redisKey)).thenReturn(expectedMap);

        Map<String, String> result = redisCacheMgr.getAllCachedAccessRules(redisKey);

        assertEquals(expectedMap, result);
        verify(jedis).hgetAll(redisKey);
        verify(jedis).close();
    }

    @Test
    void testGetAllCachedAccessRules_exception() {
        String redisKey = "accessRules";

        when(jedisPool.getResource()).thenThrow(new RuntimeException("Redis error"));

        Map<String, String> result = redisCacheMgr.getAllCachedAccessRules(redisKey);

        assertTrue(result.isEmpty());
        verify(jedisPool).getResource();
    }

    @Test
    void testPutInCache_success() {
        ReflectionTestUtils.setField(redisCacheMgr, "ttlSeconds", 600);
        String key = "testKey";
        String value = "testValue";
        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.setex(key, 600, value)).thenReturn("OK");
        assertDoesNotThrow(() -> redisCacheMgr.putInCache(key, value));
        verify(jedis).setex(key, 600, value);
        verify(jedis).close();
    }



    @Test
    void testPutInCache_exception() {
        String key = "testKey";
        String value = "testValue";

        when(jedisPool.getResource()).thenThrow(new RuntimeException("Redis error"));

        assertDoesNotThrow(() -> redisCacheMgr.putInCache(key, value));
        
        verify(jedisPool).getResource();
    }
}
