package com.igot.cb.cache;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
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
    void testGetContentFromCache_success() {
        String key = "content:123";
        String value = "{\"id\":\"content:123\",\"title\":\"Test Content\"}";

        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.get(key)).thenReturn(value);

        String result = redisCacheMgr.getFromCache(key);

        assertEquals(value, result);
        verify(jedisPool).getResource();
        verify(jedis).get(key);
        verify(jedis).close(); // try-with-resources
    }

    @Test
    void testGetContentFromCache_throwsException() {
        String key = "invalid";

        when(jedisPool.getResource()).thenThrow(new RuntimeException("Redis unavailable"));

        String result = redisCacheMgr.getFromCache(key);

        assertNull(result);
        verify(jedisPool).getResource();
    }
}
