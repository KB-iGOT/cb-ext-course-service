package com.igot.cb.config;

import com.igot.cb.util.Constants;
import com.igot.cb.util.PropertiesCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
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
    }
}