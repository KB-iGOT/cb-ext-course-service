package com.igot.cb.config;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;

import com.igot.cb.util.CbExtServerProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.JedisPoolConfig;

@ExtendWith(MockitoExtension.class)
class CbPlanRedisConfigTest {

    @Mock
    private CbExtServerProperties serverProperties;

    private CbPlanRedisConfig config;

    @BeforeEach
    void setUp() {
        config = new CbPlanRedisConfig(serverProperties);
    }

    @Test
    void buildPoolConfig_returnsExpectedSettings() throws Exception {
        Method method = CbPlanRedisConfig.class.getDeclaredMethod("buildPoolConfig");
        method.setAccessible(true);

        JedisPoolConfig poolConfig = (JedisPoolConfig) method.invoke(config);

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
