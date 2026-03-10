package com.igot.cb.health.service;

import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HealthServiceImplTest {

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private RedisCacheMgr redisCacheService;

    @InjectMocks
    private HealthServiceImpl healthService;

    @Test
    void checkHealthStatus_AllServicesHealthy() {

        when(redisCacheService.isRedisHealthy()).thenReturn(true);
        when(cassandraOperation.getRecordsByProperties(
                anyString(), anyString(), any(), any(), anyInt()))
                .thenReturn(List.of(Map.of("key", "value")));

        ApiResponse response = healthService.checkHealthStatus();

        assertTrue((Boolean) response.get(Constants.HEALTHY));

        List<Map<String, Object>> checks =
                (List<Map<String, Object>>) response.get(Constants.CHECKS);

        assertEquals(2, checks.size());
    }

    @Test
    void checkHealthStatus_RedisUnhealthy() {

        when(redisCacheService.isRedisHealthy()).thenReturn(false);
        when(cassandraOperation.getRecordsByProperties(
                anyString(), anyString(), any(), any(), anyInt()))
                .thenReturn(List.of(Map.of("key", "value")));

        ApiResponse response = healthService.checkHealthStatus();

        assertFalse((Boolean) response.get(Constants.HEALTHY));
    }

    @Test
    void checkHealthStatus_CassandraUnhealthy() {

        when(redisCacheService.isRedisHealthy()).thenReturn(true);
        when(cassandraOperation.getRecordsByProperties(
                anyString(), anyString(), any(), any(), anyInt()))
                .thenReturn(List.of());

        ApiResponse response = healthService.checkHealthStatus();

        assertFalse((Boolean) response.get(Constants.HEALTHY));
    }
}
