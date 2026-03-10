package com.igot.cb.health.service;

import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class HealthServiceImpl implements HealthService {

    private final CassandraOperation cassandraOperation;

    private final RedisCacheMgr redisCacheService;

    @Override
    public ApiResponse checkHealthStatus() {
        ApiResponse response = ApiResponse.createDefaultResponse(Constants.API_HEALTH_CHECK);
        try {
            response.put(Constants.HEALTHY, true);
            List<Map<String, Object>> healthResults = new ArrayList<>();
            response.put(Constants.CHECKS, healthResults);
            cassandraHealthStatus(response);
            redisHealthStatus(response);
        } catch (Exception e) {
            log.error("Failed to process health check. Exception: ", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    public void cassandraHealthStatus(ApiResponse response) {
        Map<String, Object> result = new HashMap<>();
        result.put(Constants.NAME, Constants.CASSANDRA_DB);

        boolean isHealthy = false;
        try {
            List<Map<String, Object>> cassandraQueryResponse = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_SYSTEM_SETTINGS,
                    null, null, 1);
            isHealthy = !cassandraQueryResponse.isEmpty();
        } catch (Exception e) {
            log.error("Cassandra health check failed: {}", e.getMessage(), e);
        }

        result.put(Constants.HEALTHY, isHealthy);
        ((List<Map<String, Object>>) response.get(Constants.CHECKS)).add(result);
        if (!isHealthy) {
            response.put(Constants.HEALTHY, false);
        }
    }

    private void redisHealthStatus(ApiResponse response) {

        Map<String, Object> result = new HashMap<>();
        result.put(Constants.NAME, Constants.REDIS_CACHE);

        boolean isHealthy = redisCacheService.isRedisHealthy();

        result.put(Constants.HEALTHY, isHealthy);

        ((List<Map<String, Object>>) response.get(Constants.CHECKS)).add(result);

        if (!isHealthy) {
            response.put(Constants.HEALTHY, false);
        }
    }

}

