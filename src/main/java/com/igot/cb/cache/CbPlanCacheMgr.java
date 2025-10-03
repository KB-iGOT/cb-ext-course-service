package com.igot.cb.cache;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.expression.spel.ast.BooleanLiteral;
import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class CbPlanCacheMgr {

    @Value("${cb.plan.cache.ttl.minutes:60}")
    private int ttlMinutes;

    private final CassandraOperation cassandraOperation;
    private Cache<String, List<Map<String, Object>>> cbPlanCache;

    ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private RedisCacheMgr redisCacheMgr;

    public CbPlanCacheMgr(CassandraOperation cassandraOperation) {
        this.cassandraOperation = cassandraOperation;
        this.cbPlanCache = Caffeine.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(Duration.ofMinutes(ttlMinutes))
                .build();
    }

    private List<Map<String, Object>> getCbPlanForAll() {
        List<Map<String, Object>> allCbPlanList = cbPlanCache.getIfPresent("all-lookup");
        if (allCbPlanList == null) {
            log.info("No CB Plans for all orgs in Cache, reading from Cassandra");
            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put(Constants.PLAN_YEAR, "ALL");
            allCbPlanList = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ALL_ORG,
                    propertiesMap,
                    new ArrayList<>(),
                    null);
            if (allCbPlanList == null) {
                allCbPlanList = new ArrayList<>();
            }
            allCbPlanList = allCbPlanList.stream()
                    .filter(plan -> Boolean.TRUE.equals(plan.get(Constants.IS_ACTIVE))).collect(Collectors.toList());
            cbPlanCache.put("all", allCbPlanList);
        } else {
            log.info("Cache hit for all orgs: Found {} records", allCbPlanList.size());
        }
        return allCbPlanList;
    }

    private List<Map<String, Object>> getCbPlanForOrgId(String orgId) {
        List<Map<String, Object>> cbPlanList = cbPlanCache.getIfPresent(orgId + "-lookup");
        
        if (cbPlanList == null) {
            log.info("No CB Plans for orgId in Cache: {}, reading from Cassandra", orgId);
            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put(Constants.ORG_ID, orgId);
            cbPlanList = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ORG,
                    propertiesMap,
                    new ArrayList<>(),
                    null);
            if (cbPlanList == null) {
                cbPlanList = new ArrayList<>();
            }
            cbPlanList = cbPlanList.stream()
                    .filter(plan -> Boolean.TRUE.equals(plan.get(Constants.IS_ACTIVE))).collect(Collectors.toList());
            cbPlanCache.put(orgId + "-lookup", cbPlanList);
            cbPlanList.addAll(getCbPlanForAll());
        } else {
            log.info("Cache hit for orgId: {}, Found {} records", orgId, cbPlanList.size());
        }

        return cbPlanList;
    }

    public List<Map<String, Object>> getCbPlanForAllAndOrgId(String orgId, String userId, AtomicBoolean isCacheEnabled) {
        // Initialize activeCbPlans at the top
        List<Map<String, Object>> activeCbPlans = new ArrayList<>();

        // Initialize planIds as null
        List<String> planIds = null;

        // Try fetching plan IDs from Redis first
        String redisPlanIdsKey = Constants.CB_PLAN_REDIS_KEY_PREFIX + userId + Constants.BY_PLANS_SUFFIX;
        String redisValue = redisCacheMgr.getFromCache(redisPlanIdsKey);
        if (redisValue != null) {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                planIds = objectMapper.readValue(redisValue, new TypeReference<List<String>>() {});
                log.info("Found plan IDs in Redis for orgId {}: {}", orgId, planIds);
            } catch (Exception e) {
                log.error("Error reading plan IDs from Redis for orgId {}: {}", orgId, e.getMessage(), e);
                planIds = null;
            }
        }

        // If planIds are null, follow the existing flow to fetch from cache or CB Plan source
        if (planIds == null) {
            activeCbPlans = cbPlanCache.getIfPresent(orgId);
            if (activeCbPlans != null) {
                log.info("Cache hit for orgId: {}, Found {} active CB Plans", orgId, activeCbPlans.size());
                return activeCbPlans;
            }

            List<Map<String, Object>> cbPlanList = getCbPlanForOrgId(orgId);
            if (cbPlanList.isEmpty()) {
                log.info("No CB Plans found for orgId: {}", orgId);
                cbPlanCache.put(orgId, cbPlanList);
                return cbPlanList;
            }

            cbPlanList = cbPlanList.stream()
                    .sorted(Comparator.comparing(m -> (Instant) m.get(Constants.END_DATE_REQUEST),
                            Comparator.reverseOrder()))
                    .collect(Collectors.toList());

            planIds = cbPlanList.stream()
                    .map(plan -> String.valueOf(plan.get(Constants.PLAN_ID))) // safe conversion to String
                    .collect(Collectors.toList());
            isCacheEnabled.set(true);

        }

        // Fetch CB Plans from Cassandra
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(Constants.PLAN_ID, planIds);

        List<Map<String, Object>> existingCbPlans = cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_CB_PLAN_V2,
                propertiesMap,
                new ArrayList<>(),
                null);

        if (existingCbPlans == null) {
            log.error("Failed to read Cassandra for CB Plan, for PlanIds: {}", planIds);
            return new ArrayList<>();
        }

        // Filter active plans
        activeCbPlans = existingCbPlans.stream()
                .filter(plan -> {
                    Object statusObj = plan.get(Constants.STATUS);
                    return statusObj != null && Constants.LIVE.equalsIgnoreCase(String.valueOf(statusObj));
                })
                .collect(Collectors.toList());

        log.info("Found {} CB Plans for orgId: {}, active count: {}", existingCbPlans.size(), orgId, activeCbPlans.size());

        // Cache in local cache
        cbPlanCache.put(orgId, activeCbPlans);

        return activeCbPlans;
    }

}
