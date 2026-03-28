package com.igot.cb.cache;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.util.Constants;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class CbPlanCacheMgr {

    @Value("${cb.plan.cache.ttl.minutes:60}")
    private int ttlMinutes;

    @Value("${cb.plan.batch.size:5}") //default fallback to 5 if missing
    private int planBatchSize;

    @Value("${cb.plan.caffine.cache.max.size:5000}")
    private int maxCacheSize;

    @Value("${cbplan.client.cache.ttl.seconds:3600}")
    private int clientCacheTtlSeconds;

    private final CassandraOperation cassandraOperation;
    private final RedisCacheMgr redisCacheMgr;
    private final ObjectMapper objectMapper = new ObjectMapper();
    // Store only active plan IDs in cache, not full plan objects
    private Cache<String, List<String>> cbPlanIdCache;

    public CbPlanCacheMgr(CassandraOperation cassandraOperation, RedisCacheMgr redisCacheMgr) {
        this.cassandraOperation = cassandraOperation;
        this.redisCacheMgr = redisCacheMgr;
    }

    @PostConstruct
    public void initCache() {
        this.cbPlanIdCache = Caffeine.newBuilder()
                .maximumSize(maxCacheSize)
                .expireAfterWrite(Duration.ofMinutes(ttlMinutes))
                .build();
    }

    /**
     * Fetches only active plan IDs for all orgs and caches them.
     */
    private List<String> getActivePlanIdsForAll() {
        String cacheKey = "all-lookup";
        // Use the same key for Redis and Caffeine
        List<String> planIds = null;
        String cached = redisCacheMgr.getFromCache(cacheKey);
        if (cached != null) {
            try {
                planIds = objectMapper.readValue(cached, new TypeReference<List<String>>() {});
                log.info("Cache hit for all orgs in Redis: Found {} plan IDs", planIds.size());
            } catch (Exception e) {
                log.warn("Failed to parse plan IDs from Redis for all orgs: {}", e.getMessage());
            }
        }
        if (CollectionUtils.isEmpty(planIds)) {
            planIds = null;
            log.info("No CB Plan IDs for all orgs in Redis, reading from Cassandra");
            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put(Constants.PLAN_YEAR, "ALL");
            List<Map<String, Object>> allCbPlanList = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ALL_ORG,
                    propertiesMap,
                    new ArrayList<>(),
                    null);
            if (allCbPlanList == null) {
                allCbPlanList = new ArrayList<>();
            }
            planIds = allCbPlanList.stream()
                    .filter(plan -> Boolean.TRUE.equals(plan.get(Constants.IS_ACTIVE)))
                    .map(plan -> (String) plan.get(Constants.PLAN_ID))
                    .collect(Collectors.toList());
            // Cache in Redis for all orgs
            try {
                redisCacheMgr.putInCache(cacheKey, objectMapper.writeValueAsString(planIds), clientCacheTtlSeconds);
            } catch (Exception e) {
                log.warn("Failed to cache plan IDs in Redis for all orgs: {}", e.getMessage());
            }
            // Optionally update Caffeine for legacy/local fallback
            cbPlanIdCache.put(cacheKey, planIds);
        } else {
            log.info("Cache hit for all orgs: Found {} plan IDs", planIds.size());
        }
        return planIds;
    }

    /**
     * Fetches only active plan IDs for a specific org and caches them in Redis and Caffeine.
     */
    private List<String> getActivePlanIdsForOrgId(String orgId) {
        String redisKey = "cbplan_ids_" + orgId;
        // Try Redis first
        String cached = redisCacheMgr.getFromCache(redisKey);
        if (cached != null) {
            try {
                List<String> planIds = objectMapper.readValue(cached, new TypeReference<List<String>>() {});
                // Only use Redis, do not update local Caffeine cache
                return planIds;
            } catch (Exception e) {
                log.warn("Failed to parse plan IDs from Redis for orgId {}: {}", orgId, e.getMessage());
            }
        }
        // Fallback to Cassandra
        log.info("No CB Plan IDs for orgId in Redis: {}, reading from Cassandra", orgId);
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(Constants.ORG_ID, orgId);
        // Batchwise fetch using planIds list, similar to getCbPlansByPlanIdsInBatch
        List<String> allPlanIds = new ArrayList<>();
        List<String> orgPlanIds = new ArrayList<>();
        // First, fetch all plan IDs for the org in one go (using offset/limit if needed)
        // (Assume propertiesMap is set up with orgId)
        List<Map<String, Object>> allLookupRows = new ArrayList<>();
        int fetchBatchSize = planBatchSize;
        int offset = 0;
        boolean moreRecords = true;
        while (moreRecords) {
            Map<String, Object> batchProps = new HashMap<>(propertiesMap);
            batchProps.put("offset", offset);
            batchProps.put("limit", fetchBatchSize);
            List<Map<String, Object>> cbPlanList = cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ORG,
                batchProps,
                new ArrayList<>(),
                null);
            if (cbPlanList == null || cbPlanList.isEmpty()) {
                moreRecords = false;
            } else {
                allLookupRows.addAll(cbPlanList);
                offset += fetchBatchSize;
                if (cbPlanList.size() < fetchBatchSize) {
                    moreRecords = false;
                }
            }
        }
        // Now, collect all plan IDs from the lookup rows
        orgPlanIds = allLookupRows.stream()
            .filter(plan -> Boolean.TRUE.equals(plan.get(Constants.IS_ACTIVE)))
            .map(plan -> (String) plan.get(Constants.PLAN_ID))
            .collect(Collectors.toList());
        // Add 'all' org plans as well
        allPlanIds.addAll(orgPlanIds);
        allPlanIds.addAll(getActivePlanIdsForAll());
        // Cache in Redis only
        try {
            redisCacheMgr.putInCache(redisKey, objectMapper.writeValueAsString(allPlanIds), clientCacheTtlSeconds);
        } catch (Exception e) {
            log.warn("Failed to cache plan IDs in Redis for orgId {}: {}", orgId, e.getMessage());
        }
        // Do not update Caffeine cache
        return allPlanIds;
    }

    /**
     * Public API: Returns full plan details for org, using only plan IDs from cache, fetching details in batch from Cassandra.
     */
    public List<Map<String, Object>> getCbPlanForAllAndOrgId(String orgId, AtomicBoolean isCacheEnabled) {
        // Try Redis first for org-specific plan IDs
        List<String> planIds = null;
        String redisKey = "cbplan_ids_" + orgId;
        String cached = redisCacheMgr.getFromCache(redisKey);
        if (cached != null) {
            try {
                planIds = objectMapper.readValue(cached, new TypeReference<List<String>>() {});
                log.info("Cache hit for orgId: {}, Found {} active CB Plan IDs in Redis", orgId, planIds.size());
                isCacheEnabled.set(true);
            } catch (Exception e) {
                log.warn("Failed to parse plan IDs from Redis for orgId {}: {}", orgId, e.getMessage());
            }
        }
        if (CollectionUtils.isEmpty(planIds)) {
            planIds = getActivePlanIdsForOrgId(orgId);
            if (planIds.isEmpty()) {
                log.info("No CB Plan IDs found for orgId: {}", orgId);
                return new ArrayList<>();
            }
        }
        // Fetch plan details in batch using only plan IDs (primary keys)
        List<Map<String, Object>> cbPlanList = getCbPlansByPlanIdsInBatch(planIds);
        if (cbPlanList.isEmpty()) {
            log.info("No CB Plans found for orgId: {} after batch fetch", orgId);
            return new ArrayList<>();
        }
        cbPlanList = cbPlanList.stream()
                .filter(m -> m.get(Constants.END_DATE_REQUEST) != null)
                .sorted(Comparator.comparing(
                        m -> (Instant) m.get(Constants.END_DATE_REQUEST),
                        Comparator.reverseOrder()
                ))
                .collect(Collectors.toList());
        // Only return LIVE plans
        List<Map<String, Object>> activeCbPlans = cbPlanList.stream()
                .filter(plan -> Constants.LIVE.equalsIgnoreCase((String) plan.get(Constants.STATUS)))
                .collect(Collectors.toList());
        log.info("Found {} CB Plans for orgId: {}, active count: {}", cbPlanList.size(), orgId, activeCbPlans.size());
        return activeCbPlans;
    }

    /**
     * Public API: Returns plan IDs and TTL for org, for client-side caching.
     */
    public Map<String, Object> getPlanIdsAndTtlForOrg(String orgId) {
        List<String> planIds = getActivePlanIdsForOrgId(orgId);
        Map<String, Object> result = new HashMap<>();
        result.put("planIds", planIds);
        result.put("cacheTtlSeconds", clientCacheTtlSeconds);
        return result;
    }

    public List<Map<String, Object>> getCbPlansByPlanIdsInBatch(List<String> planIds) {
        List<Map<String, Object>> allCbPlans = new ArrayList<>();

        if (CollectionUtils.isEmpty(planIds)) {
            log.warn("No plan IDs provided for batch fetch.");
            return allCbPlans;
        }

        log.info("Fetching CB Plan details for {} plan IDs in batches of 5", planIds.size());

        // Process in batches of 5

        for (int i = 0; i < planIds.size(); i += planBatchSize) {
            List<String> batch = planIds.subList(i, Math.min(i + planBatchSize, planIds.size()));

            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put(Constants.PLAN_ID, batch);

            try {
                List<Map<String, Object>> batchResult = cassandraOperation.getRecordsByProperties(
                        Constants.KEYSPACE_SUNBIRD,
                        Constants.TABLE_CB_PLAN_V2,
                        propertiesMap,
                        new ArrayList<>(),
                        null
                );

                if (CollectionUtils.isNotEmpty(batchResult)) {
                    allCbPlans.addAll(batchResult);
                    log.info("Fetched {} records for plan IDs batch: {}", batchResult.size(), batch);
                } else {
                    log.warn("No records found for plan IDs batch: {}", batch);
                }

            } catch (Exception e) {
                log.error("Error fetching CB Plans for plan IDs batch {}: {}", batch, e.getMessage(), e);
            }
        }


        log.info("Total CB Plans fetched from Cassandra: {}", allCbPlans.size());
        return allCbPlans;
    }

}
