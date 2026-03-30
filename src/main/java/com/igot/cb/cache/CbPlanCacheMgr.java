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

    private final CassandraOperation cassandraOperation;
    private Cache<String, List<Map<String, Object>>> cbPlanCache;
    private Cache<String, Map<String, Object>> planIdCache;

    @PostConstruct
    public void initCache() {
        this.cbPlanCache = Caffeine.newBuilder()
                .maximumSize(maxCacheSize)
                .expireAfterWrite(Duration.ofMinutes(ttlMinutes))
                .build();
        this.planIdCache = Caffeine.newBuilder()
                .maximumSize(maxCacheSize)
                .expireAfterWrite(Duration.ofMinutes(ttlMinutes))
                .build();
    }
    
    public CbPlanCacheMgr(CassandraOperation cassandraOperation) {
        this.cassandraOperation = cassandraOperation;
    }

    private List<Map<String, Object>> getCbPlanForAll() {
        String redisCacheKey = "all-lookup";
        List<Map<String, Object>> allCbPlanList = cbPlanCache.getIfPresent(redisCacheKey);
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
            cbPlanCache.put(redisCacheKey, allCbPlanList);
        } else {
            log.info("Cache hit for all orgs: Found {} records", allCbPlanList.size());
        }
        return allCbPlanList;
    }

    private List<Map<String, Object>> getCbPlanForOrgId(String orgId) {
        String redisCacheKey = orgId + "-lookup";
        List<Map<String, Object>> cbPlanList = cbPlanCache.getIfPresent(redisCacheKey);
        
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
            cbPlanCache.put(redisCacheKey, cbPlanList);
            cbPlanList.addAll(getCbPlanForAll());
        } else {
            log.info("Cache hit for orgId: {}, Found {} records", orgId, cbPlanList.size());
        }

        return cbPlanList;
    }

    public List<Map<String, Object>> getCbPlanForAllAndOrgId(String orgId, AtomicBoolean isCacheEnabled) {
        List<Map<String, Object>> activeCbPlans = cbPlanCache.getIfPresent(orgId);
        if (CollectionUtils.isNotEmpty(activeCbPlans)) {
            log.info("Cache hit for orgId: {}, Found {} active CB Plans", orgId, activeCbPlans.size());
            isCacheEnabled.set(true);
            return activeCbPlans;
        }
        List<Map<String, Object>> cbPlanList = getCbPlanForOrgId(orgId);
        if (cbPlanList.isEmpty()) {
            log.info("No CB Plans found for orgId: {}", orgId);
            cbPlanList = new ArrayList<>();
            cbPlanCache.put(orgId, cbPlanList);
            return cbPlanList;
        }
        cbPlanList  = cbPlanList.stream()
                .filter(m -> m.get(Constants.END_DATE_REQUEST) != null)
                .sorted(Comparator.comparing(
                        m -> (Instant) m.get(Constants.END_DATE_REQUEST),
                        Comparator.reverseOrder()
                ))
                .collect(Collectors.toList());
        List<String> planIds = cbPlanList.stream()
                    .map(plan -> (String) plan.get(Constants.PLAN_ID)).collect(Collectors.toList());
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(Constants.PLAN_ID, planIds);
        List<Map<String, Object>> existingCbPlans = cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_CB_PLAN_V2,
                propertiesMap,
                new ArrayList<>(),
                null);
        if (existingCbPlans == null) {
            log.error("Failed to read cassandra for cb plan, for PlanIds: {}", planIds);
            return new ArrayList<>();
        }

        activeCbPlans = existingCbPlans.stream()
                .filter(plan -> Constants.LIVE.equalsIgnoreCase((String) plan.get(Constants.STATUS))).collect(Collectors.toList());
        // Prime the planIdCache
        for (Map<String, Object> plan : existingCbPlans) {
            String id = (String) plan.get(Constants.PLAN_ID);
            if (id != null) {
                planIdCache.put(id, plan);
            }
        }
        //TODO - Need to remove draftData (if available) and also contextData.accessControl
        log.info("Found {} CB Plans for orgId: {}, active count: {}", existingCbPlans.size(), orgId, activeCbPlans.size());
        cbPlanCache.put(orgId, activeCbPlans);
        isCacheEnabled.set(true);
        return activeCbPlans;
    }

    public List<Map<String, Object>> getCbPlansByPlanIdsInBatch(List<String> planIds) {
        List<Map<String, Object>> allCbPlans = new ArrayList<>();

        if (CollectionUtils.isEmpty(planIds)) {
            log.warn("No plan IDs provided for batch fetch.");
            return allCbPlans;
        }

        List<String> missingPlanIds = new ArrayList<>();
        for (String planId : planIds) {
            Map<String, Object> cachedPlan = planIdCache.getIfPresent(planId);
            if (cachedPlan != null) {
                allCbPlans.add(cachedPlan);
            } else {
                missingPlanIds.add(planId);
            }
        }

        if (missingPlanIds.isEmpty()) {
            log.info("Full cache hit for {} plan IDs", planIds.size());
            return allCbPlans;
        }

        log.info("Fetching CB Plan details for {} missing plan IDs from Cassandra in batches of {}", missingPlanIds.size(), planBatchSize);

        // Process in batches
        for (int i = 0; i < missingPlanIds.size(); i += planBatchSize) {
            List<String> batch = missingPlanIds.subList(i, Math.min(i + planBatchSize, missingPlanIds.size()));

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
                    for (Map<String, Object> plan : batchResult) {
                        String id = (String) plan.get(Constants.PLAN_ID);
                        if (id != null) {
                            planIdCache.put(id, plan);
                        }
                    }
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
