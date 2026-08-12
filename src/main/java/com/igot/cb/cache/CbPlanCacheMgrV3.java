package com.igot.cb.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.util.Constants;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Cache Manager for CB Plan V3 with year-scoped caching.
 * Implements Caffeine + Cassandra two-tier caching pattern.
 *
 * @version 3.0
 */
@Component
@Slf4j
public class CbPlanCacheMgrV3 {
    @Value("${cb.plan.v3.cache.ttl.minutes:60}")
    private int ttlMinutes;
    @Value("${cb.plan.v3.batch.size:5}")
    private int planBatchSize;
    @Value("${cb.plan.v3.caffine.cache.max.size:5000}")
    private int maxCacheSize;
    private final CassandraOperation cassandraOperation;
    private Cache<String, List<Map<String, Object>>> cbPlanCache;
    private Cache<String, Map<String, Object>> planIdCache;

    public CbPlanCacheMgrV3(CassandraOperation cassandraOperation) {
        this.cassandraOperation = cassandraOperation;
    }

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
        log.info("Initialized CbPlanCacheMgrV3 with TTL: {} minutes, max size: {}", ttlMinutes, maxCacheSize);
    }

    /**
     * Gets CB Plans visible to all organizations for a specific plan year.
     *
     * @param planYear the plan year (e.g., "2026-27")
     * @return list of active CB Plans visible to all orgs, or empty list if none
     */
    public List<Map<String, Object>> getCbPlanForAllOrgs(String planYear) {
        String cacheKey = "all-lookup:" + planYear;
        List<Map<String, Object>> allCbPlanList = cbPlanCache.getIfPresent(cacheKey);
        if (Objects.isNull(allCbPlanList)) {
            log.debug("getCbPlanForAllOrgs: Caffeine cache miss - planYear={}", planYear);
            Map<String, Object> propertiesMap = Map.of(Constants.PLAN_YEAR, planYear);
            allCbPlanList = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_CB_PLAN_V3_LOOKUP_BY_ALL_ORG,
                    propertiesMap,
                    List.of(),
                    null);
            if (Objects.isNull(allCbPlanList)) {
                log.warn("getCbPlanForAllOrgs: Cassandra returned null - planYear={}", planYear);
                allCbPlanList = new ArrayList<>();
            }
            allCbPlanList = allCbPlanList.stream()
                    .filter(plan -> Boolean.TRUE.equals(plan.get(Constants.IS_ACTIVE)))
                    .toList();
            cbPlanCache.put(cacheKey, allCbPlanList);
            log.info("getCbPlanForAllOrgs: Loaded from Cassandra - planYear={}, activeCount={}",
                    planYear, allCbPlanList.size());
        } else {
            log.debug("getCbPlanForAllOrgs: Caffeine cache hit - planYear={}, count={}",
                    planYear, allCbPlanList.size());
        }
        return allCbPlanList;
    }

    /**
     * Gets CB Plans for a specific organization and plan year.
     *
     * @param orgId    the organization ID
     * @param planYear the plan year (e.g., "2026-27")
     * @return list of active CB Plans for this org, or empty list if none
     */
    public List<Map<String, Object>> getCbPlanForOrgId(String orgId, String planYear) {
        String cacheKey = orgId + "-lookup:" + planYear;
        List<Map<String, Object>> cbPlanList = cbPlanCache.getIfPresent(cacheKey);
        if (Objects.isNull(cbPlanList)) {
            log.debug("getCbPlanForOrgId: Caffeine cache miss - orgId={}, planYear={}", orgId, planYear);
            Map<String, Object> propertiesMap = Map.of(
                    Constants.ORG_ID, orgId,
                    Constants.PLAN_YEAR, planYear);
            cbPlanList = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_CB_PLAN_V3_LOOKUP_BY_ORG,
                    propertiesMap,
                    List.of(),
                    null);
            if (Objects.isNull(cbPlanList)) {
                log.warn("getCbPlanForOrgId: Cassandra returned null - orgId={}, planYear={}", orgId, planYear);
                cbPlanList = new ArrayList<>();
            }
            cbPlanList = cbPlanList.stream()
                    .filter(plan -> Boolean.TRUE.equals(plan.get(Constants.IS_ACTIVE)))
                    .toList();
            cbPlanCache.put(cacheKey, cbPlanList);
            log.info("getCbPlanForOrgId: Loaded from Cassandra - orgId={}, planYear={}, activeCount={}",
                    cbPlanList.size(), orgId, planYear);
        } else {
            log.debug("getCbPlanForOrgId: Caffeine cache hit - orgId={}, planYear={}, count={}",
                    orgId, planYear, cbPlanList.size());
        }
        return cbPlanList;
    }

    /**
     * Gets all CB Plans for a user's org and plan year (org-specific + all-org plans).
     *
     * @param orgId          the user's organization ID
     * @param planYear       the plan year
     * @param isCacheEnabled flag to indicate if result came from cache
     * @return combined list of active LIVE CB Plans
     */
    public List<Map<String, Object>> getCbPlanForAllAndOrgId(String orgId, String planYear,
                                                             AtomicBoolean isCacheEnabled) {
        String cacheKey = orgId + ":" + planYear;
        List<Map<String, Object>> activeCbPlans = cbPlanCache.getIfPresent(cacheKey);
        if (CollectionUtils.isNotEmpty(activeCbPlans)) {
            log.info("Cache hit for orgId: {}, planYear: {}, found {} active CB Plans", orgId, planYear, activeCbPlans.size());
            isCacheEnabled.set(true);
            return activeCbPlans;
        }
        List<Map<String, Object>> orgPlans = getCbPlanForOrgId(orgId, planYear);
        List<Map<String, Object>> allOrgPlans = getCbPlanForAllOrgs(planYear);
        List<Map<String, Object>> combinedList = new ArrayList<>(orgPlans);
        combinedList.addAll(allOrgPlans);
        if (combinedList.isEmpty()) {
            log.info("No CB Plans found for orgId: {}, planYear: {}", orgId, planYear);
            cbPlanCache.put(cacheKey, new ArrayList<>());
            return new ArrayList<>();
        }
        Map<String, Map<String, Object>> planMap = combinedList.stream()
                .filter(m -> Objects.nonNull(m.get(Constants.END_DATE_REQUEST)) && Objects.nonNull(m.get(Constants.PLAN_ID)))
                .collect(Collectors.toMap(
                        m -> (String) m.get(Constants.PLAN_ID),
                        m -> m,
                        (existing, replacement) -> existing
                ));
        List<Map<String, Object>> dedupedList = planMap.values().stream()
                .sorted(Comparator.comparing(
                        m -> (Instant) m.get(Constants.END_DATE_REQUEST),
                        Comparator.nullsLast(Comparator.reverseOrder())
                ))
                .toList();
        List<String> planIds = dedupedList.stream()
                .map(plan -> (String) plan.get(Constants.PLAN_ID))
                .toList();
        List<Map<String, Object>> fullPlans = getCbPlansByPlanIdsInBatch(planIds);
        activeCbPlans = fullPlans.stream()
                .filter(plan -> Constants.LIVE.equalsIgnoreCase((String) plan.get(Constants.STATUS)))
                .toList();
        log.info("Found {} CB Plans for orgId: {}, planYear: {}, active LIVE count: {}",
                fullPlans.size(), orgId, planYear, activeCbPlans.size());
        cbPlanCache.put(cacheKey, activeCbPlans);
        isCacheEnabled.set(true);
        return activeCbPlans;
    }

    /**
     * Fetches full CB Plan records by plan IDs in batches.
     *
     * @param planIds list of plan IDs to fetch
     * @return list of full CB Plan records
     */
    public List<Map<String, Object>> getCbPlansByPlanIdsInBatch(List<String> planIds) {
        if (CollectionUtils.isEmpty(planIds)) {
            log.warn("getCbPlansByPlanIdsInBatch: Empty plan ID list");
            return new ArrayList<>();
        }
        List<Map<String, Object>> allCbPlans = new ArrayList<>();
        List<String> missingPlanIds = collectCachedPlans(planIds, allCbPlans);
        if (missingPlanIds.isEmpty()) {
            log.debug("getCbPlansByPlanIdsInBatch: Full cache hit - count={}", planIds.size());
            return allCbPlans;
        }
        log.debug("getCbPlansByPlanIdsInBatch: Fetching from Cassandra - missing={}, batchSize={}",
                missingPlanIds.size(), planBatchSize);
        fetchMissingPlansInBatches(missingPlanIds, allCbPlans);
        log.info("getCbPlansByPlanIdsInBatch: Total={}, fromCache={}, fromCassandra={}",
                allCbPlans.size(), planIds.size() - missingPlanIds.size(),
                allCbPlans.size() - (planIds.size() - missingPlanIds.size()));
        return allCbPlans;
    }

    /**
     * Collects cached plans and returns list of missing plan IDs.
     *
     * @param planIds    input plan IDs
     * @param allCbPlans accumulator for cached plans
     * @return list of plan IDs not found in cache
     */
    private List<String> collectCachedPlans(List<String> planIds, List<Map<String, Object>> allCbPlans) {
        List<String> missingPlanIds = new ArrayList<>();
        for (String planId : planIds) {
            Map<String, Object> cachedPlan = planIdCache.getIfPresent(planId);
            if (Objects.nonNull(cachedPlan)) {
                allCbPlans.add(cachedPlan);
            } else {
                missingPlanIds.add(planId);
            }
        }
        return missingPlanIds;
    }

    /**
     * Fetches missing plans from Cassandra in configured batch sizes.
     *
     * @param missingPlanIds plan IDs to fetch
     * @param allCbPlans     accumulator for fetched plans
     */
    private void fetchMissingPlansInBatches(List<String> missingPlanIds, List<Map<String, Object>> allCbPlans) {
        for (int i = 0; i < missingPlanIds.size(); i += planBatchSize) {
            List<String> batch = missingPlanIds.subList(i, Math.min(i + planBatchSize, missingPlanIds.size()));
            fetchSingleBatch(batch, allCbPlans);
        }
    }

    /**
     * Fetches a single batch of plans from Cassandra and caches them.
     *
     * @param batch      batch of plan IDs
     * @param allCbPlans accumulator for fetched plans
     */
    private void fetchSingleBatch(List<String> batch, List<Map<String, Object>> allCbPlans) {
        Map<String, Object> propertiesMap = Map.of(Constants.PLAN_ID, batch);
        try {
            List<Map<String, Object>> batchResult = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_CB_PLAN_V3,
                    propertiesMap,
                    List.of(),
                    null);
            if (CollectionUtils.isNotEmpty(batchResult)) {
                allCbPlans.addAll(batchResult);
                cacheBatchResults(batchResult);
            }
        } catch (Exception e) {
            log.error("getCbPlansByPlanIdsInBatch: Failed to fetch batch - ids={}", batch, e);
        }
    }

    /**
     * Caches batch results to planIdCache.
     *
     * @param batchResult plans fetched from Cassandra
     */
    private void cacheBatchResults(List<Map<String, Object>> batchResult) {
        for (Map<String, Object> plan : batchResult) {
            String id = (String) plan.get(Constants.PLAN_ID);
            if (Objects.nonNull(id)) {
                planIdCache.put(id, plan);
            }
        }
    }
}
