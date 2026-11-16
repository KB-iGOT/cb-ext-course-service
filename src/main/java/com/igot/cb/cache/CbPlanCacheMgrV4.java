package com.igot.cb.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Cache Manager for CB Plan V4 with year-scoped caching.
 * All table and keyspace names are driven by {@link CbExtServerProperties}
 * so that V4 tables can be configured independently of V3.
 * <p>
 * Every cached list carries the publish-version stamp ({@link CbPlanCacheVersions}) it was
 * loaded under. Callers that hold the live stamp pass it in; an entry whose stamp no longer
 * matches is reloaded from Cassandra, so a publish on any pod is picked up by every pod on its
 * next read instead of after the TTL. Callers that pass no stamp get plain TTL behaviour.
 * Loads go through {@code Cache.get(key, loader)} so concurrent misses for one key perform a
 * single Cassandra read.
 *
 * @version 4.0
 */
@Component
@Slf4j
public class CbPlanCacheMgrV4 {
    private static final String LOOKUP_SUFFIX = "-lookup:";
    private static final String ALL_LOOKUP_PREFIX = "all-lookup:";
    private static final String MINISTRY_PREFIX = "ministry:";
    @Value("${cb.plan.v4.cache.ttl.minutes:60}")
    private int ttlMinutes;
    @Value("${cb.plan.v4.batch.size:5}")
    private int planBatchSize;
    @Value("${cb.plan.v4.caffine.cache.max.size:5000}")
    private int maxCacheSize;
    private final CassandraOperation cassandraOperation;
    private final CbExtServerProperties serverProperties;
    private Cache<String, VersionedList> cbPlanCache;
    private Cache<String, Map<String, Object>> planIdCache;

    /**
     * A cached plan list together with the version stamp it was loaded under
     * ({@code null} when loaded by a caller that supplied no stamp).
     */
    private record VersionedList(String stamp, List<Map<String, Object>> plans) {
    }

    public CbPlanCacheMgrV4(CassandraOperation cassandraOperation, CbExtServerProperties serverProperties) {
        this.cassandraOperation = cassandraOperation;
        this.serverProperties = serverProperties;
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
        log.info("Initialized CbPlanCacheMgrV4 with TTL: {} minutes, max size: {}", ttlMinutes, maxCacheSize);
    }

    /**
     * Gets CB Plans visible to all organizations for a specific plan year.
     *
     * @param planYear the plan year (e.g., "2026-27")
     * @return list of active CB Plans visible to all orgs, or empty list if none
     */
    public List<Map<String, Object>> getCbPlanForAllOrgs(String planYear) {
        return getCbPlanForAllOrgs(planYear, null);
    }

    /**
     * Version-checked variant of {@link #getCbPlanForAllOrgs(String)}.
     *
     * @param planYear the plan year
     * @param stamp    live ALL-scope version; {@code null} skips the check
     * @return list of active CB Plans visible to all orgs, or empty list if none
     */
    public List<Map<String, Object>> getCbPlanForAllOrgs(String planYear, String stamp) {
        String cacheKey = ALL_LOOKUP_PREFIX + planYear;
        return getOrLoad(cacheKey, stamp, () -> {
            log.debug("getCbPlanForAllOrgs: Caffeine cache miss - planYear={}", planYear);
            Map<String, Object> propertiesMap = Map.of(Constants.PLAN_YEAR, planYear);
            List<Map<String, Object>> allCbPlanList = cassandraOperation.getRecordsByProperties(
                    serverProperties.getCbPlanV4Keyspace(),
                    serverProperties.getCbPlanV4LookupByAllOrgTable(),
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
            log.info("getCbPlanForAllOrgs: Loaded from Cassandra - planYear={}, activeCount={}",
                    planYear, allCbPlanList.size());
            return allCbPlanList;
        }, false).plans();
    }

    /**
     * Gets CB Plans for a specific organization and plan year.
     *
     * @param orgId    the organization ID
     * @param planYear the plan year (e.g., "2026-27")
     * @return list of active CB Plans for this org, or empty list if none
     */
    public List<Map<String, Object>> getCbPlanForOrgId(String orgId, String planYear) {
        return getCbPlanForOrgId(orgId, planYear, null);
    }

    /**
     * Version-checked variant of {@link #getCbPlanForOrgId(String, String)}.
     *
     * @param orgId    the organization ID
     * @param planYear the plan year
     * @param stamp    live org version; {@code null} skips the check
     * @return list of active CB Plans for this org, or empty list if none
     */
    public List<Map<String, Object>> getCbPlanForOrgId(String orgId, String planYear, String stamp) {
        String cacheKey = orgId + LOOKUP_SUFFIX + planYear;
        return getOrLoad(cacheKey, stamp, () -> {
            log.debug("getCbPlanForOrgId: Caffeine cache miss - orgId={}, planYear={}", orgId, planYear);
            Map<String, Object> propertiesMap = Map.of(
                    Constants.ORG_ID, orgId,
                    Constants.PLAN_YEAR, planYear);
            List<Map<String, Object>> cbPlanList = cassandraOperation.getRecordsByProperties(
                    serverProperties.getCbPlanV4Keyspace(),
                    serverProperties.getCbPlanV4LookupByOrgTable(),
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
            log.info("getCbPlanForOrgId: Loaded from Cassandra - orgId={}, planYear={}, activeCount={}",
                    orgId, planYear, cbPlanList.size());
            return cbPlanList;
        }, false).plans();
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
        return getCbPlanForAllAndOrgId(orgId, planYear, isCacheEnabled, null);
    }

    /**
     * Version-checked variant of {@link #getCbPlanForAllAndOrgId(String, String, AtomicBoolean)}.
     * The combined list depends on both the org and the ALL-scope counters; when either moved,
     * the stale combined list is dropped along with the {@code planIdCache} rows of the plans it
     * held, so republished plan content is re-read rather than served from the row cache.
     *
     * @param orgId          the user's organization ID
     * @param planYear       the plan year
     * @param isCacheEnabled flag to indicate if result came from cache
     * @param versions       live version snapshot; {@code null} skips the check
     * @return combined list of active LIVE CB Plans
     */
    public List<Map<String, Object>> getCbPlanForAllAndOrgId(String orgId, String planYear,
                                                             AtomicBoolean isCacheEnabled,
                                                             CbPlanCacheVersions versions) {
        String combinedStamp = Objects.isNull(versions) ? null : versions.orgAllStamp();
        String orgStamp = Objects.isNull(versions) ? null : String.valueOf(versions.orgVersion());
        String allStamp = Objects.isNull(versions) ? null : String.valueOf(versions.allVersion());
        String cacheKey = orgId + ":" + planYear;
        VersionedList cached = cbPlanCache.getIfPresent(cacheKey);
        if (Objects.nonNull(cached) && CollectionUtils.isNotEmpty(cached.plans())) {
            if (stampMatches(cached.stamp(), combinedStamp)) {
                log.info("Cache hit for orgId: {}, planYear: {}, found {} active CB Plans", orgId, planYear,
                        cached.plans().size());
                isCacheEnabled.set(true);
                return cached.plans();
            }
            evictStale(cacheKey, cached, combinedStamp, true);
        }
        List<Map<String, Object>> orgPlans = getCbPlanForOrgId(orgId, planYear, orgStamp);
        List<Map<String, Object>> allOrgPlans = getCbPlanForAllOrgs(planYear, allStamp);
        List<Map<String, Object>> combinedList = new ArrayList<>(orgPlans);
        combinedList.addAll(allOrgPlans);
        if (combinedList.isEmpty()) {
            log.info("No CB Plans found for orgId: {}, planYear: {}", orgId, planYear);
            cbPlanCache.put(cacheKey, new VersionedList(combinedStamp, new ArrayList<>()));
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
        List<Map<String, Object>> activeCbPlans = fullPlans.stream()
                .filter(plan -> Constants.LIVE.equalsIgnoreCase((String) plan.get(Constants.STATUS)))
                .toList();
        log.info("Found {} CB Plans for orgId: {}, planYear: {}, active LIVE count: {}",
                fullPlans.size(), orgId, planYear, activeCbPlans.size());
        cbPlanCache.put(cacheKey, new VersionedList(combinedStamp, activeCbPlans));
        isCacheEnabled.set(true);
        return activeCbPlans;
    }

    /**
     * Returns the cached list for {@code cacheKey} when present and its stamp matches
     * {@code stamp}; otherwise loads it (single-flight across concurrent callers) and caches it
     * under {@code stamp}. A present-but-stale entry is evicted first.
     *
     * @param cacheKey        Caffeine key
     * @param stamp           expected version stamp; {@code null} accepts any cached entry
     * @param loader          Cassandra load for a miss
     * @param evictPlanRows   when true, a stale entry's plan IDs are also dropped from
     *                        {@code planIdCache} (for lists that hold full plan rows)
     */
    private VersionedList getOrLoad(String cacheKey, String stamp, Supplier<List<Map<String, Object>>> loader,
                                    boolean evictPlanRows) {
        VersionedList cached = cbPlanCache.getIfPresent(cacheKey);
        if (Objects.nonNull(cached)) {
            if (stampMatches(cached.stamp(), stamp)) {
                log.debug("CbPlanCacheMgrV4: Caffeine cache hit - key={}, count={}", cacheKey, cached.plans().size());
                return cached;
            }
            evictStale(cacheKey, cached, stamp, evictPlanRows);
        }
        return cbPlanCache.get(cacheKey, k -> new VersionedList(stamp, loader.get()));
    }

    private static boolean stampMatches(String cachedStamp, String expectedStamp) {
        return Objects.isNull(expectedStamp) || expectedStamp.equals(cachedStamp);
    }

    private void evictStale(String cacheKey, VersionedList stale, String liveStamp, boolean evictPlanRows) {
        log.info("CbPlanCacheMgrV4: Stale entry - key={}, cachedStamp={}, liveStamp={}",
                cacheKey, stale.stamp(), liveStamp);
        cbPlanCache.invalidate(cacheKey);
        if (evictPlanRows) {
            List<String> planIds = stale.plans().stream()
                    .map(plan -> (String) plan.get(Constants.PLAN_ID))
                    .filter(Objects::nonNull)
                    .toList();
            planIdCache.invalidateAll(planIds);
        }
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
                    serverProperties.getCbPlanV4Keyspace(),
                    serverProperties.getCbPlanV4PlanTable(),
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

    /**
     * Gets CB Plans for a specific ministry or state ID and plan year.
     *
     * @param ministryOrStateId the ministry or state ID
     * @param planYear          the plan year (e.g., "2026-27")
     * @return list of active CB Plans for this ministry/state, or empty list if none
     */
    public List<Map<String, Object>> getCbPlanForMinistryOrStateId(String ministryOrStateId, String planYear) {
        return getCbPlanForMinistryOrStateId(ministryOrStateId, planYear, null);
    }

    /**
     * Version-checked variant of {@link #getCbPlanForMinistryOrStateId(String, String)}. This list
     * holds full plan rows, so a stale entry also drops its plans from {@code planIdCache}.
     *
     * @param ministryOrStateId the ministry or state ID
     * @param planYear          the plan year
     * @param stamp             live ministry version; {@code null} skips the check
     * @return list of active LIVE CB Plans for this ministry/state, or empty list if none
     */
    public List<Map<String, Object>> getCbPlanForMinistryOrStateId(String ministryOrStateId, String planYear,
                                                                   String stamp) {
        String cacheKey = MINISTRY_PREFIX + ministryOrStateId + LOOKUP_SUFFIX + planYear;
        return getOrLoad(cacheKey, stamp, () -> {
            log.debug("getCbPlanForMinistryOrStateId: Caffeine cache miss - ministryOrStateId={}, planYear={}",
                    ministryOrStateId, planYear);
            Map<String, Object> propertiesMap = Map.of(
                    "ministryorstateid", ministryOrStateId,
                    Constants.PLAN_YEAR, planYear);
            List<Map<String, Object>> cbPlanList = cassandraOperation.getRecordsByProperties(
                    serverProperties.getCbPlanV4Keyspace(),
                    serverProperties.getCbPlanV4LookupByMinistryOrStateIdTable(),
                    propertiesMap,
                    List.of(),
                    null);
            if (Objects.isNull(cbPlanList)) {
                log.warn("getCbPlanForMinistryOrStateId: Cassandra returned null - ministryOrStateId={}, planYear={}",
                        ministryOrStateId, planYear);
                cbPlanList = new ArrayList<>();
            }
            cbPlanList = cbPlanList.stream()
                    .filter(plan -> Boolean.TRUE.equals(plan.get(Constants.IS_ACTIVE)))
                    .toList();
            List<String> planIds = cbPlanList.stream()
                    .map(plan -> (String) plan.get(Constants.PLAN_ID))
                    .filter(Objects::nonNull)
                    .toList();
            List<Map<String, Object>> fullPlans = getCbPlansByPlanIdsInBatch(planIds);
            cbPlanList = fullPlans.stream()
                    .filter(plan -> Constants.LIVE.equalsIgnoreCase((String) plan.get(Constants.STATUS)))
                    .toList();
            log.info("getCbPlanForMinistryOrStateId: Loaded from Cassandra - ministryOrStateId={}, planYear={}, activeCount={}",
                    ministryOrStateId, planYear, cbPlanList.size());
            return cbPlanList;
        }, true).plans();
    }

    /**
     * Evicts a single plan from this instance's Caffeine caches after one of its attributes changed
     * out-of-band (e.g. caLinkedId sync from Kafka): the planIdCache row, plus only those cbPlanCache
     * lists that contain a full row for the plan (the combined "orgId:planYear" lists). Lookup-table
     * lists ("*-lookup:planYear") carry no plan attributes beyond planid/enddate/isactive and are left
     * untouched, so the next dictionary build re-reads just this plan from Cassandra.
     *
     * @param planId CB Plan ID to evict
     */
    public void invalidatePlan(String planId) {
        if (Objects.isNull(planId)) {
            return;
        }
        planIdCache.invalidate(planId);
        List<String> affectedKeys = cbPlanCache.asMap().entrySet().stream()
                .filter(entry -> entry.getValue().plans().stream().anyMatch(row ->
                        planId.equals(row.get(Constants.PLAN_ID)) && row.containsKey(Constants.CA_LINKED_ID_DB)))
                .map(Map.Entry::getKey)
                .toList();
        cbPlanCache.invalidateAll(affectedKeys);
        log.info("CbPlanCacheMgrV4.invalidatePlan: planId={}, evicted {} list entries", planId, affectedKeys.size());
    }

    /**
     * Evicts the org-scoped lookup list ({@code orgId-lookup:planYear}) and the combined
     * dictionary list ({@code orgId:planYear}) for a single org, after that org's membership
     * in the org-scope lookup table changes (e.g. a SINGLE/CUSTOM-scope plan is published,
     * republished, or retired).
     *
     * @param orgId    organization ID whose lookup/combined lists are stale
     * @param planYear plan year scoping the cache key
     */
    public void invalidateOrgLookup(String orgId, String planYear) {
        if (StringUtils.isBlank(orgId) || StringUtils.isBlank(planYear)) {
            return;
        }
        cbPlanCache.invalidate(orgId + LOOKUP_SUFFIX + planYear);
        cbPlanCache.invalidate(orgId + ":" + planYear);
        log.info("CbPlanCacheMgrV4.invalidateOrgLookup: orgId={}, planYear={}", orgId, planYear);
    }

    /**
     * Evicts the all-org lookup list ({@code all-lookup:planYear}) and every combined
     * dictionary list ({@code orgId:planYear}) for the given plan year, after an ALL-scope
     * plan is published, republished, or retired. Every org's combined list must be dropped
     * since an ALL-scope plan is visible to every org.
     *
     * @param planYear plan year scoping the cache keys
     */
    public void invalidateAllOrgLookup(String planYear) {
        if (StringUtils.isBlank(planYear)) {
            return;
        }
        cbPlanCache.invalidate(ALL_LOOKUP_PREFIX + planYear);
        String combinedSuffix = ":" + planYear;
        List<String> combinedKeys = cbPlanCache.asMap().keySet().stream()
                .filter(key -> key.endsWith(combinedSuffix) && !key.contains(LOOKUP_SUFFIX))
                .toList();
        cbPlanCache.invalidateAll(combinedKeys);
        log.info("CbPlanCacheMgrV4.invalidateAllOrgLookup: planYear={}, evicted {} combined entries",
                planYear, combinedKeys.size());
    }

    /**
     * Evicts the ministry/state lookup list ({@code ministry:<id>-lookup:planYear}) after
     * that ministry/state's membership in the ministry-or-state-id lookup table changes.
     *
     * @param ministryOrStateId ministry or state ID whose lookup list is stale
     * @param planYear          plan year scoping the cache key
     */
    public void invalidateMinistryLookup(String ministryOrStateId, String planYear) {
        if (StringUtils.isBlank(ministryOrStateId) || StringUtils.isBlank(planYear)) {
            return;
        }
        cbPlanCache.invalidate(MINISTRY_PREFIX + ministryOrStateId + LOOKUP_SUFFIX + planYear);
        log.info("CbPlanCacheMgrV4.invalidateMinistryLookup: ministryOrStateId={}, planYear={}",
                ministryOrStateId, planYear);
    }
}
