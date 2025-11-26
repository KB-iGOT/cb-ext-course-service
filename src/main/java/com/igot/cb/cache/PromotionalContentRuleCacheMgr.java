package com.igot.cb.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.CachedAccessSettingRule;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;

/**
 * Cache manager for promotional content access rules.
 * Provides caching using Caffeine cache with configurable TTL.
 * Converts criteria values to BitSets for efficient rule evaluation.
 */
@Component
@Slf4j
public class PromotionalContentRuleCacheMgr {

    private final CassandraOperation cassandraOperation;
    private final CbExtServerProperties properties;
    private Cache<String, CachedAccessSettingRule> promotionalContentCache;

    /**
     * Constructs the cache manager with required dependencies.
     */
    public PromotionalContentRuleCacheMgr(CassandraOperation cassandraOperation,
                                          CbExtServerProperties properties) {
        this.cassandraOperation = cassandraOperation;
        this.properties = properties;
    }

    /**
     * Initializes the Caffeine cache with configured TTL and max size.
     * Called after dependency injection to use @Value properties.
     * Warms cache by loading rules from database if enabled.
     */
    @PostConstruct
    private void initializeCache() {
        this.promotionalContentCache = Caffeine.newBuilder()
                .maximumSize(properties.getPromotionalContentCacheMaxSize())
                .expireAfterWrite(Duration.ofMinutes(properties.getPromotionalContentCacheTtlMinutes()))
                .build();
        log.info("Promotional content cache initialized with TTL: {} minutes, Max size: {}",
                properties.getPromotionalContentCacheTtlMinutes(),
                properties.getPromotionalContentCacheMaxSize());
        if (properties.isPromotionalContentCacheWarmingEnabled()) {
            log.info("Cache warming enabled. Pre-loading promotional content rules...");
            warmCache();
        }
    }

    /**
     * Pre-loads cache with rules from database to avoid cold-start penalty.
     */
    private void warmCache() {
        try {
            long startTime = System.currentTimeMillis();
            loadAccessSettingRules();
            long duration = System.currentTimeMillis() - startTime;
            log.info("Cache warming completed in {} ms. Loaded {} rules",
                    duration, promotionalContentCache.estimatedSize());
        } catch (Exception e) {
            log.error("Cache warming failed. Cache will be loaded on first request.", e);
        }
    }

    /**
     * Retrieves all cached access rules.
     * Returns values from indexed cache (O(1) per rule lookup).
     *
     * @return collection of cached rules, empty list if none cached
     */
    public Collection<CachedAccessSettingRule> getAccessSettingRules() {
        if (promotionalContentCache.estimatedSize() == 0) {
            log.info("No promotional content rules in cache, loading from database");
            loadAccessSettingRules();
        }
        return promotionalContentCache.asMap().values();
    }

    /**
     * Loads access rules from Cassandra into indexed cache.
     * Filters out archived records at database level.
     * Implements pagination to fetch all records in batches for better performance.
     * Uses configurable batch size for each query and max query size as upper limit.
     * Optimized with Java 17 parallel streams for multi-core CPU utilization.
     */
    private void loadAccessSettingRules() {
        int batchSize = properties.getPromotionalContentCacheBatchSize();
        int maxQuerySize = properties.getPromotionalContentCacheMaxQuerySize();

        log.info("Loading access setting rules from database - Batch size: {}, Max query size: {}",
                batchSize, maxQuerySize);
        try {
            List<Map<String, Object>> allRecords = new ArrayList<>();
            int totalFetched = 0;
            int pageNumber = 1;
            boolean hasMoreRecords = true;
            while (hasMoreRecords && totalFetched < maxQuerySize) {
                log.info("Fetching page {} with batch size {}", pageNumber, batchSize);
                List<Map<String, Object>> pageRecords = cassandraOperation.getRecordsByProperties(
                        Constants.KEYSPACE_SUNBIRD_COURSE,
                        Constants.PROMOTIONAL_CONTENT_RULES,
                        null,
                        null,
                        batchSize);
                if (CollectionUtils.isEmpty(pageRecords)) {
                    log.info("No more records found on page {}", pageNumber);
                    hasMoreRecords = false;
                } else {
                    allRecords.addAll(pageRecords);
                    totalFetched += pageRecords.size();
                    log.info("Fetched {} records on page {}, total so far: {}",
                            pageRecords.size(), pageNumber, totalFetched);

                    if (pageRecords.size() < batchSize) {
                        log.info("Received fewer records than batch size. Pagination complete.");
                        hasMoreRecords = false;
                    } else {
                        pageNumber++;
                        hasMoreRecords = false;
                        log.warn("Cassandra query returned full batch size. There may be more records, " +
                                "but pagination token is not supported. Consider increasing batch size.");
                    }
                }
            }
            if (totalFetched >= maxQuerySize) {
                log.warn("Reached maximum query size limit of {}. There may be more records in the database. " +
                        "Consider increasing promotional.content.cache.max.query.size", maxQuerySize);
            }
            if (allRecords.isEmpty()) {
                log.warn("No access setting rules found in database");
                return;
            }
            log.info("Total records fetched: {}, processing with parallel streams...", allRecords.size());
            allRecords.parallelStream()
                    .map(rec -> new CachedAccessSettingRule(
                            (String) rec.get("contextId"),
                            (String) rec.get("contextIdType"),
                            (String) rec.get("contextData"),
                            false))
                    .forEach(rule -> {
                        processAndCacheRule(rule);
                        promotionalContentCache.put(rule.getCacheKey(), rule);
                    });
            long totalProcessed = promotionalContentCache.estimatedSize();
            log.info("Access setting rules loaded into cache successfully. Total rules loaded: {}",
                    totalProcessed);
        } catch (Exception e) {
            log.error("Failed to load AccessSettingRule into Cache. Exception: ", e);
        }
    }

    /**
     * Processes a rule and converts criteria values to BitSets for efficient evaluation.
     */
    private void processAndCacheRule(CachedAccessSettingRule rule) {
        try {
            Map<String, Object> contextData = rule.getContextData();
            if (MapUtils.isEmpty(contextData)) {
                log.warn("No contextData found for rule: {}", rule.getCacheKey());
                return;
            }
            processContextData(rule.getCacheKey(), contextData);
        } catch (Exception e) {
            log.error("Error processing rule {}", rule.getCacheKey(), e);
        }
    }

    /**
     * Processes context data by extracting access control and user groups.
     */
    @SuppressWarnings("unchecked")
    private void processContextData(String cacheKey, Map<String, Object> contextData) {
        Map<String, Object> accessControl = (Map<String, Object>) contextData.get(Constants.ACCESS_CONTROL_ID);
        if (accessControl == null) {
            log.warn("No accessControl found for rule: {}", cacheKey);
            return;
        }
        List<Map<String, Object>> userGroups =
                (List<Map<String, Object>>) accessControl.get(Constants.USER_GROUPS);
        if (userGroups == null || userGroups.isEmpty()) {
            log.warn("No userGroups found for rule: {}", cacheKey);
            return;
        }
        for (Map<String, Object> userGroup : userGroups) {
            processUserGroup(userGroup, cacheKey);
        }
    }

    /**
     * Processes a user group by iterating through its criteria list.
     */
    @SuppressWarnings("unchecked")
    private void processUserGroup(Map<String, Object> userGroup, String cacheKey) {
        String userGroupId = (String) userGroup.get(Constants.USER_GROUP_ID);
        String userGroupName = (String) userGroup.get(Constants.USER_GROUP_NAME);
        List<Map<String, Object>> criteriaList =
                (List<Map<String, Object>>) userGroup.get(Constants.USER_GROUP_CRITERIA_LIST);
        if (CollectionUtils.isEmpty(criteriaList)) {
            log.warn("No userGroupCriteriaList for userGroupId {} in rule {}", userGroupId, cacheKey);
            return;
        }
        for (Map<String, Object> criteria : criteriaList) {
            processCriteria(criteria, userGroupId, userGroupName, cacheKey);
        }
    }

    /**
     * Processes a single criterion by converting its values from list to BitSet.
     * BitSets enable O(1) membership checking during rule evaluation.
     */
    private void processCriteria(Map<String, Object> criteria, String userGroupId, String userGroupName, String cacheKey) {
        String criteriaKey = (String) criteria.get(Constants.CRITERIA_KEY);
        List<?> criteriaValues = (List<?>) criteria.get(Constants.CRITERIA_VALUE);
        if (StringUtils.isEmpty(criteriaKey) || CollectionUtils.isEmpty(criteriaValues)) {
            log.warn("Missing key or values in criteria for userGroupId {} in rule {}", userGroupId, cacheKey);
            return;
        }
        log.debug("Rule {} -> UserGroup {} ({}) -> CriteriaKey {} -> Values {}",
                cacheKey, userGroupName, userGroupId, criteriaKey, criteriaValues);
        List<Integer> intValues = criteriaValues.stream()
                .map(Object::toString)
                .map(val -> parseIntegerValue(val, criteriaKey, cacheKey))
                .filter(Objects::nonNull)
                .toList();
        BitSet bitSet = createBitSetForAttribute(intValues);
        criteria.put(Constants.CRITERIA_VALUE, bitSet);
    }

    /**
     * Parses a string to Integer, returns null if parsing fails.
     */
    private Integer parseIntegerValue(String val, String criteriaKey, String cacheKey) {
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            log.warn("Non-integer criteria value '{}' for key {} in rule {}", val, criteriaKey, cacheKey);
            return null;
        }
    }

    /**
     * Creates a BitSet from integer values for efficient O(1) membership checks.
     * Example: [1, 3, 5] creates BitSet with bits 1, 3, 5 set to true.
     */
    BitSet createBitSetForAttribute(Collection<Integer> attributeValues) {
        BitSet bitSet = new BitSet();
        for (Integer part : attributeValues) {
            try {
                bitSet.set(part);
            } catch (Exception ex) {
                log.error("Failed to set the bit map positing for value: {}", part, ex);
                throw ex;
            }
        }
        return bitSet;
    }
}
