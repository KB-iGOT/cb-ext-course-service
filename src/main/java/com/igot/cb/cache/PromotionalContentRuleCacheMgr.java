package com.igot.cb.cache;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.CachedAccessSettingRule;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
    Map<String, CachedAccessSettingRule> cacheMap = new ConcurrentHashMap<>();
    @Value("${promotional.content.rules.cache.expiry.ms}")
    private Integer promotionalContentRulesCacheExpiryMs;
    /**
     * Constructs the cache manager with required dependencies.
     */
    public PromotionalContentRuleCacheMgr(CassandraOperation cassandraOperation,
                                          CbExtServerProperties properties) {
        this.cassandraOperation = cassandraOperation;
        this.properties = properties;
    }

    /**
     * Retrieves all cached access rules.
     * Returns values from indexed cache (O(1) per rule lookup).
     * Automatically reloads from database when cache is empty or expired.
     *
     * @return collection of cached rules, empty collection if none available
     */
    public Collection<CachedAccessSettingRule> getAccessSettingRules() {
        Collection<CachedAccessSettingRule> cachedRules = cacheMap.values();
        if (CollectionUtils.isEmpty(cachedRules)) {
            log.info("Cache is empty (size: {}), loading from database", cacheMap.size());
            loadAccessSettingRules();
            cachedRules = cacheMap.values();
            log.info("After reload, cache contains {} rules", cachedRules.size());
            return cachedRules;
        }
        for (CachedAccessSettingRule rule : cachedRules) {
            if (rule == null) {
                log.warn("Found null cached rule entry, triggering reload");
                loadAccessSettingRules();
                return cacheMap.values();
            }
            try {
                if (rule.isExpired(promotionalContentRulesCacheExpiryMs)) {
                    log.info("Found expired rule (key={}), reloading cache", rule.getCacheKey());
                    loadAccessSettingRules();
                    return cacheMap.values();
                }
            } catch (Exception e) {
                log.warn("Error checking expiry for rule {}: {}. Triggering reload.", rule.getCacheKey(), e.getMessage());
                loadAccessSettingRules();
                return cacheMap.values();
            }
        }
        return cachedRules;
    }

    /**
     * Loads access rules from Cassandra into indexed cache.
     * Uses Cassandra driver paging to fetch rows in batches.
     * Applies configurable max query size as an upper bound for rows processed.
     * Filters out archived records during row processing.
     */
    private void loadAccessSettingRules() {
        int batchSize = properties.getPromotionalContentCacheBatchSize();
        int maxQuerySize = properties.getPromotionalContentCacheMaxQuerySize();

        log.info("Loading access setting rules from database - Batch size: {}, Max query size: {}",
                batchSize, maxQuerySize);
        cacheMap.clear();
        try {
            AtomicInteger scannedCount = new AtomicInteger();
            AtomicInteger archivedCount = new AtomicInteger();

            cassandraOperation.forEachRecordByProperties(
                    Constants.KEYSPACE_SUNBIRD_COURSE,
                    Constants.PROMOTIONAL_CONTENT_RULES,
                    null,
                    null,
                    batchSize,
                    maxQuerySize,
                    rec -> {
                        scannedCount.incrementAndGet();
                        if (Boolean.TRUE.equals(rec.get(Constants.IS_ARCHIVED_KEY))) {
                            archivedCount.incrementAndGet();
                            return;
                        }
                        CachedAccessSettingRule rule = new CachedAccessSettingRule(
                                (String) rec.get("contextId"),
                                (String) rec.get("contextIdType"),
                                (String) rec.get("contextData"),
                                false);
                        processAndCacheRule(rule);
                        cacheMap.put(rule.getCacheKey(), rule);
                    });

            if (scannedCount.get() == 0) {
                log.warn("No access setting rules found in database");
                return;
            }
            if (maxQuerySize > 0 && scannedCount.get() >= maxQuerySize) {
                log.warn("Reached maximum query size limit of {} while loading promotional rules", maxQuerySize);
            }
            long totalProcessed = cacheMap.size();
            log.info("Promotional Content rules loaded into cache successfully. Total rules loaded: {}",
                    totalProcessed);
            log.info("Total scanned: {}, archived skipped: {}", scannedCount.get(), archivedCount.get());
        } catch (Exception e) {
            log.error("Failed to load Promotional Content rules into Cache. Exception: ", e);
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
     * Parses a string to Integer, returns null if parsing fails or value is out of bounds.
     */
    private Integer parseIntegerValue(String val, String criteriaKey, String cacheKey) {
        try {
            int parsedVal = Integer.parseInt(val);
            if (parsedVal < 0 || parsedVal > Constants.MAX_BITSET_INDEX) {
                log.warn("Criteria value '{}' for key {} in rule {} is out of valid BitSet range [0, {}] - skipping to avoid heap exhaustion",
                        val, criteriaKey, cacheKey, Constants.MAX_BITSET_INDEX);
                return null;
            }
            return parsedVal;
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
        if (CollectionUtils.isEmpty(attributeValues)) {
            return bitSet;
        }
        for (Integer part : attributeValues) {
            if (part == null || part < 0 || part > Constants.MAX_BITSET_INDEX) {
                log.warn("Skipping invalid or out-of-range bit index: {}", part);
                continue;
            }
            try {
                bitSet.set(part);
            } catch (Throwable ex) {
                log.error("Failed to set the bit map position for value: {}", part, ex);
            }
        }
        return bitSet;
    }
}
