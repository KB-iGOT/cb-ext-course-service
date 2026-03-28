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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
    private final RedisCacheMgr redisCacheMgr;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final String REDIS_KEY = "promotional_content_rules";
    @Value("${promotional.content.rules.cache.expiry.ms}")
    private Integer promotionalContentRulesCacheExpiryMs;
    /**
     * Constructs the cache manager with required dependencies.
     */

    public PromotionalContentRuleCacheMgr(CassandraOperation cassandraOperation,
                                          CbExtServerProperties properties,
                                          RedisCacheMgr redisCacheMgr) {
        this.cassandraOperation = cassandraOperation;
        this.properties = properties;
        this.redisCacheMgr = redisCacheMgr;
    }

    /**
     * Retrieves all cached access rules from Redis. If Redis is empty, loads from Cassandra and updates Redis.
     * @return collection of cached rules, empty collection if none available
     */
    public Collection<CachedAccessSettingRule> getAccessSettingRules() {
        try {
            String redisData = redisCacheMgr.getFromCache(REDIS_KEY);
            if (StringUtils.isNotBlank(redisData)) {
                // Deserialize as Map<String, Map<String, Object>>
                Map<String, Map<String, Object>> redisMap = objectMapper.readValue(
                        redisData, new TypeReference<Map<String, Map<String, Object>>>() {});
                Map<String, CachedAccessSettingRule> result = new HashMap<>();
                for (Map.Entry<String, Map<String, Object>> entry : redisMap.entrySet()) {
                    Map<String, Object> value = entry.getValue();
                    String contextId = (String) value.getOrDefault("contextId", value.get("contextid"));
                    String contextIdType = (String) value.getOrDefault("contextIdType", value.get("contextidtype"));
                    Object contextDataObj = value.getOrDefault("contextData", value.get("contextdata"));
                    String contextDataStr;
                    if (contextDataObj instanceof String) {
                        contextDataStr = (String) contextDataObj;
                    } else {
                        contextDataStr = objectMapper.writeValueAsString(contextDataObj);
                    }
                    boolean isArchived = false;
                    if (value.containsKey("isArchived")) {
                        isArchived = Boolean.TRUE.equals(value.get("isArchived"));
                    } else if (value.containsKey("isarchived")) {
                        isArchived = Boolean.TRUE.equals(value.get("isarchived"));
                    } else if (value.containsKey("archived")) {
                        isArchived = Boolean.TRUE.equals(value.get("archived"));
                    }
                    CachedAccessSettingRule rule = new CachedAccessSettingRule(contextId, contextIdType, contextDataStr, isArchived);
                    result.put(entry.getKey(), rule);
                }
                log.info("Loaded {} promotional content rules from Redis cache", result.size());
                return result.values();
            } else {
                log.info("Redis cache is empty, loading from Cassandra");
                Map<String, CachedAccessSettingRule> loaded = loadAccessSettingRulesFromCassandraAndUpdateRedis();
                return loaded.values();
            }
        } catch (Exception e) {
            log.error("Failed to load promotional content rules from Redis. Error: {}", e.getMessage(), e);
            Map<String, CachedAccessSettingRule> loaded = loadAccessSettingRulesFromCassandraAndUpdateRedis();
            return loaded.values();
        }
    }

    /**
     * Loads access rules from Cassandra, updates Redis, and returns the loaded rules.
     */
    private Map<String, CachedAccessSettingRule> loadAccessSettingRulesFromCassandraAndUpdateRedis() {
        int batchSize = properties.getPromotionalContentCacheBatchSize();
        int maxQuerySize = properties.getPromotionalContentCacheMaxQuerySize();
        log.info("Loading access setting rules from database - Batch size: {}, Max query size: {}",
                batchSize, maxQuerySize);
        Map<String, CachedAccessSettingRule> loadedMap = new HashMap<>();
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
                return loadedMap;
            }
            log.info("Total records fetched: {}, processing with parallel streams...", allRecords.size());
            allRecords.parallelStream()
                    .filter(rec -> {
                        Boolean isArchived = (Boolean) rec.get(Constants.IS_ARCHIVED_KEY);
                        return !isArchived;
                    })
                    .map(rec -> new CachedAccessSettingRule(
                            (String) rec.get("contextId"),
                            (String) rec.get("contextIdType"),
                            (String) rec.get("contextData"),
                            false))
                    .forEach(rule -> {
                        processAndCacheRule(rule);
                        loadedMap.put(rule.getCacheKey(), rule);
                    });
            long totalProcessed = loadedMap.size();
            log.info("Promotional Content rules loaded from Cassandra. Total rules loaded: {}",
                    totalProcessed);
            // After loading from Cassandra, update Redis
            try {
                String json = objectMapper.writeValueAsString(loadedMap);
                redisCacheMgr.putInCache(REDIS_KEY, json, promotionalContentRulesCacheExpiryMs / 1000);
                log.info("Promotional content rules updated in Redis cache");
            } catch (Exception e) {
                log.warn("Failed to update promotional content rules in Redis: {}", e.getMessage());
            }
        } catch (Exception e) {
            log.error("Failed to load Promotional Content rules from Cassandra. Exception: ", e);
        }
        return loadedMap;
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
