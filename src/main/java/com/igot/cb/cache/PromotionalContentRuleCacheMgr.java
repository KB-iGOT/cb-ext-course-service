package com.igot.cb.cache;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.CachedAccessSettingRule;
import com.igot.cb.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Cache manager for promotional content access rules.
 * Provides multi-tier caching: in-memory (1 hour TTL) → Redis → Cassandra.
 * Converts criteria values to BitSets for efficient rule evaluation.
 */
@Component
@Slf4j
public class PromotionalContentRuleCacheMgr {
    private static final long LOCAL_CACHE_TTL = 3600000L;
    private static final String PROMOTIONAL_CONTENT_CACHE_KEY = "promotionalContents";

    private final RedisCacheMgr redisCacheMgr;
    private final CassandraOperation cassandraOperation;
    private Map<String, CachedAccessSettingRule> cachedAccessSettingRules = new ConcurrentHashMap<>();

    /**
     * Constructs the cache manager with required dependencies.
     */
    public PromotionalContentRuleCacheMgr(RedisCacheMgr redisCacheMgr, CassandraOperation cassandraOperation) {
        this.redisCacheMgr = redisCacheMgr;
        this.cassandraOperation = cassandraOperation;
    }

    /**
     * Retrieves all cached access rules.
     * Checks TTL expiration and reloads from Redis/Cassandra if needed.
     *
     * @return collection of cached rules, empty list if none cached
     */
    public Collection<CachedAccessSettingRule> getAccessSettingRules() {
        boolean isCacheLoadRequired = false;
        if (MapUtils.isNotEmpty(cachedAccessSettingRules)) {
            // Check the cached value's ttl. If expired load again
            for (CachedAccessSettingRule rule : cachedAccessSettingRules.values()) {
                if (rule.isExpired(LOCAL_CACHE_TTL)) {
                    cachedAccessSettingRules = null; // Invalidate cache
                    isCacheLoadRequired = true;
                    break;
                }
            }
        } else {
            isCacheLoadRequired = true;
        }

        if (isCacheLoadRequired) {
            loadAccessSettingRules();
        }

        if (MapUtils.isEmpty(cachedAccessSettingRules)) {
            return List.of(); // Return empty list if no rules are cached
        }
        return cachedAccessSettingRules.values();
    }

    /**
     * Loads access rules from Redis or Cassandra into local cache.
     * Tries Redis first, falls back to Cassandra if empty.
     */
    private void loadAccessSettingRules() {
        log.info("Loading access setting rules from cache or database");
        try {
            Map<String, String> cachedRules = redisCacheMgr.getAllCachedAccessRules(PROMOTIONAL_CONTENT_CACHE_KEY);
            if (MapUtils.isNotEmpty(cachedRules)) {
                cachedAccessSettingRules = cachedRules.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> new CachedAccessSettingRule(entry.getValue())));
            } else {
                List<Map<String, Object>> accessSettingRuleMapList = cassandraOperation.getRecordsByProperties(
                        Constants.KEYSPACE_SUNBIRD_COURSE, Constants.PROMOTIONAL_CONTENT_RULES, null,
                        null, null);
                cachedAccessSettingRules = accessSettingRuleMapList.stream()
                        .map(rec -> new CachedAccessSettingRule(
                                (String) rec.get("contextId"),
                                (String) rec.get("contextIdType"),
                                (String) rec.get("contextData"),
                                false))
                        .collect(Collectors.toMap(
                                CachedAccessSettingRule::getCacheKey,
                                rule -> rule));
                for (CachedAccessSettingRule rule : cachedAccessSettingRules.values()) {
                    processAndCacheRule(rule);
                }
            }
            log.info("Access setting rules loaded into cache successfully. Number of rules loaded: {}",
                    cachedAccessSettingRules.size());
        } catch (Exception e) {
            log.error("Failed to load AccessSettingRule into Cache. Exception: ", e);
        }
    }

    /**
     * Processes a rule and caches it to both local memory and Redis.
     * Converts criteria values to BitSets for efficient evaluation.
     */
    private void processAndCacheRule(CachedAccessSettingRule rule) {
        try {
            Map<String, Object> contextData = rule.getContextData();
            if (contextData == null) {
                log.warn("No contextData found for rule: {}", rule.getCacheKey());
                return;
            }
            processContextData(rule.getCacheKey(), contextData);
            cachedAccessSettingRules.put(rule.getCacheKey(), rule);
            redisCacheMgr.setAccessSettingRuleCache(PROMOTIONAL_CONTENT_CACHE_KEY, rule.getCacheKey(),
                    contextData);
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
        if (criteriaList == null || criteriaList.isEmpty()) {
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
        if (criteriaKey == null || criteriaValues == null) {
            log.warn("Missing key or values in criteria for userGroupId {} in rule {}", userGroupId, cacheKey);
            return;
        }
        log.info("Rule {} -> UserGroup {} ({}) -> CriteriaKey {} -> Values {}",
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
