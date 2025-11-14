package com.igot.cb.cache;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.igot.cb.cassandra.CassandraOperation;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.springframework.stereotype.Component;

import com.igot.cb.model.CachedAccessSettingRule;
import com.igot.cb.util.Constants;

import lombok.extern.slf4j.Slf4j;

/**
 * Cache manager for access setting rules.
 * It loads access setting rules from Redis or Cassandra and caches them locally.
 */
@Component
@Slf4j
public class AccessSettingRuleCacheMgr {
    private final RedisCacheMgr redisCacheMgr;
    private final CassandraOperation cassandraOperation;
    private Map<String, CachedAccessSettingRule> cachedAccessSettingRules = new ConcurrentHashMap<>();

    private final long LOCAL_CACHE_TTL = 3600000;

    private final String ACCESS_SETTINGS_CACHE_KEY = "accessSettingRules";

    /**
     * Constructor for AccessSettingRuleCacheMgr.
     *
     * @param redisCacheMgr      Cache manager for Redis operations.
     * @param cassandraOperation Cassandra operations for database interactions.
     */
    public AccessSettingRuleCacheMgr(RedisCacheMgr redisCacheMgr, CassandraOperation cassandraOperation) {
        this.redisCacheMgr = redisCacheMgr;
        this.cassandraOperation = cassandraOperation;
    }

    /**
     * Retrieves the cached access setting rules.
     * If the cache is empty or expired, it loads the rules from Redis or Cassandra.
     *
     * @return A collection of cached access setting rules.
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
     * Retrieves a specific cached access setting rule by its context ID.
     *
     * @param contextId The context ID of the access setting rule.
     * @return The cached access setting rule, or null if not found.
     */
    private void loadAccessSettingRules() {
        log.info("Loading access setting rules from cache or database");
        try {
            Map<String, String> cachedRules = redisCacheMgr.getAllCachedAccessRules(ACCESS_SETTINGS_CACHE_KEY);
            if (MapUtils.isNotEmpty(cachedRules)) {
                cachedAccessSettingRules = cachedRules.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> new CachedAccessSettingRule(entry.getValue())));
            } else {
                List<Map<String, Object>> accessSettingRuleMapList = cassandraOperation.getRecordsByProperties(
                        Constants.KEYSPACE_SUNBIRD_COURSE, Constants.ACCESS_SETTINGS_RULES_TABLE_V2, null,
                        null, null);
                cachedAccessSettingRules = accessSettingRuleMapList.stream()
                        .map(record -> new CachedAccessSettingRule(
                                (String) record.get("contextId"),
                                (String) record.get("contextIdType"),
                                (String) record.get("contextData"),
                                false))
                        .collect(Collectors.toMap(
                                CachedAccessSettingRule::getCacheKey,
                                rule -> rule));
                // Cache the rules in Redis
                for (CachedAccessSettingRule rule : cachedAccessSettingRules.values()) {

                    try {

                        Map<String, Object> contextData = rule.getContextData();
                        if (contextData == null) {
                            log.warn("No contextData found for rule: {}", rule.getCacheKey());
                            continue;
                        }

                        // Call the new method for processing
                        processContextData(rule.getCacheKey(), contextData);
                        cachedAccessSettingRules.put(rule.getCacheKey(), rule);

                        // Finally, push the raw contextData to Redis
                        redisCacheMgr.setAccessSettingRuleCache(ACCESS_SETTINGS_CACHE_KEY, rule.getCacheKey(),
                                contextData);

                    } catch (Exception e) {
                        log.error("Error processing rule {}", rule.getCacheKey(), e);
                    }
                }
            }
            log.info("Access setting rules loaded into cache successfully. Number of rules loaded: {}",
                    cachedAccessSettingRules.size());
        } catch (Exception e) {
            log.error("Failed to load AccessSettingRule into Cache. Exception: ", e);
        }
    }



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
            String userGroupId = (String) userGroup.get(Constants.USER_GROUP_ID);
            String userGroupName = (String) userGroup.get(Constants.USER_GROUP_NAME);

            List<Map<String, Object>> criteriaList =
                    (List<Map<String, Object>>) userGroup.get(Constants.USER_GROUP_CRITERIA_LIST);
            if (criteriaList == null || criteriaList.isEmpty()) {
                log.warn("No userGroupCriteriaList for userGroupId {} in rule {}", userGroupId, cacheKey);
                continue;
            }

            for (Map<String, Object> criteria : criteriaList) {
                String criteriaKey = (String) criteria.get(Constants.CRITERIA_KEY);
                List<?> criteriaValues = (List<?>) criteria.get(Constants.CRITERIA_VALUE);

                if (criteriaKey == null || criteriaValues == null) {
                    log.warn("Missing key or values in criteria for userGroupId {} in rule {}", userGroupId, cacheKey);
                    continue;
                }

                log.info("Rule {} -> UserGroup {} ({}) -> CriteriaKey {} -> Values {}",
                        cacheKey, userGroupName, userGroupId, criteriaKey, criteriaValues);

                // Convert the List<?> to a List<Integer>
                List<Integer> intValues = criteriaValues.stream()
                        .map(Object::toString)
                        .map(val -> {
                            try {
                                return Integer.parseInt(val);
                            } catch (NumberFormatException e) {
                                log.warn("Non-integer criteria value '{}' for key {} in rule {}", val, criteriaKey, cacheKey);
                                return null;
                            }
                        })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

                // Convert to BitSet and update the input object
                BitSet bitSet = createBitSetForAttribute(intValues);
                criteria.put(Constants.CRITERIA_VALUE, bitSet);
                // Save to in-memory cache or use as needed
            }
        }
    }



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


    public CachedAccessSettingRule getOrLoadAccessSettingRule(String courseId, String contextId) {

        String cacheKey = courseId + "|" + contextId;
        if (MapUtils.isEmpty(cachedAccessSettingRules)) {
            cachedAccessSettingRules = new ConcurrentHashMap<>();
        }
        CachedAccessSettingRule rule = cachedAccessSettingRules.get(cacheKey);
        if (rule != null) {
            log.debug("Cache hit for rule key: {}", cacheKey);
            return rule;
        }
        log.info("Cache miss for rule key: {}, loading from Cassandra...", cacheKey);
        try {
            Map<String, Object> filter = new HashMap<>();
            filter.put(Constants.CONTEXT_ID, courseId);
            filter.put(Constants.CONTEXT_ID_TYPE_KEY, contextId);

            List<Map<String, Object>> records = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD_COURSE,
                    Constants.ACCESS_SETTINGS_RULES_TABLE_V2,
                    filter,
                    null,
                    null
            );
            if (CollectionUtils.isEmpty(records)) {
                log.warn("No access setting rule found in Cassandra for key: {}", cacheKey);
                return null;
            }
            Map<String, Object> r = records.get(0);
            CachedAccessSettingRule loadedRule = new CachedAccessSettingRule(
                    (String) r.get(Constants.CONTEXT_ID_KEY),
                    (String) r.get(Constants.CONTEXT_ID_TYPE),
                    (String) r.get(Constants.CONTEXT_DATA_KEY),
                    false
            );
            try {
                Map<String, Object> contextData = loadedRule.getContextData();
                if (MapUtils.isNotEmpty(contextData)) {
                    processContextData(cacheKey, contextData);
                }
                cachedAccessSettingRules.put(cacheKey, loadedRule);
                log.info("Loaded and cached rule for key: {}", cacheKey);
            } catch (Exception e) {
                log.error("Error processing rule {}: {}", cacheKey, e.getMessage(), e);
            }
            return loadedRule;
        } catch (Exception e) {
            log.error("Failed to load rule from Cassandra for key {}: {}", cacheKey, e.getMessage(), e);
            return null;
        }
    }

}
