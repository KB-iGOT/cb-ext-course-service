package com.igot.cb.cache;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import jakarta.annotation.PostConstruct;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.springframework.beans.factory.annotation.Value;
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
    private static final String ACCESS_SETTINGS_CACHE_KEY = "accessSettingRules";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final RedisCacheMgr redisCacheMgr;
    private final CassandraOperation cassandraOperation;
    private Map<String, CachedAccessSettingRule> cachedAccessSettingRules = new ConcurrentHashMap<>();

    private Cache<String, CachedAccessSettingRule> accessSettingsCache;


    private final long LOCAL_CACHE_TTL = 3600000;

    @Value("${access.rule.ttl.minutes}")
    private int ttlMinutes;

    @Value("${access.settings.cache.batch.size:500}")
    private int accessSettingsCacheBatchSize = 500;

    @Value("${access.settings.cache.max.query.size:5000}")
    private int accessSettingsCacheMaxQuerySize = 5000;

    @PostConstruct
    public void initCache() {
        accessSettingsCache = Caffeine.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(Duration.ofMinutes(ttlMinutes))
                .build();
    }


    /**
     * Constructor for AccessSettingRuleCacheMgr.
     *
     * @param redisCacheMgr      Cache manager for Redis operations.
     * @param cassandraOperation Cassandra operations for database interactions.
     */
    public AccessSettingRuleCacheMgr(RedisCacheMgr redisCacheMgr,
                                     CassandraOperation cassandraOperation) {
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

    public void refreshRuleCache(String contextId, String contextIdType, String contextDataJson, boolean archived) {
        String cacheKey = buildCacheKey(contextId, contextIdType);
        if (archived || contextDataJson == null || contextDataJson.isBlank()) {
            cachedAccessSettingRules.remove(cacheKey);
            redisCacheMgr.deleteHashField(ACCESS_SETTINGS_CACHE_KEY, cacheKey);
            accessSettingsCache.invalidate(cacheKey);
            return;
        }

        CachedAccessSettingRule rule = new CachedAccessSettingRule(contextId, contextIdType, contextDataJson, false);
        try {
            Map<String, Object> contextData = rule.getContextData();
            if (MapUtils.isNotEmpty(contextData)) {
                processContextData(cacheKey, contextData);
            }
            redisCacheMgr.setHashValue(ACCESS_SETTINGS_CACHE_KEY, cacheKey,
                    buildRedisRulePayload(contextId, contextIdType, contextData));
            cachedAccessSettingRules.put(cacheKey, rule);
            accessSettingsCache.put(cacheKey, rule);
        } catch (Exception e) {
            log.error("Failed to refresh access setting rule cache for key: {}", cacheKey, e);
        }
    }

    public void invalidateAll() {
        cachedAccessSettingRules = new ConcurrentHashMap<>();
        if (accessSettingsCache != null) {
            accessSettingsCache.invalidateAll();
        }
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
                Map<String, CachedAccessSettingRule> cassandraLoadedRules = new ConcurrentHashMap<>();
                int totalFetched = cassandraOperation.forEachRecordByPropertiesPaged(
                        Constants.KEYSPACE_SUNBIRD_COURSE,
                        Constants.ACCESS_SETTINGS_RULES_TABLE_V2,
                        null,
                        null,
                        accessSettingsCacheBatchSize,
                        accessSettingsCacheMaxQuerySize,
                        record -> {
                            try {
                                CachedAccessSettingRule rule = new CachedAccessSettingRule(
                                        (String) record.get("contextId"),
                                        (String) record.get("contextIdType"),
                                        (String) record.get("contextData"),
                                        false);
                                Map<String, Object> contextData = rule.getContextData();
                                if (contextData == null) {
                                    log.warn("No contextData found for rule: {}", rule.getCacheKey());
                                    return;
                                }
                                processContextData(rule.getCacheKey(), contextData);
                                cassandraLoadedRules.put(rule.getCacheKey(), rule);
                                redisCacheMgr.setHashValue(ACCESS_SETTINGS_CACHE_KEY, rule.getCacheKey(),
                                        buildRedisRulePayload(rule.getContextId(), rule.getContextIdType(), contextData));
                            } catch (Exception e) {
                                log.error("Error processing access setting rule record from Cassandra", e);
                            }
                        }
                );
                if (totalFetched >= accessSettingsCacheMaxQuerySize) {
                    log.warn("Reached access settings max query size limit: {}. " +
                                    "Consider increasing access.settings.cache.max.query.size",
                            accessSettingsCacheMaxQuerySize);
                }
                cachedAccessSettingRules = cassandraLoadedRules;
            }
            log.info("Access setting rules loaded into cache successfully. Number of rules loaded: {}",
                    cachedAccessSettingRules.size());
        } catch (Exception e) {
            log.error("Failed to load AccessSettingRule into Cache. Exception: ", e);
        }
    }

    private String buildCacheKey(String contextId, String contextIdType) {
        return contextId + "|" + contextIdType;
    }

    private String buildRedisRulePayload(String contextId, String contextIdType, Map<String, Object> contextData) {
        try {
            Map<String, Object> rulePayload = new HashMap<>();
            rulePayload.put(Constants.CONTEXT_ID, contextId);
            rulePayload.put(Constants.CONTEXT_ID_TYPE, contextIdType);
            rulePayload.put(Constants.CONTEXT_DATA, sanitizeContextDataForRedis(contextData));
            rulePayload.put(Constants.IS_ARCHIVED, false);
            return OBJECT_MAPPER.writeValueAsString(rulePayload);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize access setting rule for Redis", e);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> sanitizeContextDataForRedis(Map<String, Object> contextData) {
        Map<String, Object> sanitized = OBJECT_MAPPER.convertValue(contextData, Map.class);
        Object accessControlObj = sanitized.get(Constants.ACCESS_CONTROL_ID);
        if (!(accessControlObj instanceof Map<?, ?> accessControl)) {
            return sanitized;
        }
        Object userGroupsObj = accessControl.get(Constants.USER_GROUPS);
        if (!(userGroupsObj instanceof List<?> userGroups)) {
            return sanitized;
        }
        for (Object userGroupObj : userGroups) {
            if (!(userGroupObj instanceof Map<?, ?> userGroup)) {
                continue;
            }
            Object criteriaListObj = userGroup.get(Constants.USER_GROUP_CRITERIA_LIST);
            if (!(criteriaListObj instanceof List<?> criteriaList)) {
                continue;
            }
            for (Object criteriaObj : criteriaList) {
                if (!(criteriaObj instanceof Map<?, ?> criteriaMap)) {
                    continue;
                }
                Object valueObj = criteriaMap.get(Constants.CRITERIA_VALUE);
                if (valueObj instanceof BitSet bitSet) {
                    List<Integer> bitSetValues = bitSet.stream().boxed().collect(Collectors.toList());
                    ((Map<String, Object>) criteriaMap).put(Constants.CRITERIA_VALUE, bitSetValues);
                }
            }
        }
        return sanitized;
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
        String cacheKey = buildCacheKey(courseId, contextId);
        try {
            // 1. Try Redis first
            Map<String, String> cachedRules = redisCacheMgr.getAllCachedAccessRules(ACCESS_SETTINGS_CACHE_KEY);
            if (MapUtils.isNotEmpty(cachedRules) && cachedRules.containsKey(cacheKey)) {
                return new CachedAccessSettingRule(cachedRules.get(cacheKey));
            }
            // 2. Batchwise fetch from Cassandra if not found in Redis
            final CachedAccessSettingRule[] foundRule = {null};
            cassandraOperation.forEachRecordByPropertiesPaged(
                    Constants.KEYSPACE_SUNBIRD_COURSE,
                    Constants.ACCESS_SETTINGS_RULES_TABLE_V2,
                    null,
                    null,
                    accessSettingsCacheBatchSize,
                    accessSettingsCacheMaxQuerySize,
                    record -> {
                        try {
                            String recContextId = (String) record.get("contextId");
                            String recContextIdType = (String) record.get("contextIdType");
                            String recContextData = (String) record.get("contextData");
                            String recCacheKey = buildCacheKey(recContextId, recContextIdType);
                            if (cacheKey.equals(recCacheKey)) {
                                CachedAccessSettingRule rule = new CachedAccessSettingRule(
                                        recContextId, recContextIdType, recContextData, false);
                                Map<String, Object> contextData = rule.getContextData();
                                if (contextData != null) {
                                    processContextData(rule.getCacheKey(), contextData);
                                }
                                // Optionally cache in Redis for future
                                redisCacheMgr.setHashValue(ACCESS_SETTINGS_CACHE_KEY, rule.getCacheKey(),
                                        buildRedisRulePayload(rule.getContextId(), rule.getContextIdType(), contextData));
                                foundRule[0] = rule;
                            }
                        } catch (Exception e) {
                            log.error("Error processing access setting rule record from Cassandra", e);
                        }
                    }
            );
            return foundRule[0];
        } catch (Exception e) {
            log.error("Failed to load AccessSettingRule from Redis or Cassandra for key: {}", cacheKey, e);
            return null;
        }
    }

}
