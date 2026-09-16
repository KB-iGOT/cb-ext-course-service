package com.igot.cb.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.igot.cb.cassandra.BatchQueryParams;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.util.Constants;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;

/**
 * Cache Manager for CB Plan V4 User Groups with Caffeine in-memory caching.
 * User groups are read-heavy, write-light domain objects perfect for caching.
 *
 * @version 4.0
 */
@Component
@Slf4j
public class CbPlanUserGroupCacheMgr {
    @Value("${cb.plan.v4.usergroup.cache.ttl.minutes:30}")
    private int ttlMinutes;
    @Value("${cb.plan.v4.usergroup.cache.max.size:10000}")
    private int maxCacheSize;
    @Value("${cb.plan.v4.usergroup.batch.size:20}")
    private int batchSize;
    private final CassandraOperation cassandraOperation;
    private Cache<String, Map<String, Object>> userGroupCache;

    public CbPlanUserGroupCacheMgr(CassandraOperation cassandraOperation) {
        this.cassandraOperation = cassandraOperation;
    }

    @PostConstruct
    public void initCache() {
        this.userGroupCache = Caffeine.newBuilder()
                .maximumSize(maxCacheSize)
                .expireAfterWrite(Duration.ofMinutes(ttlMinutes))
                .build();
        log.info("Initialized CbPlanUserGroupCacheMgr with TTL: {} minutes, max size: {}, batch size: {}",
                ttlMinutes, maxCacheSize, batchSize);
    }

    /**
     * Fetches a single user group by composite key (orgId, userGroupId).
     * Checks Caffeine cache first, falls back to Cassandra on miss.
     *
     * @param userGroupId user group ID
     * @param orgId       organization ID the group must belong to
     * @return the user group entity, empty map when not found
     */
    public Map<String, Object> getUserGroup(String userGroupId, String orgId) {
        if (StringUtils.isBlank(userGroupId) || StringUtils.isBlank(orgId)) {
            return Collections.emptyMap();
        }
        String cacheKey = buildCacheKey(orgId, userGroupId);
        Map<String, Object> cachedGroup = userGroupCache.getIfPresent(cacheKey);
        if (Objects.nonNull(cachedGroup)) {
            log.debug("CbPlanUserGroupCacheMgr.getUserGroup: Cache hit - userGroupId={}, orgId={}", userGroupId, orgId);
            return cachedGroup;
        }
        log.debug("CbPlanUserGroupCacheMgr.getUserGroup: Cache miss - userGroupId={}, orgId={}", userGroupId, orgId);
        Map<String, Object> group = fetchFromCassandra(userGroupId, orgId);
        if (MapUtils.isNotEmpty(group)) {
            userGroupCache.put(cacheKey, group);
        }
        return group;
    }

    /**
     * Batch fetches multiple user groups with smart caching:
     * checks cache first, fetches only uncached groups from Cassandra in batches.
     *
     * @param userGroupIds list of user group IDs to fetch
     * @param orgId        organization ID the groups must belong to
     * @return map of userGroupId → user group entity; missing entries for groups not found
     */
    public Map<String, Map<String, Object>> getUserGroups(List<String> userGroupIds, String orgId) {
        if (CollectionUtils.isEmpty(userGroupIds) || StringUtils.isBlank(orgId)) {
            return Collections.emptyMap();
        }
        Map<String, Map<String, Object>> resultMap = new HashMap<>();
        List<String> uncachedIds = new ArrayList<>();
        for (String userGroupId : userGroupIds) {
            String cacheKey = buildCacheKey(orgId, userGroupId);
            Map<String, Object> cached = userGroupCache.getIfPresent(cacheKey);
            if (Objects.nonNull(cached)) {
                resultMap.put(userGroupId, cached);
            } else {
                uncachedIds.add(userGroupId);
            }
        }
        log.debug("CbPlanUserGroupCacheMgr.getUserGroups: Cache hits={}, misses={}, total requested={}",
                resultMap.size(), uncachedIds.size(), userGroupIds.size());
        if (uncachedIds.isEmpty()) {
            return resultMap;
        }
        Map<String, Map<String, Object>> fetchedGroups = batchFetchFromCassandra(uncachedIds, orgId);
        for (Map.Entry<String, Map<String, Object>> entry : fetchedGroups.entrySet()) {
            String userGroupId = entry.getKey();
            Map<String, Object> group = entry.getValue();
            String cacheKey = buildCacheKey(orgId, userGroupId);
            userGroupCache.put(cacheKey, group);
            resultMap.put(userGroupId, group);
        }
        log.info("CbPlanUserGroupCacheMgr.getUserGroups: Returned {} groups (cached={}, fetched={}) for orgId={}",
                resultMap.size(), userGroupIds.size() - uncachedIds.size(), fetchedGroups.size(), orgId);
        return resultMap;
    }

    private String buildCacheKey(String orgId, String userGroupId) {
        return orgId + ":" + userGroupId;
    }

    private Map<String, Object> fetchFromCassandra(String userGroupId, String orgId) {
        try {
            Map<String, Object> compositeKey = Map.of(
                    Constants.COL_ORGID, orgId,
                    Constants.COL_USERGROUPID, userGroupId
            );
            List<Map<String, Object>> results = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_USER_GROUP_INFO,
                    compositeKey,
                    List.of(),
                    null
            );
            if (CollectionUtils.isEmpty(results)) {
                log.debug("CbPlanUserGroupCacheMgr.fetchFromCassandra: User group not found - userGroupId={}, orgId={}",
                        userGroupId, orgId);
                return Collections.emptyMap();
            }
            return results.get(0);
        } catch (Exception e) {
            log.error("CbPlanUserGroupCacheMgr.fetchFromCassandra: Failed to fetch user group - userGroupId={}, orgId={}",
                    userGroupId, orgId, e);
            return Collections.emptyMap();
        }
    }

    private Map<String, Map<String, Object>> batchFetchFromCassandra(List<String> userGroupIds, String orgId) {
        Map<String, Map<String, Object>> resultMap = new HashMap<>();
        List<List<String>> chunks = chunkList(userGroupIds, batchSize);
        for (List<String> chunk : chunks) {
            Map<String, Map<String, Object>> chunkResults = fetchChunk(chunk, orgId);
            resultMap.putAll(chunkResults);
        }
        return resultMap;
    }

    private Map<String, Map<String, Object>> fetchChunk(List<String> userGroupIds, String orgId) {
        Map<String, Map<String, Object>> chunkMap = new HashMap<>();
        try {
            BatchQueryParams params = BatchQueryParams.builder()
                    .keyspaceName(Constants.KEYSPACE_SUNBIRD)
                    .tableName(Constants.TABLE_USER_GROUP_INFO)
                    .partitionKeyColumn(Constants.COL_ORGID)
                    .partitionKeyValue(orgId)
                    .clusteringColumn(Constants.COL_USERGROUPID)
                    .clusteringValues(userGroupIds)
                    .build();
            List<Map<String, Object>> results = cassandraOperation.getRecordsByIdsWithGivenPartitionKey(params);
            if (CollectionUtils.isNotEmpty(results)) {
                for (Map<String, Object> group : results) {
                    String userGroupId = (String) group.get(Constants.COL_USERGROUPID);
                    if (StringUtils.isNotBlank(userGroupId)) {
                        chunkMap.put(userGroupId, group);
                    }
                }
            }
            log.debug("CbPlanUserGroupCacheMgr.fetchChunk: Fetched {} groups from chunk of {} for orgId={}",
                    chunkMap.size(), userGroupIds.size(), orgId);
        } catch (Exception e) {
            log.error("CbPlanUserGroupCacheMgr.fetchChunk: Failed to batch fetch user groups - orgId={}, chunk size={}",
                    orgId, userGroupIds.size(), e);
        }
        return chunkMap;
    }

    private List<List<String>> chunkList(List<String> list, int chunkSize) {
        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < list.size(); i += chunkSize) {
            chunks.add(list.subList(i, Math.min(i + chunkSize, list.size())));
        }
        return chunks;
    }

    public void invalidateUserGroup(String userGroupId, String orgId) {
        String cacheKey = buildCacheKey(orgId, userGroupId);
        userGroupCache.invalidate(cacheKey);
        log.info("CbPlanUserGroupCacheMgr.invalidateUserGroup: Invalidated cache - userGroupId={}, orgId={}",
                userGroupId, orgId);
    }

    public void invalidateAll() {
        userGroupCache.invalidateAll();
        log.info("CbPlanUserGroupCacheMgr.invalidateAll: Cleared all cached user groups");
    }
}
