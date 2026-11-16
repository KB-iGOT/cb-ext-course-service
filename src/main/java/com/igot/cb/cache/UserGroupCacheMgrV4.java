package com.igot.cb.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.igot.cb.cbplan.service.impl.v4.CbPlanUserGroupLookupServiceV4Impl;
import com.igot.cb.util.Constants;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Caffeine cache manager for CB Plan V4 user group data.
 * Wraps {@link CbPlanUserGroupLookupServiceV4Impl} with a per-JVM Caffeine cache
 * keyed by {@code orgId:userGroupId}. Cache misses are served from Cassandra via
 * the lookup service; results are cached for the configured TTL.
 *
 * <p>Every entry is stamped with the owning org's user-group counter
 * ({@code cbplan:v4ver:ug:<orgId>}, see {@link CbPlanVersionMgr}) it was loaded under and is
 * served only while the live counter still matches; {@link #invalidateUserGroup(String, String)}
 * evicts locally and bumps that counter, so a group edit on one pod is picked up by every pod
 * on its next read rather than after the TTL.
 *
 * @version 4.0
 */
@Component
@Slf4j
public class UserGroupCacheMgrV4 {

    @Value("${cb.plan.v4.usergroup.cache.ttl.minutes:30}")
    private int ttlMinutes = 30;

    @Value("${cb.plan.v4.usergroup.cache.max.size:10000}")
    private int maxCacheSize = 10000;

    private final CbPlanUserGroupLookupServiceV4Impl userGroupLookupService;
    private final CbPlanVersionMgr versionMgr;

    private Cache<String, VersionedGroup> userGroupCache;

    /**
     * A cached user group together with the owning org's user-group counter it was loaded under.
     */
    private record VersionedGroup(long version, Map<String, Object> group) {
    }

    public UserGroupCacheMgrV4(CbPlanUserGroupLookupServiceV4Impl userGroupLookupService,
                               CbPlanVersionMgr versionMgr) {
        this.userGroupLookupService = userGroupLookupService;
        this.versionMgr = versionMgr;
    }

    @PostConstruct
    public void initCache() {
        this.userGroupCache = Caffeine.newBuilder()
                .maximumSize(maxCacheSize)
                .expireAfterWrite(Duration.ofMinutes(ttlMinutes))
                .build();
        log.info("UserGroupCacheMgrV4: Initialized user group cache - ttl={}min, maxSize={}", ttlMinutes, maxCacheSize);
    }

    /**
     * Returns a user group by composite key, serving from Caffeine on hit and
     * falling back to Cassandra on miss.
     *
     * @param userGroupId user group ID
     * @param orgId       owning organisation ID
     * @return the user group row, or an empty map when not found
     */
    public Map<String, Object> fetchUserGroupById(String userGroupId, String orgId) {
        String cacheKey = buildCacheKey(orgId, userGroupId);
        long liveVersion = versionMgr.getUserGroupVersion(orgId);
        Map<String, Object> cached = getIfFresh(cacheKey, liveVersion);
        if (Objects.nonNull(cached)) {
            log.debug("UserGroupCacheMgrV4: Cache hit - orgId={}, userGroupId={}", orgId, userGroupId);
            return cached;
        }
        Map<String, Object> result = userGroupLookupService.fetchUserGroupById(userGroupId, orgId);
        if (MapUtils.isNotEmpty(result)) {
            userGroupCache.put(cacheKey, new VersionedGroup(liveVersion, result));
        }
        return result;
    }

    /**
     * Batch-fetches user groups, serving cached entries without a Cassandra round-trip
     * and fetching only the cache-miss IDs from Cassandra.
     *
     * @param userGroupIds user group IDs to resolve
     * @param orgId        owning organisation ID
     * @return map of userGroupId → user group entity
     */
    public Map<String, Map<String, Object>> fetchUserGroupsByIds(List<String> userGroupIds, String orgId) {
        if (CollectionUtils.isEmpty(userGroupIds)) {
            return Collections.emptyMap();
        }
        Map<String, Map<String, Object>> resultMap = new HashMap<>();
        long liveVersion = versionMgr.getUserGroupVersion(orgId);
        List<String> cacheMisses = partitionCacheHitsMisses(userGroupIds, orgId, liveVersion, resultMap);
        if (cacheMisses.isEmpty()) {
            log.debug("UserGroupCacheMgrV4.fetchUserGroupsByIds: Full cache hit - orgId={}, count={}", orgId, userGroupIds.size());
            return resultMap;
        }
        log.debug("UserGroupCacheMgrV4.fetchUserGroupsByIds: cacheHits={}, cacheMisses={}, orgId={}",
                userGroupIds.size() - cacheMisses.size(), cacheMisses.size(), orgId);
        fetchAndCacheMisses(cacheMisses, orgId, liveVersion, resultMap);
        log.info("UserGroupCacheMgrV4.fetchUserGroupsByIds: Resolved {}/{} user groups for orgId={}",
                resultMap.size(), userGroupIds.size(), orgId);
        return resultMap;
    }

    private List<String> partitionCacheHitsMisses(List<String> userGroupIds, String orgId, long liveVersion,
                                                  Map<String, Map<String, Object>> resultMap) {
        List<String> cacheMisses = new ArrayList<>();
        for (String userGroupId : userGroupIds) {
            Map<String, Object> cached = getIfFresh(buildCacheKey(orgId, userGroupId), liveVersion);
            if (Objects.nonNull(cached)) {
                resultMap.put(userGroupId, cached);
            } else {
                cacheMisses.add(userGroupId);
            }
        }
        return cacheMisses;
    }

    private void fetchAndCacheMisses(List<String> cacheMisses, String orgId, long liveVersion,
                                     Map<String, Map<String, Object>> resultMap) {
        Map<String, Map<String, Object>> fetched = userGroupLookupService.fetchUserGroupsByIds(cacheMisses, orgId);
        fetched.forEach((id, group) -> {
            userGroupCache.put(buildCacheKey(orgId, id), new VersionedGroup(liveVersion, group));
            resultMap.put(id, group);
        });
    }

    /**
     * Returns the cached group when present and loaded under {@code liveVersion}; a present but
     * stale entry is evicted and treated as a miss.
     */
    private Map<String, Object> getIfFresh(String cacheKey, long liveVersion) {
        VersionedGroup cached = userGroupCache.getIfPresent(cacheKey);
        if (Objects.isNull(cached)) {
            return null;
        }
        if (cached.version() != liveVersion) {
            log.info("UserGroupCacheMgrV4: Stale entry - key={}, cachedVersion={}, liveVersion={}",
                    cacheKey, cached.version(), liveVersion);
            userGroupCache.invalidate(cacheKey);
            return null;
        }
        return cached.group();
    }

    /**
     * Evicts a single user group entry from the local Caffeine cache and bumps the owning org's
     * user-group counter so other pods' entries for that org, and dictionary entries built
     * against them, are invalidated too. Called after a successful update or delete.
     *
     * @param orgId       owning organisation ID
     * @param userGroupId user group ID to evict
     */
    public void invalidateUserGroup(String orgId, String userGroupId) {
        userGroupCache.invalidate(buildCacheKey(orgId, userGroupId));
        versionMgr.bumpUserGroup(orgId);
        log.info("UserGroupCacheMgrV4: Evicted cache entry - orgId={}, userGroupId={}", orgId, userGroupId);
    }

    private String buildCacheKey(String orgId, String userGroupId) {
        return orgId + ":" + userGroupId;
    }
}
