package com.igot.cb.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Publish-version counters for CB Plan V4 dictionary caching.
 * <p>
 * One Redis counter exists per scope and plan year:
 * <pre>
 *   cbplan:v4ver:org:&lt;orgId&gt;:&lt;planYear&gt;
 *   cbplan:v4ver:ministry:&lt;ministryOrStateId&gt;:&lt;planYear&gt;
 *   cbplan:v4ver:all:&lt;planYear&gt;
 * </pre>
 * Publishing, republishing or retiring a plan {@code INCR}s the counters of every scope it
 * touches. Per-user dictionary entries are stamped with the counters they were built from
 * ({@code _cv} block) and are served only while the live counters still match; a bump therefore
 * invalidates every dependent entry on every pod instantly, with no key scans and no per-user
 * writes. The counters deliberately live under their own prefix so that pattern deletes of
 * {@code cbplan:v4:*} cannot remove them.
 * <p>
 * Redis is best-effort throughout: a failed read yields version 0 for every scope, which
 * degrades to the pre-versioning behaviour (serve whatever is cached until TTL).
 *
 * @version 4.0
 */
@Component
@Slf4j
public class CbPlanVersionMgr {

    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF = new TypeReference<>() {
    };
    private static final String SCOPE_ORG = "org:";
    private static final String SCOPE_MINISTRY = "ministry:";
    private static final String SCOPE_ALL = "all:";

    private final CbExtRedisCacheMgr redisCacheMgr;
    private final ObjectMapper mapper;

    public CbPlanVersionMgr(@Qualifier("cbPlanRedisCacheMgr") CbExtRedisCacheMgr redisCacheMgr) {
        this.redisCacheMgr = redisCacheMgr;
        this.mapper = new ObjectMapper();
    }

    /**
     * Reads the live counters for a user's scopes in a single {@code MGET}.
     *
     * @param orgId             user's root org ID
     * @param ministryOrStateId user's ministry/state ID; blank when the user has none
     * @param planYear          plan year
     * @return current snapshot; absent or unreadable counters read as 0
     */
    public CbPlanCacheVersions getVersions(String orgId, String ministryOrStateId, String planYear) {
        boolean hasMinistry = StringUtils.isNotBlank(ministryOrStateId);
        String[] keys = hasMinistry
                ? new String[]{orgKey(orgId, planYear), allKey(planYear), ministryKey(ministryOrStateId, planYear)}
                : new String[]{orgKey(orgId, planYear), allKey(planYear)};
        List<String> values = redisCacheMgr.mgetFromCache(keys);
        long org = parseVersion(values, 0);
        long all = parseVersion(values, 1);
        long ministry = hasMinistry ? parseVersion(values, 2) : 0L;
        return new CbPlanCacheVersions(org, ministry, all, hasMinistry ? ministryOrStateId : null);
    }

    /**
     * Reads the per-user dictionary entry and returns it only when its {@code _cv} stamp matches
     * {@code expected}. Used where the caller already holds the user's profile (and therefore the
     * live snapshot). The returned payload has the {@code _cv} block removed.
     *
     * @param cacheKey per-user dictionary key
     * @param expected live snapshot for this user
     * @return the cached payload on a fresh hit; empty on miss, stale stamp, pre-versioning entry
     *         or malformed JSON
     */
    public Optional<Map<String, Object>> readValidatedDict(String cacheKey, CbPlanCacheVersions expected) {
        Map<String, Object> cached = readAndParse(cacheKey);
        if (Objects.isNull(cached)) {
            return Optional.empty();
        }
        return validate(cacheKey, cached, expected);
    }

    /**
     * Reads the per-user dictionary entry without a user profile: the ministry/state the entry
     * was built for is taken from its own {@code _cv} stamp, the live counters are then fetched
     * and compared. Used by the private Flink-facing lookup API, which must not pay for a
     * profile load on the hot path.
     *
     * @param cacheKey per-user dictionary key
     * @param orgId    user's root org ID (part of the key, needed for the org counter)
     * @param planYear plan year
     * @return the cached payload on a fresh hit; empty on miss, stale stamp, pre-versioning entry
     *         or malformed JSON
     */
    public Optional<Map<String, Object>> readValidatedDict(String cacheKey, String orgId, String planYear) {
        Map<String, Object> cached = readAndParse(cacheKey);
        if (Objects.isNull(cached)) {
            return Optional.empty();
        }
        CbPlanCacheVersions stamped = CbPlanCacheVersions.fromCacheMeta(cached);
        if (Objects.isNull(stamped)) {
            log.info("CbPlanVersionMgr.readValidatedDict: Unstamped entry treated as miss - key={}", cacheKey);
            return Optional.empty();
        }
        CbPlanCacheVersions live = getVersions(orgId, stamped.ministryOrStateId(), planYear);
        return validate(cacheKey, cached, live);
    }

    /**
     * Returns a copy of {@code result} carrying the {@code _cv} stamp, ready to be cached.
     * The input is not mutated, so the API response the caller is still holding stays clean.
     */
    public Map<String, Object> stamp(Map<String, Object> result, CbPlanCacheVersions versions) {
        Map<String, Object> stamped = new LinkedHashMap<>(result);
        stamped.put(Constants.CB_PLAN_V4_CACHE_VERSIONS_KEY, versions.toCacheMeta());
        return stamped;
    }

    /**
     * Bumps the counters for every scope a publish touched. Each {@code INCR} is independent and
     * idempotent in effect (any bump invalidates), so a partial failure only delays
     * invalidation for the scopes that failed, until the next publish or TTL.
     *
     * @param orgIds              root org IDs whose org counter must move (may be empty)
     * @param ministryOrStateIds  ministry/state IDs whose counter must move (may be empty)
     * @param bumpAll             true when the ALL-org scope is involved (new or previous scope)
     * @param planYear            plan year
     */
    public void bump(Collection<String> orgIds, Collection<String> ministryOrStateIds, boolean bumpAll,
                     String planYear) {
        if (StringUtils.isBlank(planYear)) {
            return;
        }
        if (bumpAll) {
            redisCacheMgr.incrementCounter(allKey(planYear));
        }
        if (Objects.nonNull(orgIds)) {
            orgIds.stream().filter(StringUtils::isNotBlank)
                    .forEach(orgId -> redisCacheMgr.incrementCounter(orgKey(orgId, planYear)));
        }
        if (Objects.nonNull(ministryOrStateIds)) {
            ministryOrStateIds.stream().filter(StringUtils::isNotBlank)
                    .forEach(id -> redisCacheMgr.incrementCounter(ministryKey(id, planYear)));
        }
        log.info("CbPlanVersionMgr.bump: planYear={}, all={}, orgCount={}, ministryCount={}", planYear, bumpAll,
                Objects.isNull(orgIds) ? 0 : orgIds.size(),
                Objects.isNull(ministryOrStateIds) ? 0 : ministryOrStateIds.size());
    }

    private Optional<Map<String, Object>> validate(String cacheKey, Map<String, Object> cached,
                                                   CbPlanCacheVersions live) {
        CbPlanCacheVersions stamped = CbPlanCacheVersions.fromCacheMeta(cached);
        if (Objects.isNull(stamped)) {
            log.info("CbPlanVersionMgr.readValidatedDict: Unstamped entry treated as miss - key={}", cacheKey);
            return Optional.empty();
        }
        if (!live.matches(stamped)) {
            log.info("CbPlanVersionMgr.readValidatedDict: Stale entry - key={}, stamped={}, live={}",
                    cacheKey, stamped, live);
            return Optional.empty();
        }
        cached.remove(Constants.CB_PLAN_V4_CACHE_VERSIONS_KEY);
        return Optional.of(cached);
    }

    private Map<String, Object> readAndParse(String cacheKey) {
        String cachedJson = redisCacheMgr.getFromCache(cacheKey);
        if (StringUtils.isBlank(cachedJson)) {
            return null;
        }
        try {
            return mapper.readValue(cachedJson, MAP_TYPE_REF);
        } catch (JsonProcessingException e) {
            log.warn("CbPlanVersionMgr.readAndParse: Failed to deserialize - key={}", cacheKey, e);
            return null;
        }
    }

    private static long parseVersion(List<String> values, int index) {
        if (Objects.isNull(values) || values.size() <= index) {
            return 0L;
        }
        String raw = values.get(index);
        if (StringUtils.isBlank(raw)) {
            return 0L;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            log.warn("CbPlanVersionMgr.parseVersion: Non-numeric counter value '{}' treated as 0", raw);
            return 0L;
        }
    }

    private static String orgKey(String orgId, String planYear) {
        return Constants.CB_PLAN_V4_VERSION_KEY_PREFIX + SCOPE_ORG + orgId + ":" + planYear;
    }

    private static String ministryKey(String ministryOrStateId, String planYear) {
        return Constants.CB_PLAN_V4_VERSION_KEY_PREFIX + SCOPE_MINISTRY + ministryOrStateId + ":" + planYear;
    }

    private static String allKey(String planYear) {
        return Constants.CB_PLAN_V4_VERSION_KEY_PREFIX + SCOPE_ALL + planYear;
    }
}
