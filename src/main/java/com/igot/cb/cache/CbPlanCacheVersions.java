package com.igot.cb.cache;

import com.igot.cb.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Snapshot of the CB Plan V4 publish-version counters a user's dictionary depends on:
 * the user's root org, the user's ministry/state (when any) and the ALL-org scope, all for one
 * plan year. A cached dictionary is stamped with the snapshot it was built from; it is served
 * only while the live counters still match, so a publish anywhere in those scopes makes every
 * dependent entry stale at once, on every pod, without touching the entries themselves.
 *
 * @param orgVersion        counter for the user's root org
 * @param ministryVersion   counter for the user's ministry/state; 0 when the user has none
 * @param allVersion        counter for the ALL-org scope
 * @param ministryOrStateId ministry/state ID the ministry counter belongs to; null when none
 */
public record CbPlanCacheVersions(long orgVersion, long ministryVersion, long allVersion, String ministryOrStateId) {

    private static final String META_ORG = "orgVer";
    private static final String META_MINISTRY = "ministryVer";
    private static final String META_ALL = "allVer";
    private static final String META_MINISTRY_ID = "ministryId";

    /**
     * Stamp for Caffeine lists that depend on the org and ALL scopes together
     * (the combined {@code orgId:planYear} dictionary list).
     */
    public String orgAllStamp() {
        return orgVersion + "." + allVersion;
    }

    /**
     * Serialises this snapshot as the {@code _cv} block stored beside the cached dictionary.
     */
    public Map<String, Object> toCacheMeta() {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put(META_ORG, orgVersion);
        meta.put(META_MINISTRY, ministryVersion);
        meta.put(META_ALL, allVersion);
        if (StringUtils.isNotBlank(ministryOrStateId)) {
            meta.put(META_MINISTRY_ID, ministryOrStateId);
        }
        return meta;
    }

    /**
     * Rebuilds a snapshot from a cached {@code _cv} block.
     *
     * @param cached deserialized dictionary payload
     * @return the stamped snapshot, or null when the payload carries no usable {@code _cv}
     *         (pre-versioning entries), which callers treat as a cache miss
     */
    public static CbPlanCacheVersions fromCacheMeta(Map<String, Object> cached) {
        if (Objects.isNull(cached)) {
            return null;
        }
        Object metaObj = cached.get(Constants.CB_PLAN_V4_CACHE_VERSIONS_KEY);
        if (!(metaObj instanceof Map<?, ?> meta)) {
            return null;
        }
        Object org = meta.get(META_ORG);
        Object ministry = meta.get(META_MINISTRY);
        Object all = meta.get(META_ALL);
        if (!(org instanceof Number) || !(ministry instanceof Number) || !(all instanceof Number)) {
            return null;
        }
        Object ministryId = meta.get(META_MINISTRY_ID);
        return new CbPlanCacheVersions(((Number) org).longValue(), ((Number) ministry).longValue(),
                ((Number) all).longValue(), ministryId instanceof String s ? s : null);
    }

    /**
     * True when the cached stamp still matches the live counters, including that the entry
     * was built for the same ministry/state the user currently belongs to.
     */
    public boolean matches(CbPlanCacheVersions other) {
        return Objects.nonNull(other)
                && orgVersion == other.orgVersion
                && ministryVersion == other.ministryVersion
                && allVersion == other.allVersion
                && Objects.equals(StringUtils.defaultIfBlank(ministryOrStateId, null),
                        StringUtils.defaultIfBlank(other.ministryOrStateId, null));
    }
}
