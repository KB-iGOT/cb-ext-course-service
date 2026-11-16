package com.igot.cb.cache;

import com.igot.cb.util.Constants;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CbPlanCacheVersionsTest {

    @Test
    void toCacheMeta_roundTripsThroughFromCacheMeta() {
        CbPlanCacheVersions original = new CbPlanCacheVersions(5, 2, 9, "ministry1");
        Map<String, Object> cached = new HashMap<>();
        cached.put(Constants.CB_PLAN_V4_CACHE_VERSIONS_KEY, original.toCacheMeta());

        assertEquals(original, CbPlanCacheVersions.fromCacheMeta(cached));
    }

    @Test
    void toCacheMeta_omitsMinistryIdWhenAbsent() {
        Map<String, Object> meta = new CbPlanCacheVersions(1, 0, 0, null).toCacheMeta();

        assertFalse(meta.containsKey("ministryId"));
        assertEquals(1L, meta.get("orgVer"));
    }

    @Test
    void fromCacheMeta_acceptsIntegerValuesFromJson() {
        Map<String, Object> meta = new HashMap<>();
        meta.put("orgVer", 3);
        meta.put("ministryVer", 0);
        meta.put("allVer", 1);
        Map<String, Object> cached = Map.of(Constants.CB_PLAN_V4_CACHE_VERSIONS_KEY, meta);

        assertEquals(new CbPlanCacheVersions(3, 0, 1, null), CbPlanCacheVersions.fromCacheMeta(cached));
    }

    @Test
    void fromCacheMeta_missingOrMalformed_returnsNull() {
        assertNull(CbPlanCacheVersions.fromCacheMeta(null));
        assertNull(CbPlanCacheVersions.fromCacheMeta(Map.of()));
        assertNull(CbPlanCacheVersions.fromCacheMeta(Map.of(Constants.CB_PLAN_V4_CACHE_VERSIONS_KEY, "x")));
        assertNull(CbPlanCacheVersions.fromCacheMeta(Map.of(Constants.CB_PLAN_V4_CACHE_VERSIONS_KEY, Map.of("orgVer", 1))));
        assertNull(CbPlanCacheVersions.fromCacheMeta(Map.of(Constants.CB_PLAN_V4_CACHE_VERSIONS_KEY,
                Map.of("orgVer", "1", "ministryVer", 0, "allVer", 0))));
    }

    @Test
    void matches_requiresAllCountersAndMinistryIdToAgree() {
        CbPlanCacheVersions base = new CbPlanCacheVersions(1, 2, 3, "m1");

        assertTrue(base.matches(new CbPlanCacheVersions(1, 2, 3, "m1")));
        assertFalse(base.matches(new CbPlanCacheVersions(2, 2, 3, "m1")));
        assertFalse(base.matches(new CbPlanCacheVersions(1, 3, 3, "m1")));
        assertFalse(base.matches(new CbPlanCacheVersions(1, 2, 4, "m1")));
        assertFalse(base.matches(new CbPlanCacheVersions(1, 2, 3, "m2")));
        assertFalse(base.matches(null));
    }

    @Test
    void matches_treatsBlankAndNullMinistryIdAsEqual() {
        assertTrue(new CbPlanCacheVersions(1, 0, 0, null).matches(new CbPlanCacheVersions(1, 0, 0, "")));
    }

    @Test
    void orgAllStamp_combinesOrgAndAllCounters() {
        assertEquals("4.11", new CbPlanCacheVersions(4, 7, 11, "m").orgAllStamp());
    }
}
