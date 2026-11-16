package com.igot.cb.cache;

import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanVersionMgrTest {

    private static final String ORG_ID = "org1";
    private static final String MINISTRY_ID = "ministry1";
    private static final String PLAN_YEAR = "2026-27";
    private static final String DICT_KEY = Constants.CB_PLAN_V4_REDIS_KEY_PREFIX + ORG_ID + ":user1:" + PLAN_YEAR + ":dict";
    private static final String ORG_KEY = Constants.CB_PLAN_V4_VERSION_KEY_PREFIX + "org:" + ORG_ID + ":" + PLAN_YEAR;
    private static final String ALL_KEY = Constants.CB_PLAN_V4_VERSION_KEY_PREFIX + "all:" + PLAN_YEAR;
    private static final String MINISTRY_KEY = Constants.CB_PLAN_V4_VERSION_KEY_PREFIX + "ministry:" + MINISTRY_ID + ":" + PLAN_YEAR;

    @Mock
    private CbExtRedisCacheMgr redisCacheMgr;

    private CbPlanVersionMgr versionMgr;

    @BeforeEach
    void setUp() {
        versionMgr = new CbPlanVersionMgr(redisCacheMgr);
    }

    private static String dictJson(String cvBlock) {
        String body = "\"" + PLAN_YEAR + "\":{\"aparPlanList\":{\"p1\":{\"planId\":\"p1\"}},\"nonAparPlanList\":{}}";
        return cvBlock == null ? "{" + body + "}" : "{" + cvBlock + "," + body + "}";
    }

    @Test
    void getVersions_withoutMinistry_readsOrgAndAllKeysOnly() {
        when(redisCacheMgr.mgetFromCache(ORG_KEY, ALL_KEY)).thenReturn(Arrays.asList("7", "3"));

        CbPlanCacheVersions v = versionMgr.getVersions(ORG_ID, null, PLAN_YEAR);

        assertEquals(7, v.orgVersion());
        assertEquals(3, v.allVersion());
        assertEquals(0, v.ministryVersion());
        assertNull(v.ministryOrStateId());
        assertEquals("7.3", v.orgAllStamp());
    }

    @Test
    void getVersions_withMinistry_readsThreeKeysInOrder() {
        when(redisCacheMgr.mgetFromCache(ORG_KEY, ALL_KEY, MINISTRY_KEY)).thenReturn(Arrays.asList("1", null, "9"));

        CbPlanCacheVersions v = versionMgr.getVersions(ORG_ID, MINISTRY_ID, PLAN_YEAR);

        assertEquals(1, v.orgVersion());
        assertEquals(0, v.allVersion());
        assertEquals(9, v.ministryVersion());
        assertEquals(MINISTRY_ID, v.ministryOrStateId());
    }

    @Test
    void getVersions_redisFailureOrGarbage_degradesToZero() {
        when(redisCacheMgr.mgetFromCache(any(String[].class))).thenReturn(Arrays.asList("abc", null));

        CbPlanCacheVersions v = versionMgr.getVersions(ORG_ID, "", PLAN_YEAR);

        assertEquals(new CbPlanCacheVersions(0, 0, 0, null), v);
    }

    @Test
    void readValidatedDict_withExpected_matchingStamp_returnsPayloadWithoutCv() {
        when(redisCacheMgr.getFromCache(DICT_KEY))
                .thenReturn(dictJson("\"_cv\":{\"orgVer\":7,\"ministryVer\":0,\"allVer\":3}"));

        Optional<Map<String, Object>> result = versionMgr.readValidatedDict(DICT_KEY,
                new CbPlanCacheVersions(7, 0, 3, null));

        assertTrue(result.isPresent());
        assertFalse(result.get().containsKey(Constants.CB_PLAN_V4_CACHE_VERSIONS_KEY));
        assertTrue(result.get().containsKey(PLAN_YEAR));
        verify(redisCacheMgr, never()).mgetFromCache(any(String[].class));
    }

    @Test
    void readValidatedDict_withExpected_staleStamp_isMiss() {
        when(redisCacheMgr.getFromCache(DICT_KEY))
                .thenReturn(dictJson("\"_cv\":{\"orgVer\":7,\"ministryVer\":0,\"allVer\":3}"));

        assertTrue(versionMgr.readValidatedDict(DICT_KEY, new CbPlanCacheVersions(8, 0, 3, null)).isEmpty());
        assertTrue(versionMgr.readValidatedDict(DICT_KEY, new CbPlanCacheVersions(7, 0, 4, null)).isEmpty());
        assertTrue(versionMgr.readValidatedDict(DICT_KEY, new CbPlanCacheVersions(7, 1, 3, MINISTRY_ID)).isEmpty());
    }

    @Test
    void readValidatedDict_withExpected_ministryChanged_isMiss() {
        when(redisCacheMgr.getFromCache(DICT_KEY))
                .thenReturn(dictJson("\"_cv\":{\"orgVer\":1,\"ministryVer\":2,\"allVer\":0,\"ministryId\":\"old\"}"));

        assertTrue(versionMgr.readValidatedDict(DICT_KEY, new CbPlanCacheVersions(1, 2, 0, MINISTRY_ID)).isEmpty());
    }

    @Test
    void readValidatedDict_unstampedLegacyEntry_isMiss() {
        when(redisCacheMgr.getFromCache(DICT_KEY)).thenReturn(dictJson(null));

        assertTrue(versionMgr.readValidatedDict(DICT_KEY, new CbPlanCacheVersions(0, 0, 0, null)).isEmpty());
        assertTrue(versionMgr.readValidatedDict(DICT_KEY, ORG_ID, PLAN_YEAR).isEmpty());
        verify(redisCacheMgr, never()).mgetFromCache(any(String[].class));
    }

    @Test
    void readValidatedDict_blankOrMalformed_isMiss() {
        when(redisCacheMgr.getFromCache(DICT_KEY)).thenReturn("", "not-json{");

        assertTrue(versionMgr.readValidatedDict(DICT_KEY, new CbPlanCacheVersions(0, 0, 0, null)).isEmpty());
        assertTrue(versionMgr.readValidatedDict(DICT_KEY, new CbPlanCacheVersions(0, 0, 0, null)).isEmpty());
    }

    @Test
    void readValidatedDict_profileLess_usesMinistryFromStampAndMatches() {
        when(redisCacheMgr.getFromCache(DICT_KEY))
                .thenReturn(dictJson("\"_cv\":{\"orgVer\":1,\"ministryVer\":2,\"allVer\":0,\"ministryId\":\"" + MINISTRY_ID + "\"}"));
        when(redisCacheMgr.mgetFromCache(ORG_KEY, ALL_KEY, MINISTRY_KEY)).thenReturn(Arrays.asList("1", null, "2"));

        Optional<Map<String, Object>> result = versionMgr.readValidatedDict(DICT_KEY, ORG_ID, PLAN_YEAR);

        assertTrue(result.isPresent());
        assertFalse(result.get().containsKey(Constants.CB_PLAN_V4_CACHE_VERSIONS_KEY));
    }

    @Test
    void readValidatedDict_profileLess_ministryCounterMoved_isMiss() {
        when(redisCacheMgr.getFromCache(DICT_KEY))
                .thenReturn(dictJson("\"_cv\":{\"orgVer\":1,\"ministryVer\":2,\"allVer\":0,\"ministryId\":\"" + MINISTRY_ID + "\"}"));
        when(redisCacheMgr.mgetFromCache(ORG_KEY, ALL_KEY, MINISTRY_KEY)).thenReturn(Arrays.asList("1", null, "3"));

        assertTrue(versionMgr.readValidatedDict(DICT_KEY, ORG_ID, PLAN_YEAR).isEmpty());
    }

    @Test
    void stamp_addsCvToCopyAndLeavesOriginalUntouched() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(PLAN_YEAR, Map.of());

        Map<String, Object> stamped = versionMgr.stamp(result, new CbPlanCacheVersions(4, 0, 2, null));

        assertFalse(result.containsKey(Constants.CB_PLAN_V4_CACHE_VERSIONS_KEY));
        assertTrue(stamped.containsKey(Constants.CB_PLAN_V4_CACHE_VERSIONS_KEY));
        assertEquals(new CbPlanCacheVersions(4, 0, 2, null), CbPlanCacheVersions.fromCacheMeta(stamped));
    }

    @Test
    void bump_incrementsEveryAffectedScope() {
        versionMgr.bump(Set.of(ORG_ID), Set.of(MINISTRY_ID), true, PLAN_YEAR);

        verify(redisCacheMgr).incrementCounter(ALL_KEY);
        verify(redisCacheMgr).incrementCounter(ORG_KEY);
        verify(redisCacheMgr).incrementCounter(MINISTRY_KEY);
        verifyNoMoreInteractions(redisCacheMgr);
    }

    @Test
    void bump_skipsBlankIdsNullCollectionsAndBlankYear() {
        versionMgr.bump(List.of("", " "), null, false, PLAN_YEAR);
        versionMgr.bump(Set.of(ORG_ID), Set.of(MINISTRY_ID), true, "");

        verify(redisCacheMgr, never()).incrementCounter(anyString());
    }
}
