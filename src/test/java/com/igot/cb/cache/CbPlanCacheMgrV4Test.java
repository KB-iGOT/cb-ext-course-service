package com.igot.cb.cache;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanCacheMgrV4Test {

    private static final String ORG_ID = "org1";
    private static final String PLAN_YEAR = "2026-27";
    private static final int BATCH_SIZE = 2;
    private static final String V4_PLAN_TABLE = "cb_plan_v4_test";
    private static final String V4_LOOKUP_BY_ORG_TABLE = "cb_plan_v4_lookup_by_org_test";
    private static final String V4_LOOKUP_BY_ALL_ORG_TABLE = "cb_plan_v4_lookup_by_all_org_test";
    private static final String V4_LOOKUP_BY_MINISTRY_TABLE = "cb_plan_v4_lookup_by_ministry_test";

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private CbExtServerProperties serverProperties;

    @InjectMocks
    private CbPlanCacheMgrV4 cbPlanCacheMgrV4;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(cbPlanCacheMgrV4, "ttlMinutes", 60);
        ReflectionTestUtils.setField(cbPlanCacheMgrV4, "planBatchSize", BATCH_SIZE);
        ReflectionTestUtils.setField(cbPlanCacheMgrV4, "maxCacheSize", 5000);
        cbPlanCacheMgrV4.initCache();
    }

    private static Map<String, Object> lookupEntry(String planId, boolean isActive, Instant endDate) {
        Map<String, Object> entry = new HashMap<>();
        entry.put(Constants.PLAN_ID, planId);
        entry.put(Constants.IS_ACTIVE, isActive);
        entry.put(Constants.END_DATE_REQUEST, endDate);
        return entry;
    }

    private static Map<String, Object> fullPlan(String planId, String status) {
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.PLAN_ID, planId);
        plan.put(Constants.STATUS, status);
        return plan;
    }

    private void stubAllOrgLookup(List<Map<String, Object>> rows) {
        when(serverProperties.getCbPlanV4Keyspace()).thenReturn(Constants.KEYSPACE_SUNBIRD);
        when(serverProperties.getCbPlanV4LookupByAllOrgTable()).thenReturn(V4_LOOKUP_BY_ALL_ORG_TABLE);
        when(cassandraOperation.getRecordsByProperties(anyString(),
                eq(V4_LOOKUP_BY_ALL_ORG_TABLE), anyMap(), any(), any())).thenReturn(rows);
    }

    private void stubOrgLookup(List<Map<String, Object>> rows) {
        when(serverProperties.getCbPlanV4Keyspace()).thenReturn(Constants.KEYSPACE_SUNBIRD);
        when(serverProperties.getCbPlanV4LookupByOrgTable()).thenReturn(V4_LOOKUP_BY_ORG_TABLE);
        when(cassandraOperation.getRecordsByProperties(anyString(),
                eq(V4_LOOKUP_BY_ORG_TABLE), anyMap(), any(), any())).thenReturn(rows);
    }

    private void stubFullPlanFetch(List<Map<String, Object>> rows) {
        when(serverProperties.getCbPlanV4Keyspace()).thenReturn(Constants.KEYSPACE_SUNBIRD);
        when(serverProperties.getCbPlanV4PlanTable()).thenReturn(V4_PLAN_TABLE);
        when(cassandraOperation.getRecordsByProperties(anyString(),
                eq(V4_PLAN_TABLE), anyMap(), any(), any())).thenReturn(rows);
    }

    private void stubMinistryLookup(List<Map<String, Object>> rows) {
        when(serverProperties.getCbPlanV4Keyspace()).thenReturn(Constants.KEYSPACE_SUNBIRD);
        when(serverProperties.getCbPlanV4LookupByMinistryOrStateIdTable()).thenReturn(V4_LOOKUP_BY_MINISTRY_TABLE);
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(V4_LOOKUP_BY_MINISTRY_TABLE),
                anyMap(), any(), any())).thenReturn(rows);
    }

    @Test
    void testInitCacheCreatesBothCaches() {
        assertNotNull(ReflectionTestUtils.getField(cbPlanCacheMgrV4, "cbPlanCache"));
        assertNotNull(ReflectionTestUtils.getField(cbPlanCacheMgrV4, "planIdCache"));
    }

    // ---------------- getCbPlanForAllOrgs ----------------

    @Test
    void testGetCbPlanForAllOrgsLoadsFromCassandraOnMiss() {
        stubAllOrgLookup(List.of(lookupEntry("plan1", true, Instant.EPOCH)));
        List<Map<String, Object>> result = cbPlanCacheMgrV4.getCbPlanForAllOrgs(PLAN_YEAR);
        assertEquals(1, result.size());
        assertEquals("plan1", result.get(0).get(Constants.PLAN_ID));
    }

    @Test
    void testGetCbPlanForAllOrgsFiltersInactiveEntries() {
        stubAllOrgLookup(List.of(
                lookupEntry("plan1", true, Instant.EPOCH),
                lookupEntry("plan2", false, Instant.EPOCH)));
        List<Map<String, Object>> result = cbPlanCacheMgrV4.getCbPlanForAllOrgs(PLAN_YEAR);
        assertEquals(1, result.size());
        assertEquals("plan1", result.get(0).get(Constants.PLAN_ID));
    }

    @Test
    void testGetCbPlanForAllOrgsServesSecondCallFromCache() {
        stubAllOrgLookup(List.of(lookupEntry("plan1", true, Instant.EPOCH)));
        cbPlanCacheMgrV4.getCbPlanForAllOrgs(PLAN_YEAR);
        List<Map<String, Object>> second = cbPlanCacheMgrV4.getCbPlanForAllOrgs(PLAN_YEAR);
        assertEquals(1, second.size());
        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(),
                eq(V4_LOOKUP_BY_ALL_ORG_TABLE), anyMap(), any(), any());
    }

    @Test
    void testGetCbPlanForAllOrgsCachesPerPlanYear() {
        stubAllOrgLookup(List.of(lookupEntry("plan1", true, Instant.EPOCH)));
        cbPlanCacheMgrV4.getCbPlanForAllOrgs(PLAN_YEAR);
        cbPlanCacheMgrV4.getCbPlanForAllOrgs("2025-26");
        verify(cassandraOperation, times(2)).getRecordsByProperties(anyString(),
                eq(V4_LOOKUP_BY_ALL_ORG_TABLE), anyMap(), any(), any());
    }

    @Test
    void testGetCbPlanForAllOrgsReturnsEmptyWhenCassandraReturnsNull() {
        stubAllOrgLookup(null);
        assertTrue(cbPlanCacheMgrV4.getCbPlanForAllOrgs(PLAN_YEAR).isEmpty());
    }

    // ---------------- getCbPlanForOrgId ----------------

    @Test
    void testGetCbPlanForOrgIdLoadsFromCassandraOnMiss() {
        stubOrgLookup(List.of(lookupEntry("plan1", true, Instant.EPOCH)));
        List<Map<String, Object>> result = cbPlanCacheMgrV4.getCbPlanForOrgId(ORG_ID, PLAN_YEAR);
        assertEquals(1, result.size());
    }

    @Test
    void testGetCbPlanForOrgIdFiltersInactiveEntries() {
        stubOrgLookup(List.of(
                lookupEntry("plan1", true, Instant.EPOCH),
                lookupEntry("plan2", false, Instant.EPOCH)));
        assertEquals(1, cbPlanCacheMgrV4.getCbPlanForOrgId(ORG_ID, PLAN_YEAR).size());
    }

    @Test
    void testGetCbPlanForOrgIdServesSecondCallFromCache() {
        stubOrgLookup(List.of(lookupEntry("plan1", true, Instant.EPOCH)));
        cbPlanCacheMgrV4.getCbPlanForOrgId(ORG_ID, PLAN_YEAR);
        cbPlanCacheMgrV4.getCbPlanForOrgId(ORG_ID, PLAN_YEAR);
        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(),
                eq(V4_LOOKUP_BY_ORG_TABLE), anyMap(), any(), any());
    }

    @Test
    void testGetCbPlanForOrgIdCachesPerOrg() {
        stubOrgLookup(List.of(lookupEntry("plan1", true, Instant.EPOCH)));
        cbPlanCacheMgrV4.getCbPlanForOrgId(ORG_ID, PLAN_YEAR);
        cbPlanCacheMgrV4.getCbPlanForOrgId("org2", PLAN_YEAR);
        verify(cassandraOperation, times(2)).getRecordsByProperties(anyString(),
                eq(V4_LOOKUP_BY_ORG_TABLE), anyMap(), any(), any());
    }

    @Test
    void testGetCbPlanForOrgIdReturnsEmptyWhenCassandraReturnsNull() {
        stubOrgLookup(null);
        assertTrue(cbPlanCacheMgrV4.getCbPlanForOrgId(ORG_ID, PLAN_YEAR).isEmpty());
    }

    // ---------------- getCbPlanForAllAndOrgId ----------------

    @Test
    void testGetCbPlanForAllAndOrgIdCombinesOrgAndAllOrgPlans() {
        stubOrgLookup(List.of(lookupEntry("plan1", true, Instant.parse("2026-12-31T00:00:00Z"))));
        stubAllOrgLookup(List.of(lookupEntry("plan2", true, Instant.parse("2026-06-30T00:00:00Z"))));
        stubFullPlanFetch(List.of(fullPlan("plan1", Constants.LIVE), fullPlan("plan2", Constants.LIVE)));
        AtomicBoolean isCacheEnabled = new AtomicBoolean(false);
        List<Map<String, Object>> result =
                cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(ORG_ID, PLAN_YEAR, isCacheEnabled);
        assertEquals(2, result.size());
        assertTrue(isCacheEnabled.get());
    }

    @Test
    void testGetCbPlanForAllAndOrgIdKeepsOnlyLivePlans() {
        stubOrgLookup(List.of(lookupEntry("plan1", true, Instant.EPOCH)));
        stubAllOrgLookup(List.of());
        stubFullPlanFetch(List.of(fullPlan("plan1", Constants.DRAFT)));
        List<Map<String, Object>> result =
                cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(ORG_ID, PLAN_YEAR, new AtomicBoolean(false));
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetCbPlanForAllAndOrgIdDeduplicatesByPlanId() {
        Instant endDate = Instant.parse("2026-12-31T00:00:00Z");
        stubOrgLookup(List.of(lookupEntry("plan1", true, endDate)));
        stubAllOrgLookup(List.of(lookupEntry("plan1", true, endDate)));
        stubFullPlanFetch(List.of(fullPlan("plan1", Constants.LIVE)));
        List<Map<String, Object>> result =
                cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(ORG_ID, PLAN_YEAR, new AtomicBoolean(false));
        assertEquals(1, result.size());
    }

    @Test
    void testGetCbPlanForAllAndOrgIdSkipsEntriesMissingEndDateOrPlanId() {
        Map<String, Object> noEndDate = lookupEntry("plan1", true, null);
        Map<String, Object> noPlanId = lookupEntry(null, true, Instant.EPOCH);
        stubOrgLookup(List.of(noEndDate, noPlanId));
        stubAllOrgLookup(List.of());
        List<Map<String, Object>> result =
                cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(ORG_ID, PLAN_YEAR, new AtomicBoolean(false));
        assertTrue(result.isEmpty());
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(),
                eq(V4_PLAN_TABLE), anyMap(), any(), any());
    }

    @Test
    void testGetCbPlanForAllAndOrgIdReturnsEmptyWhenNoPlansFound() {
        stubOrgLookup(List.of());
        stubAllOrgLookup(List.of());
        AtomicBoolean isCacheEnabled = new AtomicBoolean(false);
        List<Map<String, Object>> result =
                cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(ORG_ID, PLAN_YEAR, isCacheEnabled);
        assertTrue(result.isEmpty());
        assertFalse(isCacheEnabled.get());
    }

    @Test
    void testGetCbPlanForAllAndOrgIdServesSecondCallFromCache() {
        stubOrgLookup(List.of(lookupEntry("plan1", true, Instant.EPOCH)));
        stubAllOrgLookup(List.of());
        stubFullPlanFetch(List.of(fullPlan("plan1", Constants.LIVE)));
        cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(ORG_ID, PLAN_YEAR, new AtomicBoolean(false));
        AtomicBoolean secondCallFlag = new AtomicBoolean(false);
        List<Map<String, Object>> second =
                cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(ORG_ID, PLAN_YEAR, secondCallFlag);
        assertEquals(1, second.size());
        assertTrue(secondCallFlag.get());
        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(),
                eq(V4_LOOKUP_BY_ORG_TABLE), anyMap(), any(), any());
    }

    @Test
    void testGetCbPlanForAllAndOrgIdNeverServesEmptyResultFromItsOwnCache() {
        stubOrgLookup(List.of());
        stubAllOrgLookup(List.of());
        AtomicBoolean firstCallFlag = new AtomicBoolean(false);
        AtomicBoolean secondCallFlag = new AtomicBoolean(false);
        cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(ORG_ID, PLAN_YEAR, firstCallFlag);
        List<Map<String, Object>> second =
                cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(ORG_ID, PLAN_YEAR, secondCallFlag);
        assertTrue(second.isEmpty());
        assertFalse(firstCallFlag.get());
        assertFalse(secondCallFlag.get());
        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(),
                eq(V4_LOOKUP_BY_ORG_TABLE), anyMap(), any(), any());
    }

    // ---------------- getCbPlansByPlanIdsInBatch ----------------

    @Test
    void testGetCbPlansByPlanIdsInBatchReturnsEmptyForEmptyInput() {
        assertTrue(cbPlanCacheMgrV4.getCbPlansByPlanIdsInBatch(List.of()).isEmpty());
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    @Test
    void testGetCbPlansByPlanIdsInBatchReturnsEmptyForNullInput() {
        assertTrue(cbPlanCacheMgrV4.getCbPlansByPlanIdsInBatch(null).isEmpty());
    }

    @Test
    void testGetCbPlansByPlanIdsInBatchFetchesFromCassandra() {
        stubFullPlanFetch(List.of(fullPlan("plan1", Constants.LIVE)));
        List<Map<String, Object>> result = cbPlanCacheMgrV4.getCbPlansByPlanIdsInBatch(List.of("plan1"));
        assertEquals(1, result.size());
        assertEquals("plan1", result.get(0).get(Constants.PLAN_ID));
    }

    @Test
    void testGetCbPlansByPlanIdsInBatchServesSecondCallFromPlanIdCache() {
        stubFullPlanFetch(List.of(fullPlan("plan1", Constants.LIVE)));
        cbPlanCacheMgrV4.getCbPlansByPlanIdsInBatch(List.of("plan1"));
        List<Map<String, Object>> second = cbPlanCacheMgrV4.getCbPlansByPlanIdsInBatch(List.of("plan1"));
        assertEquals(1, second.size());
        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(),
                eq(V4_PLAN_TABLE), anyMap(), any(), any());
    }

    @Test
    void testGetCbPlansByPlanIdsInBatchSplitsIntoConfiguredBatchSize() {
        when(serverProperties.getCbPlanV4Keyspace()).thenReturn(Constants.KEYSPACE_SUNBIRD);
        when(serverProperties.getCbPlanV4PlanTable()).thenReturn(V4_PLAN_TABLE);
        List<String> planIds = List.of("plan1", "plan2", "plan3", "plan4", "plan5");
        when(cassandraOperation.getRecordsByProperties(anyString(), eq(V4_PLAN_TABLE),
                anyMap(), any(), any())).thenReturn(new ArrayList<>());
        cbPlanCacheMgrV4.getCbPlansByPlanIdsInBatch(planIds);
        verify(cassandraOperation, times(3)).getRecordsByProperties(anyString(),
                eq(V4_PLAN_TABLE), anyMap(), any(), any());
    }

    @Test
    void testGetCbPlansByPlanIdsInBatchMixesCachedAndFetchedPlans() {
        stubFullPlanFetch(List.of(fullPlan("plan1", Constants.LIVE)));
        cbPlanCacheMgrV4.getCbPlansByPlanIdsInBatch(List.of("plan1"));
        when(cassandraOperation.getRecordsByProperties(anyString(), eq(V4_PLAN_TABLE),
                anyMap(), any(), any())).thenReturn(List.of(fullPlan("plan2", Constants.LIVE)));
        List<Map<String, Object>> result = cbPlanCacheMgrV4.getCbPlansByPlanIdsInBatch(List.of("plan1", "plan2"));
        assertEquals(2, result.size());
    }

    @Test
    void testGetCbPlansByPlanIdsInBatchSwallowsCassandraException() {
        when(serverProperties.getCbPlanV4Keyspace()).thenReturn(Constants.KEYSPACE_SUNBIRD);
        when(serverProperties.getCbPlanV4PlanTable()).thenReturn(V4_PLAN_TABLE);
        when(cassandraOperation.getRecordsByProperties(anyString(), eq(V4_PLAN_TABLE),
                anyMap(), any(), any())).thenThrow(new RuntimeException("cassandra down"));
        assertTrue(cbPlanCacheMgrV4.getCbPlansByPlanIdsInBatch(List.of("plan1")).isEmpty());
    }

    @Test
    void testGetCbPlansByPlanIdsInBatchContinuesAfterFailedBatch() {
        when(serverProperties.getCbPlanV4Keyspace()).thenReturn(Constants.KEYSPACE_SUNBIRD);
        when(serverProperties.getCbPlanV4PlanTable()).thenReturn(V4_PLAN_TABLE);
        when(cassandraOperation.getRecordsByProperties(anyString(), eq(V4_PLAN_TABLE),
                anyMap(), any(), any()))
                .thenThrow(new RuntimeException("cassandra down"))
                .thenReturn(List.of(fullPlan("plan3", Constants.LIVE)));
        List<Map<String, Object>> result =
                cbPlanCacheMgrV4.getCbPlansByPlanIdsInBatch(List.of("plan1", "plan2", "plan3"));
        assertEquals(1, result.size());
        assertEquals("plan3", result.get(0).get(Constants.PLAN_ID));
    }

    @Test
    void testGetCbPlansByPlanIdsInBatchSkipsCachingEntriesWithoutPlanId() {
        Map<String, Object> planWithoutId = new HashMap<>();
        planWithoutId.put(Constants.STATUS, Constants.LIVE);
        stubFullPlanFetch(List.of(planWithoutId));
        cbPlanCacheMgrV4.getCbPlansByPlanIdsInBatch(List.of("plan1"));
        cbPlanCacheMgrV4.getCbPlansByPlanIdsInBatch(List.of("plan1"));
        verify(cassandraOperation, times(2)).getRecordsByProperties(anyString(),
                eq(V4_PLAN_TABLE), anyMap(), any(), any());
    }

    @Test
    void testConstructor() {
        assertNotNull(new CbPlanCacheMgrV4(cassandraOperation, serverProperties));
    }

    // ---------------- getCbPlanForMinistryOrStateId ----------------

    @Test
    void testGetCbPlanForMinistryOrStateIdFetchesFromCassandra() {
        List<Map<String, Object>> lookupEntries = List.of(
                lookupEntry("plan1", true, null),
                lookupEntry("plan2", true, null)
        );
        stubMinistryLookup(lookupEntries);
        List<Map<String, Object>> result = cbPlanCacheMgrV4.getCbPlanForMinistryOrStateId("ORG_001", PLAN_YEAR);
        assertEquals(2, result.size());
        assertEquals("plan1", result.get(0).get(Constants.PLAN_ID));
    }

    @Test
    void testGetCbPlanForMinistryOrStateIdFiltersInactivePlans() {
        List<Map<String, Object>> lookupEntries = List.of(
                lookupEntry("plan1", true, null),
                lookupEntry("plan2", false, null),
                lookupEntry("plan3", true, null)
        );
        stubMinistryLookup(lookupEntries);
        List<Map<String, Object>> result = cbPlanCacheMgrV4.getCbPlanForMinistryOrStateId("ORG_001", PLAN_YEAR);
        assertEquals(2, result.size());
        assertEquals("plan1", result.get(0).get(Constants.PLAN_ID));
        assertEquals("plan3", result.get(1).get(Constants.PLAN_ID));
    }

    @Test
    void testGetCbPlanForMinistryOrStateIdHandlesCassandraReturningNull() {
        stubMinistryLookup(null);
        List<Map<String, Object>> result = cbPlanCacheMgrV4.getCbPlanForMinistryOrStateId("ORG_001", PLAN_YEAR);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetCbPlanForMinistryOrStateIdCachesResult() {
        stubMinistryLookup(List.of(lookupEntry("plan1", true, null)));
        cbPlanCacheMgrV4.getCbPlanForMinistryOrStateId("ORG_001", PLAN_YEAR);
        List<Map<String, Object>> second = cbPlanCacheMgrV4.getCbPlanForMinistryOrStateId("ORG_001", PLAN_YEAR);
        assertEquals(1, second.size());
        verify(cassandraOperation, times(1)).getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(V4_LOOKUP_BY_MINISTRY_TABLE),
                anyMap(), any(), any());
    }

    @Test
    void testGetCbPlanForMinistryOrStateIdCachesByMinistryIdAndYear() {
        when(serverProperties.getCbPlanV4Keyspace()).thenReturn(Constants.KEYSPACE_SUNBIRD);
        when(serverProperties.getCbPlanV4LookupByMinistryOrStateIdTable()).thenReturn(V4_LOOKUP_BY_MINISTRY_TABLE);
        List<Map<String, Object>> lookupEntries1 = List.of(lookupEntry("plan1", true, null));
        List<Map<String, Object>> lookupEntries2 = List.of(lookupEntry("plan2", true, null));
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(V4_LOOKUP_BY_MINISTRY_TABLE),
                anyMap(), any(), any()))
                .thenReturn(lookupEntries1)
                .thenReturn(lookupEntries2);
        cbPlanCacheMgrV4.getCbPlanForMinistryOrStateId("ORG_001", PLAN_YEAR);
        List<Map<String, Object>> result2 = cbPlanCacheMgrV4.getCbPlanForMinistryOrStateId("ORG_002", PLAN_YEAR);
        assertEquals("plan2", result2.get(0).get(Constants.PLAN_ID));
        verify(cassandraOperation, times(2)).getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(V4_LOOKUP_BY_MINISTRY_TABLE),
                anyMap(), any(), any());
    }

    // ---------------- invalidatePlan ----------------

    private static Map<String, Object> fullPlanWithCa(String planId, String caLinkedId) {
        Map<String, Object> plan = fullPlan(planId, Constants.LIVE);
        plan.put(Constants.CA_LINKED_ID_DB, caLinkedId);
        return plan;
    }

    @Test
    void testInvalidatePlanEvictsOnlyListsHoldingThatPlanAndKeepsLookupLists() {
        stubOrgLookup(List.of());
        when(cassandraOperation.getRecordsByProperties(anyString(), eq(V4_LOOKUP_BY_ORG_TABLE),
                eq(Map.of(Constants.ORG_ID, ORG_ID, Constants.PLAN_YEAR, PLAN_YEAR)), any(), any()))
                .thenReturn(List.of(lookupEntry("plan1", true, Instant.parse("2026-12-31T00:00:00Z"))));
        stubAllOrgLookup(List.of(lookupEntry("plan2", true, Instant.parse("2026-06-30T00:00:00Z"))));
        stubFullPlanFetch(List.of(fullPlanWithCa("plan1", null), fullPlanWithCa("plan2", null)));

        cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(ORG_ID, PLAN_YEAR, new AtomicBoolean(false));
        cbPlanCacheMgrV4.getCbPlanForAllAndOrgId("org2", PLAN_YEAR, new AtomicBoolean(false));
        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(), eq(V4_PLAN_TABLE),
                anyMap(), any(), any());

        cbPlanCacheMgrV4.invalidatePlan("plan1");

        cbPlanCacheMgrV4.getCbPlanForAllAndOrgId("org2", PLAN_YEAR, new AtomicBoolean(false));
        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(), eq(V4_PLAN_TABLE),
                anyMap(), any(), any());
        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(),
                eq(V4_LOOKUP_BY_ALL_ORG_TABLE), anyMap(), any(), any());

        cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(ORG_ID, PLAN_YEAR, new AtomicBoolean(false));
        verify(cassandraOperation, times(2)).getRecordsByProperties(anyString(), eq(V4_PLAN_TABLE),
                anyMap(), any(), any());
        verify(cassandraOperation).getRecordsByProperties(anyString(), eq(V4_PLAN_TABLE),
                eq(Map.of(Constants.PLAN_ID, List.of("plan1"))), any(), any());
        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(),
                eq(V4_LOOKUP_BY_ALL_ORG_TABLE), anyMap(), any(), any());
        verify(cassandraOperation, times(2)).getRecordsByProperties(anyString(),
                eq(V4_LOOKUP_BY_ORG_TABLE), anyMap(), any(), any());
    }

    @Test
    void testInvalidatePlanWithUnknownOrNullPlanIdIsNoOp() {
        stubOrgLookup(List.of(lookupEntry("plan1", true, Instant.EPOCH)));
        stubAllOrgLookup(List.of());
        stubFullPlanFetch(List.of(fullPlanWithCa("plan1", "do_ca")));
        cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(ORG_ID, PLAN_YEAR, new AtomicBoolean(false));

        cbPlanCacheMgrV4.invalidatePlan("does-not-exist");
        cbPlanCacheMgrV4.invalidatePlan(null);

        cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(ORG_ID, PLAN_YEAR, new AtomicBoolean(false));
        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(), eq(V4_PLAN_TABLE),
                anyMap(), any(), any());
    }
}
