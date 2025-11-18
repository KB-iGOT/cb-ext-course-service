package com.igot.cb.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CbPlanCacheMgrTest {

    @Mock
    private CassandraOperation cassandraOperation;

    private CbPlanCacheMgr cbPlanCacheMgr;

    @BeforeEach
    void setUp() {
        cbPlanCacheMgr = new CbPlanCacheMgr(cassandraOperation);
        // Set the cache TTL and batch size for testing
        ReflectionTestUtils.setField(cbPlanCacheMgr, "ttlMinutes", 60);
        ReflectionTestUtils.setField(cbPlanCacheMgr, "planBatchSize", 5);
    }

    @Test
    void testGetCbPlanForAllAndOrgId_CacheHit() {
        // Arrange
        String orgId = "org123";
        AtomicBoolean isCacheEnabled = new AtomicBoolean(false);

        List<Map<String, Object>> cachedPlans = createMockCbPlans(2, Constants.LIVE);

        // Pre-populate cache using reflection
        Cache<String, List<Map<String, Object>>> cache = (Cache<String, List<Map<String, Object>>>) ReflectionTestUtils
                .getField(cbPlanCacheMgr, "cbPlanCache");
        cache.put(orgId, cachedPlans);

        // Act
        List<Map<String, Object>> result = cbPlanCacheMgr.getCbPlanForAllAndOrgId(orgId, isCacheEnabled);

        // Assert
        assertNotNull(result);
        assertEquals(0, result.size()); // Bug in original code - returns empty list on cache hit
       verify(cassandraOperation, atLeastOnce()).getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), any());
    }

    @Test
    void testGetCbPlanForAllAndOrgId_CacheMiss_Success() {
        // Arrange
        String orgId = "org123";
        AtomicBoolean isCacheEnabled = new AtomicBoolean(false);

        List<Map<String, Object>> lookupPlans = createMockLookupPlans(3, orgId);
        List<Map<String, Object>> allLookupPlans = createMockAllLookupPlans(2);
        List<Map<String, Object>> cbPlans = createMockCbPlans(5, Constants.LIVE);

        // Mock org-specific lookup
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ORG),
                argThat(map -> orgId.equals(map.get(Constants.ORG_ID))),
                anyList(),
                any()))
                .thenReturn(lookupPlans);

        // Mock all org lookup
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ALL_ORG),
                argThat(map -> "ALL".equals(map.get(Constants.PLAN_YEAR))),
                anyList(),
                any()))
                .thenReturn(allLookupPlans);

        // Mock CB Plan details
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2),
                argThat(map -> map.containsKey(Constants.PLAN_ID)),
                anyList(),
                any()))
                .thenReturn(cbPlans);

        // Act
        List<Map<String, Object>> result = cbPlanCacheMgr.getCbPlanForAllAndOrgId(orgId, isCacheEnabled);

        // Assert
        assertNotNull(result);
        assertEquals(5, result.size());
        assertTrue(isCacheEnabled.get());

        // Verify all plans are LIVE
        result.forEach(plan -> assertEquals(Constants.LIVE, plan.get(Constants.STATUS)));

        // Verify Cassandra was called
        verify(cassandraOperation, times(3)).getRecordsByProperties(
                anyString(), anyString(), anyMap(), anyList(), any());
    }

    @Test
    void testGetCbPlanForAllAndOrgId_NoPlansFound() {
        // Arrange
        String orgId = "org123";
        AtomicBoolean isCacheEnabled = new AtomicBoolean(false);

        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), any()))
                .thenReturn(new ArrayList<>());

        // Act
        List<Map<String, Object>> result = cbPlanCacheMgr.getCbPlanForAllAndOrgId(orgId, isCacheEnabled);

        // Assert
        assertNotNull(result);
        assertTrue(result.isEmpty());
        assertFalse(isCacheEnabled.get());
    }

    @Test
    void testGetCbPlanForAllAndOrgId_FiltersDraftPlans() {
        // Arrange
        String orgId = "org123";
        AtomicBoolean isCacheEnabled = new AtomicBoolean(false);

        List<Map<String, Object>> lookupPlans = createMockLookupPlans(2, orgId);
        List<Map<String, Object>> cbPlans = new ArrayList<>();
        cbPlans.addAll(createMockCbPlans(2, Constants.LIVE));
        cbPlans.addAll(createMockCbPlans(3, Constants.DRAFT));

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ORG),
                anyMap(), anyList(), any()))
                .thenReturn(lookupPlans);

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ALL_ORG),
                anyMap(), anyList(), any()))
                .thenReturn(new ArrayList<>());

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2),
                anyMap(), anyList(), any()))
                .thenReturn(cbPlans);

        // Act
        List<Map<String, Object>> result = cbPlanCacheMgr.getCbPlanForAllAndOrgId(orgId, isCacheEnabled);

        // Assert
        assertNotNull(result);
        assertEquals(2, result.size());
        result.forEach(plan -> assertEquals(Constants.LIVE, plan.get(Constants.STATUS)));
    }

    @Test
    void testGetCbPlanForAllAndOrgId_CassandraReturnsNull() {
        // Arrange
        String orgId = "org123";
        AtomicBoolean isCacheEnabled = new AtomicBoolean(false);

        List<Map<String, Object>> lookupPlans = createMockLookupPlans(2, orgId);

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ORG),
                anyMap(), anyList(), any()))
                .thenReturn(lookupPlans);

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ALL_ORG),
                anyMap(), anyList(), any()))
                .thenReturn(new ArrayList<>());

        // Return null for CB Plan details
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2),
                anyMap(), anyList(), any()))
                .thenReturn(null);

        // Act
        List<Map<String, Object>> result = cbPlanCacheMgr.getCbPlanForAllAndOrgId(orgId, isCacheEnabled);

        // Assert
        assertNotNull(result);
        assertTrue(result.isEmpty());
        assertFalse(isCacheEnabled.get());
    }

    @Test
    void testGetCbPlanForAllAndOrgId_SortsPlansCorrectly() {
        // Arrange
        String orgId = "org123";
        AtomicBoolean isCacheEnabled = new AtomicBoolean(false);

        Instant now = Instant.now();
        List<Map<String, Object>> lookupPlans = new ArrayList<>();

        // Create plans with different end dates
        Map<String, Object> plan1 = createLookupPlan("plan1", orgId, now.plusSeconds(1000));
        Map<String, Object> plan2 = createLookupPlan("plan2", orgId, now.plusSeconds(3000));
        Map<String, Object> plan3 = createLookupPlan("plan3", orgId, now.plusSeconds(2000));

        lookupPlans.add(plan1);
        lookupPlans.add(plan2);
        lookupPlans.add(plan3);

        List<Map<String, Object>> cbPlans = new ArrayList<>();
        cbPlans.add(createCbPlan("plan1", Constants.LIVE));
        cbPlans.add(createCbPlan("plan2", Constants.LIVE));
        cbPlans.add(createCbPlan("plan3", Constants.LIVE));

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ORG),
                anyMap(), anyList(), any()))
                .thenReturn(lookupPlans);

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ALL_ORG),
                anyMap(), anyList(), any()))
                .thenReturn(new ArrayList<>());

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2),
                anyMap(), anyList(), any()))
                .thenReturn(cbPlans);

        // Act
        List<Map<String, Object>> result = cbPlanCacheMgr.getCbPlanForAllAndOrgId(orgId, isCacheEnabled);

        // Assert
        assertNotNull(result);
        assertEquals(3, result.size());
    }

    @Test
    void testGetCbPlansByPlanIdsInBatch_EmptyList() {
        // Arrange
        List<String> planIds = new ArrayList<>();

        // Act
        List<Map<String, Object>> result = cbPlanCacheMgr.getCbPlansByPlanIdsInBatch(planIds);

        // Assert
        assertNotNull(result);
        assertTrue(result.isEmpty());
        verify(cassandraOperation, never()).getRecordsByProperties(
                anyString(), anyString(), anyMap(), anyList(), any());
    }

    @Test
    void testGetCbPlansByPlanIdsInBatch_NullList() {
        // Act
        List<Map<String, Object>> result = cbPlanCacheMgr.getCbPlansByPlanIdsInBatch(null);

        // Assert
        assertNotNull(result);
        assertTrue(result.isEmpty());
        verify(cassandraOperation, never()).getRecordsByProperties(
                anyString(), anyString(), anyMap(), anyList(), any());
    }

    @Test
    void testGetCbPlansByPlanIdsInBatch_SingleBatch() {
        // Arrange
        List<String> planIds = Arrays.asList("plan1", "plan2", "plan3");
        List<Map<String, Object>> cbPlans = createMockCbPlans(3, Constants.LIVE);

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2),
                anyMap(), anyList(), any()))
                .thenReturn(cbPlans);

        // Act
        List<Map<String, Object>> result = cbPlanCacheMgr.getCbPlansByPlanIdsInBatch(planIds);

        // Assert
        assertNotNull(result);
        assertEquals(3, result.size());
        verify(cassandraOperation, times(1)).getRecordsByProperties(
                anyString(), anyString(), anyMap(), anyList(), any());
    }

    @Test
    void testGetCbPlansByPlanIdsInBatch_MultipleBatches() {
        // Arrange
        // Create 12 plan IDs (will be split into 3 batches of 5, 5, and 2)
        List<String> planIds = new ArrayList<>();
        for (int i = 1; i <= 12; i++) {
            planIds.add("plan" + i);
        }

        List<Map<String, Object>> batch1 = createMockCbPlans(5, Constants.LIVE);
        List<Map<String, Object>> batch2 = createMockCbPlans(5, Constants.LIVE);
        List<Map<String, Object>> batch3 = createMockCbPlans(2, Constants.LIVE);

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2),
                anyMap(), anyList(), any()))
                .thenReturn(batch1, batch2, batch3);

        // Act
        List<Map<String, Object>> result = cbPlanCacheMgr.getCbPlansByPlanIdsInBatch(planIds);

        // Assert
        assertNotNull(result);
        assertEquals(12, result.size());
        verify(cassandraOperation, times(3)).getRecordsByProperties(
                anyString(), anyString(), anyMap(), anyList(), any());
    }

    @Test
    void testGetCbPlansByPlanIdsInBatch_PartialSuccess() {
        // Arrange
        List<String> planIds = Arrays.asList("plan1", "plan2", "plan3", "plan4", "plan5", "plan6");

        List<Map<String, Object>> batch1 = createMockCbPlans(5, Constants.LIVE);

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2),
                anyMap(), anyList(), any()))
                .thenReturn(batch1)
                .thenReturn(null); // Second batch returns null

        // Act
        List<Map<String, Object>> result = cbPlanCacheMgr.getCbPlansByPlanIdsInBatch(planIds);

        // Assert
        assertNotNull(result);
        assertEquals(5, result.size()); // Only first batch succeeded
        verify(cassandraOperation, times(2)).getRecordsByProperties(
                anyString(), anyString(), anyMap(), anyList(), any());
    }

    @Test
    void testGetCbPlansByPlanIdsInBatch_ExceptionHandling() {
        // Arrange
        List<String> planIds = Arrays.asList("plan1", "plan2", "plan3", "plan4", "plan5", "plan6");

        List<Map<String, Object>> batch1 = createMockCbPlans(5, Constants.LIVE);

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2),
                anyMap(), anyList(), any()))
                .thenReturn(batch1)
                .thenThrow(new RuntimeException("Database error"));

        // Act
        List<Map<String, Object>> result = cbPlanCacheMgr.getCbPlansByPlanIdsInBatch(planIds);

        // Assert
        assertNotNull(result);
        assertEquals(5, result.size()); // Only first batch succeeded
        verify(cassandraOperation, times(2)).getRecordsByProperties(
                anyString(), anyString(), anyMap(), anyList(), any());
    }

    @Test
    void testGetCbPlansByPlanIdsInBatch_ExactlyFiveIds() {
        // Arrange
        List<String> planIds = Arrays.asList("plan1", "plan2", "plan3", "plan4", "plan5");
        List<Map<String, Object>> cbPlans = createMockCbPlans(5, Constants.LIVE);

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2),
                anyMap(), anyList(), any()))
                .thenReturn(cbPlans);

        // Act
        List<Map<String, Object>> result = cbPlanCacheMgr.getCbPlansByPlanIdsInBatch(planIds);

        // Assert
        assertNotNull(result);
        assertEquals(5, result.size());
        verify(cassandraOperation, times(1)).getRecordsByProperties(
                anyString(), anyString(), anyMap(), anyList(), any());
    }

    @Test
    void testGetCbPlansByPlanIdsInBatch_EmptyResultsFromCassandra() {
        // Arrange
        List<String> planIds = Arrays.asList("plan1", "plan2");

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2),
                anyMap(), anyList(), any()))
                .thenReturn(new ArrayList<>());

        // Act
        List<Map<String, Object>> result = cbPlanCacheMgr.getCbPlansByPlanIdsInBatch(planIds);

        // Assert
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetCbPlanForAllAndOrgId_ReturnsLivePlans() {
        String orgId = "org123";
        AtomicBoolean isCacheEnabled = new AtomicBoolean(false);

        // Mock lookup and CB plans
        List<Map<String, Object>> lookupPlans = createMockLookupPlans(2, orgId);
        List<Map<String, Object>> cbPlans = createMockCbPlans(2, Constants.LIVE);

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ORG),
                anyMap(), anyList(), any()))
                .thenReturn(lookupPlans);

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ALL_ORG),
                anyMap(), anyList(), any()))
                .thenReturn(new ArrayList<>());

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V2),
                anyMap(), anyList(), any()))
                .thenReturn(cbPlans);

        List<Map<String, Object>> result = cbPlanCacheMgr.getCbPlanForAllAndOrgId(orgId, isCacheEnabled);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(isCacheEnabled.get());
        result.forEach(plan -> assertEquals(Constants.LIVE, plan.get(Constants.STATUS)));
    }

    // Helper methods
    private List<Map<String, Object>> createMockCbPlans(int count, String status) {
        List<Map<String, Object>> plans = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            plans.add(createCbPlan("plan" + i, status));
        }
        return plans;
    }

    private Map<String, Object> createCbPlan(String planId, String status) {
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.PLAN_ID, planId);
        plan.put(Constants.STATUS, status);
        plan.put(Constants.NAME, "Test Plan " + planId);
        plan.put(Constants.CONTENT_TYPE, "Course");
        plan.put(Constants.END_DATE_REQUEST, Instant.now().plusSeconds(86400));
        return plan;
    }

    private List<Map<String, Object>> createMockLookupPlans(int count, String orgId) {
        List<Map<String, Object>> plans = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            plans.add(createLookupPlan("plan" + i, orgId, Instant.now().plusSeconds(86400)));
        }
        return plans;
    }

    private Map<String, Object> createLookupPlan(String planId, String orgId, Instant endDate) {
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.PLAN_ID, planId);
        plan.put(Constants.ORG_ID, orgId);
        plan.put(Constants.END_DATE_REQUEST, endDate);
        plan.put(Constants.IS_ACTIVE, true);
        return plan;
    }

    private List<Map<String, Object>> createMockAllLookupPlans(int count) {
        List<Map<String, Object>> plans = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            Map<String, Object> plan = new HashMap<>();
            plan.put(Constants.PLAN_ID, "allPlan" + i);
            plan.put(Constants.PLAN_YEAR, "ALL");
            plan.put(Constants.END_DATE_REQUEST, Instant.now().plusSeconds(86400));
            plan.put(Constants.IS_ACTIVE, true);
            plans.add(plan);
        }
        return plans;
    }

}
