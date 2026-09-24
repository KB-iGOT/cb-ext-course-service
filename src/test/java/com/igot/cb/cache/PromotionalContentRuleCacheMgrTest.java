package com.igot.cb.cache;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.roaringbitmap.RoaringBitmap;
import java.util.function.Consumer;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.CachedAccessSettingRule;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
class PromotionalContentRuleCacheMgrTest {

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private CbExtServerProperties properties;

    private PromotionalContentRuleCacheMgr cacheMgr;

    @BeforeEach
    void setup() {
        lenient().when(properties.getPromotionalContentCacheMaxSize()).thenReturn(5000);
        lenient().when(properties.isPromotionalContentCacheWarmingEnabled()).thenReturn(false);
        lenient().when(properties.getPromotionalContentCacheBatchSize()).thenReturn(500);
        lenient().when(properties.getPromotionalContentCacheMaxQuerySize()).thenReturn(5000);
        cacheMgr = new PromotionalContentRuleCacheMgr(cassandraOperation, properties);
        ReflectionTestUtils.setField(cacheMgr, "promotionalContentRulesCacheExpiryMs", 3600000);
    }

    @Test
    void testGetAccessSettingRules_EmptyCache_LoadsFromCassandra() {
        List<Map<String, Object>> cassandraRecords = createCassandraRecords(3);
        mockForEachPromoRules(cassandraRecords);
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertEquals(3, result.size());
    }

    @Test
    void testGetAccessSettingRules_ReturnsEmptyCollection_WhenNoDataAvailable() {
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetAccessSettingRules_ReturnsCachedData_OnSubsequentCalls() {
        List<Map<String, Object>> cassandraRecords = createCassandraRecords(2);
        mockForEachPromoRules(cassandraRecords);
        Collection<CachedAccessSettingRule> result1 = cacheMgr.getAccessSettingRules();
        assertEquals(2, result1.size());
        reset(cassandraOperation);
        Collection<CachedAccessSettingRule> result2 = cacheMgr.getAccessSettingRules();
        assertEquals(2, result2.size());
        verify(cassandraOperation, never()).forEachRecordByProperties(
                anyString(), anyString(), any(), any(), any(), any(), any()
        );
    }

    @Test
    void testLoadAccessSettingRules_HandlesException() {
        doThrow(new RuntimeException("Database error"))
                .when(cassandraOperation)
                .forEachRecordByProperties(anyString(), anyString(), any(), any(), any(), any(), any());
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testProcessAndCacheRule_WithValidContextData() {
        List<Map<String, Object>> cassandraRecords = List.of(
                createCassandraRecordWithFullData("do_test_123", "Course")
        );
        mockForEachPromoRules(cassandraRecords);
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertEquals(1, result.size());
        CachedAccessSettingRule rule = result.iterator().next();
        Map<String, Object> contextData = rule.getContextData();
        @SuppressWarnings("unchecked")
        Map<String, Object> accessControl = (Map<String, Object>) contextData.get("accessControlId");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> userGroups = (List<Map<String, Object>>) accessControl.get("userGroups");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> criteriaList = (List<Map<String, Object>>) userGroups.get(0).get("userGroupCriteriaList");
        Object criteriaValue = criteriaList.get(0).get("criteriaValue");
        assertInstanceOf(RoaringBitmap.class, criteriaValue);
        RoaringBitmap bitSet = (RoaringBitmap) criteriaValue;
        assertTrue(bitSet.contains(1));
        assertTrue(bitSet.contains(2));
        assertTrue(bitSet.contains(3));
    }

    @Test
    void testProcessCriteria_WithNonIntegerValues() {
        String contextData = "{\"accessControlId\":{\"version\":1,\"userGroups\":[{\"userGroupId\":\"group-1\",\"userGroupName\":\"Group 1\",\"userGroupCriteriaList\":[{\"criteriaKey\":\"designation\",\"criteriaValue\":[\"1\",\"invalid\",\"3\",\"not-a-number\",\"5\"]}]}]}}";
        Map<String, Object> nonIntegerRecord = createCassandraRecord("do_non_integer", "Course", contextData);
        mockForEachPromoRules(List.of(nonIntegerRecord));
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertEquals(1, result.size());
        CachedAccessSettingRule rule = result.iterator().next();
        @SuppressWarnings("unchecked")
        Map<String, Object> accessControl = (Map<String, Object>) rule.getContextData().get("accessControlId");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> userGroups = (List<Map<String, Object>>) accessControl.get("userGroups");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> criteriaList = (List<Map<String, Object>>) userGroups.get(0).get("userGroupCriteriaList");
        RoaringBitmap bitSet = (RoaringBitmap) criteriaList.get(0).get("criteriaValue");
        assertTrue(bitSet.contains(1));
        assertTrue(bitSet.contains(3));
        assertTrue(bitSet.contains(5));
        assertEquals(3, bitSet.getCardinality());
    }

    @Test
    void testProcessContextData_WithNoAccessControl() {
        Map<String, Object> noAccessControlRecord = createCassandraRecord("do_no_access", "Course", "{}");
        mockForEachPromoRules(List.of(noAccessControlRecord));
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertEquals(1, result.size());
    }

    @Test
    void testCreateBitmapForAttribute_ValidIntegers() {
        List<Integer> values = Arrays.asList(1, 3, 5, 10);
        RoaringBitmap result = cacheMgr.createBitmapForAttribute(values);
        assertNotNull(result);
        assertTrue(result.contains(1));
        assertTrue(result.contains(3));
        assertTrue(result.contains(5));
        assertTrue(result.contains(10));
    }

    @Test
    void testCreateBitmapForAttribute_EmptyCollection() {
        List<Integer> values = List.of();
        RoaringBitmap result = cacheMgr.createBitmapForAttribute(values);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testCreateBitmapForAttribute_DuplicateValues() {
        List<Integer> values = Arrays.asList(1, 1, 2, 2, 3);
        RoaringBitmap result = cacheMgr.createBitmapForAttribute(values);
        assertNotNull(result);
        assertTrue(result.contains(1));
        assertTrue(result.contains(2));
        assertTrue(result.contains(3));
        assertEquals(3, result.getCardinality());
    }

    @Test
    void testCreateBitmapForAttribute_LargeAndNegativeValues() {
        List<Integer> values = Arrays.asList(1, -1, 1145607574, 2000000000, 3);
        RoaringBitmap result = cacheMgr.createBitmapForAttribute(values);
        assertNotNull(result);
        assertTrue(result.contains(1));
        assertTrue(result.contains(3));
        assertTrue(result.contains(1145607574));
        assertTrue(result.contains(2000000000));
        assertFalse(result.contains(-1));
        assertEquals(4, result.getCardinality());
    }

    @Test
    void testProcessAndCacheRule_WithLargeValues_RetainsLargeIds() {
        String contextData = "{\"accessControlId\":{\"version\":1,\"userGroups\":[{\"userGroupId\":\"group-1\",\"userGroupName\":\"Group 1\",\"userGroupCriteriaList\":[{\"criteriaKey\":\"designation\",\"criteriaValue\":[\"1\",\"1145607574\",\"-5\",\"invalid\"]}]}]}}";
        List<Map<String, Object>> records = List.of(createCassandraRecord("do_promo_large", "Course", contextData));
        mockForEachPromoRules(records);
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertEquals(1, result.size());
        CachedAccessSettingRule rule = result.iterator().next();
        Map<String, Object> accessControl = (Map<String, Object>) rule.getContextData().get("accessControlId");
        List<Map<String, Object>> userGroups = (List<Map<String, Object>>) accessControl.get("userGroups");
        List<Map<String, Object>> criteriaList = (List<Map<String, Object>>) userGroups.get(0).get("userGroupCriteriaList");
        RoaringBitmap bitSet = (RoaringBitmap) criteriaList.get(0).get("criteriaValue");
        assertNotNull(bitSet);
        assertTrue(bitSet.contains(1));
        assertTrue(bitSet.contains(1145607574));
        assertEquals(2, bitSet.getCardinality());
    }

    private List<Map<String, Object>> createCassandraRecords(int count) {
        List<Map<String, Object>> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(createCassandraRecordWithFullData("do_promo_" + i, "Course"));
        }
        return records;
    }

    private Map<String, Object> createCassandraRecord(String contextId, String contextIdType, String contextData) {
        Map<String, Object> createCassandraRecord = new HashMap<>();
        createCassandraRecord.put("contextId", contextId);
        createCassandraRecord.put("contextIdType", contextIdType);
        createCassandraRecord.put("contextData", contextData);
        createCassandraRecord.put("isArchived", false);
        return createCassandraRecord;
    }

    private Map<String, Object> createCassandraRecordWithFullData(String contextId, String contextIdType) {
        String contextData = "{\"accessControlId\":{\"version\":1,\"userGroups\":[{\"userGroupId\":\"group-1\",\"userGroupName\":\"Group 1\",\"userGroupCriteriaList\":[{\"criteriaKey\":\"designation\",\"criteriaValue\":[\"1\",\"2\",\"3\"]}]}]}}";
        return createCassandraRecord(contextId, contextIdType, contextData);
    }

    @SuppressWarnings("unchecked")
    private void mockForEachPromoRules(List<Map<String, Object>> records) {
        doAnswer(invocation -> {
            Consumer<Map<String, Object>> consumer = invocation.getArgument(6);
            records.forEach(consumer);
            return null;
        }).when(cassandraOperation).forEachRecordByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.PROMOTIONAL_CONTENT_RULES),
                isNull(),
                isNull(),
                any(),
                any(),
                any()
        );
    }
}
