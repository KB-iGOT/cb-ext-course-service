package com.igot.cb.cache;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*;

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
        ReflectionTestUtils.setField(cacheMgr, "promotionalContentCacheTtlMiliSeconds", 3600000);
    }

    @Test
    void testGetAccessSettingRules_EmptyCache_LoadsFromCassandra() {
        List<Map<String, Object>> cassandraRecords = createCassandraRecords(3);
        when(cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD_COURSE,
                Constants.PROMOTIONAL_CONTENT_RULES,
                null, null, 500
        )).thenReturn(cassandraRecords);
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertEquals(3, result.size());
    }

    @Test
    void testGetAccessSettingRules_ReturnsEmptyCollection_WhenNoDataAvailable() {
        when(cassandraOperation.getRecordsByProperties(
                anyString(), anyString(), any(), any(), anyInt()
        )).thenReturn(List.of());
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetAccessSettingRules_ReturnsCachedData_OnSubsequentCalls() {
        List<Map<String, Object>> cassandraRecords = createCassandraRecords(2);
        when(cassandraOperation.getRecordsByProperties(
                anyString(), anyString(), any(), any(), anyInt()
        )).thenReturn(cassandraRecords);
        Collection<CachedAccessSettingRule> result1 = cacheMgr.getAccessSettingRules();
        assertEquals(2, result1.size());
        reset(cassandraOperation);
        Collection<CachedAccessSettingRule> result2 = cacheMgr.getAccessSettingRules();
        assertEquals(2, result2.size());
        verify(cassandraOperation, never()).getRecordsByProperties(
                anyString(), anyString(), any(), any(), anyInt()
        );
    }

    @Test
    void testLoadAccessSettingRules_HandlesException() {
        when(cassandraOperation.getRecordsByProperties(
                anyString(), anyString(), any(), any(), anyInt()
        )).thenThrow(new RuntimeException("Database error"));
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testProcessAndCacheRule_WithValidContextData() {
        List<Map<String, Object>> cassandraRecords = List.of(
                createCassandraRecordWithFullData("do_test_123", "Course")
        );
        when(cassandraOperation.getRecordsByProperties(
                anyString(), anyString(), any(), any(), anyInt()
        )).thenReturn(cassandraRecords);
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
        assertInstanceOf(BitSet.class, criteriaValue);
        BitSet bitSet = (BitSet) criteriaValue;
        assertTrue(bitSet.get(1));
        assertTrue(bitSet.get(2));
        assertTrue(bitSet.get(3));
    }

    @Test
    void testProcessCriteria_WithNonIntegerValues() {
        String contextData = "{\"accessControlId\":{\"version\":1,\"userGroups\":[{\"userGroupId\":\"group-1\",\"userGroupName\":\"Group 1\",\"userGroupCriteriaList\":[{\"criteriaKey\":\"designation\",\"criteriaValue\":[\"1\",\"invalid\",\"3\",\"not-a-number\",\"5\"]}]}]}}";
        Map<String, Object> nonIntegerRecord = createCassandraRecord("do_non_integer", "Course", contextData);
        when(cassandraOperation.getRecordsByProperties(
                anyString(), anyString(), any(), any(), anyInt()
        )).thenReturn(List.of(nonIntegerRecord));
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertEquals(1, result.size());
        CachedAccessSettingRule rule = result.iterator().next();
        @SuppressWarnings("unchecked")
        Map<String, Object> accessControl = (Map<String, Object>) rule.getContextData().get("accessControlId");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> userGroups = (List<Map<String, Object>>) accessControl.get("userGroups");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> criteriaList = (List<Map<String, Object>>) userGroups.get(0).get("userGroupCriteriaList");
        BitSet bitSet = (BitSet) criteriaList.get(0).get("criteriaValue");
        assertTrue(bitSet.get(1));
        assertTrue(bitSet.get(3));
        assertTrue(bitSet.get(5));
        assertEquals(3, bitSet.cardinality());
    }

    @Test
    void testProcessContextData_WithNoAccessControl() {
        Map<String, Object> noAccessControlRecord = createCassandraRecord("do_no_access", "Course", "{}");
        when(cassandraOperation.getRecordsByProperties(
                anyString(), anyString(), any(), any(), anyInt()
        )).thenReturn(List.of(noAccessControlRecord));
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertEquals(1, result.size());
    }

    @Test
    void testCreateBitSetForAttribute_ValidIntegers() {
        List<Integer> values = Arrays.asList(1, 3, 5, 10);
        BitSet result = cacheMgr.createBitSetForAttribute(values);
        assertNotNull(result);
        assertTrue(result.get(1));
        assertTrue(result.get(3));
        assertTrue(result.get(5));
        assertTrue(result.get(10));
    }

    @Test
    void testCreateBitSetForAttribute_EmptyCollection() {
        List<Integer> values = List.of();
        BitSet result = cacheMgr.createBitSetForAttribute(values);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testCreateBitSetForAttribute_DuplicateValues() {
        List<Integer> values = Arrays.asList(1, 1, 2, 2, 3);
        BitSet result = cacheMgr.createBitSetForAttribute(values);
        assertNotNull(result);
        assertTrue(result.get(1));
        assertTrue(result.get(2));
        assertTrue(result.get(3));
        assertEquals(3, result.cardinality());
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
}
