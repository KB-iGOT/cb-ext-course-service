package com.igot.cb.cache;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
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

    @Mock
    private RedisCacheMgr redisCacheMgr;

    private PromotionalContentRuleCacheMgr cacheMgr;

    @BeforeEach
    void setup() {
        lenient().when(properties.getPromotionalContentCacheMaxSize()).thenReturn(5000);
        lenient().when(properties.isPromotionalContentCacheWarmingEnabled()).thenReturn(false);
        lenient().when(properties.getPromotionalContentCacheBatchSize()).thenReturn(500);
        lenient().when(properties.getPromotionalContentCacheMaxQuerySize()).thenReturn(5000);
        cacheMgr = new PromotionalContentRuleCacheMgr(cassandraOperation, properties, redisCacheMgr);
        ReflectionTestUtils.setField(cacheMgr, "promotionalContentRulesCacheExpiryMs", 3600000);
    }

    @Test
    void testGetAccessSettingRules_EmptyCache_LoadsFromCassandra() {
        // Redis miss, Cassandra returns data
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
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
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(
                anyString(), anyString(), any(), any(), anyInt()
        )).thenReturn(List.of());
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetAccessSettingRules_ReturnsCachedData_OnSubsequentCalls() throws Exception {
        // Prepare a rule and its JSON
        List<Map<String, Object>> cassandraRecords = createCassandraRecords(2);
        Map<String, Map<String, Object>> ruleMap = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        for (Map<String, Object> rec : cassandraRecords) {
            Map<String, Object> ruleJson = new HashMap<>();
            ruleJson.put("contextId", rec.get("contextId"));
            ruleJson.put("contextIdType", rec.get("contextIdType"));
            // contextData must be a JSON string for the constructor, but in Redis it's stored as a Map
            Object contextData = rec.get("contextData");
            if (contextData instanceof String) {
                // Try to parse it to Map for Redis simulation
                contextData = mapper.readValue((String) contextData, Map.class);
            }
            ruleJson.put("contextData", contextData);
            ruleJson.put("isArchived", false); // Use the correct key as per POJO
            ruleMap.put(rec.get("contextId") + "|" + rec.get("contextIdType"), ruleJson);
        }
        String json = new ObjectMapper().writeValueAsString(ruleMap);
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(json);
        Collection<CachedAccessSettingRule> result1 = cacheMgr.getAccessSettingRules();
        assertEquals(2, result1.size());
        // On subsequent call, should still return from Redis
        Collection<CachedAccessSettingRule> result2 = cacheMgr.getAccessSettingRules();
        assertEquals(2, result2.size());
        verify(cassandraOperation, never()).getRecordsByProperties(
                anyString(), anyString(), any(), any(), anyInt()
        );
    }

    @Test
    void testLoadAccessSettingRules_HandlesException() {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(
                anyString(), anyString(), any(), any(), anyInt()
        )).thenThrow(new RuntimeException("Database error"));
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testProcessAndCacheRule_WithValidContextData() {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
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
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
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
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        Map<String, Object> noAccessControlRecord = createCassandraRecord("do_no_access", "Course", "{}" );
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

    private Map<String, Object> createCassandraRecord(String contextId, String contextIdType, Object contextData) {
        Map<String, Object> createCassandraRecord = new HashMap<>();
        createCassandraRecord.put("contextId", contextId);
        createCassandraRecord.put("contextIdType", contextIdType);
        // Always store as JSON string for contextData
        if (contextData instanceof String) {
            createCassandraRecord.put("contextData", contextData);
        } else {
            try {
                createCassandraRecord.put("contextData", new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(contextData));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        createCassandraRecord.put("isArchived", false);
        return createCassandraRecord;
    }

    private Map<String, Object> createCassandraRecordWithFullData(String contextId, String contextIdType) {
        // Use a nested map for contextData
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("criteriaKey", "designation");
        criteria.put("criteriaValue", Arrays.asList("1", "2", "3"));
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put("userGroupId", "group-1");
        userGroup.put("userGroupName", "Group 1");
        userGroup.put("userGroupCriteriaList", List.of(criteria));
        Map<String, Object> accessControlId = new HashMap<>();
        accessControlId.put("version", 1);
        accessControlId.put("userGroups", List.of(userGroup));
        Map<String, Object> contextData = new HashMap<>();
        contextData.put("accessControlId", accessControlId);
        return createCassandraRecord(contextId, contextIdType, contextData);
    }
}
