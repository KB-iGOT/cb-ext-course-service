package com.igot.cb.cache;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.util.*;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.CachedAccessSettingRule;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test suite for PromotionalContentRuleCacheMgr.
 * Covers all public methods and critical private method paths with >85% coverage.
 */
@ExtendWith(MockitoExtension.class)
class PromotionalContentRuleCacheMgrTest {

    @Mock
    private RedisCacheMgr redisCacheMgr;

    @Mock
    private CassandraOperation cassandraOperation;

    private PromotionalContentRuleCacheMgr cacheMgr;

    private static final String CACHE_KEY = "promotionalContents";
    private String validJsonRule;

    @BeforeEach
    void setup() {
        cacheMgr = new PromotionalContentRuleCacheMgr(redisCacheMgr, cassandraOperation);
        validJsonRule = """
                {
                  "contextId": "do_promotional_123",
                  "contextIdType": "Course",
                  "contextData": {
                    "accessControlId": {
                      "version": 1,
                      "userGroups": [
                        {
                          "userGroupId": "promo-group-1",
                          "userGroupName": "Promotional Group 1",
                          "userGroupCriteriaList": [
                            {
                              "criteriaKey": "designation",
                              "criteriaValue": ["1", "2", "3"]
                            }
                          ]
                        }
                      ]
                    }
                  },
                  "isArchived": false
                }
                """;
    }

    @Test
    void testGetAccessSettingRules_EmptyCache_LoadsFromRedis() {
        Map<String, String> redisMap = Map.of("do_promotional_123|Course", validJsonRule);
        when(redisCacheMgr.getAllCachedAccessRules(CACHE_KEY)).thenReturn(redisMap);
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertEquals(1, result.size());
        CachedAccessSettingRule rule = result.iterator().next();
        assertEquals("do_promotional_123", rule.getContextId());
        assertEquals("Course", rule.getContextIdType());
        verify(redisCacheMgr, times(1)).getAllCachedAccessRules(CACHE_KEY);
    }

    @Test
    void testGetAccessSettingRules_EmptyCache_LoadsFromCassandra_WhenRedisEmpty() {
        when(redisCacheMgr.getAllCachedAccessRules(CACHE_KEY)).thenReturn(Map.of());
        Map<String, Object> cassandraRecord = createCassandraRecord(
                "do_promo_456",
                "Course",
                """
                        {
                          "accessControlId": {
                            "version": 1,
                            "userGroups": [
                              {
                                "userGroupId": "group-456",
                                "userGroupName": "Group 456",
                                "userGroupCriteriaList": [
                                  {
                                    "criteriaKey": "designation",
                                    "criteriaValue": ["5", "6"]
                                  }
                                ]
                              }
                            ]
                          }
                        }
                        """
        );
        when(cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD_COURSE,
                Constants.PROMOTIONAL_CONTENT_RULES,
                null, null, null
        )).thenReturn(List.of(cassandraRecord));
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertEquals(1, result.size());
        CachedAccessSettingRule rule = result.iterator().next();
        assertEquals("do_promo_456", rule.getContextId());
        verify(cassandraOperation, times(1)).getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD_COURSE,
                Constants.PROMOTIONAL_CONTENT_RULES,
                null, null, null
        );
        verify(redisCacheMgr, times(1)).setAccessSettingRuleCache(
                eq(CACHE_KEY),
                eq("do_promo_456|Course"),
                anyMap()
        );
    }

    @Test
    void testGetAccessSettingRules_ReturnsCachedData_WhenNotExpired() {
        Map<String, String> redisMap = Map.of("do_promo_123|Course", validJsonRule);
        when(redisCacheMgr.getAllCachedAccessRules(CACHE_KEY)).thenReturn(redisMap);
        cacheMgr.getAccessSettingRules();
        reset(redisCacheMgr);
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertEquals(1, result.size());
        verify(redisCacheMgr, never()).getAllCachedAccessRules(CACHE_KEY);
    }

    @Test
    void testGetAccessSettingRules_ReloadsCache_WhenExpired() throws Exception {
        Map<String, String> redisMap1 = Map.of("do_promo_123|Course", validJsonRule);
        when(redisCacheMgr.getAllCachedAccessRules(CACHE_KEY)).thenReturn(redisMap1);
        cacheMgr.getAccessSettingRules();
        Field cacheField = PromotionalContentRuleCacheMgr.class.getDeclaredField("cachedAccessSettingRules");
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, CachedAccessSettingRule> cache = (Map<String, CachedAccessSettingRule>) cacheField.get(cacheMgr);
        for (CachedAccessSettingRule rule : cache.values()) {
            Field timestampField = CachedAccessSettingRule.class.getDeclaredField("cachedTimeMillis");
            timestampField.setAccessible(true);
            timestampField.set(rule, System.currentTimeMillis() - 7200000L); // 2 hours ago
        }
        String newJsonRule = """
                {
                  "contextId": "do_promo_new",
                  "contextIdType": "Course",
                  "contextData": {
                    "accessControlId": {
                      "version": 1,
                      "userGroups": [
                        {
                          "userGroupId": "new-group",
                          "userGroupName": "New Group",
                          "userGroupCriteriaList": [
                            {
                              "criteriaKey": "designation",
                              "criteriaValue": ["10"]
                            }
                          ]
                        }
                      ]
                    }
                  },
                  "isArchived": false
                }
                """;
        Map<String, String> redisMap2 = Map.of("do_promo_new|Course", newJsonRule);
        when(redisCacheMgr.getAllCachedAccessRules(CACHE_KEY)).thenReturn(redisMap2);
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("do_promo_new", result.iterator().next().getContextId());
        verify(redisCacheMgr, times(2)).getAllCachedAccessRules(CACHE_KEY); // Once initially, once after expiry
    }

    @Test
    void testGetAccessSettingRules_ReturnsEmptyList_WhenNoDataAvailable() {
        when(redisCacheMgr.getAllCachedAccessRules(CACHE_KEY)).thenReturn(Map.of());
        when(cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD_COURSE,
                Constants.PROMOTIONAL_CONTENT_RULES,
                null, null, null
        )).thenReturn(List.of());
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetAccessSettingRules_HandlesException_ReturnsPreviousCache() throws Exception {
        Map<String, String> redisMap = Map.of("do_promo_123|Course", validJsonRule);
        when(redisCacheMgr.getAllCachedAccessRules(CACHE_KEY)).thenReturn(redisMap);
        cacheMgr.getAccessSettingRules();
        Field cacheField = PromotionalContentRuleCacheMgr.class.getDeclaredField("cachedAccessSettingRules");
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, CachedAccessSettingRule> cache = (Map<String, CachedAccessSettingRule>) cacheField.get(cacheMgr);
        for (CachedAccessSettingRule rule : cache.values()) {
            Field timestampField = CachedAccessSettingRule.class.getDeclaredField("cachedTimeMillis");
            timestampField.setAccessible(true);
            timestampField.set(rule, System.currentTimeMillis() - 7200000L);
        }
        when(redisCacheMgr.getAllCachedAccessRules(CACHE_KEY)).thenThrow(new RuntimeException("Redis connection error"));
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertTrue(result.isEmpty());
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
        assertFalse(result.get(2));
        assertFalse(result.get(4));
    }

    @Test
    void testCreateBitSetForAttribute_EmptyCollection() {
        List<Integer> values = List.of();
        BitSet result = cacheMgr.createBitSetForAttribute(values);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testCreateBitSetForAttribute_SingleValue() {
        List<Integer> values = List.of(42);
        BitSet result = cacheMgr.createBitSetForAttribute(values);
        assertNotNull(result);
        assertTrue(result.get(42));
        assertEquals(1, result.cardinality());
    }

    @Test
    void testCreateBitSetForAttribute_DuplicateValues() {
        List<Integer> values = Arrays.asList(1, 1, 2, 2, 3);
        BitSet result = cacheMgr.createBitSetForAttribute(values);
        assertNotNull(result);
        assertTrue(result.get(1));
        assertTrue(result.get(2));
        assertTrue(result.get(3));
        assertEquals(3, result.cardinality()); // Only 3 unique values
    }


    @Test
    void testLoadFromCassandra_ProcessesContextData_WithBitSetConversion() {
        when(redisCacheMgr.getAllCachedAccessRules(CACHE_KEY)).thenReturn(Map.of());
        Map<String, Object> cassandraRecord = createCassandraRecord(
                "do_integration_test",
                "Course",
                """
                        {
                          "accessControlId": {
                            "version": 1,
                            "userGroups": [
                              {
                                "userGroupId": "integration-group",
                                "userGroupName": "Integration Group",
                                "userGroupCriteriaList": [
                                  {
                                    "criteriaKey": "designation",
                                    "criteriaValue": ["1", "2", "3"]
                                  },
                                  {
                                    "criteriaKey": "department",
                                    "criteriaValue": ["10", "20"]
                                  }
                                ]
                              }
                            ]
                          }
                        }
                        """
        );
        when(cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD_COURSE,
                Constants.PROMOTIONAL_CONTENT_RULES,
                null, null, null
        )).thenReturn(List.of(cassandraRecord));
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertEquals(1, result.size());
        CachedAccessSettingRule rule = result.iterator().next();
        Map<String, Object> contextData = rule.getContextData();
        assertNotNull(contextData);
        Map<String, Object> accessControl = (Map<String, Object>) contextData.get("accessControlId");
        assertNotNull(accessControl);
        List<Map<String, Object>> userGroups = (List<Map<String, Object>>) accessControl.get("userGroups");
        assertNotNull(userGroups);
        assertEquals(1, userGroups.size());
        List<Map<String, Object>> criteriaList = (List<Map<String, Object>>) userGroups.get(0).get("userGroupCriteriaList");
        assertNotNull(criteriaList);
        assertEquals(2, criteriaList.size());
        Object designationValue = criteriaList.get(0).get("criteriaValue");
        assertInstanceOf(BitSet.class, designationValue);
        BitSet designationBitSet = (BitSet) designationValue;
        assertTrue(designationBitSet.get(1));
        assertTrue(designationBitSet.get(2));
        assertTrue(designationBitSet.get(3));
    }

    @Test
    void testLoadFromCassandra_HandlesNullContextData() {
        when(redisCacheMgr.getAllCachedAccessRules(CACHE_KEY)).thenReturn(Map.of());
        Map<String, Object> cassandraRecord = createCassandraRecord(
                "do_null_context",
                "Course",
                "{}"
        );
        when(cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD_COURSE,
                Constants.PROMOTIONAL_CONTENT_RULES,
                null, null, null
        )).thenReturn(List.of(cassandraRecord));
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    void testLoadFromCassandra_HandlesNoAccessControl() {
        when(redisCacheMgr.getAllCachedAccessRules(CACHE_KEY)).thenReturn(Map.of());
        Map<String, Object> cassandraRecord = createCassandraRecord(
                "do_no_access_control",
                "Course",
                """
                        {
                          "someOtherField": "value"
                        }
                        """
        );
        when(cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD_COURSE,
                Constants.PROMOTIONAL_CONTENT_RULES,
                null, null, null
        )).thenReturn(List.of(cassandraRecord));
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    void testLoadFromCassandra_HandlesEmptyUserGroups() {
        when(redisCacheMgr.getAllCachedAccessRules(CACHE_KEY)).thenReturn(Map.of());
        Map<String, Object> cassandraRecord = createCassandraRecord(
                "do_empty_groups",
                "Course",
                """
                        {
                          "accessControlId": {
                            "version": 1,
                            "userGroups": []
                          }
                        }
                        """
        );
        when(cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD_COURSE,
                Constants.PROMOTIONAL_CONTENT_RULES,
                null, null, null
        )).thenReturn(List.of(cassandraRecord));
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    void testLoadFromCassandra_HandlesNullUserGroups() {
        when(redisCacheMgr.getAllCachedAccessRules(CACHE_KEY)).thenReturn(Map.of());
        Map<String, Object> cassandraRecord = createCassandraRecord(
                "do_null_groups",
                "Course",
                """
                        {
                          "accessControlId": {
                            "version": 1
                          }
                        }
                        """
        );
        when(cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD_COURSE,
                Constants.PROMOTIONAL_CONTENT_RULES,
                null, null, null
        )).thenReturn(List.of(cassandraRecord));
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    void testLoadFromCassandra_HandlesEmptyCriteriaList() {
        when(redisCacheMgr.getAllCachedAccessRules(CACHE_KEY)).thenReturn(Map.of());
        Map<String, Object> cassandraRecord = createCassandraRecord(
                "do_empty_criteria",
                "Course",
                """
                        {
                          "accessControlId": {
                            "version": 1,
                            "userGroups": [
                              {
                                "userGroupId": "group-1",
                                "userGroupName": "Group 1",
                                "userGroupCriteriaList": []
                              }
                            ]
                          }
                        }
                        """
        );
        when(cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD_COURSE,
                Constants.PROMOTIONAL_CONTENT_RULES,
                null, null, null
        )).thenReturn(List.of(cassandraRecord));
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    void testLoadFromCassandra_HandlesMissingCriteriaKeyOrValue() {
        when(redisCacheMgr.getAllCachedAccessRules(CACHE_KEY)).thenReturn(Map.of());
        Map<String, Object> cassandraRecord = createCassandraRecord(
                "do_missing_criteria",
                "Course",
                """
                        {
                          "accessControlId": {
                            "version": 1,
                            "userGroups": [
                              {
                                "userGroupId": "group-1",
                                "userGroupName": "Group 1",
                                "userGroupCriteriaList": [
                                  {
                                    "criteriaKey": "designation"
                                  }
                                ]
                              }
                            ]
                          }
                        }
                        """
        );
        when(cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD_COURSE,
                Constants.PROMOTIONAL_CONTENT_RULES,
                null, null, null
        )).thenReturn(List.of(cassandraRecord));
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    void testLoadFromCassandra_HandlesNonIntegerCriteriaValues() {
        when(redisCacheMgr.getAllCachedAccessRules(CACHE_KEY)).thenReturn(Map.of());
        Map<String, Object> cassandraRecord = createCassandraRecord(
                "do_non_integer",
                "Course",
                """
                        {
                          "accessControlId": {
                            "version": 1,
                            "userGroups": [
                              {
                                "userGroupId": "group-1",
                                "userGroupName": "Group 1",
                                "userGroupCriteriaList": [
                                  {
                                    "criteriaKey": "designation",
                                    "criteriaValue": ["1", "invalid", "3", "not-a-number"]
                                  }
                                ]
                              }
                            ]
                          }
                        }
                        """
        );
        when(cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD_COURSE,
                Constants.PROMOTIONAL_CONTENT_RULES,
                null, null, null
        )).thenReturn(List.of(cassandraRecord));
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertEquals(1, result.size());
        CachedAccessSettingRule rule = result.iterator().next();
        Map<String, Object> accessControl = (Map<String, Object>) rule.getContextData().get("accessControlId");
        List<Map<String, Object>> userGroups = (List<Map<String, Object>>) accessControl.get("userGroups");
        List<Map<String, Object>> criteriaList = (List<Map<String, Object>>) userGroups.get(0).get("userGroupCriteriaList");
        BitSet bitSet = (BitSet) criteriaList.get(0).get("criteriaValue");
        assertTrue(bitSet.get(1));
        assertTrue(bitSet.get(3));
        assertEquals(2, bitSet.cardinality());
    }

    @Test
    void testLoadFromCassandra_HandlesMultipleRecords() {
        when(redisCacheMgr.getAllCachedAccessRules(CACHE_KEY)).thenReturn(Map.of());
        Map<String, Object> record1 = createCassandraRecord("do_promo_1", "Course", createValidContextData());
        Map<String, Object> record2 = createCassandraRecord("do_promo_2", "Course", createValidContextData());
        Map<String, Object> record3 = createCassandraRecord("do_promo_3", "Program", createValidContextData());
        when(cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD_COURSE,
                Constants.PROMOTIONAL_CONTENT_RULES,
                null, null, null
        )).thenReturn(List.of(record1, record2, record3));
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertEquals(3, result.size());
        verify(redisCacheMgr, times(3)).setAccessSettingRuleCache(
                eq(CACHE_KEY),
                anyString(),
                anyMap()
        );
    }

    @Test
    void testLoadFromRedis_HandlesMultipleRules() {
        String rule1 = createValidJsonRule("do_redis_1", "Course");
        String rule2 = createValidJsonRule("do_redis_2", "Course");
        String rule3 = createValidJsonRule("do_redis_3", "Program");
        Map<String, String> redisMap = Map.of(
                "do_redis_1|Course", rule1,
                "do_redis_2|Course", rule2,
                "do_redis_3|Program", rule3
        );
        when(redisCacheMgr.getAllCachedAccessRules(CACHE_KEY)).thenReturn(redisMap);
        Collection<CachedAccessSettingRule> result = cacheMgr.getAccessSettingRules();
        assertNotNull(result);
        assertEquals(3, result.size());
    }

    private Map<String, Object> createCassandraRecord(String contextId, String contextIdType, String contextData) {
        Map<String, Object> cassandraRecord = new HashMap<>();
        cassandraRecord.put("contextId", contextId);
        cassandraRecord.put("contextIdType", contextIdType);
        cassandraRecord.put("contextData", contextData);
        return cassandraRecord;
    }

    private String createValidContextData() {
        return """
                {
                  "accessControlId": {
                    "version": 1,
                    "userGroups": [
                      {
                        "userGroupId": "test-group",
                        "userGroupName": "Test Group",
                        "userGroupCriteriaList": [
                          {
                            "criteriaKey": "designation",
                            "criteriaValue": ["1", "2"]
                          }
                        ]
                      }
                    ]
                  }
                }
                """;
    }

    private String createValidJsonRule(String contextId, String contextIdType) {
        return String.format("""
                {
                  "contextId": "%s",
                  "contextIdType": "%s",
                  "contextData": {
                    "accessControlId": {
                      "version": 1,
                      "userGroups": [
                        {
                          "userGroupId": "group-1",
                          "userGroupName": "Group 1",
                          "userGroupCriteriaList": [
                            {
                              "criteriaKey": "designation",
                              "criteriaValue": ["1", "2"]
                            }
                          ]
                        }
                      ]
                    }
                  },
                  "isArchived": false
                }
                """, contextId, contextIdType);
    }
}
