package com.igot.cb.cache;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.roaringbitmap.RoaringBitmap;
import java.lang.reflect.Field;
import java.util.function.Consumer;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.CachedAccessSettingRule;

import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AccessSettingRuleCacheMgrTest {

    @Mock
    private RedisCacheMgr redisCacheMgr;

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private IdMapCacheMgr idMapCacheMgr;

    private AccessSettingRuleCacheMgr cacheMgr;

    private String redisKey = "accessSettingRules";
    private String validJsonRule;

    @BeforeEach
    void setup() throws Exception {
        cacheMgr = new AccessSettingRuleCacheMgr(redisCacheMgr, cassandraOperation);
        try {
            Field ttlField = AccessSettingRuleCacheMgr.class.getDeclaredField("ttlMinutes");
            ttlField.setAccessible(true);
            ttlField.setInt(cacheMgr, 10);

            Field maxSizeField = AccessSettingRuleCacheMgr.class.getDeclaredField("maxCacheSize");
            maxSizeField.setAccessible(true);
            maxSizeField.setInt(cacheMgr, 5000);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set cache configuration in test", e);
        }
        try {
            var method = AccessSettingRuleCacheMgr.class.getDeclaredMethod("initCache");
            method.setAccessible(true);
            method.invoke(cacheMgr);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize cache in test", e);
        }

        validJsonRule = """
        {
          "contextId": "do_123",
          "contextIdType": "Course",
          "contextData": {
            "accessControlId": {
              "version": 1,
              "userGroups": [
                {
                  "userGroupId": "group-123",
                  "userGroupName": "Test Group",
                  "userGroupCriteriaList": [
                    {
                      "criteriaKey": "designation",
                      "criteriaValue": [1, 2]
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
    void testGetAccessSettingRules_fromRedis() {
        Map<String, String> redisMap = Map.of("do_123|Course", validJsonRule);
        when(redisCacheMgr.getAllCachedAccessRules(redisKey)).thenReturn(redisMap);

        var result = cacheMgr.getAccessSettingRules();

        assertEquals(1, result.size());
        assertEquals("do_123", result.iterator().next().getContextId());
    }

    @Test
    void testGetAccessSettingRules_fromCassandra_whenRedisEmpty() {
        when(redisCacheMgr.getAllCachedAccessRules(redisKey)).thenReturn(Map.of());

        Map<String, Object> recordMap = Map.of(
                "contextId", "do_123",
                "contextIdType", "Course",
                "contextData", """
                    {
                      "accessControlId": {
                        "version": 1,
                        "userGroups": [
                          {
                            "userGroupId": "group-123",
                            "userGroupName": "Test Group",
                            "userGroupCriteriaList": [
                              {
                                "criteriaKey": "designation",
                                "criteriaValue": [1, 2]
                              }
                            ]
                          }
                        ]
                      }
                    }
                    """
        );
        mockForEachAccessRules(List.of(recordMap));

        var result = cacheMgr.getAccessSettingRules();

        assertEquals(1, result.size());
        assertEquals("do_123", result.iterator().next().getContextId());
        verify(redisCacheMgr).setAccessSettingRuleCache(eq(redisKey), eq("do_123|Course"), any());
    }

    @Test
    void testGetAccessSettingRules_returnsEmpty_whenBothSourcesEmpty() {
        when(redisCacheMgr.getAllCachedAccessRules(redisKey)).thenReturn(Map.of());

        var result = cacheMgr.getAccessSettingRules();
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetAccessSettingRules_cacheExpiryTriggersReload() throws Exception {
        // First call - load from Redis
        Map<String, String> redisMap = Map.of("do_123|Course", validJsonRule);
        when(redisCacheMgr.getAllCachedAccessRules(redisKey)).thenReturn(redisMap);
        
        cacheMgr.getAccessSettingRules();
        
        // Simulate cache expiry by modifying the cached rule's timestamp
        Field cacheField = AccessSettingRuleCacheMgr.class.getDeclaredField("cachedAccessSettingRules");
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, CachedAccessSettingRule> cache = (Map<String, CachedAccessSettingRule>) cacheField.get(cacheMgr);
        
        if (cache != null && !cache.isEmpty()) {
            CachedAccessSettingRule rule = cache.values().iterator().next();
            rule.setCachedTimeMillis(System.currentTimeMillis() - (2 * 3600000)); // Expired
        }
        
        // Second call should trigger reload
        when(redisCacheMgr.getAllCachedAccessRules(redisKey)).thenReturn(redisMap);
        var result = cacheMgr.getAccessSettingRules();
        
        assertNotNull(result);
        verify(redisCacheMgr, atLeast(2)).getAllCachedAccessRules(redisKey);
    }

    @Test
    void testGetAccessSettingRules_cassandraException() {
        when(redisCacheMgr.getAllCachedAccessRules(redisKey)).thenReturn(Map.of());
        doThrow(new RuntimeException("Database error"))
                .when(cassandraOperation)
                .forEachRecordByProperties(anyString(), anyString(), isNull(), isNull(), any(), isNull(), any());

        var result = cacheMgr.getAccessSettingRules();
        
        assertTrue(result.isEmpty());
    }

    @Test
    void testProcessContextData_noAccessControl() {
        when(redisCacheMgr.getAllCachedAccessRules(redisKey)).thenReturn(Map.of());
        
        Map<String, Object> recordMap = Map.of(
                "contextId", "do_123",
                "contextIdType", "Course",
                "contextData", "{}"
        );
        mockForEachAccessRules(List.of(recordMap));

        var result = cacheMgr.getAccessSettingRules();
        
        assertEquals(1, result.size());
    }

    @Test
    void testProcessContextData_noUserGroups() {
        when(redisCacheMgr.getAllCachedAccessRules(redisKey)).thenReturn(Map.of());
        
        Map<String, Object> recordMap = Map.of(
                "contextId", "do_123",
                "contextIdType", "Course",
                "contextData", "{\"accessControlId\": {}}"
        );
        mockForEachAccessRules(List.of(recordMap));

        var result = cacheMgr.getAccessSettingRules();
        
        assertEquals(1, result.size());
    }

    @Test
    void testProcessContextData_noCriteriaList() {
        when(redisCacheMgr.getAllCachedAccessRules(redisKey)).thenReturn(Map.of());
        
        Map<String, Object> recordMap = Map.of(
                "contextId", "do_123",
                "contextIdType", "Course",
                "contextData", """
                    {
                      "accessControlId": {
                        "userGroups": [
                          {
                            "userGroupId": "group-123",
                            "userGroupName": "Test Group"
                          }
                        ]
                      }
                    }
                    """
        );
        mockForEachAccessRules(List.of(recordMap));

        var result = cacheMgr.getAccessSettingRules();
        
        assertEquals(1, result.size());
    }

    @Test
    void testProcessContextData_invalidCriteriaValues() {
        when(redisCacheMgr.getAllCachedAccessRules(redisKey)).thenReturn(Map.of());
        
        Map<String, Object> recordMap = Map.of(
                "contextId", "do_123",
                "contextIdType", "Course",
                "contextData", """
                    {
                      "accessControlId": {
                        "userGroups": [
                          {
                            "userGroupId": "group-123",
                            "userGroupName": "Test Group",
                            "userGroupCriteriaList": [
                              {
                                "criteriaKey": "designation",
                                "criteriaValue": ["invalid", "2"]
                              }
                            ]
                          }
                        ]
                      }
                    }
                    """
        );
        mockForEachAccessRules(List.of(recordMap));

        var result = cacheMgr.getAccessSettingRules();
        
        assertEquals(1, result.size());
    }

    @Test
    void testCreateBitmapForAttribute() {
        Collection<Integer> values = Arrays.asList(1, 3, 5);
        
        RoaringBitmap result = cacheMgr.createBitmapForAttribute(values);
        
        assertTrue(result.contains(1));
        assertTrue(result.contains(3));
        assertTrue(result.contains(5));
        assertFalse(result.contains(2));
        assertFalse(result.contains(4));
    }

    @Test
    void testCreateBitmapForAttribute_withLargeAndNegativeValues() {
        // Valid values, a negative value (skipped) and large ids that must be retained
        List<Integer> values = List.of(1, 5, -1, 40000000, 1145607574, 2000000000);
        RoaringBitmap bitSet = cacheMgr.createBitmapForAttribute(values);

        assertNotNull(bitSet);
        assertTrue(bitSet.contains(1));
        assertTrue(bitSet.contains(5));
        assertTrue(bitSet.contains(40000000));
        assertTrue(bitSet.contains(1145607574));
        assertTrue(bitSet.contains(2000000000));
        assertFalse(bitSet.contains(0));
        assertFalse(bitSet.contains(2));
        assertFalse(bitSet.contains(-1));
        assertEquals(5, bitSet.getCardinality());
    }

    @Test
    void testProcessContextData_withLargeValues_retainsLargeIds() throws Exception {
        String ruleWithLargeValues = """
        {
          "accessControlId": {
            "version": 1,
            "userGroups": [
              {
                "userGroupId": "group-123",
                "userGroupName": "Test Group",
                "userGroupCriteriaList": [
                  {
                    "criteriaKey": "designation",
                    "criteriaValue": [1, 2, "1145607574", -5, "invalid_number"]
                  }
                ]
              }
            ]
          }
        }
        """;
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        Map<String, Object> contextData = mapper.readValue(ruleWithLargeValues, new com.fasterxml.jackson.core.type.TypeReference<>() {});

        var method = AccessSettingRuleCacheMgr.class.getDeclaredMethod("processContextData", String.class, Map.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(cacheMgr, "do_123|Course", contextData));

        Map<String, Object> accessControlId = (Map<String, Object>) contextData.get("accessControlId");
        List<Map<String, Object>> userGroups = (List<Map<String, Object>>) accessControlId.get("userGroups");
        List<Map<String, Object>> criteriaList = (List<Map<String, Object>>) userGroups.get(0).get("userGroupCriteriaList");
        RoaringBitmap bitSet = (RoaringBitmap) criteriaList.get(0).get("criteriaValue");

        assertNotNull(bitSet);
        assertTrue(bitSet.contains(1));
        assertTrue(bitSet.contains(2));
        assertTrue(bitSet.contains(1145607574));
        assertEquals(3, bitSet.getCardinality());
    }

    @Test
    void testGetOrLoadAccessSettingRule_cacheHit() {
        // Mock Cassandra to return a valid record as fallback
        Map<String, Object> cassRecord = new HashMap<>();
        cassRecord.put(Constants.CONTEXT_ID_KEY, "do_123");
        cassRecord.put(Constants.CONTEXT_ID_TYPE, "Course");
        cassRecord.put(Constants.CONTEXT_DATA_KEY, "{}");
        cassRecord.put(Constants.IS_ARCHIVED, false);
        when(cassandraOperation.getRecordsByProperties(
                anyString(), anyString(), any(), any(), any()))
                .thenReturn(List.of(cassRecord));

        // Pre-load cache by calling the method once
        CachedAccessSettingRule firstCall = cacheMgr.getOrLoadAccessSettingRule("do_123", "Course");
        assertNotNull(firstCall, "First call should load from Cassandra");
        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(), anyString(), any(), any(), any());

        // Second call should hit cache
        CachedAccessSettingRule result = cacheMgr.getOrLoadAccessSettingRule("do_123", "Course");

        assertNotNull(result, "Second call should return cached entry");
        assertEquals("do_123", result.getContextId());
        assertEquals("Course", result.getContextIdType());
        // Verify Cassandra was only called once (from first call)
        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(), anyString(), any(), any(), any());
    }

    @Test
    void testGetOrLoadAccessSettingRule_cacheMiss_loadsFromCassandra() {
        Map<String, Object> cassRecord = new HashMap<>();
        cassRecord.put(Constants.CONTEXT_ID_KEY, "do_123");
        cassRecord.put(Constants.CONTEXT_ID_TYPE, "Course");
        cassRecord.put(Constants.CONTEXT_DATA_KEY, "{\"sample\":true}");
        when(cassandraOperation.getRecordsByProperties(
                anyString(), anyString(), any(), any(), any()))
                .thenReturn(List.of(cassRecord));
        CachedAccessSettingRule result =
                cacheMgr.getOrLoadAccessSettingRule("do_123", "Course");
        assertNotNull(result);
        assertEquals("do_123", result.getContextId());
        assertEquals("Course", result.getContextIdType());
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> captor =
                (ArgumentCaptor<Map<String, Object>>) (ArgumentCaptor<?>) ArgumentCaptor.forClass(Map.class);
        verify(cassandraOperation).getRecordsByProperties(
                anyString(),
                anyString(),
                captor.capture(),
                isNull(),
                isNull()
        );
        Map<String, Object> filter = captor.getValue();
        assertEquals("do_123", filter.get(Constants.CONTEXT_ID));
        assertEquals("Course", filter.get(Constants.CONTEXT_ID_TYPE_KEY));
        CachedAccessSettingRule cached =
                cacheMgr.getOrLoadAccessSettingRule("do_123", "Course");
        assertEquals("do_123", cached.getContextId());
        verifyNoMoreInteractions(cassandraOperation);
    }


    @Test
    void testGetOrLoadAccessSettingRule_cacheMiss_noRecord() {
        when(cassandraOperation.getRecordsByProperties(
                anyString(), anyString(), any(), any(), any()))
                .thenReturn(List.of()); // No records

        CachedAccessSettingRule result =
                cacheMgr.getOrLoadAccessSettingRule("do_999", "Course");

        assertNull(result);
    }

    @Test
    void testGetOrLoadAccessSettingRule_cassandraThrowsException() {
        when(cassandraOperation.getRecordsByProperties(
                anyString(), anyString(), any(), any(), any()))
                .thenThrow(new RuntimeException("DB error"));

        CachedAccessSettingRule result =
                cacheMgr.getOrLoadAccessSettingRule("do_500", "Course");

        assertNull(result);
    }

    @SuppressWarnings("unchecked")
    private void mockForEachAccessRules(List<Map<String, Object>> records) {
        doAnswer(invocation -> {
            Consumer<Map<String, Object>> consumer = invocation.getArgument(6);
            records.forEach(consumer);
            return null;
        }).when(cassandraOperation).forEachRecordByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.ACCESS_SETTINGS_RULES_TABLE_V2),
                isNull(),
                isNull(),
                any(),
                isNull(),
                any()
        );
    }

}
