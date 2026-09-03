package com.igot.cb.cache;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.lang.reflect.Field;
import java.util.function.Consumer;

import com.github.benmanes.caffeine.cache.Cache;
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
        } catch (Exception e) {
            throw new RuntimeException("Failed to set ttlMinutes in test", e);
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
    void testCreateBitSetForAttribute() {
        Collection<Integer> values = Arrays.asList(1, 3, 5);
        
        BitSet result = cacheMgr.createBitSetForAttribute(values);
        
        assertTrue(result.get(1));
        assertTrue(result.get(3));
        assertTrue(result.get(5));
        assertFalse(result.get(2));
        assertFalse(result.get(4));
    }

    @Test
    void testGetOrLoadAccessSettingRule_cacheHit() {
        // Use reflection to insert an entry into the Caffeine cache
        CachedAccessSettingRule rule = new CachedAccessSettingRule("do_123", "Course", "{}", false);

        try {
            Field field = AccessSettingRuleCacheMgr.class.getDeclaredField("accessSettingsCache");
            field.setAccessible(true);
            Cache<String, CachedAccessSettingRule> cache =
                (Cache<String, CachedAccessSettingRule>) field.get(cacheMgr);
            assertNotNull(cache, "Cache should be initialized");
            cache.put("do_123|Course", rule);
            CachedAccessSettingRule verifyPut = cache.getIfPresent("do_123|Course");
            assertNotNull(verifyPut, "Entry should be in cache after put");
        } catch (Exception e) {
            fail("Reflection failed: " + e.getMessage());
        }

        CachedAccessSettingRule result = cacheMgr.getOrLoadAccessSettingRule("do_123", "Course");

        assertNotNull(result, "getOrLoadAccessSettingRule should return cached entry");
        assertEquals("do_123", result.getContextId());
        assertEquals("Course", result.getContextIdType());
        verifyNoInteractions(cassandraOperation);
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
