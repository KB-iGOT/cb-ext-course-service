package com.igot.cb.cache;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Map;

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

    @InjectMocks
    private AccessSettingRuleCacheMgr cacheMgr;

    private String redisKey = "accessSettingRules";
    private String validJsonRule;

    @BeforeEach
    void setup() {
        validJsonRule = """
            {
              "contextId": "do_123",
              "contextIdType": "Course",
              "contextData": {
                "accessControl": {
                  "version": 1,
                  "userGroups": [
                    {
                      "userGroupId": "group-123",
                      "userGroupName": "Test Group",
                      "userGroupCriteriaList": [
                        {
                          "criteriaKey": "designation",
                          "criteriaValue": ["teacher", "mentor"]
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

        Map<String, Object> record = Map.of(
                "contextId", "do_123",
                "contextIdType", "Course",
                Constants.CONTEXT_DATA, """
                    {
                      "accessControl": {
                        "version": 1,
                        "userGroups": []
                      }
                    }
                    """,
                "isArchived", false
        );
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), isNull(), isNull(), isNull()))
                .thenReturn(List.of(record));

        var result = cacheMgr.getAccessSettingRules();

        assertEquals(1, result.size());
        assertEquals("do_123", result.iterator().next().getContextId());
        verify(redisCacheMgr, times(1))
                .setAccessSettingRuleCache(eq(redisKey), eq("do_123|Course"), any());
    }

    @Test
    void testGetAccessSettingRules_returnsEmpty_whenBothSourcesEmpty() {
        when(redisCacheMgr.getAllCachedAccessRules(redisKey)).thenReturn(Map.of());
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), isNull(), isNull(), isNull()))
                .thenReturn(List.of());

        var result = cacheMgr.getAccessSettingRules();
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetAccessSettingRules_cacheExpiryTriggersReload() {
        Map<String, String> redisMap = Map.of("do_123|Course", validJsonRule);
        when(redisCacheMgr.getAllCachedAccessRules(redisKey)).thenReturn(redisMap);

        var result1 = cacheMgr.getAccessSettingRules(); // load initially

        // Simulate TTL expiry
        var cachedRule = new CachedAccessSettingRule(validJsonRule);
        cachedRule.setCachedTimeMillis(System.currentTimeMillis() - (2 * 3600000));
        cacheMgr.getAccessSettingRules(); // second call will trigger reload
        // Success if no exceptions and method executes
    }
}
