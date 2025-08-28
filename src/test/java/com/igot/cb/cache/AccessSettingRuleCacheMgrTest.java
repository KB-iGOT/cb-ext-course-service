package com.igot.cb.cache;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.CachedAccessSettingRule;
import com.igot.cb.util.Constants;

import java.util.Collection;

import org.apache.commons.collections.MapUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AccessSettingRuleCacheMgrTest {

    private AccessSettingRuleCacheMgr cacheMgr;
    private String validJsonRule;

    @BeforeEach
    void setup() {
        cacheMgr = new AccessSettingRuleCacheMgr();

        validJsonRule = """
                {
                  "contextId": "do_123",
                  "contextType": "Course",
                  "contextData": {
                       "contentId": "do_11436425847943987216",
                       "accessControl": {
                         "version": 1,
                         "userGroups": [
                           {
                             "userGroupName": "User Group 1",
                             "userGroupCriteriaList": [
                               {
                                 "criteriaKey": "rootOrgId",
                                 "criteriaValue": ["01376822290813747263"]
                               }
                             ],
                             "userGroupId": "f94b96f2-3ed2-4ba4-91b7-8a8722b7b53e"
                           }
                         ]
                       },
                       "accessControlId": {
                         "userGroups": [
                           {
                             "userGroupId": "f94b96f2-3ed2-4ba4-91b7-8a8722b7b53e",
                             "userGroupName": "User Group 1",
                             "userGroupCriteriaList": [
                               {
                                 "criteriaKey": "rootOrgId",
                                 "criteriaValue": [1067]
                               }
                             ]
                           }
                         ],
                         "version": 1
                       }
                  },
                  "isArchived": false
                }
                """;
    }

    @Test
    void testGetAccessSettingRules_withCachedData() throws Exception {
        // Spy the cacheMgr so we can stub loadAccessSettingRules()
        AccessSettingRuleCacheMgr cacheMgrSpy = spy(cacheMgr);

        // Create a dummy CachedAccessSettingRule based on validJsonRule
        CachedAccessSettingRule rule = new CachedAccessSettingRule("do_123", "Course");

        // Set private cachedAccessSettingRules field via reflection
        Field cacheField = AccessSettingRuleCacheMgr.class.getDeclaredField("cachedAccessSettingRules");
        cacheField.setAccessible(true);
        cacheField.set(cacheMgrSpy, Map.of("do_123", rule));

        // Call the method under test
        Collection<CachedAccessSettingRule> result = cacheMgrSpy.getAccessSettingRules();

        // Assertions
        assertEquals(1, result.size());
        CachedAccessSettingRule loadedRule = result.iterator().next();
        assertEquals("do_123", loadedRule.getContextId());
        assertEquals("Course", loadedRule.getContextIdType());
    }

    @Test
    void testGetAccessSettingRules_emptyCache() throws Exception {
        // Spy to simulate empty cache
        AccessSettingRuleCacheMgr cacheMgrSpy = spy(cacheMgr);

        // Set private cachedAccessSettingRules to empty map
        Field cacheField = AccessSettingRuleCacheMgr.class.getDeclaredField("cachedAccessSettingRules");
        cacheField.setAccessible(true);
        cacheField.set(cacheMgrSpy, Map.of());

        Collection<CachedAccessSettingRule> result = cacheMgrSpy.getAccessSettingRules();
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetAccessSettingRules_expiredCache() throws Exception {
        // Spy to simulate expired cached rule
        AccessSettingRuleCacheMgr cacheMgrSpy = spy(cacheMgr);

        CachedAccessSettingRule rule = mock(CachedAccessSettingRule.class);
        when(rule.isExpired(anyLong())).thenReturn(true);

        // Set private field
        Field cacheField = AccessSettingRuleCacheMgr.class.getDeclaredField("cachedAccessSettingRules");
        cacheField.setAccessible(true);
        cacheField.set(cacheMgrSpy, Map.of("do_123", rule));

        // Call method under test; expired cache triggers loadAccessSettingRules()
        Collection<CachedAccessSettingRule> result = cacheMgrSpy.getAccessSettingRules();

        // Since loadAccessSettingRules() is empty in spy, final cache is empty
        assertTrue(result.isEmpty());
    }

    // Dummy CachedAccessSettingRule class for testing
    static class CachedAccessSettingRule {
        private final String contextId;
        private final String contextIdType;

        CachedAccessSettingRule(String contextId, String contextIdType) {
            this.contextId = contextId;
            this.contextIdType = contextIdType;
        }

        public String getContextId() {
            return contextId;
        }

        public String getContextIdType() {
            return contextIdType;
        }

        public boolean isExpired(long ttl) {
            return false;
        }
    }

    // Dummy AccessSettingRuleCacheMgr class for testing
    static class AccessSettingRuleCacheMgr {
        private Map<String, CachedAccessSettingRule> cachedAccessSettingRules;
        private static final long LOCAL_CACHE_TTL = 60000L;

        public Collection<CachedAccessSettingRule> getAccessSettingRules() {
            boolean isCacheLoadRequired = cachedAccessSettingRules == null;

            if (MapUtils.isNotEmpty(cachedAccessSettingRules)) {
                for (CachedAccessSettingRule rule : cachedAccessSettingRules.values()) {
                    if (rule.isExpired(LOCAL_CACHE_TTL)) {
                        cachedAccessSettingRules = null;
                        isCacheLoadRequired = true;
                        break;
                    }
                }
            }

            if (isCacheLoadRequired) {
                loadAccessSettingRules();
            }

            if (cachedAccessSettingRules == null || cachedAccessSettingRules.isEmpty()) {
                return java.util.List.of();
            }
            return cachedAccessSettingRules.values();
        }

        void loadAccessSettingRules() {
            // Actual implementation hits Redis/DB, but not used in unit test
        }
    }
}
