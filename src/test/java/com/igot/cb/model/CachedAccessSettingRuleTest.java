package com.igot.cb.model;

import static org.junit.jupiter.api.Assertions.*;


import org.junit.jupiter.api.Test;

class CachedAccessSettingRuleTest {

    private final String validJson = """
        {
          "contextId": "do_456",
          "contextIdType": "Course",
          "contextData": {
            "accessControl": {
              "version": 1,
              "userGroups": [
                {
                  "userGroupId": "group-1",
                  "userGroupName": "Test Group",
                  "userGroupCriteriaList": [
                    {
                      "criteriaKey": "designation",
                      "criteriaValue": ["analyst"]
                    }
                  ]
                }
              ]
            }
          },
          "isArchived": false
        }
        """;

    private final String contextDataStr = """
        {
          "accessControl": {
            "version": 1,
            "userGroups": []
          }
        }
        """;

    @Test
    void testJsonConstructor_validInput() {
        CachedAccessSettingRule rule = new CachedAccessSettingRule(validJson);
        assertEquals("do_456", rule.getContextId());
        assertEquals("Course", rule.getContextIdType());
        assertFalse(rule.isArchived());
        assertNotNull(rule.getContextData());
        assertTrue(rule.getContextData().containsKey("accessControl"));
    }

    @Test
    void testJsonConstructor_invalidJson_throwsException() {
        String badJson = "{ invalid json }";
        Exception ex = assertThrows(RuntimeException.class, () -> new CachedAccessSettingRule(badJson));
        assertTrue(ex.getMessage().contains("Failed to parse access setting rule"));
    }

    @Test
    void testFieldConstructor_validInput() {
        CachedAccessSettingRule rule = new CachedAccessSettingRule("ctx123", "Batch", contextDataStr, false);
        assertEquals("ctx123", rule.getContextId());
        assertEquals("Batch", rule.getContextIdType());
        assertFalse(rule.isArchived());
        assertNotNull(rule.getContextData());
        assertEquals("ctx123|Batch", rule.getCacheKey());
    }

    @Test
    void testFieldConstructor_invalidContextData_throwsException() {
        String invalidJson = "{ unquoted: 'value' }";
        Exception ex = assertThrows(RuntimeException.class,
                () -> new CachedAccessSettingRule("id", "type", invalidJson, false));
        assertTrue(ex.getMessage().contains("Failed to parse context data"));
    }

    @Test
    void testFieldConstructor_emptyContextData_throwsException() {
        Exception ex = assertThrows(RuntimeException.class,
                () -> new CachedAccessSettingRule("id", "type", "", false));
        assertTrue(ex.getMessage().contains("Invalid context data"));
    }

    @Test
    void testIsExpired_behavior() {
        CachedAccessSettingRule rule = new CachedAccessSettingRule("ctx789", "Topic", contextDataStr, false);
        assertFalse(rule.isExpired(5000)); // Not expired yet
    }

    @Test
    void testGetCacheKey_returnsCorrectFormat() {
        CachedAccessSettingRule rule = new CachedAccessSettingRule("abc", "def", contextDataStr, false);
        assertEquals("abc|def", rule.getCacheKey());
    }
}
