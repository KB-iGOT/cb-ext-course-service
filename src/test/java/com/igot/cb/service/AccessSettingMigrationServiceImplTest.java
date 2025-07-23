package com.igot.cb.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.IdMapCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;

@ExtendWith(MockitoExtension.class)
class AccessSettingMigrationServiceImplTest {

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private ContentServiceImpl contentService;

    @Mock
    private IdMapCacheMgr idMapCacheMgr;

    @InjectMocks
    private AccessSettingMigrationServiceImpl migrationService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private Map<String, Object> buildValidAccessSetting(String contextId) throws Exception {
        Map<String, Object> criteria = Map.of(
                Constants.CRITERIA_KEY, "designation",
                Constants.CRITERIA_VALUE, List.of("teacher", "mentor"));

        Map<String, Object> userGroup = Map.of(
                Constants.USER_GROUP_ID, "group-123",
                Constants.USER_GROUP_NAME, "Test Group",
                Constants.USER_GROUP_CRTIRIA_LIST, List.of(criteria));

        Map<String, Object> accessControl = Map.of(Constants.USER_GROUPS, List.of(userGroup));

        Map<String, Object> contextData = Map.of(Constants.ACCESS_CONTROL, accessControl);

        Map<String, Object> accessSettingMap = new HashMap<>();
        accessSettingMap.put(Constants.CONTEXT_ID, contextId);
        accessSettingMap.put(Constants.CONTEXT_DATA, objectMapper.writeValueAsString(contextData));
        return accessSettingMap;
    }

    @Test
    void testMigrateAccessSettingRules_success() throws Exception {
        String contextId = "do_123";
        Map<String, Object> accessSettingMap = buildValidAccessSetting(contextId);
        List<Map<String, Object>> dbRecords = List.of(new HashMap<>(accessSettingMap));

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.ACCESS_SETTINGS_RULES_TABLE),
                isNull(), isNull(), isNull()))
                .thenReturn(dbRecords);

        when(contentService.readCourseCategoryForContent(contextId)).thenReturn("Course");

        Map<String, Long> idMap = Map.of("teacher", 1L, "mentor", 2L);
        when(idMapCacheMgr.getId(anyList())).thenReturn(idMap);

        ApiResponse response = migrationService.migrateAccessSettingRules();

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getResult().get(contextId));
        verify(cassandraOperation, times(1)).insertRecord(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.ACCESS_SETTINGS_RULES_TABLE_V2),
                anyMap());
    }

    @Test
    void testMigrateAccessSettingRules_emptyContextData() {
        String contextId = "do_456";
        Map<String, Object> accessSettingMap = new HashMap<>();
        accessSettingMap.put(Constants.CONTEXT_ID, contextId);
        accessSettingMap.put(Constants.CONTEXT_DATA, "");

        when(cassandraOperation.getRecordsByProperties(
                anyString(), anyString(), isNull(), isNull(), isNull()))
                .thenReturn(List.of(accessSettingMap));

        ApiResponse response = migrationService.migrateAccessSettingRules();

        assertEquals(Constants.FAILED, response.getResult().get(contextId));
        verify(cassandraOperation, never()).insertRecord(any(), any(), any());
    }

    @Test
    void testMigrateAccessSettingRules_parsingFailure() {
        String contextId = "do_789";
        Map<String, Object> accessSettingMap = new HashMap<>();
        accessSettingMap.put(Constants.CONTEXT_ID, contextId);
        accessSettingMap.put(Constants.CONTEXT_DATA, "invalid_json");

        when(cassandraOperation.getRecordsByProperties(
                anyString(), anyString(), isNull(), isNull(), isNull()))
                .thenReturn(List.of(accessSettingMap));

        ApiResponse response = migrationService.migrateAccessSettingRules();

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertFalse(response.getResult().containsKey(contextId));
    }

    @Test
    void testMigrateAccessSettingRules_exception() {
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenThrow(new RuntimeException("DB error"));

        ApiResponse response = migrationService.migrateAccessSettingRules();

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals("Migration failed due to an error", response.getParams().getErrMsg());
    }

    @Test
    void testUpdateContextDataWithIdMap_emptyUserGroups() throws Exception {
        String contextId = "ctx-empty-groups";

        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, List.of()); // empty list

        Map<String, Object> accessControlIdMap = new HashMap<>();

        migrationService = new AccessSettingMigrationServiceImpl(cassandraOperation, contentService, idMapCacheMgr);

        // Use reflection to test private method or move it to package-private for
        // easier testing
        var method = AccessSettingMigrationServiceImpl.class.getDeclaredMethod(
                "updateContextDataWithIdMap", String.class, Map.class, Map.class);
        method.setAccessible(true);

        method.invoke(migrationService, contextId, accessControl, accessControlIdMap);

        assertTrue(accessControlIdMap.isEmpty());
    }

    @Test
    void testUpdateContextDataWithIdMap_emptyCriteriaValues() throws Exception {
        String contextId = "ctx-empty-criteria";

        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, "designation");
        criteria.put(Constants.CRITERIA_VALUE, List.of()); // empty list

        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_ID, "g1");
        userGroup.put(Constants.USER_GROUP_NAME, "UG1");
        userGroup.put(Constants.USER_GROUP_CRTIRIA_LIST, List.of(criteria));

        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, List.of(userGroup));

        Map<String, Object> accessControlIdMap = new HashMap<>();

        var method = AccessSettingMigrationServiceImpl.class.getDeclaredMethod(
                "updateContextDataWithIdMap", String.class, Map.class, Map.class);
        method.setAccessible(true);

        method.invoke(migrationService, contextId, accessControl, accessControlIdMap);

        assertTrue(accessControlIdMap.isEmpty());
    }

    @Test
    void testUpdateContextDataWithIdMap_emptyIdMap() throws Exception {
        String contextId = "ctx-empty-idmap";

        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, "designation");
        criteria.put(Constants.CRITERIA_VALUE, List.of("officer"));

        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_ID, "g1");
        userGroup.put(Constants.USER_GROUP_NAME, "UG1");
        userGroup.put(Constants.USER_GROUP_CRTIRIA_LIST, List.of(criteria));

        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, List.of(userGroup));

        Map<String, Object> accessControlIdMap = new HashMap<>();

        when(idMapCacheMgr.getId(anyList())).thenReturn(Collections.emptyMap());

        var method = AccessSettingMigrationServiceImpl.class.getDeclaredMethod(
                "updateContextDataWithIdMap", String.class, Map.class, Map.class);
        method.setAccessible(true);

        method.invoke(migrationService, contextId, accessControl, accessControlIdMap);

        assertTrue(accessControlIdMap.isEmpty());
    }

    @Test
    void testUpdateContextDataWithIdMap_sizeMismatch() throws Exception {
        String contextId = "ctx-size-mismatch";

        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, "designation");
        criteria.put(Constants.CRITERIA_VALUE, List.of("officer", "clerk"));

        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_ID, "g1");
        userGroup.put(Constants.USER_GROUP_NAME, "UG1");
        userGroup.put(Constants.USER_GROUP_CRTIRIA_LIST, List.of(criteria));

        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, List.of(userGroup));

        Map<String, Object> accessControlIdMap = new HashMap<>();

        // Only one entry returned instead of two
        when(idMapCacheMgr.getId(anyList())).thenReturn(Map.of("officer", 1L));

        var method = AccessSettingMigrationServiceImpl.class.getDeclaredMethod(
                "updateContextDataWithIdMap", String.class, Map.class, Map.class);
        method.setAccessible(true);

        method.invoke(migrationService, contextId, accessControl, accessControlIdMap);

        assertTrue(accessControlIdMap.isEmpty());
    }

}
