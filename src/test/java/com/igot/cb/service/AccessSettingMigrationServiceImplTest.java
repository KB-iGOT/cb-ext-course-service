package com.igot.cb.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

import java.util.*;

import com.igot.cb.elasticsearch.service.EsUtilService;
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
        private ContentInfoServiceImpl contentService;

        @Mock
        private IdMapCacheMgr idMapCacheMgr;

        @Mock
        private EsUtilService esUtilService;

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
                                Constants.USER_GROUP_CRITERIA_LIST, List.of(criteria));

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

                Map<String, Integer> idMap = Map.of("teacher", 1, "mentor", 2);
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
        accessControl.put("version", 1);

        List<Map<String, Object>> userGroups = new ArrayList<>();

        // User Group 1
        Map<String, Object> userGroup1 = new HashMap<>();
        userGroup1.put("userGroupId", "uuid1");
        userGroup1.put("userGroupName", "User Group 1");
        List<Map<String, Object>> criteriaList1 = new ArrayList<>();

        Map<String, Object> rule1 = new HashMap<>();
        rule1.put("criteriaKey", "rootOrgId");
        rule1.put("criteriaValue", new ArrayList<>()); // empty list to trigger failure

        Map<String, Object> rule2 = new HashMap<>();
        rule2.put("criteriaKey", "designation");
        rule2.put("criteriaValue", Arrays.asList("Post Master", "Accountant"));

        criteriaList1.add(rule1);
        criteriaList1.add(rule2);
        userGroup1.put("userGroupCriteriaList", criteriaList1);
        userGroups.add(userGroup1);

        // User Group 2
        Map<String, Object> userGroup2 = new HashMap<>();
        userGroup2.put("userGroupId", "uuid2");
        userGroup2.put("userGroupName", "User Group 2");
        List<Map<String, Object>> criteriaList2 = new ArrayList<>();

        Map<String, Object> rule3 = new HashMap<>();
        rule3.put("criteriaKey", "rootOrgId");
        rule3.put("criteriaValue", Arrays.asList("orgId3"));

        criteriaList2.add(rule3);
        userGroup2.put("userGroupCriteriaList", criteriaList2);
        userGroups.add(userGroup2);

        // User Group 3
        Map<String, Object> userGroup3 = new HashMap<>();
        userGroup3.put("userGroupId", "uuid3");
        userGroup3.put("userGroupName", "User Group 3");
        List<Map<String, Object>> criteriaList3 = new ArrayList<>();

        Map<String, Object> rule4 = new HashMap<>();
        rule4.put("criteriaKey", "user");
        rule4.put("criteriaValue", Arrays.asList("userId1", "userId2"));

        criteriaList3.add(rule4);
        userGroup3.put("userGroupCriteriaList", criteriaList3);
        userGroups.add(userGroup3);

        accessControl.put("userGroups", userGroups);

        Map<String, Object> accessControlIdMap = new HashMap<>();
        migrationService = new AccessSettingMigrationServiceImpl(
                cassandraOperation, contentService, idMapCacheMgr, esUtilService);

        var method = AccessSettingMigrationServiceImpl.class.getDeclaredMethod(
                "updateContextDataWithIdMap", String.class, Map.class, Map.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(migrationService, contextId, accessControl, accessControlIdMap);

        // Assertions
        assertFalse(result);
        assertTrue(accessControlIdMap.containsKey(Constants.USER_GROUPS));
        assertTrue(((List<?>) accessControlIdMap.get(Constants.USER_GROUPS)).isEmpty());
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
                userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, List.of(criteria));

                Map<String, Object> accessControl = new HashMap<>();
                accessControl.put(Constants.USER_GROUPS, List.of(userGroup));

                Map<String, Object> accessControlIdMap = new HashMap<>();

                var method = AccessSettingMigrationServiceImpl.class.getDeclaredMethod(
                                "updateContextDataWithIdMap", String.class, Map.class, Map.class);
                method.setAccessible(true);

                method.invoke(migrationService, contextId, accessControl, accessControlIdMap);

                boolean result = (boolean) method.invoke(migrationService, contextId, accessControl,
                                accessControlIdMap);
                assertFalse(result);
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
                userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, List.of(criteria));

                Map<String, Object> accessControl = new HashMap<>();
                accessControl.put(Constants.USER_GROUPS, List.of(userGroup));

                Map<String, Object> accessControlIdMap = new HashMap<>();

                when(idMapCacheMgr.getId(anyList())).thenReturn(Collections.emptyMap());

                var method = AccessSettingMigrationServiceImpl.class.getDeclaredMethod(
                                "updateContextDataWithIdMap", String.class, Map.class, Map.class);
                method.setAccessible(true);

                method.invoke(migrationService, contextId, accessControl, accessControlIdMap);

                boolean result = (boolean) method.invoke(migrationService, contextId, accessControl,
                                accessControlIdMap);
                assertFalse(result);
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
                userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, List.of(criteria));

                Map<String, Object> accessControl = new HashMap<>();
                accessControl.put(Constants.USER_GROUPS, List.of(userGroup));

                Map<String, Object> accessControlIdMap = new HashMap<>();

                // Only one entry returned instead of two
                when(idMapCacheMgr.getId(anyList())).thenReturn(Map.of("officer", 1));

                var method = AccessSettingMigrationServiceImpl.class.getDeclaredMethod(
                                "updateContextDataWithIdMap", String.class, Map.class, Map.class);
                method.setAccessible(true);

                method.invoke(migrationService, contextId, accessControl, accessControlIdMap);

                boolean result = (boolean) method.invoke(migrationService, contextId, accessControl,
                                accessControlIdMap);
                assertFalse(result);
        }

        @Test
        void testProcessAccessSettingRule_emptyContextDataMap() throws Exception {
                String contextId = "ctx-empty-map";

                Map<String, Object> accessSettingMap = new HashMap<>();
                accessSettingMap.put(Constants.CONTEXT_ID, contextId);
                accessSettingMap.put(Constants.CONTEXT_DATA, "{}");

                when(contentService.readCourseCategoryForContent(contextId)).thenReturn("Course");

                boolean result = migrationService.processAccessSettingRule(accessSettingMap);

                assertFalse(result);
        }

        @Test
        void testProcessAccessSettingRule_missingAccessControl() throws Exception {
                String contextId = "ctx-missing-access-control";

                Map<String, Object> contextData = Map.of("other", "data");
                Map<String, Object> accessSettingMap = new HashMap<>();
                accessSettingMap.put(Constants.CONTEXT_ID, contextId);
                accessSettingMap.put(Constants.CONTEXT_DATA, objectMapper.writeValueAsString(contextData));

                when(contentService.readCourseCategoryForContent(contextId)).thenReturn("Course");

                boolean result = migrationService.processAccessSettingRule(accessSettingMap);

                assertFalse(result);
        }

        @Test
        void testProcessAccessSettingRule_userGroupSizeMismatch() throws Exception {
                String contextId = "ctx-ug-mismatch";

                Map<String, Object> criteria = Map.of(
                                Constants.CRITERIA_KEY, "designation",
                                Constants.CRITERIA_VALUE, List.of("A"));

                Map<String, Object> userGroup = Map.of(
                                Constants.USER_GROUP_ID, "g1",
                                Constants.USER_GROUP_NAME, "name",
                                Constants.USER_GROUP_CRITERIA_LIST, List.of(criteria));

                Map<String, Object> accessControl = Map.of(Constants.USER_GROUPS, List.of(userGroup));
                Map<String, Object> contextData = Map.of(Constants.ACCESS_CONTROL, accessControl);

                Map<String, Object> accessSettingMap = new HashMap<>();
                accessSettingMap.put(Constants.CONTEXT_ID, contextId);
                accessSettingMap.put(Constants.CONTEXT_DATA, objectMapper.writeValueAsString(contextData));

                when(contentService.readCourseCategoryForContent(contextId)).thenReturn("Course");

                var spyService = new AccessSettingMigrationServiceImpl(cassandraOperation, contentService,
                                idMapCacheMgr,esUtilService) {
                        @Override
                        public boolean updateContextDataWithIdMap(String ctxId, Map<String, Object> accessControl,
                                        Map<String, Object> accessControlIdMap) {
                                // Input had 1 user group, but we fake 2 here to simulate mismatch
                                List<Map<String, Object>> fakeList = List.of(
                                                Map.of(Constants.USER_GROUP_ID, "g1"),
                                                Map.of(Constants.USER_GROUP_ID, "g2"));
                                accessControlIdMap.put(Constants.USER_GROUPS, fakeList);
                                return true;
                        }
                };

                boolean result = spyService.processAccessSettingRule(accessSettingMap);
                assertFalse(result);
        }

        @Test
        void testCreateBitSetForAttribute_invalidValue() {
                List<Integer> invalidValues = List.of(-1, Integer.MAX_VALUE); // or simulate overflow

                try {
                        migrationService.createBitSetForAttribute(invalidValues);
                } catch (Exception ex) {
                        assertTrue(ex instanceof IndexOutOfBoundsException);
                }
        }

        @Test
        void testMigrateAccessSettingRules_partialSuccess() throws Exception {
                String successContextId = "do_success";
                String failContextId = "do_fail";

                Map<String, Object> successMap = buildValidAccessSetting(successContextId);
                Map<String, Object> failMap = new HashMap<>();
                failMap.put(Constants.CONTEXT_ID, failContextId);
                failMap.put(Constants.CONTEXT_DATA, "");

                when(cassandraOperation.getRecordsByProperties(any(), any(), isNull(), isNull(), isNull()))
                                .thenReturn(List.of(successMap, failMap));

                when(contentService.readCourseCategoryForContent(successContextId)).thenReturn("Course");
                when(idMapCacheMgr.getId(anyList())).thenReturn(Map.of("teacher", 1, "mentor", 2));

                ApiResponse response = migrationService.migrateAccessSettingRules();

                assertEquals(Constants.SUCCESS, response.getResult().get(successContextId));
                assertEquals(Constants.FAILED, response.getResult().get(failContextId));
        }

        @Test
        void testProcessAccessSettingRule_multipleUserGroupsAndCriteria() throws Exception {
                String contextId = "do_multi_criteria";

                Map<String, Object> criteria1 = Map.of(
                                Constants.CRITERIA_KEY, "designation",
                                Constants.CRITERIA_VALUE, List.of("teacher", "mentor"));

                Map<String, Object> criteria2 = Map.of(
                                Constants.CRITERIA_KEY, "department",
                                Constants.CRITERIA_VALUE, List.of("math", "science"));

                Map<String, Object> userGroup1 = Map.of(
                                Constants.USER_GROUP_ID, "group-001",
                                Constants.USER_GROUP_NAME, "Group A",
                                Constants.USER_GROUP_CRITERIA_LIST, List.of(criteria1));

                Map<String, Object> userGroup2 = Map.of(
                                Constants.USER_GROUP_ID, "group-002",
                                Constants.USER_GROUP_NAME, "Group B",
                                Constants.USER_GROUP_CRITERIA_LIST, List.of(criteria2));

                Map<String, Object> accessControl = Map.of(
                                Constants.USER_GROUPS, List.of(userGroup1, userGroup2));

                Map<String, Object> contextData = Map.of(Constants.ACCESS_CONTROL, accessControl);

                Map<String, Object> accessSettingMap = new HashMap<>();
                accessSettingMap.put(Constants.CONTEXT_ID, contextId);
                accessSettingMap.put(Constants.CONTEXT_DATA, new ObjectMapper().writeValueAsString(contextData));

                // Mock category and ID map results
                when(contentService.readCourseCategoryForContent(contextId)).thenReturn("Course");
                when(idMapCacheMgr.getId(anyList())).thenAnswer(invocation -> {
                        List<String> values = invocation.getArgument(0);
                        Map<String, Integer> result = new HashMap<>();
                        for (int i = 0; i < values.size(); i++) {
                                result.put(values.get(i), i + 1); // teacher=1, mentor=2, etc.
                        }
                        return result;
                });

                boolean result = migrationService.processAccessSettingRule(accessSettingMap);

                // Assertions
                assertTrue(result);

                // Validate that contextData was updated with accessControlId map
                String updatedContextDataJson = (String) accessSettingMap.get(Constants.CONTEXT_DATA);
                Map<String, Object> updatedContextData = new ObjectMapper().readValue(updatedContextDataJson,
                                Map.class);

                assertTrue(updatedContextData.containsKey(Constants.ACCESS_CONTROL_ID));
                Map<String, Object> accessControlId = (Map<String, Object>) updatedContextData
                                .get(Constants.ACCESS_CONTROL_ID);
                List<Map<String, Object>> updatedUserGroups = (List<Map<String, Object>>) accessControlId
                                .get(Constants.USER_GROUPS);
                assertEquals(2, updatedUserGroups.size());

                for (Map<String, Object> group : updatedUserGroups) {
                        List<Map<String, Object>> criteriaList = (List<Map<String, Object>>) group
                                        .get(Constants.USER_GROUP_CRITERIA_LIST);
                        assertEquals(1, criteriaList.size());
                        for (Map<String, Object> crit : criteriaList) {
                                assertTrue(crit.containsKey(Constants.CRITERIA_KEY));
                                assertTrue(crit.containsKey(Constants.CRITERIA_VALUE)); // should be BitSet
                        }
                }
        }

        @Test
        void testProcessAccessSettingRule_bitSetThrowsException() throws Exception {
                String contextId = "do_bitset_error";

                Map<String, Object> criteria = Map.of(
                                Constants.CRITERIA_KEY, "designation",
                                Constants.CRITERIA_VALUE, List.of("invalid-designation"));

                Map<String, Object> userGroup = Map.of(
                                Constants.USER_GROUP_ID, "group-error",
                                Constants.USER_GROUP_NAME, "Error Group",
                                Constants.USER_GROUP_CRITERIA_LIST, List.of(criteria));

                Map<String, Object> accessControl = Map.of(
                                Constants.USER_GROUPS, List.of(userGroup));
                Map<String, Object> contextData = Map.of(Constants.ACCESS_CONTROL, accessControl);

                Map<String, Object> accessSettingMap = new HashMap<>();
                accessSettingMap.put(Constants.CONTEXT_ID, contextId);
                accessSettingMap.put(Constants.CONTEXT_DATA, new ObjectMapper().writeValueAsString(contextData));

                when(contentService.readCourseCategoryForContent(contextId)).thenReturn("Course");

                // Force invalid ID value (e.g., -1)
                when(idMapCacheMgr.getId(anyList())).thenReturn(Map.of("invalid-designation", -1));

                // Expect the process method to throw
                assertThrows(IndexOutOfBoundsException.class, () -> {
                        migrationService.processAccessSettingRule(accessSettingMap);
                });
        }

        @Test
        void testMigrateCBPlanAccessSettingRules_success() {
                List<Map<String, Object>> cbPlanList = new ArrayList<>();
                Map<String, Object> cbPlan = new HashMap<>();
                cbPlan.put(Constants.STATUS, Constants.DRAFT);
                cbPlan.put(Constants.DRAFT_DATA, "{\"name\":\"Test Plan\",\"endDate\":\"2024-12-31\",\"contentList\":[\"content1\"],\"contentType\":\"Course\"}");
                cbPlan.put(Constants.ORG_ID, "org1");
                cbPlan.put(Constants.ID, "plan1");
                cbPlan.put(Constants.ASSIGNMENT_TYPE, "Designation");
                cbPlan.put(Constants.ASSIGNMENT_TYPE_INFO, Arrays.asList("teacher"));
                cbPlan.put(Constants.CREATED_AT_KEY, java.time.Instant.now());
                cbPlan.put(Constants.CREATED_BY, "user1");
                cbPlan.put(Constants.IS_APAR, true);
                cbPlan.put(Constants.PUBLISHED_AT_KEY, java.time.Instant.now());
                cbPlan.put(Constants.CB_PUBLISHED_BY, "user1");
                cbPlan.put(Constants.COMMENT, "Test comment");
                cbPlan.put(Constants.UPDATED_AT, java.time.Instant.now());
                cbPlan.put(Constants.UPDATED_BY, "user1");
                cbPlanList.add(cbPlan);

                when(cassandraOperation.getRecordsByProperties(
                        eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.CB_PLAN_TABLE),
                        isNull(), isNull(), isNull())).thenReturn(cbPlanList);



                ApiResponse dbResponse = new ApiResponse();
                dbResponse.put(Constants.RESPONSE, Constants.SUCCESS);
                when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap())).thenReturn(dbResponse);

                ApiResponse response = migrationService.migrateCBPlanAccessSettingRules();

                assertEquals(HttpStatus.OK, response.getResponseCode());
                assertEquals(1, response.getResult().get("Successful"));
                assertEquals(0, response.getResult().get("Skipped"));
        }

        @Test
        void testMigrateCBPlanAccessSettingRules_livePlan() {
                List<Map<String, Object>> cbPlanList = new ArrayList<>();
                Map<String, Object> cbPlan = new HashMap<>();
                cbPlan.put(Constants.STATUS, Constants.LIVE);
                cbPlan.put(Constants.NAME, "Live Plan");
                cbPlan.put(Constants.END_DATE_KEY, java.time.Instant.now());
                cbPlan.put(Constants.CONTENT_LIST, Arrays.asList("content1"));
                cbPlan.put(Constants.CONTENT_TYPE, "Course");
                cbPlan.put(Constants.ORG_ID, "org1");
                cbPlan.put(Constants.ID, "plan1");
                cbPlan.put(Constants.ASSIGNMENT_TYPE, "AllUser");
                cbPlan.put(Constants.CREATED_AT_KEY, java.time.Instant.now());
                cbPlan.put(Constants.CREATED_BY, "user1");
                cbPlanList.add(cbPlan);

                when(cassandraOperation.getRecordsByProperties(
                        eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.CB_PLAN_TABLE),
                        isNull(), isNull(), isNull())).thenReturn(cbPlanList);



                ApiResponse dbResponse = new ApiResponse();
                dbResponse.put(Constants.RESPONSE, Constants.SUCCESS);
                when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap())).thenReturn(dbResponse);

                ApiResponse response = migrationService.migrateCBPlanAccessSettingRules();

                assertEquals(HttpStatus.OK, response.getResponseCode());
        }

        @Test
        void testMigrateCBPlanAccessSettingRules_customUserType() {
                List<Map<String, Object>> cbPlanList = new ArrayList<>();
                Map<String, Object> cbPlan = new HashMap<>();
                cbPlan.put(Constants.STATUS, Constants.DRAFT);
                cbPlan.put(Constants.DRAFT_DATA, "{\"name\":\"Test Plan\"}");
                cbPlan.put(Constants.ORG_ID, "org1");
                cbPlan.put(Constants.ID, "plan1");
                cbPlan.put(Constants.ASSIGNMENT_TYPE, "CustomUser");
                cbPlan.put(Constants.ASSIGNMENT_TYPE_INFO, Arrays.asList("user1", "user2"));
                cbPlan.put(Constants.CREATED_AT_KEY, java.time.Instant.now());
                cbPlan.put(Constants.CREATED_BY, "user1");
                cbPlanList.add(cbPlan);

                when(cassandraOperation.getRecordsByProperties(
                        eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.CB_PLAN_TABLE),
                        isNull(), isNull(), isNull())).thenReturn(cbPlanList);


                ApiResponse dbResponse = new ApiResponse();
                dbResponse.put(Constants.RESPONSE, Constants.SUCCESS);
                when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap())).thenReturn(dbResponse);

                ApiResponse response = migrationService.migrateCBPlanAccessSettingRules();

                assertEquals(HttpStatus.OK, response.getResponseCode());
        }

        @Test
        void testMigrateCBPlanAccessSettingRules_dbError() {
                List<Map<String, Object>> cbPlanList = new ArrayList<>();
                Map<String, Object> cbPlan = new HashMap<>();
                cbPlan.put(Constants.STATUS, Constants.DRAFT);
                cbPlan.put(Constants.DRAFT_DATA, "{\"name\":\"Test Plan\"}");
                cbPlan.put(Constants.ORG_ID, "org1");
                cbPlan.put(Constants.ID, "plan1");
                cbPlan.put(Constants.ASSIGNMENT_TYPE, "AllUser");
                cbPlan.put(Constants.CREATED_AT_KEY, java.time.Instant.now());
                cbPlan.put(Constants.CREATED_BY, "user1");
                cbPlanList.add(cbPlan);

                when(cassandraOperation.getRecordsByProperties(
                        eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.CB_PLAN_TABLE),
                        isNull(), isNull(), isNull())).thenReturn(cbPlanList);



                ApiResponse dbResponse = new ApiResponse();
                dbResponse.put(Constants.RESPONSE, Constants.FAILED);
                dbResponse.put(Constants.ERROR_MESSAGE, "Database error");
                when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap())).thenReturn(dbResponse);

                ApiResponse response = migrationService.migrateCBPlanAccessSettingRules();

                assertEquals(0, response.getResult().get("Successful"));
                assertEquals(1, response.getResult().get("Skipped"));
        }

        @Test
        void testMigrateCBPlanAccessSettingRules_exception() {
                when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                        .thenThrow(new RuntimeException("DB error"));

                ApiResponse response = migrationService.migrateCBPlanAccessSettingRules();

                assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        }



}
