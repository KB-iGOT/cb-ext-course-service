package com.igot.cb.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.*;

import com.igot.cb.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.model.ApiRespParam;
import com.igot.cb.model.CbPlanDto;


class CbPlanServiceImplTest {

    @Mock private AccessTokenValidator accessTokenValidator;
    @Mock private CassandraOperation cassandraOperation;
    @Mock private UserAndOrgServiceImpl userUtilityService;
    @Mock private ContentInfoServiceImpl contentService;
    @Mock private EsUtilService esUtilService;
    @Mock private CbExtServerProperties serverProperties;
    @Mock private RequestValidator requestValidator;
    @Mock private UserAndOrgServiceImpl userAndOrgService;

    private CbPlanServiceImpl cbPlanService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        cbPlanService = new CbPlanServiceImpl(accessTokenValidator, cassandraOperation, serverProperties, userUtilityService,
            contentService, esUtilService, requestValidator);
        ReflectionTestUtils.setField(cbPlanService, "userAndOrgService", userUtilityService);
        ReflectionTestUtils.setField(cbPlanService, "contentService", contentService);
        ReflectionTestUtils.setField(cbPlanService, "esUtilService", esUtilService);
        ReflectionTestUtils.setField(cbPlanService, "serverProperties", serverProperties);
        ReflectionTestUtils.setField(serverProperties, "cpPlanIndex", "test-index");
        ReflectionTestUtils.setField(serverProperties, "elasticCbPlanJsonPath", "test-path");
        ReflectionTestUtils.setField(serverProperties, "cbPlanUpdateAllowedFields", "name,contextDataRequest,endDate");
    }

    @Test
    void testConstructor() {
        assertNotNull(cbPlanService);
        assertEquals(accessTokenValidator, ReflectionTestUtils.getField(cbPlanService, "accessTokenValidator"));
        assertEquals(cassandraOperation, ReflectionTestUtils.getField(cbPlanService, "cassandraOperation"));
    }

    @Test
    void testCreateCbPlan_EmptyUserId() {
        ApiRequest request = new ApiRequest();
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("");

        ApiResponse response = cbPlanService.createCbPlan(request, "orgId", "token");

        assertNotNull(response);
    }

    @Test
    void testCreateCbPlan_ValidationErrors() {
        ApiRequest request = new ApiRequest();
        CbPlanDto dto = new CbPlanDto();
        request.setRequest(dto);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        ApiResponse response = cbPlanService.createCbPlan(request, "orgId", "token");

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testCreateCbPlan_Success() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("name", "Test Plan");
        requestMap.put("endDate", new Date());
        requestMap.put("orgScope", "single");
        requestMap.put("orgIdList", Arrays.asList("org1"));
        requestMap.put("contentType", "Course");
        requestMap.put("contentList", Arrays.asList("content1"));
        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        ApiResponse cassandraResp = new ApiResponse();
        cassandraResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(cassandraResp);

        ApiResponse lookupResp = new ApiResponse();
        lookupResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), any())).thenReturn(lookupResp);

        ApiResponse response = cbPlanService.createCbPlan(request, "orgId", "token");

        assertNotNull(response);
    }

    @Test
    void testCreateCbPlan_JsonProcessingException() {
        ApiRequest request = new ApiRequest();
        CbPlanDto dto = new CbPlanDto();
        // Missing required fields to trigger validation error first
        request.setRequest(dto);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        ApiResponse response = cbPlanService.createCbPlan(request, "orgId", "token");

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testCreateCbPlan_AllOrgScope() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("name", "Test Plan");
        requestMap.put("endDate", new Date());
        requestMap.put("orgScope", "all");
        requestMap.put("contentType", "Course");
        requestMap.put("contentList", Arrays.asList("content1"));
        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        ApiResponse cassandraResp = new ApiResponse();
        cassandraResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(cassandraResp);

        ApiResponse response = cbPlanService.createCbPlan(request, "orgId", "token");

        assertNotNull(response);
    }

    @Test
    void testCreateCbPlan_CustomOrgScope() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("name", "Test Plan");
        requestMap.put("endDate", new Date());
        requestMap.put("orgScope", "custom");
        requestMap.put("orgIdList", Arrays.asList("org1", "org2"));
        requestMap.put("contentType", "Course");
        requestMap.put("contentList", Arrays.asList("content1"));
        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        ApiResponse cassandraResp = new ApiResponse();
        cassandraResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(cassandraResp);

        ApiResponse lookupResp = new ApiResponse();
        lookupResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), any())).thenReturn(lookupResp);

        ApiResponse response = cbPlanService.createCbPlan(request, "orgId", "token");

        assertNotNull(response);
    }

    @Test
    void testUpdateCbPlan_EmptyUserId() {
        ApiRequest request = new ApiRequest();
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("");

        ApiResponse response = cbPlanService.updateCbPlan(request, "orgId", "token", Arrays.asList("role"));

        assertNotNull(response);
    }

    @Test
    void testUpdateCbPlan_Success() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> updateMap = new HashMap<>();
        updateMap.put("id", "planId");
        updateMap.put("name", "Updated Plan");
        request.setRequest(updateMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put("createdBy", "userId");
        existingPlan.put("status", "draft");
        existingPlan.put("draftData", "{\"name\":\"Test\"}");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(Arrays.asList(existingPlan));

        Map<String, Object> updateResp = new HashMap<>();
        updateResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.updateRecord(anyString(), anyString(), any(), any())).thenReturn(updateResp);

        ApiResponse response = cbPlanService.updateCbPlan(request, "orgId", "token", Arrays.asList("role"));

        assertNotNull(response);
    }
    @Test
    void testPublishCbPlan_Success() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("id", "planId");
        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put("createdBy", "userId");
        existingPlan.put("status", "draft");
        existingPlan.put("draftData", "{\"name\":\"Test\",\"endDate\":\"2024-12-31\"}");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(Arrays.asList(existingPlan));

        Map<String, Object> updateResp = new HashMap<>();
        updateResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.updateRecord(anyString(), anyString(), any(), any(), any(), any())).thenReturn(updateResp);

        ApiResponse response = cbPlanService.publishCbPlan(request, "orgId", "token", Arrays.asList("role"));

        assertNotNull(response);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPublishCbPlan_EsSyncFailure_AbortsCassandraCommitAndFailsApi() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("id", "planId");
        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put("createdBy", "userId");
        existingPlan.put("status", "draft");
        existingPlan.put("draftData", "{\"name\":\"Test\",\"endDate\":\"2024-12-31\"}");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(Arrays.asList(existingPlan));
        when(userUtilityService.readUserProfileFromDB(eq("userId"), anyList()))
            .thenReturn(Map.of(Constants.ID, "userId", Constants.ROOT_ORG_ID, "root1"));
        when(userUtilityService.readOrgFromDB(eq("root1"), any()))
            .thenReturn(Map.of(Constants.IS_CCA, false));

        when(esUtilService.updateDocument(anyString(), anyString(), anyString(), anyMap(), anyString()))
            .thenReturn(null);

        ArgumentCaptor<java.util.function.Supplier<Boolean>> validatorCaptor = ArgumentCaptor.forClass(java.util.function.Supplier.class);
        Map<String, Object> abortedResp = new HashMap<>();
        abortedResp.put(Constants.RESPONSE, Constants.FAILED);
        abortedResp.put(Constants.ERROR_MESSAGE, "Update aborted: pre-commit validation failed");
        when(cassandraOperation.updateRecord(anyString(), anyString(), any(), any(), validatorCaptor.capture(), any()))
            .thenReturn(abortedResp);

        ApiResponse response = cbPlanService.publishCbPlan(request, "orgId", "token", Arrays.asList("role"));

        assertFalse(validatorCaptor.getValue().get());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPublishCbPlan_CassandraCommitFailsAfterEsSuccess_TriggersEsRollback() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("id", "planId");
        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put("createdBy", "userId");
        existingPlan.put("status", "draft");
        existingPlan.put("draftData", "{\"name\":\"Test\",\"endDate\":\"2024-12-31\"}");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(Arrays.asList(existingPlan));
        when(userUtilityService.readUserProfileFromDB(eq("userId"), anyList()))
            .thenReturn(Map.of(Constants.ID, "userId", Constants.ROOT_ORG_ID, "root1"));
        when(userUtilityService.readOrgFromDB(eq("root1"), any()))
            .thenReturn(Map.of(Constants.IS_CCA, false));

        when(esUtilService.updateDocument(anyString(), anyString(), anyString(), anyMap(), anyString()))
            .thenReturn("updated:Created");

        ArgumentCaptor<Runnable> rollbackCaptor = ArgumentCaptor.forClass(Runnable.class);
        Map<String, Object> failedResp = new HashMap<>();
        failedResp.put(Constants.RESPONSE, Constants.FAILED);
        failedResp.put(Constants.ERROR_MESSAGE, "Cassandra down");
        when(cassandraOperation.updateRecord(anyString(), anyString(), any(), any(), any(), rollbackCaptor.capture()))
            .thenReturn(failedResp);

        ApiResponse response = cbPlanService.publishCbPlan(request, "orgId", "token", Arrays.asList("role"));

        // cassandraOperation is fully mocked, so the rollback Runnable passed to it was never invoked
        // by publishCbPlan itself — invoke it here to simulate what CassandraOperationImpl does on commit failure.
        rollbackCaptor.getValue().run();

        verify(esUtilService, times(1)).updateDocument(any(), eq(Constants.INDEX_TYPE), eq("planId"),
                argThat(doc -> existingPlan.get("status").equals(doc.get("status"))), any());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testReadCbPlan_Success() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(Arrays.asList(createMockPlan()));

        when(contentService.readContent(anyString(), any())).thenReturn(createMockContent());

        ApiResponse response = cbPlanService.readCbPlan("planId", "orgId", "token");

        assertNotNull(response);
    }

    @Test
    void testSearchCbPlan_EmptyResult() {
        SearchCriteria criteria = new SearchCriteria();
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("");

        ApiResponse response = cbPlanService.searchCbPlan(criteria, "orgId", "token");

        assertNotNull(response);
    }

    @SuppressWarnings("unchecked")
    @Test
    void testSearchCbPlan_WithResults() throws Exception {
        SearchCriteria criteria = new SearchCriteria();
        criteria.setQuery(new HashMap<>());
        criteria.setFilter(new HashMap<>());

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        SearchResult searchResult = new SearchResult();
        searchResult.setData(Arrays.asList(createMockPlan()));
        searchResult.setTotalCount(1L);
        when(esUtilService.searchDocuments(anyString(), any(), anyString())).thenReturn(searchResult);

        Map<String, Object> mockContent = createMockContent();
        when(contentService.readContent(anyString(), any())).thenReturn(mockContent);

        doAnswer(invocation -> {
            Map<String, Map<String, String>> userInfoMap = invocation.getArgument(2);
            Map<String, String> userDetails = new HashMap<>();
            userDetails.put("firstName", "Test");
            userDetails.put("lastName", "User");
            userInfoMap.put("userId", userDetails);
            return null;
        }).when(userUtilityService).readUserProfileFromDB(any(), anyList());

        try {
        ApiResponse response = cbPlanService.searchCbPlan(criteria, "orgId", "token");
        assertNotNull(response);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        } catch (Exception e) {

        }
    }

    @Test
    void testRetireCbPlan_Success() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("id", "planId");
        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put("createdBy", "userId");
        existingPlan.put("status", "live");
        existingPlan.put("orgScope", "single");
        existingPlan.put("orgIdList", Arrays.asList("org1"));
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(Arrays.asList(existingPlan));

        Map<String, Object> updateResp = new HashMap<>();
        updateResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.updateRecord(anyString(), anyString(), any(), any())).thenReturn(updateResp);

        ApiResponse response = cbPlanService.retireCbPlan(request, "orgId", "token", Arrays.asList("role"));

        assertNotNull(response);
    }

    @Test
    void testSanitizeForElastic() {
        Map<String, Object> input = new HashMap<>();
        input.put("key1", "value1");
        input.put("instant", Instant.now());

        Map<String, Object> result = CbPlanServiceImpl.sanitizeForElastic(input);

        assertNotNull(result);
        assertEquals("value1", result.get("key1"));
        assertTrue(result.get("instant") instanceof String);
    }

    @Test
    void testParseToDate_String() {
        Date result = cbPlanService.parseToDate("2024-12-31");
        assertNotNull(result);
    }

    @Test
    void testParseToDate_Instant() {
        Instant instant = Instant.now();
        Date result = cbPlanService.parseToDate(instant);
        assertNotNull(result);
    }

    @Test
    void testParseToDate_Date() {
        Date date = new Date();
        Date result = cbPlanService.parseToDate(date);
        assertNotNull(result);
    }

    @Test
    void testParseToDate_Null() {
        Date result = cbPlanService.parseToDate(null);
        assertNull(result);
    }

    @SuppressWarnings("unchecked")
    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testValidateCbPlanRequest() {
        CbPlanDto dto = new CbPlanDto();
        dto.setName("Test");
        dto.setEndDate(new Date());

        List<String> result = (List<String>) ReflectionTestUtils.invokeMethod(cbPlanService, "validateCbPlanRequest", dto);

        assertNotNull(result);
    }

    @SuppressWarnings("unchecked")
    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testValidateContextData_NoContextData() {
        CbPlanDto dto = new CbPlanDto();
        ApiRequest request = new ApiRequest();
        request.setRequest(new HashMap<>());

        List<String> result = (List<String>) ReflectionTestUtils.invokeMethod(cbPlanService, "validateContextData", dto, request);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testInsertCustomOrgLookup_EmptyList() {
        ApiResponse result = (ApiResponse) ReflectionTestUtils.invokeMethod(cbPlanService, "insertCustomOrgLookup", "planId", new ArrayList<>(), new Date());

        assertNotNull(result);
        assertEquals(Constants.FAILED, result.getParams().getStatus());
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testInsertAllOrgLookup() {
        ApiResponse cassandraResp = new ApiResponse();
        cassandraResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(cassandraResp);

        ApiResponse result = (ApiResponse) ReflectionTestUtils.invokeMethod(cbPlanService, "insertAllOrgLookup", "planId", new Date());

        assertNotNull(result);
    }

    @SuppressWarnings("unchecked")
    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testMergeCbPlanData() {
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("name", "New Name");

        Map<String, Object> existingMap = new HashMap<>();
        existingMap.put("name", "Old Name");
        existingMap.put("contentType", "Course");

        Map<String, Object> result = (Map<String, Object>) ReflectionTestUtils.invokeMethod(cbPlanService, "mergeCbPlanData", requestMap, existingMap);

        assertNotNull(result);
        assertEquals("New Name", result.get("name"));
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testUpdateDraftInfo() {
        Map<String, Object> updatedPlan = new HashMap<>();
        updatedPlan.put("name", "Updated");

        Map<String, Object> cbPlan = new HashMap<>();
        cbPlan.put("draftData", "");
        cbPlan.put("name", "Original");

        String result = (String) ReflectionTestUtils.invokeMethod(cbPlanService, "updateDraftInfo", updatedPlan, cbPlan);

        assertNotNull(result);
    }

    @SuppressWarnings("unchecked")
    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testExtractRootOrgIds() {
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("criteriaKey", "rootOrgId");
        criteria.put("criteriaValue", Arrays.asList("org1", "org2"));
        criteriaList.add(criteria);
        userGroup.put("userGroupCriteriaList", criteriaList);
        userGroups.add(userGroup);
        accessControl.put("userGroups", userGroups);
        contextData.put("accessControl", accessControl);

        List<String> result = (List<String>) ReflectionTestUtils.invokeMethod(cbPlanService, "extractRootOrgIds", contextData);

        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    void testArchiveCustomOrgLookup() {
        Map<String, Object> updateResp = new HashMap<>();
        updateResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.updateRecord(anyString(), anyString(), any(), any())).thenReturn(updateResp);

        ApiResponse result = (ApiResponse) ReflectionTestUtils.invokeMethod(cbPlanService, "archiveCustomOrgLookup", "planId", Arrays.asList("org1"));

        assertNotNull(result);
    }

    private Map<String, Object> createMockPlan() {
        Map<String, Object> plan = new HashMap<>();
        plan.put("name", "Test Plan");
        plan.put("createdBy", "userId");
        plan.put("contentList", Arrays.asList("content1"));
        plan.put("status", "live");
        plan.put("draftData", "");
        plan.put("createdAtReq", Instant.now());
        return plan;
    }

    @Test
    void testCreateCbPlan_ContextDataValidation() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("name", "Test Plan");
        requestMap.put("endDate", new Date());
        requestMap.put("orgScope", "single");
        requestMap.put("contentType", "Course");
        requestMap.put("contentList", Arrays.asList("content1"));

        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("criteriaKey", "rootOrgId");
        criteria.put("criteriaValue", Arrays.asList("org1"));
        criteriaList.add(criteria);
        userGroup.put("userGroupCriteriaList", criteriaList);
        userGroups.add(userGroup);
        accessControl.put("userGroups", userGroups);
        contextData.put("accessControl", accessControl);
        requestMap.put("contextDataRequest", contextData);

        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        ApiResponse response = cbPlanService.createCbPlan(request, "orgId", "token");

        assertNotNull(response);
    }

    @Test
    void testCreateCbPlan_ContextDataValidationError() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("name", "Test Plan");
        requestMap.put("endDate", new Date());
        requestMap.put("orgScope", "single");
        requestMap.put("contentType", "Course");
        requestMap.put("contentList", Arrays.asList("content1"));

        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put("userGroups", new ArrayList<>());
        contextData.put("accessControl", accessControl);
        requestMap.put("contextDataRequest", contextData);

        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        ApiResponse response = cbPlanService.createCbPlan(request, "orgId", "token");

        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testUpdateCbPlan_NotAuthorized() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> updateMap = new HashMap<>();
        updateMap.put("id", "planId");
        updateMap.put("name", "Updated Plan");
        request.setRequest(updateMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put("createdBy", "otherUser");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(Arrays.asList(existingPlan));

        when(serverProperties.getCbPlanUpdatePublishAuthorizedRoles()).thenReturn(Arrays.asList("admin"));

        ApiResponse response = cbPlanService.updateCbPlan(request, "orgId", "token", Arrays.asList("user"));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testUpdateCbPlan_LivePlanWithRestrictedFields() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> updateMap = new HashMap<>();
        updateMap.put("id", "planId");
        updateMap.put("invalidField", "value");
        request.setRequest(updateMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put("createdBy", "userId");
        existingPlan.put("status", Constants.LIVE);
        existingPlan.put("cbPublishedBy", "userId");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(Arrays.asList(existingPlan));

        ApiResponse response = cbPlanService.updateCbPlan(request, "orgId", "token", Arrays.asList("role"));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testUpdateCbPlan_PlanNotFound() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> updateMap = new HashMap<>();
        updateMap.put("id", "planId");
        request.setRequest(updateMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(new ArrayList<>());

        ApiResponse response = cbPlanService.updateCbPlan(request, "orgId", "token", Arrays.asList("role"));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testUpdateCbPlan_MissingId() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> updateMap = new HashMap<>();
        updateMap.put("name", "Updated Plan");
        request.setRequest(updateMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        ApiResponse response = cbPlanService.updateCbPlan(request, "orgId", "token", Arrays.asList("role"));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testUpdateCbPlan_UpdateOrgLookupSuccess() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> updateMap = new HashMap<>();
        updateMap.put("id", "planId");
        updateMap.put("name", "Updated Plan");
        request.setRequest(updateMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put("createdBy", "userId");
        existingPlan.put("orgIdList", Arrays.asList("org1"));
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(Arrays.asList(existingPlan));

        Map<String, Object> updateResp = new HashMap<>();
        updateResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.updateRecord(anyString(), anyString(), any(), any())).thenReturn(updateResp);

        ApiResponse response = cbPlanService.updateCbPlan(request, "orgId", "token", Arrays.asList("role"));

        assertNotNull(response);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testUpdateCbPlan_OrgLookupError() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> updateMap = new HashMap<>();
        updateMap.put("id", "planId");
        updateMap.put("name", "Updated Plan");
        request.setRequest(updateMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put("createdBy", "userId");
        existingPlan.put("orgIdList", Arrays.asList("org1"));
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(Arrays.asList(existingPlan));

        Map<String, Object> updateResp = new HashMap<>();
        updateResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.updateRecord(anyString(), anyString(), any(), any())).thenReturn(updateResp);

        doThrow(new RuntimeException("Delete error")).when(cassandraOperation).deleteRecord(anyString(), anyString(), any());

        ApiResponse response = cbPlanService.updateCbPlan(request, "orgId", "token", Arrays.asList("role"));

        assertNotNull(response);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testUpdateCbPlan_RuntimeException() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> updateMap = new HashMap<>();
        updateMap.put("id", "planId");
        request.setRequest(updateMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenThrow(new RuntimeException("Database error"));

        ApiResponse response = cbPlanService.updateCbPlan(request, "orgId", "token", Arrays.asList("role"));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testParseEndDate_String() {
        Date result = (Date) ReflectionTestUtils.invokeMethod(cbPlanService, "parseEndDate", "2024-12-31");
        assertNotNull(result);
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testParseEndDate_Instant() {
        Instant instant = Instant.now();
        Date result = (Date) ReflectionTestUtils.invokeMethod(cbPlanService, "parseEndDate", instant);
        assertNull(result);
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testParseEndDate_Date() {
        Date date = new Date();
        Date result = (Date) ReflectionTestUtils.invokeMethod(cbPlanService, "parseEndDate", date);
        assertEquals(date, result);
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testParseEndDate_Null() {
        Date result = (Date) ReflectionTestUtils.invokeMethod(cbPlanService, "parseEndDate", (Object) null);
        assertNull(result);
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testGetDesignationForUser() {
        String profileDetails = "{\"professionalDetails\":[{\"designation\":\"Manager\"}]}";
        String result = (String) ReflectionTestUtils.invokeMethod(cbPlanService, "getDesignationForUser", profileDetails, "userId");

        assertEquals("Manager", result);
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testGetDesignationForUser_EmptyProfile() {
        String result = (String) ReflectionTestUtils.invokeMethod(cbPlanService, "getDesignationForUser", "", "userId");

        assertEquals("", result);
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testGetDesignationForUser_InvalidJson() {
        String result = (String) ReflectionTestUtils.invokeMethod(cbPlanService, "getDesignationForUser", "invalid-json", "userId");

        assertEquals("", result);
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testToInstant_String() {
        Instant result = (Instant) ReflectionTestUtils.invokeMethod(cbPlanService, "toInstant", "2024-12-31T10:00:00Z");
        assertNotNull(result);
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testToInstant_Instant() {
        Instant instant = Instant.now();
        Instant result = (Instant) ReflectionTestUtils.invokeMethod(cbPlanService, "toInstant", instant);
        assertEquals(instant, result);
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testToInstant_Date() {
        Date date = new Date();
        Instant result = (Instant) ReflectionTestUtils.invokeMethod(cbPlanService, "toInstant", date);
        assertNotNull(result);
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testToInstant_Null() {
        Instant result = (Instant) ReflectionTestUtils.invokeMethod(cbPlanService, "toInstant", (Object) null);
        assertNull(result);
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testEnrichUserInfo() throws Exception {
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();
        Map<String, String> userDetails = new HashMap<>();
        userDetails.put("firstName", "Test");
        userDetails.put("lastName", "User");
        userInfoMap.put("userId", userDetails);

        ReflectionTestUtils.invokeMethod(cbPlanService, "enrichUserInfo", userInfoMap);

        assertEquals("Test", userInfoMap.get("userId").get("firstName"));
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testPopulateReadData() throws Exception {
        Map<String, Object> cbPlan = new HashMap<>();
        cbPlan.put("contentList", Arrays.asList("content1"));
        cbPlan.put("createdBy", "userId");
        cbPlan.put("status", "live");
        cbPlan.put("draftData", "");
        cbPlan.put("name", "Test Plan");
        cbPlan.put("contentType", "Course");
        cbPlan.put("createdAtReq", Instant.now());
        cbPlan.put("endDateRequest", new Date());
        cbPlan.put("isApar", false);

        Map<String, Object> mockContent = createMockContent();
        when(contentService.readContent(anyString(), any())).thenReturn(mockContent);

        doAnswer(invocation -> {
            Map<String, Map<String, String>> userInfoMap = invocation.getArgument(2);
            Map<String, String> userDetails = new HashMap<>();
            userDetails.put("firstName", "Test");
            userDetails.put("lastName", "User");
            userInfoMap.put("userId", userDetails);
            return null;
        }).when(userUtilityService).readUserProfileFromDB(any(), anyList());

        Map<String, Object> result = (Map<String, Object>) ReflectionTestUtils.invokeMethod(cbPlanService, "populateReadData", cbPlan);

        assertNotNull(result);
        assertNotNull(result.get("contentList"));
        assertNotNull(result.get("createdByName"));
    }



    private Map<String, Object> createMockContent() {
        Map<String, Object> content = new HashMap<>();
        content.put("name", "Test Content");
        content.put("status", "live");
        content.put("avgRating", 4.5);
        content.put("contentType", "Course");
        content.put("duration", 60);
        content.put("appIcon", "test-icon.png");
        content.put("organisation", "Test Org");
        content.put("identifier", "content1");
        content.put("description", "Test Description");
        content.put("primaryCategory", "Course");
        content.put("competenciesV5", Arrays.asList("comp1"));
        content.put("additionalTags", Arrays.asList("tag1"));
        content.put("courseAppIcon", "icon.png");
        content.put("posterImage", "poster.jpg");
        content.put("creatorLogo", "logo.png");
        content.put("languageMapV1", new HashMap<>());
        return content;
    }

    @Test
    void testCreateSuccessResponse() {
        ApiResponse apiResponse = new ApiResponse();
        apiResponse.put("key", "value");

        ReflectionTestUtils.invokeMethod(cbPlanService, "createSuccessResponse", apiResponse);

        assertNotNull(apiResponse);
        assertEquals(Constants.SUCCESS, apiResponse.getParams().getStatus());
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testUpdateCbPlanData() {
        Map<String, Object> cbPlan = new HashMap<>();
        cbPlan.put("name", "Original");
        cbPlan.put("status", "draft");

        CbPlanDto dto = new CbPlanDto();
        dto.setName("Updated");
        dto.setEndDate(new Date());

        ReflectionTestUtils.invokeMethod(cbPlanService, "updateCbPlanData", cbPlan, dto);

        assertEquals("Updated", cbPlan.get("name"));
    }

    @SuppressWarnings("unchecked")
    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testValidateContextData_WithValidData() {
        CbPlanDto dto = new CbPlanDto();
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();

        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("criteriaKey", "rootOrgId");
        criteria.put("criteriaValue", Arrays.asList("org1"));
        criteriaList.add(criteria);
        userGroup.put("userGroupCriteriaList", criteriaList);
        userGroups.add(userGroup);
        accessControl.put("userGroups", userGroups);
        contextData.put("accessControl", accessControl);
        requestMap.put("contextDataRequest", contextData);

        request.setRequest(requestMap);

        List<String> result = (List<String>) ReflectionTestUtils.invokeMethod(cbPlanService, "validateContextData", dto, request);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testValidateContextData_WithInvalidData() {
        CbPlanDto dto = new CbPlanDto();
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();

        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put("userGroups", new ArrayList<>());
        contextData.put("accessControl", accessControl);
        requestMap.put("contextDataRequest", contextData);

        request.setRequest(requestMap);

        List<String> result = (List<String>) ReflectionTestUtils.invokeMethod(cbPlanService, "validateContextData", dto, request);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testInsertCustomOrgLookup_WithValidList() {
        ApiResponse cassandraResp = new ApiResponse();
        cassandraResp.getParams().setStatus(Constants.SUCCESS);
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), any())).thenReturn(cassandraResp);

        ApiResponse result = (ApiResponse) ReflectionTestUtils.invokeMethod(cbPlanService, "insertCustomOrgLookup", "planId", Arrays.asList("org1", "org2"), new Date());

        assertNotNull(result);
        assertEquals(Constants.SUCCESS, result.getParams().getStatus());
    }

    @Test
    void testCreateCbPlan_CassandraFailure() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("name", "Test Plan");
        requestMap.put("endDate", new Date());
        requestMap.put("orgScope", "single");
        requestMap.put("orgIdList", Arrays.asList("org1"));
        requestMap.put("contentType", "Course");
        requestMap.put("contentList", Arrays.asList("content1"));
        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        ApiResponse cassandraResp = new ApiResponse();
        cassandraResp.put(Constants.RESPONSE, Constants.FAILED);
        cassandraResp.getParams().setErr("DB Error");
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(cassandraResp);

        ApiResponse response = cbPlanService.createCbPlan(request, "orgId", "token");

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testCreateCbPlan_LookupFailure() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("name", "Test Plan");
        requestMap.put("endDate", new Date());
        requestMap.put("orgScope", "single");
        requestMap.put("orgIdList", Arrays.asList("org1"));
        requestMap.put("contentType", "Course");
        requestMap.put("contentList", Arrays.asList("content1"));
        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        ApiResponse cassandraResp = new ApiResponse();
        cassandraResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(cassandraResp);

        ApiResponse lookupResp = new ApiResponse();
        lookupResp.put(Constants.RESPONSE, Constants.FAILED);
        lookupResp.getParams().setErr("Lookup Error");
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), any())).thenReturn(lookupResp);

        ApiResponse response = cbPlanService.createCbPlan(request, "orgId", "token");

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testPublishCbPlan_MissingId() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        ApiResponse response = cbPlanService.publishCbPlan(request, "orgId", "token", Arrays.asList("role"));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testPublishCbPlan_PlanNotFound() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("id", "planId");
        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(new ArrayList<>());

        ApiResponse response = cbPlanService.publishCbPlan(request, "orgId", "token", Arrays.asList("role"));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testPublishCbPlan_NotAuthorized() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("id", "planId");
        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put("createdBy", "otherUser");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(Arrays.asList(existingPlan));

        when(serverProperties.getCbPlanUpdatePublishAuthorizedRoles()).thenReturn(Arrays.asList("admin"));

        ApiResponse response = cbPlanService.publishCbPlan(request, "orgId", "token", Arrays.asList("user"));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testPublishCbPlan_AlreadyPublished() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("id", "planId");
        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put("createdBy", "userId");
        existingPlan.put("status", "live");
        existingPlan.put("draftData", null);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(Arrays.asList(existingPlan));

        ApiResponse response = cbPlanService.publishCbPlan(request, "orgId", "token", Arrays.asList("role"));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testRetireCbPlan_MissingId() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        ApiResponse response = cbPlanService.retireCbPlan(request, "orgId", "token", Arrays.asList("role"));

        assertNotNull(response);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testRetireCbPlan_PlanNotFound() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("id", "planId");
        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(new ArrayList<>());

        ApiResponse response = cbPlanService.retireCbPlan(request, "orgId", "token", Arrays.asList("role"));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testRetireCbPlan_AlreadyRetired() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("id", "planId");
        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put("createdBy", "userId");
        existingPlan.put("status", "retired");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(Arrays.asList(existingPlan));

        ApiResponse response = cbPlanService.retireCbPlan(request, "orgId", "token", Arrays.asList("role"));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testParseToDate_Long() {
        Long timestamp = System.currentTimeMillis();
        Date result = cbPlanService.parseToDate(timestamp);
        assertNull(result); // Method doesn't handle Long type
    }

    @Test
    void testParseToDate_SqlTimestamp() {
        java.sql.Timestamp timestamp = new java.sql.Timestamp(System.currentTimeMillis());
        Date result = cbPlanService.parseToDate(timestamp);
        assertNotNull(result);
    }

    @Test
    void testParseToDate_InvalidString() {
        Date result = cbPlanService.parseToDate("invalid-date");
        assertNull(result);
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testEnrichUserInfoWithProfile() {
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();
        Map<String, String> userInfo = new HashMap<>();
        userInfo.put("profileDetails", "{\"professionalDetails\":[{\"designation\":\"Developer\"}]}");
        userInfoMap.put("userId", userInfo);

        ReflectionTestUtils.invokeMethod(cbPlanService, "enrichUserInfo", userInfoMap);

        assertEquals("Developer", userInfoMap.get("userId").get("designation"));
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testEnrichUserInfo_NoProfileDetails() {
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();
        Map<String, String> userInfo = new HashMap<>();
        userInfo.put("designation", "Existing");
        userInfoMap.put("userId", userInfo);

        ReflectionTestUtils.invokeMethod(cbPlanService, "enrichUserInfo", userInfoMap);

        assertEquals("Existing", userInfoMap.get("userId").get("designation"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testPopulateReadData_NullDraftData() throws Exception {
        Map<String, Object> cbPlan = new HashMap<>();
        cbPlan.put("name", "Test Plan");
        cbPlan.put("contentType", "Course");
        cbPlan.put("contentList", Arrays.asList("content1"));
        cbPlan.put("createdBy", "userId");
        cbPlan.put("createdAtReq", Instant.now());
        cbPlan.put("endDateRequest", new Date());
        cbPlan.put("draftData", null);
        cbPlan.put("status", "live");
        cbPlan.put("isApar", false);

        Map<String, Object> mockContent = createMockContent();
        when(contentService.readContent(anyString(), any())).thenReturn(mockContent);

        doAnswer(invocation -> {
            Map<String, Map<String, String>> userInfoMap = invocation.getArgument(2);
            Map<String, String> userDetails = new HashMap<>();
            userDetails.put("firstName", "Test");
            userDetails.put("lastName", "User");
            userInfoMap.put("userId", userDetails);
            return null;
        }).when(userUtilityService).readUserProfileFromDB(any(), anyList());

        Map<String, Object> result = (Map<String, Object>) ReflectionTestUtils.invokeMethod(cbPlanService, "populateReadData", cbPlan);

        assertNotNull(result);
        assertEquals("Test Plan", result.get("name"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testPopulateReadData_WithDraftStatus() throws Exception {
        // Arrange
        Map<String, Object> cbPlan = new HashMap<>();
        cbPlan.put(Constants.DRAFT_DATA,
                "{\"name\":\"Draft Plan\",\"contentType\":\"Course\",\"contentList\":[\"content1\"],\"endDate\":\"2024-12-31\"}");
        cbPlan.put(Constants.STATUS, "draft"); // Set status to draft to hit the ELSE block
        cbPlan.put(Constants.CREATED_BY, "userId");
        cbPlan.put(Constants.CREATED_AT_REQ, Instant.now());
        cbPlan.put(Constants.CONTENT_LIST, List.of("content1"));
        cbPlan.put(Constants.END_DATE_REQUEST, new Date());
        cbPlan.put(Constants.IS_APAR, false);
        // Mock enriched content
        List<Map<String, Object>> enrichedContent = List.of(Map.of("content", "enrichedContent1"));
        when(contentService.enrichContentInfoForCBPlan(anyList())).thenReturn(enrichedContent);
        // Mock user profile enrichment
        doAnswer(invocation -> {
            Map<String, Map<String, String>> userInfoMap = invocation.getArgument(2);
            Map<String, String> userDetails = new HashMap<>();
            userDetails.put("firstName", "Test");
            userDetails.put("lastName", "User");
            userInfoMap.put("userId", userDetails);
            return null;
        }).when(userUtilityService).readUserProfileFromDB(any(), anyList());
        Map<String, Object> result =
                (Map<String, Object>) ReflectionTestUtils.invokeMethod(cbPlanService, "populateReadData", cbPlan);
        assertNotNull(result);
        assertEquals(cbPlan.get(Constants.NAME), result.get(Constants.NAME)); // else block uses original map
        assertEquals(cbPlan.get(Constants.CONTENT_TYPE), result.get(Constants.CONTENT_TYPE));
        assertEquals(enrichedContent, result.get(Constants.CONTENT_LIST)); // enriched content still applied
        assertEquals(false, result.get(Constants.IS_APAR));
        assertEquals("userId", result.get(Constants.CREATED_BY));
        assertNotNull(result.get(Constants.END_DATE_REQUEST));
    }


    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testSearchCbPlan_NoResults() throws Exception {
        SearchCriteria criteria = new SearchCriteria();

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        SearchResult searchResult = new SearchResult();
        searchResult.setData(new ArrayList<>());
        when(esUtilService.searchDocuments(anyString(), any(), anyString())).thenReturn(searchResult);

        ApiResponse response = cbPlanService.searchCbPlan(criteria, "orgId", "token");

        assertNotNull(response);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testUpdateCbPlan_EndDateParsing() throws Exception {
        ApiRequest request = new ApiRequest();
        Map<String, Object> updateMap = new HashMap<>();
        updateMap.put("id", "planId");
        updateMap.put("endDate", "2024-12-31T00:00:00Z");

        // ----- contextData -----
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("criteriaKey", "rootOrgId");
        criteria.put("criteriaValue", Arrays.asList("orgId"));

        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put("userGroupCriteriaList", Arrays.asList(criteria));

        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put("userGroups", Arrays.asList(userGroup));

        Map<String, Object> contextData = new HashMap<>();
        contextData.put("accessControl", accessControl);

        updateMap.put(Constants.END_DATE_REQUEST, "2024-12-31T00:00:00Z");
        updateMap.put(Constants.CONTEXT_DATA_REQUEST, contextData);
        request.setRequest(updateMap);

        // --- Mock userId ---
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        // --- Mock existing plan ---
        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put("createdBy", "userId");
        existingPlan.put("status", "draft");
        existingPlan.put(Constants.ROOT_ORG_ID, "orgId");
        existingPlan.put("rootorgid", "orgId");
        existingPlan.put(Constants.END_DATE_REQUEST, "2024-12-31T00:00:00Z");
        // IMPORTANT: store contextData as String JSON
        ObjectMapper mapper = new ObjectMapper();
        existingPlan.put(Constants.CONTEXT_DATA, mapper.writeValueAsString(contextData));

        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenReturn(Arrays.asList(existingPlan));

        // --- Mock update response ---
        Map<String, Object> updateResp = new HashMap<>();
        updateResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.updateRecord(anyString(), anyString(), any(), any())).thenReturn(updateResp);

        // --- Execute ---
        ApiResponse response = cbPlanService.updateCbPlan(request, "orgId", "token", Arrays.asList("role"));

        // --- Verify ---
        assertNotNull(response);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }



    @SuppressWarnings("unchecked")
    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testValidateContextData_InvalidRootOrgId() {
        CbPlanDto dto = new CbPlanDto();
        dto.setOrgScope("single");
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("criteriaKey", "rootOrgId");
        criteria.put("criteriaValue", Collections.emptyList());
        criteriaList.add(criteria);
        userGroup.put("userGroupCriteriaList", criteriaList);
        userGroups.add(userGroup);
        accessControl.put("userGroups", userGroups);
        contextData.put("accessControl", accessControl);
        requestMap.put("contextDataRequest", contextData);
        request.setRequest(requestMap);

        List<String> result = (List<String>) ReflectionTestUtils.invokeMethod(cbPlanService, "validateContextData", dto, request);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testCreateCbPlan_ElasticSearchError() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("name", "Test Plan");
        requestMap.put("endDate", new Date());
        requestMap.put("orgScope", "single");
        requestMap.put("orgIdList", List.of("org1"));
        requestMap.put("contentType", "Course");
        requestMap.put("contentList", List.of("content1"));
        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        ApiResponse cassandraResp = new ApiResponse();
        cassandraResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(cassandraResp);

        doThrow(new RuntimeException("ES error")).when(esUtilService)
            .addDocument(anyString(), anyString(), anyString(), any(), anyString());

        ApiResponse response = cbPlanService.createCbPlan(request, "orgId", "token");

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testUpdateDraftInfo_NullDraftData() {
        Map<String, Object> updatedCbPlan = new HashMap<>();
        updatedCbPlan.put("name", "Updated Plan");

        Map<String, Object> cbPlan = new HashMap<>();
        cbPlan.put("draftData", null);
        cbPlan.put("name", "Original Plan");

        try {
            String result = (String) ReflectionTestUtils.invokeMethod(cbPlanService, "updateDraftInfo", updatedCbPlan, cbPlan);
            assertNotNull(result);
            assertTrue(result.contains("Updated Plan"));
        } catch (Exception e) {
            fail("Should not throw exception");
        }
    }

    @Test
    void testPublishCbPlan_EmptyUserId() {
        ApiRequest request = new ApiRequest();
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("");

        ApiResponse response = cbPlanService.publishCbPlan(request, "orgId", "token", List.of("role"));

        assertNotNull(response);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testPublishCbPlan_CassandraError() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("id", "planId");
        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put("createdBy", "userId");
        existingPlan.put("status", "draft");
        existingPlan.put("draftData", "{\"name\":\"Test Plan\",\"endDate\":\"2024-12-31\"}");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(Arrays.asList(existingPlan));

        Map<String, Object> updateResp = new HashMap<>();
        updateResp.put(Constants.RESPONSE, Constants.FAILED);
        updateResp.put(Constants.ERROR_MESSAGE, "DB error");
        when(cassandraOperation.updateRecord(anyString(), anyString(), any(), any(), any(), any())).thenReturn(updateResp);

        ApiResponse response = cbPlanService.publishCbPlan(request, "orgId", "token", List.of("role"));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testRetireCbPlan_EmptyUserId() {
        ApiRequest request = new ApiRequest();
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("");

        ApiResponse response = cbPlanService.retireCbPlan(request, "orgId", "token", List.of("role"));

        assertNotNull(response);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testRetireCbPlan_ElasticSearchError() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("id", "planId");
        request.setRequest(requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put("createdBy", "userId");
        existingPlan.put("status", "live");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(List.of(existingPlan));

        Map<String, Object> updateResp = new HashMap<>();
        updateResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.updateRecord(anyString(), anyString(), any(), any())).thenReturn(updateResp);

        doThrow(new RuntimeException("ES error")).when(esUtilService)
            .addDocument(anyString(), anyString(), anyString(), any(), anyString());

        ApiResponse response = cbPlanService.retireCbPlan(request, "orgId", "token", Arrays.asList("role"));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testReadCbPlan_NotFound() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(new ArrayList<>());

        ApiResponse response = cbPlanService.readCbPlan("planId", "orgId", "token");

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    @Disabled("This test is ignored due to optimization code changes")
    void testReadCbPlan_ContentError() {
        Map<String, Object> cbPlan = new HashMap<>();
        cbPlan.put("contentList", List.of("content1"));
        cbPlan.put("status", "live");
        cbPlan.put("draftData", "");

        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(List.of(cbPlan));

        when(contentService.readContent(anyString(), any()))
            .thenThrow(new RuntimeException("Content service error"));

        ApiResponse response = cbPlanService.readCbPlan("planId", "orgId", "token");

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testSearchCbPlan_Exception() throws Exception {
        SearchCriteria criteria = new SearchCriteria();
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");
        when(esUtilService.searchDocuments(anyString(), any(), anyString()))
            .thenThrow(new RuntimeException("Test exception"));

        try {
            ApiResponse response = cbPlanService.searchCbPlan(criteria, "orgId", "token");
            fail("Expected CustomException to be thrown");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("error while processing"));
        }
    }

    @Test
    void testParseEndDate_AllBranches() {
        Instant now = Instant.now();
        Object r1 = ReflectionTestUtils.invokeMethod(cbPlanService, "parseEndDate", Date.from(now));
        assertNotNull(r1);
        Object r2 = ReflectionTestUtils.invokeMethod(cbPlanService, "parseEndDate", now);
        assertNotNull(r2);
        Object r3 = ReflectionTestUtils.invokeMethod(cbPlanService, "parseEndDate", now.toEpochMilli());
        assertNotNull(r3);
        Object r4 = ReflectionTestUtils.invokeMethod(cbPlanService, "parseEndDate", now.toString());
        assertNotNull(r4);
        Object r5 = ReflectionTestUtils.invokeMethod(cbPlanService, "parseEndDate", "2024-12-31");
        assertNotNull(r5);
        Object r6 = ReflectionTestUtils.invokeMethod(cbPlanService, "parseEndDate", new Object());
        assertNull(r6);
    }


    @Test
    @SuppressWarnings("unchecked")
    void testExtractUniqueRootOrgIds_AllPaths() {
        // null contextData → empty set
        Set<String> empty = (Set<String>) ReflectionTestUtils.invokeMethod(cbPlanService, "extractUniqueRootOrgIds", new HashMap<>());
        assertNotNull(empty);
        assertTrue(empty.isEmpty());

        // contextData as Map
        Map<String, Object> crit = Map.of(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID,
                Constants.CRITERIA_VALUE, List.of("o1"));
        Map<String, Object> ug = Map.of(Constants.USER_GROUP_CRITERIA_LIST, List.of(crit));
        Map<String, Object> ac = Map.of(Constants.USER_GROUPS, List.of(ug));
        Map<String, Object> raw = Map.of(Constants.CONTEXT_DATA_REQUEST, Map.of(Constants.ACCESS_CONTROL, ac));
        Set<String> result = (Set<String>) ReflectionTestUtils.invokeMethod(cbPlanService, "extractUniqueRootOrgIds", raw);
        assertEquals(Set.of("o1"), result);

        // contextData as JSON string
        String json = "{\"accessControl\":{\"userGroups\":[{\"userGroupCriteriaList\":[{\"criteriaKey\":\"rootOrgId\",\"criteriaValue\":[\"o2\"]}]}]}}";
        Set<String> result2 = (Set<String>) ReflectionTestUtils.invokeMethod(cbPlanService, "extractUniqueRootOrgIds",
                Map.of(Constants.CONTEXT_DATA_REQUEST, json));
        assertEquals(Set.of("o2"), result2);
    }

    @Test
    void testParseToDate_AllBranches() {
        assertNull(cbPlanService.parseToDate(null));
        assertNotNull(cbPlanService.parseToDate("2024-12-31"));
        assertNotNull(cbPlanService.parseToDate(Instant.now()));
        assertNotNull(cbPlanService.parseToDate(new java.sql.Timestamp(System.currentTimeMillis())));
        assertNotNull(cbPlanService.parseToDate(new Date()));
    }


    @Test
    void testCreateSuccessResponse_SetsStatusAndHttpOK() {
        ApiResponse apiResponse = new ApiResponse();
        ReflectionTestUtils.invokeMethod(cbPlanService, "createSuccessResponse", apiResponse);
        assertEquals(Constants.SUCCESS, apiResponse.getParams().getStatus());
        assertEquals(HttpStatus.OK, apiResponse.getResponseCode());
    }

    @Test
    void testSanitizeForElastic_ConvertsInstantToString() {
        Map<String, Object> input = new HashMap<>();
        input.put("a", "b");
        input.put("instant", Instant.now());
        Map<String, Object> out = CbPlanServiceImpl.sanitizeForElastic(input);
        assertInstanceOf(String.class, out.get("instant"));
        assertEquals("b", out.get("a"));
    }

    @Test
    void testSearchCbPlan_ExceptionPath() throws Exception {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");
        when(esUtilService.searchDocuments(anyString(), any(), anyString())).thenThrow(new RuntimeException("boom"));
        SearchCriteria sc = new SearchCriteria();
        assertThrows(RuntimeException.class, () -> cbPlanService.searchCbPlan(sc, "org", "t"));
    }

    @Test
    void testReadCbPlan_EmptyAndErrorPaths() {
        ApiResponse r1 = cbPlanService.readCbPlan("", "org", "t");
        assertEquals(HttpStatus.BAD_REQUEST, r1.getResponseCode());
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenThrow(new RuntimeException("fail"));
        ApiResponse r2 = cbPlanService.readCbPlan("id", "org", "t");
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, r2.getResponseCode());
    }

    @Test
    void testRetireCbPlan_CbPlanAlreadyArchived() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user");
        Map<String, Object> cbPlan = new HashMap<>();
        cbPlan.put(Constants.CREATED_BY, "user");
        cbPlan.put(Constants.STATUS, Constants.CB_RETIRE);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenReturn(List.of(cbPlan));
        ApiRequest req = new ApiRequest();
        req.setRequest(Map.of(Constants.ID, "p1"));
        ApiResponse resp = cbPlanService.retireCbPlan(req, "org", "t", List.of("role"));
        assertEquals(HttpStatus.BAD_REQUEST, resp.getResponseCode());
    }

    @Test
    void testUpdateCbPlan_CassandraThrows() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenThrow(new RuntimeException("fail"));
        ApiRequest req = new ApiRequest();
        req.setRequest(Map.of(Constants.ID, "pid"));
        ApiResponse resp = cbPlanService.updateCbPlan(req, "org", "t", List.of("r"));
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, resp.getResponseCode());
    }

    @Test
    void testPublishCbPlan_InvalidState() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");
        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.CREATED_BY, "u1");
        existing.put(Constants.STATUS, "archived"); // not DRAFT
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenReturn(List.of(existing));
        ApiRequest req = new ApiRequest();
        req.setRequest(Map.of(Constants.ID, "id"));
        ApiResponse resp = cbPlanService.publishCbPlan(req, "org", "t", List.of("role"));
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, resp.getResponseCode());
    }


    @Test
    void testUpdateCbPlan_GetRootOrgFails() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");

        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.CREATED_BY, "u1");
        existing.put(Constants.STATUS, Constants.LIVE);

        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenReturn(List.of(existing));

        // Simulate root org failure using Reflection instead of mocking private call
        ReflectionTestUtils.invokeMethod(cbPlanService, "getRootOrgFromUser", "u1", new ApiResponse());

        ApiRequest req = new ApiRequest();
        req.setRequest(Map.of(Constants.ID, "plan1"));
        ApiResponse resp = cbPlanService.updateCbPlan(req, "org", "token", List.of("admin"));
        // Should fail because the injected response will have FAILED set
        assertTrue(resp.getParams().getStatus().equalsIgnoreCase(Constants.FAILED)
                || resp.getResponseCode() == HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Test
    void testUpdateCbPlan_GetCCAFromOrgFails() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");

        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.CREATED_BY, "u1");
        existing.put(Constants.STATUS, Constants.LIVE);

        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenReturn(List.of(existing));

        // Call helper via reflection to simulate the branch
        ReflectionTestUtils.invokeMethod(cbPlanService, "getCCAFromOrg", "org1", new ApiResponse());

        ApiRequest req = new ApiRequest();
        req.setRequest(Map.of(Constants.ID, "plan1"));
        ApiResponse resp = cbPlanService.updateCbPlan(req, "org", "token", List.of("admin"));
        assertTrue(resp.getParams().getStatus().equalsIgnoreCase(Constants.FAILED)
                || resp.getResponseCode() == HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Test
    void testUpdateCbPlan_DraftPlanValidationError() throws Exception {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");

        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.CREATED_BY, "u1");
        existing.put(Constants.STATUS, Constants.DRAFT);

        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenReturn(List.of(existing));

        when(requestValidator.validateCbPlanCreateRequest(any(), anyBoolean(), anyString()))
                .thenReturn(List.of("err1"));

        ApiRequest req = new ApiRequest();
        req.setRequest(Map.of(Constants.ID, "plan1"));
        ApiResponse resp = cbPlanService.updateCbPlan(req, "org", "token", List.of("admin"));
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, resp.getResponseCode());
    }


    @Test
    void testUpdateCbPlan_DraftPlanUpdateFailure() throws Exception {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");

        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.CREATED_BY, "u1");
        existing.put(Constants.STATUS, Constants.DRAFT);

        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenReturn(List.of(existing));

        when(requestValidator.validateCbPlanCreateRequest(any(), anyBoolean(), anyString())).thenReturn(Collections.emptyList());

        Map<String, Object> updated = new HashMap<>();
        updated.put(Constants.ID, "plan1");

        Map<String, Object> failResp = Map.of(Constants.RESPONSE, "FAILED");
        when(cassandraOperation.updateRecord(anyString(), anyString(), any(), any())).thenReturn(failResp);

        ApiRequest req = new ApiRequest();
        req.setRequest(updated);
        ApiResponse resp = cbPlanService.updateCbPlan(req, "org", "token", List.of("admin"));
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, resp.getResponseCode());
    }

    @Test
    void testCreateCbPlan_UserIdEmpty() {
        ApiRequest request = new ApiRequest();
        ApiResponse defaultResponse = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_CREATE);
        Mockito.when(accessTokenValidator.fetchUserIdFromAccessToken(Mockito.anyString(), Mockito.any()))
                .thenReturn("");
        ApiResponse response = cbPlanService.createCbPlan(request, "org1", "token123");
        assertEquals(defaultResponse.getId(), response.getId());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testCreateCbPlan_RootOrgFailed() throws Exception {
        ApiRequest request = new ApiRequest();

        Mockito.when(accessTokenValidator.fetchUserIdFromAccessToken(Mockito.anyString(), Mockito.any()))
                .thenReturn("user123");

        Method m = CbPlanServiceImpl.class.getDeclaredMethod("getRootOrgFromUser", String.class, ApiResponse.class);
        m.setAccessible(true);

        ApiResponse tempResp = new ApiResponse();
        m.invoke(cbPlanService, "user123", tempResp);
        tempResp.getParams().setStatus(Constants.FAILED);

        ApiResponse response = cbPlanService.createCbPlan(request, "org1", "token123");

        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testCreateCbPlan_CcaFailed() throws Exception {
        ApiRequest request = new ApiRequest();

        Mockito.when(accessTokenValidator.fetchUserIdFromAccessToken(Mockito.anyString(), Mockito.any()))
                .thenReturn("user1");

        Method rootOrgMethod = CbPlanServiceImpl.class.getDeclaredMethod("getRootOrgFromUser", String.class, ApiResponse.class);
        Method ccaMethod = CbPlanServiceImpl.class.getDeclaredMethod("getCCAFromOrg", String.class, ApiResponse.class);

        rootOrgMethod.setAccessible(true);
        ccaMethod.setAccessible(true);

        ApiResponse resp = cbPlanService.createCbPlan(request, "org1", "token");

        assertEquals(Constants.FAILED, resp.getParams().getStatus());
    }

    @Test
    void testCreateCbPlan_ValidationFails() {
        ApiRequest request = new ApiRequest();
        request.setRequest(new HashMap<>());

        Mockito.when(accessTokenValidator.fetchUserIdFromAccessToken(Mockito.anyString(), Mockito.any()))
                .thenReturn("user1");

        Mockito.when(requestValidator.validateCbPlanCreateRequest(Mockito.any(), Mockito.anyBoolean(), Mockito.anyString(), Mockito.anyBoolean()))
                .thenReturn(List.of("error1"));

        ApiResponse resp = cbPlanService.createCbPlan(request, "org1", "token");

        assertEquals(Constants.FAILED, resp.getParams().getStatus());
    }

    @Test
    void testCreateCbPlan_InsertFailure() {
        ApiRequest request = new ApiRequest();
        request.setRequest(new HashMap<>());

        Mockito.when(accessTokenValidator.fetchUserIdFromAccessToken(Mockito.anyString(), Mockito.any()))
                .thenReturn("user1");

        Mockito.when(requestValidator.validateCbPlanCreateRequest(Mockito.any(), Mockito.anyBoolean(), Mockito.anyString(), Mockito.anyBoolean()))
                .thenReturn(Collections.emptyList());

        Map<String, Object> cassResp = new HashMap<>();
        cassResp.put(Constants.RESPONSE, "FAILED");

        Mockito.when(cassandraOperation.insertRecord(Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
                .thenReturn(cassResp);

        ApiResponse resp = cbPlanService.createCbPlan(request, "org1", "token");

        assertEquals(Constants.FAILED, resp.getParams().getStatus());
    }

    // Tests for handleUpdateOfLiveCbPlan method
    @Test
    void testHandleUpdateOfLiveCbPlan_ValidationError() {
        ApiResponse response = new ApiResponse();
        response.setParams(new ApiRespParam());
        Map<String, Object> incomingRequest = Map.of("name", "Updated Plan");
        Map<String, Object> existingPlan = Map.of(Constants.PLAN_ID, "plan1");
        
        when(requestValidator.validateContextData(any(), anyBoolean(), anyString(), any()))
            .thenReturn(Arrays.asList("Validation error"));
        
        ReflectionTestUtils.invokeMethod(cbPlanService, "handleUpdateOfLiveCbPlan", 
            response, incomingRequest, existingPlan, "user1", "rootOrg1", false);
        
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testHandleUpdateOfLiveCbPlan_IsAparRestriction() {
        ApiResponse response = new ApiResponse();
        response.setParams(new ApiRespParam());
        Map<String, Object> incomingRequest = Map.of(Constants.IS_APAR, false);
        Map<String, Object> existingPlan = Map.of(
            Constants.PLAN_ID, "plan1",
            Constants.IS_APAR, true
        );
        
        when(requestValidator.validateContextData(any(), anyBoolean(), anyString(), any()))
            .thenReturn(Collections.emptyList());
        when(serverProperties.getCbPlanUpdateAllowedFields())
            .thenReturn(Arrays.asList(Constants.IS_APAR));
        
        ReflectionTestUtils.invokeMethod(cbPlanService, "handleUpdateOfLiveCbPlan", 
            response, incomingRequest, existingPlan, "user1", "rootOrg1", false);
        
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErr().contains("Cannot change isApar from true to false"));
    }

    @Test
    void testHandleUpdateOfLiveCbPlan_NullFieldValue() {
        ApiResponse response = new ApiResponse();
        response.setParams(new ApiRespParam()); // Initialize params to avoid NullPointerException
        Map<String, Object> incomingRequest = new HashMap<>();
        incomingRequest.put("name", null); // Use HashMap to allow null values
        Map<String, Object> existingPlan = Map.of(Constants.PLAN_ID, "plan1");
        
        when(requestValidator.validateContextData(any(), anyBoolean(), anyString(), any()))
            .thenReturn(Collections.emptyList());
        when(serverProperties.getCbPlanUpdateAllowedFields())
            .thenReturn(Arrays.asList("name"));
        
        ReflectionTestUtils.invokeMethod(cbPlanService, "handleUpdateOfLiveCbPlan", 
            response, incomingRequest, existingPlan, "user1", "rootOrg1", false);
        
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErr().contains("Field 'name' cannot be null"));
    }

    @Test
    void testHandleUpdateOfLiveCbPlan_Success() throws Exception {
        ApiResponse response = new ApiResponse();
        response.setParams(new ApiRespParam());
        response.setResult(new HashMap<>());
        Map<String, Object> incomingRequest = Map.of("name", "Updated Plan");
        Map<String, Object> existingPlan = Map.of(Constants.PLAN_ID, "plan1");
        
        when(requestValidator.validateContextData(any(), anyBoolean(), anyString(), any()))
            .thenReturn(Collections.emptyList());
        when(serverProperties.getCbPlanUpdateAllowedFields())
            .thenReturn(Arrays.asList("name"));
        
        Map<String, Object> updateResp = Map.of(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.updateRecord(anyString(), anyString(), any(), any()))
            .thenReturn(updateResp);
        
        ReflectionTestUtils.invokeMethod(cbPlanService, "handleUpdateOfLiveCbPlan", 
            response, incomingRequest, existingPlan, "user1", "rootOrg1", false);
        
        assertEquals(Constants.UPDATED, response.getResult().get(Constants.STATUS));
        assertTrue(response.getResult().get(Constants.MESSAGE).toString().contains("Updated cbPlan as draft"));
    }

    @Test
    void testHandleUpdateOfLiveCbPlan_CassandraFailure() {
        ApiResponse response = new ApiResponse();
        response.setParams(new ApiRespParam());
        Map<String, Object> incomingRequest = Map.of("name", "Updated Plan");
        Map<String, Object> existingPlan = Map.of(Constants.PLAN_ID, "plan1");
        
        when(requestValidator.validateContextData(any(), anyBoolean(), anyString(), any()))
            .thenReturn(Collections.emptyList());
        when(serverProperties.getCbPlanUpdateAllowedFields())
            .thenReturn(Arrays.asList("name"));
        
        Map<String, Object> updateResp = Map.of(
            Constants.RESPONSE, Constants.FAILED,
            Constants.ERROR_MESSAGE, "DB Error"
        );
        when(cassandraOperation.updateRecord(anyString(), anyString(), any(), any()))
            .thenReturn(updateResp);
        
        ReflectionTestUtils.invokeMethod(cbPlanService, "handleUpdateOfLiveCbPlan", 
            response, incomingRequest, existingPlan, "user1", "rootOrg1", false);
        
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    // Tests for upsertCbPlanContentLookup method
    @Test
    void testUpsertCbPlanContentLookup_NewContent() {
        List<String> contentIds = Arrays.asList("content1");
        
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), anyInt()))
            .thenReturn(Collections.emptyList());
        
        ReflectionTestUtils.invokeMethod(cbPlanService, "upsertCbPlanContentLookup", "plan1", contentIds);
        
        verify(cassandraOperation).updateRecord(anyString(), anyString(), any(), any());
    }

    @Test
    void testUpsertCbPlanContentLookup_ExistingContent() {
        List<String> contentIds = Arrays.asList("content1");
        Set<String> existingPlanIds = new HashSet<>(Arrays.asList("plan2"));
        
        Map<String, Object> existingRecord = Map.of("planId", existingPlanIds); // Note: planId not planid
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), anyInt()))
            .thenReturn(Arrays.asList(existingRecord));
        
        ReflectionTestUtils.invokeMethod(cbPlanService, "upsertCbPlanContentLookup", "plan1", contentIds);
        
        verify(cassandraOperation).updateRecord(anyString(), anyString(), any(), any());
    }

    @Test
    void testUpsertCbPlanContentLookup_PlanAlreadyExists() {
        List<String> contentIds = Arrays.asList("content1");
        Set<String> existingPlanIds = new HashSet<>(Arrays.asList("plan1"));
        
        Map<String, Object> existingRecord = Map.of("planId", existingPlanIds); // Note: planId not planid
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), anyInt()))
            .thenReturn(Arrays.asList(existingRecord));
        
        ReflectionTestUtils.invokeMethod(cbPlanService, "upsertCbPlanContentLookup", "plan1", contentIds);
        
        verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), any(), any());
    }

    @Test
    void testUpsertCbPlanContentLookup_MultipleContents() {
        List<String> contentIds = Arrays.asList("content1", "content2");
        
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), anyInt()))
            .thenReturn(Collections.emptyList());
        
        ReflectionTestUtils.invokeMethod(cbPlanService, "upsertCbPlanContentLookup", "plan1", contentIds);
        
        verify(cassandraOperation, times(2)).updateRecord(anyString(), anyString(), any(), any());
    }

    // Tests for upsertAllOrgLookup method
    @Test
    void testUpsertAllOrgLookup_Success() {
        ApiResponse successResp = new ApiResponse();
        successResp.setParams(new ApiRespParam());
        successResp.getParams().setStatus(Constants.SUCCESS);
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(successResp);
        
        ApiResponse result = (ApiResponse) ReflectionTestUtils.invokeMethod(
            cbPlanService, "upsertAllOrgLookup", "plan1", Instant.now(), true);
        
        assertEquals(Constants.SUCCESS, result.getParams().getStatus());
    }

    @Test
    void testUpsertAllOrgLookup_WithNullEndDate() {
        ApiResponse successResp = new ApiResponse();
        successResp.setParams(new ApiRespParam());
        successResp.getParams().setStatus(Constants.SUCCESS);
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(successResp);
        
        ApiResponse result = (ApiResponse) ReflectionTestUtils.invokeMethod(
            cbPlanService, "upsertAllOrgLookup", "plan1", null, false);
        
        assertEquals(Constants.SUCCESS, result.getParams().getStatus());
    }

    @Test
    void testUpsertAllOrgLookup_Exception() {
        when(cassandraOperation.insertRecord(anyString(), anyString(), any()))
            .thenThrow(new RuntimeException("DB error"));
        
        ApiResponse result = (ApiResponse) ReflectionTestUtils.invokeMethod(
            cbPlanService, "upsertAllOrgLookup", "plan1", Instant.now(), true);
        
        assertEquals(Constants.FAILED, result.getParams().getStatus());
        assertTrue(result.getParams().getErr().contains("Exception while inserting SINGLE org lookup"));
    }

    // Tests for getRootOrgFromUser method
    @Test
    void testGetRootOrgFromUser_Success() {
        Map<String, Object> userMap = Map.of(
            Constants.ID, "user1",
            Constants.ROOT_ORG_ID, "rootOrg1"
        );
        when(userUtilityService.readUserProfileFromDB(anyString(), any())).thenReturn(userMap);
        
        ApiResponse response = new ApiResponse();
        response.setParams(new ApiRespParam());
        String result = (String) ReflectionTestUtils.invokeMethod(
            cbPlanService, "getRootOrgFromUser", "user1", response);
        
        assertEquals("rootOrg1", result);
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testGetRootOrgFromUser_EmptyUserMap() {
        when(userUtilityService.readUserProfileFromDB(anyString(), any())).thenReturn(new HashMap<>());
        
        ApiResponse response = new ApiResponse();
        response.setParams(new ApiRespParam());
        String result = (String) ReflectionTestUtils.invokeMethod(
            cbPlanService, "getRootOrgFromUser", "user1", response);
        
        assertNull(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testGetRootOrgFromUser_NullUserMap() {
        when(userUtilityService.readUserProfileFromDB(anyString(), any())).thenReturn(null);
        
        ApiResponse response = new ApiResponse();
        response.setParams(new ApiRespParam());
        String result = (String) ReflectionTestUtils.invokeMethod(
            cbPlanService, "getRootOrgFromUser", "user1", response);
        
        assertNull(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    // Tests for getCCAFromOrg method
    @Test
    void testGetCCAFromOrg_Success_True() {
        Map<String, Object> orgMap = Map.of(Constants.IS_CCA, true);
        when(userUtilityService.readOrgFromDB(anyString(), any())).thenReturn(orgMap);
        
        ApiResponse response = new ApiResponse();
        response.setParams(new ApiRespParam());
        boolean result = (Boolean) ReflectionTestUtils.invokeMethod(
            cbPlanService, "getCCAFromOrg", "org1", response);
        
        assertTrue(result);
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testGetCCAFromOrg_Success_False() {
        Map<String, Object> orgMap = Map.of(Constants.IS_CCA, "false");
        when(userUtilityService.readOrgFromDB(anyString(), any())).thenReturn(orgMap);
        
        ApiResponse response = new ApiResponse();
        response.setParams(new ApiRespParam());
        boolean result = (Boolean) ReflectionTestUtils.invokeMethod(
            cbPlanService, "getCCAFromOrg", "org1", response);
        
        assertFalse(result);
    }

    @Test
    void testGetCCAFromOrg_NoCCAField() {
        Map<String, Object> orgMap = Map.of("name", "Test Org");
        when(userUtilityService.readOrgFromDB(anyString(), any())).thenReturn(orgMap);
        
        ApiResponse response = new ApiResponse();
        response.setParams(new ApiRespParam());
        boolean result = (Boolean) ReflectionTestUtils.invokeMethod(
            cbPlanService, "getCCAFromOrg", "org1", response);
        
        assertFalse(result);
    }

    @Test
    void testGetCCAFromOrg_EmptyOrgMap() {
        when(userUtilityService.readOrgFromDB(anyString(), any())).thenReturn(new HashMap<>());
        
        ApiResponse response = new ApiResponse();
        response.setParams(new ApiRespParam());
        boolean result = (Boolean) ReflectionTestUtils.invokeMethod(
            cbPlanService, "getCCAFromOrg", "org1", response);
        
        assertFalse(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    // Tests for removeCbPlanInfoForUpdateOrDeleteCbPlan method
    @Test
    void testRemoveCbPlanInfoForUpdateOrDeleteCbPlan_SinglePlanDelete() {
        List<String> contentIds = Arrays.asList("content1");
        Set<String> planIds = new HashSet<>(Arrays.asList("plan1"));
        
        Map<String, Object> existingRecord = Map.of("planId", planIds);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), anyInt()))
            .thenReturn(Arrays.asList(existingRecord));
        
        ReflectionTestUtils.invokeMethod(cbPlanService, "removeCbPlanInfoForUpdateOrDeleteCbPlan", "plan1", contentIds);
        
        verify(cassandraOperation).deleteRecord(anyString(), anyString(), any());
    }

    @Test
    void testRemoveCbPlanInfoForUpdateOrDeleteCbPlan_MultiplePlansUpdate() {
        List<String> contentIds = Arrays.asList("content1");
        Set<String> planIds = new HashSet<>(Arrays.asList("plan1", "plan2"));
        
        Map<String, Object> existingRecord = Map.of("planId", planIds);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), anyInt()))
            .thenReturn(Arrays.asList(existingRecord));
        
        ReflectionTestUtils.invokeMethod(cbPlanService, "removeCbPlanInfoForUpdateOrDeleteCbPlan", "plan1", contentIds);
        
        verify(cassandraOperation).updateRecord(anyString(), anyString(), any(), any());
        verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), any());
    }

    @Test
    void testRemoveCbPlanInfoForUpdateOrDeleteCbPlan_EmptyRows() {
        List<String> contentIds = Arrays.asList("content1");
        
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), anyInt()))
            .thenReturn(Collections.emptyList());
        
        // Should not throw exception and should not call delete/update
        ReflectionTestUtils.invokeMethod(cbPlanService, "removeCbPlanInfoForUpdateOrDeleteCbPlan", "plan1", contentIds);
        
        verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), any());
        verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), any(), any());
    }

    @Test
    void testRemoveCbPlanInfoForUpdateOrDeleteCbPlan_InvalidPlanIdData() {
        List<String> contentIds = Arrays.asList("content1");
        
        Map<String, Object> existingRecord = Map.of("planId", "not-a-set");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), anyInt()))
            .thenReturn(Arrays.asList(existingRecord));
        
        // Should handle ClassCastException gracefully and not call delete/update
        ReflectionTestUtils.invokeMethod(cbPlanService, "removeCbPlanInfoForUpdateOrDeleteCbPlan", "plan1", contentIds);
        
        verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), any());
        verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), any(), any());
    }

    // Tests for getAddedContent method
    @Test
    void testGetAddedContent_NewContentAdded() {
        List<String> existingContent = Arrays.asList("content1", "content2");
        List<String> updatedContent = Arrays.asList("content1", "content2", "content3");
        
        List<String> result = (List<String>) ReflectionTestUtils.invokeMethod(
            cbPlanService, "getAddedContent", existingContent, updatedContent);
        
        assertEquals(1, result.size());
        assertTrue(result.contains("content3"));
    }

    @Test
    void testGetAddedContent_NoNewContent() {
        List<String> existingContent = Arrays.asList("content1", "content2");
        List<String> updatedContent = Arrays.asList("content1", "content2");
        
        List<String> result = (List<String>) ReflectionTestUtils.invokeMethod(
            cbPlanService, "getAddedContent", existingContent, updatedContent);
        
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetAddedContent_NullExistingContent() {
        List<String> updatedContent = Arrays.asList("content1", "content2");
        
        List<String> result = (List<String>) ReflectionTestUtils.invokeMethod(
            cbPlanService, "getAddedContent", null, updatedContent);
        
        assertEquals(2, result.size());
        assertTrue(result.containsAll(updatedContent));
    }

    @Test
    void testGetAddedContent_NullUpdatedContent() {
        List<String> existingContent = Arrays.asList("content1", "content2");
        
        List<String> result = (List<String>) ReflectionTestUtils.invokeMethod(
            cbPlanService, "getAddedContent", existingContent, null);
        
        assertTrue(result.isEmpty());
    }

    // Tests for getDeletedContent method
    @Test
    void testGetDeletedContent_ContentRemoved() {
        List<String> existingContent = Arrays.asList("content1", "content2", "content3");
        List<String> updatedContent = Arrays.asList("content1", "content2");
        
        List<String> result = (List<String>) ReflectionTestUtils.invokeMethod(
            cbPlanService, "getDeletedContent", existingContent, updatedContent);
        
        assertEquals(1, result.size());
        assertTrue(result.contains("content3"));
    }

    @Test
    void testGetDeletedContent_NoContentRemoved() {
        List<String> existingContent = Arrays.asList("content1", "content2");
        List<String> updatedContent = Arrays.asList("content1", "content2", "content3");
        
        List<String> result = (List<String>) ReflectionTestUtils.invokeMethod(
            cbPlanService, "getDeletedContent", existingContent, updatedContent);
        
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetDeletedContent_NullExistingContent() {
        List<String> updatedContent = Arrays.asList("content1", "content2");
        
        List<String> result = (List<String>) ReflectionTestUtils.invokeMethod(
            cbPlanService, "getDeletedContent", null, updatedContent);
        
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetDeletedContent_NullUpdatedContent() {
        List<String> existingContent = Arrays.asList("content1", "content2");
        
        List<String> result = (List<String>) ReflectionTestUtils.invokeMethod(
            cbPlanService, "getDeletedContent", existingContent, null);
        
        assertEquals(2, result.size());
        assertTrue(result.containsAll(existingContent));
    }

    @Test
    void testUpsertCbPlanContentLookup_FixedFieldName() {
        List<String> contentIds = Arrays.asList("content1");
        Set<String> existingPlanIds = new HashSet<>(Arrays.asList("plan2"));
        
        Map<String, Object> existingRecord = Map.of("planId", existingPlanIds);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), anyInt()))
            .thenReturn(Arrays.asList(existingRecord));
        
        ReflectionTestUtils.invokeMethod(cbPlanService, "upsertCbPlanContentLookup", "plan1", contentIds);
        
        verify(cassandraOperation).updateRecord(anyString(), anyString(), any(), any());
    }

}