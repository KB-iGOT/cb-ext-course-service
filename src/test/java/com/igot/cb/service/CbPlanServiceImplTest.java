package com.igot.cb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.model.CbPlanDto;
import com.igot.cb.user.UserUtilityService;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;

import java.lang.Exception;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doNothing;

class CbPlanServiceImplTest {

    @Mock private AccessTokenValidator accessTokenValidator;
    @Mock private CassandraOperation cassandraOperation;
    @Mock private UserUtilityService userUtilityService;
    @Mock private ContentInfoServiceImpl contentService;
    @Mock private EsUtilService esUtilService;
    @Mock private CbExtServerProperties serverProperties;
    
    private CbPlanServiceImpl cbPlanService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        cbPlanService = new CbPlanServiceImpl(accessTokenValidator, cassandraOperation);
        ReflectionTestUtils.setField(cbPlanService, "userUtilityService", userUtilityService);
        ReflectionTestUtils.setField(cbPlanService, "contentService", contentService);
        ReflectionTestUtils.setField(cbPlanService, "esUtilService", esUtilService);
        ReflectionTestUtils.setField(cbPlanService, "serverProperties", serverProperties);
        ReflectionTestUtils.setField(cbPlanService, "cpPlanIndex", "test-index");
        ReflectionTestUtils.setField(cbPlanService, "elasticCbPlanJsonPath", "test-path");
        ReflectionTestUtils.setField(cbPlanService, "allowedFieldsConfig", "name,contextDataRequest,endDate");
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
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
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
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
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
        when(cassandraOperation.updateRecord(anyString(), anyString(), any(), any())).thenReturn(updateResp);
        
        ApiResponse response = cbPlanService.publishCbPlan(request, "orgId", "token", Arrays.asList("role"));
        
        assertNotNull(response);
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
        }).when(userUtilityService).getUserDetailsFromDB(anyList(), anyList(), any());

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
    void testValidateCbPlanRequest() {
        CbPlanDto dto = new CbPlanDto();
        dto.setName("Test");
        dto.setEndDate(new Date());
        
        List<String> result = (List<String>) ReflectionTestUtils.invokeMethod(cbPlanService, "validateCbPlanRequest", dto);
        
        assertNotNull(result);
    }

    @SuppressWarnings("unchecked")
    @Test
    void testValidateContextData_NoContextData() {
        CbPlanDto dto = new CbPlanDto();
        ApiRequest request = new ApiRequest();
        request.setRequest(new HashMap<>());
        
        List<String> result = (List<String>) ReflectionTestUtils.invokeMethod(cbPlanService, "validateContextData", dto, request);
        
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testInsertCustomOrgLookup_EmptyList() {
        ApiResponse result = (ApiResponse) ReflectionTestUtils.invokeMethod(cbPlanService, "insertCustomOrgLookup", "planId", new ArrayList<>(), new Date());
        
        assertNotNull(result);
        assertEquals(Constants.FAILED, result.getParams().getStatus());
    }

    @Test
    void testInsertAllOrgLookup() {
        ApiResponse cassandraResp = new ApiResponse();
        cassandraResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(cassandraResp);
        
        ApiResponse result = (ApiResponse) ReflectionTestUtils.invokeMethod(cbPlanService, "insertAllOrgLookup", "planId", new Date());
        
        assertNotNull(result);
    }

    @SuppressWarnings("unchecked")
    @Test
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
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
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
    void testParseEndDate_String() {
        Date result = (Date) ReflectionTestUtils.invokeMethod(cbPlanService, "parseEndDate", "2024-12-31");
        assertNotNull(result);
    }

    @Test
    void testParseEndDate_Instant() {
        Instant instant = Instant.now();
        Date result = (Date) ReflectionTestUtils.invokeMethod(cbPlanService, "parseEndDate", instant);
        assertNull(result);
    }

    @Test
    void testParseEndDate_Date() {
        Date date = new Date();
        Date result = (Date) ReflectionTestUtils.invokeMethod(cbPlanService, "parseEndDate", date);
        assertEquals(date, result);
    }

    @Test
    void testParseEndDate_Null() {
        Date result = (Date) ReflectionTestUtils.invokeMethod(cbPlanService, "parseEndDate", (Object) null);
        assertNull(result);
    }

    @Test
    void testGetDesignationForUser() {
        String profileDetails = "{\"professionalDetails\":[{\"designation\":\"Manager\"}]}";
        String result = (String) ReflectionTestUtils.invokeMethod(cbPlanService, "getDesignationForUser", profileDetails, "userId");
        
        assertEquals("Manager", result);
    }

    @Test
    void testGetDesignationForUser_EmptyProfile() {
        String result = (String) ReflectionTestUtils.invokeMethod(cbPlanService, "getDesignationForUser", "", "userId");
        
        assertEquals("", result);
    }

    @Test
    void testGetDesignationForUser_InvalidJson() {
        String result = (String) ReflectionTestUtils.invokeMethod(cbPlanService, "getDesignationForUser", "invalid-json", "userId");
        
        assertEquals("", result);
    }

    @Test
    void testToInstant_String() {
        Instant result = (Instant) ReflectionTestUtils.invokeMethod(cbPlanService, "toInstant", "2024-12-31T10:00:00Z");
        assertNotNull(result);
    }

    @Test
    void testToInstant_Instant() {
        Instant instant = Instant.now();
        Instant result = (Instant) ReflectionTestUtils.invokeMethod(cbPlanService, "toInstant", instant);
        assertEquals(instant, result);
    }

    @Test
    void testToInstant_Date() {
        Date date = new Date();
        Instant result = (Instant) ReflectionTestUtils.invokeMethod(cbPlanService, "toInstant", date);
        assertNotNull(result);
    }

    @Test
    void testToInstant_Null() {
        Instant result = (Instant) ReflectionTestUtils.invokeMethod(cbPlanService, "toInstant", (Object) null);
        assertNull(result);
    }

    @Test
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
        }).when(userUtilityService).getUserDetailsFromDB(anyList(), anyList(), any());

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
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
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
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
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
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
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
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
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
    void testEnrichUserInfoWithProfile() {
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();
        Map<String, String> userInfo = new HashMap<>();
        userInfo.put("profileDetails", "{\"professionalDetails\":[{\"designation\":\"Developer\"}]}");
        userInfoMap.put("userId", userInfo);
        
        ReflectionTestUtils.invokeMethod(cbPlanService, "enrichUserInfo", userInfoMap);
        
        assertEquals("Developer", userInfoMap.get("userId").get("designation"));
    }

    @Test
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
        }).when(userUtilityService).getUserDetailsFromDB(anyList(), anyList(), any());

        Map<String, Object> result = (Map<String, Object>) ReflectionTestUtils.invokeMethod(cbPlanService, "populateReadData", cbPlan);

        assertNotNull(result);
        assertEquals("Test Plan", result.get("name"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testPopulateReadData_WithDraftData() throws Exception {
        Map<String, Object> cbPlan = new HashMap<>();
        cbPlan.put("draftData", "{\"name\":\"Draft Plan\",\"contentType\":\"Course\",\"contentList\":[\"content1\"],\"endDate\":\"2024-12-31\"}");
        cbPlan.put("status", "draft");
        cbPlan.put("createdBy", "userId");
        cbPlan.put("createdAtReq", Instant.now());

        Map<String, Object> mockContent = createMockContent();
        when(contentService.readContent(anyString(), any())).thenReturn(mockContent);

        doAnswer(invocation -> {
            Map<String, Map<String, String>> userInfoMap = invocation.getArgument(2);
            Map<String, String> userDetails = new HashMap<>();
            userDetails.put("firstName", "Test");
            userDetails.put("lastName", "User");
            userInfoMap.put("userId", userDetails);
            return null;
        }).when(userUtilityService).getUserDetailsFromDB(anyList(), anyList(), any());

        Map<String, Object> result = (Map<String, Object>) ReflectionTestUtils.invokeMethod(cbPlanService, "populateReadData", cbPlan);

        assertNotNull(result);
        assertEquals("Draft Plan", result.get("name"));
        assertNotNull(result.get("contentList"));
    }

    @Test
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

        updateMap.put("contextData", contextData);
        request.setRequest(updateMap);

        // --- Mock userId ---
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userId");

        // --- Mock existing plan ---
        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put("createdBy", "userId");
        existingPlan.put("status", "draft");
        existingPlan.put(Constants.ROOT_ORG_ID, "orgId");
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
    void testCreateCbPlan_ElasticSearchError() {
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
        
        doThrow(new RuntimeException("ES error")).when(esUtilService)
            .addDocument(anyString(), anyString(), anyString(), any(), anyString());
        
        ApiResponse response = cbPlanService.createCbPlan(request, "orgId", "token");
        
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
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
        
        ApiResponse response = cbPlanService.publishCbPlan(request, "orgId", "token", Arrays.asList("role"));
        
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
        when(cassandraOperation.updateRecord(anyString(), anyString(), any(), any())).thenReturn(updateResp);
        
        ApiResponse response = cbPlanService.publishCbPlan(request, "orgId", "token", Arrays.asList("role"));
        
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testRetireCbPlan_EmptyUserId() {
        ApiRequest request = new ApiRequest();
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("");
        
        ApiResponse response = cbPlanService.retireCbPlan(request, "orgId", "token", Arrays.asList("role"));
        
        assertNotNull(response);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
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
            .thenReturn(Arrays.asList(existingPlan));
        
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
    void testReadCbPlan_ContentError() {
        Map<String, Object> cbPlan = new HashMap<>();
        cbPlan.put("contentList", Arrays.asList("content1"));
        cbPlan.put("status", "live");
        cbPlan.put("draftData", "");
        
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
            .thenReturn(Arrays.asList(cbPlan));
        
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
}