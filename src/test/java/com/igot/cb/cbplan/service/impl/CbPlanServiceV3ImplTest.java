package com.igot.cb.cbplan.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.igot.cb.cache.CbPlanCacheMgrV3;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.cbplan.dto.CbPlanContentOccurrence;
import com.igot.cb.cbplan.dto.CbPlanDictionaryCacheEntry;
import com.igot.cb.cbplan.dto.CbPlanReadResponseDto;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.ContentInfoServiceImpl;
import com.igot.cb.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.service.UserAndOrgServiceImpl;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanServiceV3ImplTest {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private static final String TOKEN = "token";
    private static final String USER_ID = "user1";
    private static final String ORG_ID = "org1";
    private static final String PLAN_ID = "plan1";
    private static final String PLAN_YEAR = "2026-27";
    private static final String DICT_CACHE_KEY = Constants.CB_PLAN_REDIS_KEY_PREFIX + USER_ID + ":" + PLAN_YEAR + ":dict";
    private static final String BASIC_PROFILE_CACHE_KEY = Constants.USER + ":basicProfile:" + USER_ID;

    @Mock
    private AccessTokenValidator accessTokenValidator;

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private CbExtServerProperties serverProperties;

    @Mock
    private UserAndOrgServiceImpl userAndOrgService;

    @Mock
    private EsUtilService esUtilService;

    @Mock
    private ContentInfoServiceImpl contentService;

    @Mock
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @Mock
    private CbPlanCacheMgrV3 cbPlanCacheMgrV3;

    @Mock
    private RedisCacheMgr redisCacheMgr;

    @InjectMocks
    private CbPlanServiceV3Impl cbPlanService;

    private static ApiRequest apiRequest(Map<String, Object> requestMap) {
        ApiRequest request = new ApiRequest();
        request.setRequest(requestMap);
        return request;
    }

    private static ApiRequest requestWithPlanId() {
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, PLAN_ID);
        return apiRequest(requestMap);
    }

    private static Map<String, Object> validContextData() {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID);
        criteria.put(Constants.CRITERIA_VALUE, List.of(ORG_ID));
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, List.of(criteria));
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, List.of(userGroup));
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        return contextData;
    }

    private static Map<String, Object> validPlanPayload() {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID);
        criteria.put(Constants.CRITERIA_VALUE, List.of(ORG_ID));
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, List.of(criteria));
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, List.of(userGroup));
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.CONTEXT_DATA_REQUEST, contextData);
        payload.put(Constants.NAME, "planName");
        payload.put(Constants.CONTENT_TYPE, "Course");
        payload.put(Constants.CONTENT_LIST, List.of("do_123"));
        payload.put(Constants.END_DATE_REQUEST, "2030-12-31");
        return payload;
    }

    private static ApiRequest validPlanRequest() {
        return apiRequest(validPlanPayload());
    }

    private static ApiRequest validPlanRequestWithId() {
        Map<String, Object> payload = validPlanPayload();
        payload.put(Constants.ID, PLAN_ID);
        return apiRequest(payload);
    }

    private static ApiResponse cassandraInsertSuccess() {
        ApiResponse response = new ApiResponse();
        response.getParams().setStatus(Constants.SUCCESS);
        response.put(Constants.RESPONSE, Constants.SUCCESS);
        return response;
    }

    @BeforeEach
    void setUpAuthorizedRoles() {
        lenient().when(serverProperties.getCbPlanUpdatePublishAuthorizedRoles())
                .thenReturn(List.of("CONTENT_CREATOR", "ADMIN"));
    }

    private void mockAuthenticatedUser() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn(USER_ID);
        mockUserProfileLookup();
    }

    private void mockValidUserAndOrg(boolean isCCA) {
        mockAuthenticatedUser();
        Map<String, Object> userMap = new HashMap<>();
        userMap.put(Constants.ROOT_ORG_ID, ORG_ID);
        lenient().when(userAndOrgService.readUserProfileFromDB(anyString(), anyList())).thenReturn(userMap);
        Map<String, Object> orgMap = new HashMap<>();
        orgMap.put(Constants.IS_CCA, isCCA);
        lenient().when(userAndOrgService.readOrgFromDB(anyString(), any())).thenReturn(orgMap);
    }

    private void mockUserProfileLookup() {
        lenient().when(redisCacheMgr.getFromCache(BASIC_PROFILE_CACHE_KEY)).thenReturn(null);
        Map<String, Object> userRow = new HashMap<>();
        userRow.put(Constants.ID, USER_ID);
        userRow.put(Constants.ROOT_ORG_ID, ORG_ID);
        lenient().when(cassandraOperation.getRecordsByProperties(
                        anyString(), eq(Constants.USER), anyMap(), any(), any()))
                .thenReturn(List.of(userRow));
        lenient().when(cassandraOperation.getRecordsByProperties(
                        anyString(), eq(Constants.TABLE_USER_EXTENDED_PROFILE), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());
    }

    private void mockExistingPlan(Map<String, Object> plan) {
        when(cassandraOperation.getRecordsByProperties(
                        anyString(), eq(Constants.TABLE_CB_PLAN_V3), anyMap(), any(), any()))
                .thenReturn(List.of(plan));
    }

    private void mockEsProperties() {
        when(serverProperties.getCpPlanIndex()).thenReturn("cbplan-index");
        when(serverProperties.getElasticCbPlanJsonPath()).thenReturn("path.json");
    }

    // ---------------- createCbPlan ----------------

    @Test
    void testCreateCbPlanSuccess() {
        mockValidUserAndOrg(false);
        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap())).thenReturn(cassandraInsertSuccess());
        ApiResponse response = cbPlanService.createCbPlan(validPlanRequest(), ORG_ID, TOKEN);
        assertEquals(HttpStatus.CREATED, response.getResponseCode());
        assertEquals(Constants.CREATED, response.getResult().get(Constants.STATUS));
        assertNotNull(response.getResult().get(Constants.ID));
    }

    @Test
    void testCreateCbPlanFailsWhenTokenInvalid() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn(null);
        ApiResponse response = cbPlanService.createCbPlan(apiRequest(new HashMap<>()), ORG_ID, TOKEN);
        assertNotNull(response);
        verify(cassandraOperation, never()).insertRecord(anyString(), anyString(), anyMap());
    }

    @Test
    void testCreateCbPlanFailsWhenUserOrgNotFound() {
        mockAuthenticatedUser();
        when(cassandraOperation.getRecordsByProperties(anyString(), eq(Constants.USER), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());
        ApiResponse response = cbPlanService.createCbPlan(apiRequest(new HashMap<>()), ORG_ID, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        verify(cassandraOperation, never()).insertRecord(anyString(), anyString(), anyMap());
    }

    @Test
    void testCreateCbPlanFailsOnValidationErrors() {
        mockValidUserAndOrg(false);
        ApiResponse response = cbPlanService.createCbPlan(apiRequest(new HashMap<>()), ORG_ID, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        verify(cassandraOperation, never()).insertRecord(anyString(), anyString(), anyMap());
    }

    @Test
    void testCreateCbPlanFailsWhenInsertFails() {
        mockValidUserAndOrg(false);
        ApiResponse insertFailed = new ApiResponse();
        insertFailed.put(Constants.RESPONSE, Constants.FAILED);
        insertFailed.getParams().setErr("insert error");
        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap())).thenReturn(insertFailed);
        ApiResponse response = cbPlanService.createCbPlan(validPlanRequest(), ORG_ID, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testCreateCbPlanHandlesUnexpectedException() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any()))
                .thenThrow(new RuntimeException("boom"));
        ApiResponse response = cbPlanService.createCbPlan(apiRequest(new HashMap<>()), ORG_ID, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    // ---------------- updateCbPlan ----------------

    @Test
    void testUpdateCbPlanFailsWhenPlanIdMissing() {
        mockAuthenticatedUser();
        ApiResponse response = cbPlanService.updateCbPlan(apiRequest(new HashMap<>()), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testUpdateCbPlanFailsWhenPlanNotFound() {
        mockAuthenticatedUser();
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(List.of());
        ApiResponse response = cbPlanService.updateCbPlan(requestWithPlanId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testUpdateCbPlanFailsWhenUnauthorized() {
        mockAuthenticatedUser();
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, "otherUser");
        existingCbPlan.put(Constants.STATUS, Constants.DRAFT);
        mockExistingPlan(existingCbPlan);
        ApiResponse response = cbPlanService.updateCbPlan(requestWithPlanId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.FORBIDDEN, response.getResponseCode());
    }

    @Test
    void testUpdateCbPlanDraftSuccess() {
        mockValidUserAndOrg(false);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.DRAFT);
        mockExistingPlan(existingCbPlan);
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));
        ApiResponse response = cbPlanService.updateCbPlan(validPlanRequestWithId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.UPDATED, response.getResult().get(Constants.STATUS));
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testUpdateCbPlanDraftFailsOnValidationErrors() {
        mockValidUserAndOrg(false);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.DRAFT);
        mockExistingPlan(existingCbPlan);
        ApiResponse response = cbPlanService.updateCbPlan(requestWithPlanId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap());
    }

    @Test
    void testUpdateCbPlanDraftFailsWhenCassandraUpdateFails() {
        mockValidUserAndOrg(false);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.DRAFT);
        mockExistingPlan(existingCbPlan);
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.FAILED));
        ApiResponse response = cbPlanService.updateCbPlan(validPlanRequestWithId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testUpdateCbPlanLiveSavesAsDraft() {
        mockValidUserAndOrg(false);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.LIVE);
        existingCbPlan.put(Constants.PLAN_ID, PLAN_ID);
        mockExistingPlan(existingCbPlan);
        when(serverProperties.getCbPlanUpdateAllowedFields()).thenReturn(List.of(Constants.NAME));
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, PLAN_ID);
        requestMap.put(Constants.NAME, "updatedLiveName");
        requestMap.put(Constants.CONTEXT_DATA_REQUEST, validContextData());
        ApiResponse response = cbPlanService.updateCbPlan(apiRequest(requestMap), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.UPDATED, response.getResult().get(Constants.STATUS));
        assertNotNull(response.getResult().get(Constants.MESSAGE));
    }

    @Test
    void testUpdateCbPlanLiveFailsOnContextDataErrors() {
        mockValidUserAndOrg(false);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.LIVE);
        mockExistingPlan(existingCbPlan);
        ApiResponse response = cbPlanService.updateCbPlan(requestWithPlanId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testUpdateCbPlanHandlesUnexpectedException() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any()))
                .thenThrow(new RuntimeException("boom"));
        ApiResponse response = cbPlanService.updateCbPlan(requestWithPlanId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    // ---------------- publishCbPlan ----------------

    @Test
    void testPublishCbPlanFailsWhenPlanIdMissing() {
        mockAuthenticatedUser();
        ApiResponse response = cbPlanService.publishCbPlan(apiRequest(new HashMap<>()), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testPublishCbPlanFailsWhenPlanNotFound() {
        mockAuthenticatedUser();
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(List.of());
        ApiResponse response = cbPlanService.publishCbPlan(requestWithPlanId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testPublishCbPlanFailsWhenUnauthorized() {
        mockAuthenticatedUser();
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, "otherUser");
        mockExistingPlan(existingCbPlan);
        ApiResponse response = cbPlanService.publishCbPlan(requestWithPlanId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.FORBIDDEN, response.getResponseCode());
    }

    @Test
    void testPublishCbPlanFailsForInvalidStatus() {
        mockValidUserAndOrg(false);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.CB_RETIRE);
        mockExistingPlan(existingCbPlan);
        ApiResponse response = cbPlanService.publishCbPlan(requestWithPlanId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }


    @Test
    void testPublishCbPlanDraftFailsOnContextDataErrors() {
        mockValidUserAndOrg(false);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.DRAFT);
        mockExistingPlan(existingCbPlan);
        ApiResponse response = cbPlanService.publishCbPlan(requestWithPlanId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testPublishCbPlanFailsWhenCassandraTransactionFails() {
        mockValidUserAndOrg(false);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.DRAFT);
        existingCbPlan.put(Constants.PLAN_YEAR, PLAN_YEAR);
        existingCbPlan.put(Constants.CONTEXT_DATA_REQUEST, validContextData());
        mockExistingPlan(existingCbPlan);
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap(), any(), any()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.FAILED, Constants.ERROR_MESSAGE, "commit failed"));
        ApiResponse response = cbPlanService.publishCbPlan(requestWithPlanId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testPublishCbPlanHandlesUnexpectedException() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any()))
                .thenThrow(new RuntimeException("boom"));
        ApiResponse response = cbPlanService.publishCbPlan(requestWithPlanId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    // ---------------- retireCbPlan ----------------

    @Test
    void testRetireCbPlanFailsWhenPlanIdMissing() {
        mockAuthenticatedUser();
        ApiResponse response = cbPlanService.retireCbPlan(apiRequest(new HashMap<>()), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testRetireCbPlanFailsWhenPlanIdBlank() {
        mockAuthenticatedUser();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, "   ");
        ApiResponse response = cbPlanService.retireCbPlan(apiRequest(requestMap), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testRetireCbPlanFailsWhenPlanNotFound() {
        mockAuthenticatedUser();
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(List.of());
        ApiResponse response = cbPlanService.retireCbPlan(requestWithPlanId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testRetireCbPlanFailsWhenUnauthorized() {
        mockAuthenticatedUser();
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, "otherUser");
        mockExistingPlan(existingCbPlan);
        ApiResponse response = cbPlanService.retireCbPlan(requestWithPlanId(), ORG_ID, TOKEN, List.of());
        assertEquals(HttpStatus.FORBIDDEN, response.getResponseCode());
    }

    @Test
    void testRetireCbPlanFailsWhenAlreadyArchived() {
        mockAuthenticatedUser();
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.CB_RETIRE);
        mockExistingPlan(existingCbPlan);
        ApiResponse response = cbPlanService.retireCbPlan(requestWithPlanId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testRetireCbPlanSuccess() {
        mockAuthenticatedUser();
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.DRAFT);
        existingCbPlan.put(Constants.ORG_SCOPE, Constants.ALL);
        existingCbPlan.put(Constants.PLAN_YEAR, PLAN_YEAR);
        mockExistingPlan(existingCbPlan);
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));
        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap())).thenReturn(cassandraInsertSuccess());
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, PLAN_ID);
        requestMap.put(Constants.COMMENT, "archiving");
        ApiResponse response = cbPlanService.retireCbPlan(apiRequest(requestMap), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.UPDATED, response.getResult().get(Constants.STATUS));
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testRetireCbPlanFailsWhenCassandraUpdateFails() {
        mockAuthenticatedUser();
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.DRAFT);
        mockExistingPlan(existingCbPlan);
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.FAILED, Constants.ERROR_MESSAGE, "update failed"));
        ApiResponse response = cbPlanService.retireCbPlan(requestWithPlanId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testRetireCbPlanHandlesUnexpectedException() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any()))
                .thenThrow(new RuntimeException("boom"));
        ApiResponse response = cbPlanService.retireCbPlan(requestWithPlanId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    // ---------------- searchCbPlan ----------------

    @Test
    void testSearchCbPlanSuccess() throws Exception {
        mockAuthenticatedUser();
        mockEsProperties();
        Map<String, Object> item = new HashMap<>();
        item.put(Constants.ID, PLAN_ID);
        SearchResult searchResult = new SearchResult(
                new ArrayList<>(List.of(item)), new HashMap<>(), 1L, new ArrayList<>());
        when(esUtilService.searchDocuments(anyString(), any(SearchCriteria.class), anyString()))
                .thenReturn(searchResult);
        ApiResponse response = cbPlanService.searchCbPlan(new SearchCriteria(), ORG_ID, TOKEN);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertNotNull(response.getResult().get(Constants.RESULT));
    }

    @Test
    void testSearchCbPlanWithEmptyResultsDoesNotPopulateResult() throws Exception {
        mockAuthenticatedUser();
        mockEsProperties();
        SearchResult searchResult = new SearchResult(new ArrayList<>(), new HashMap<>(), 0L, new ArrayList<>());
        when(esUtilService.searchDocuments(anyString(), any(SearchCriteria.class), anyString()))
                .thenReturn(searchResult);
        ApiResponse response = cbPlanService.searchCbPlan(new SearchCriteria(), ORG_ID, TOKEN);
        assertFalse(response.getResult().containsKey(Constants.RESULT));
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testSearchCbPlanFailsWhenTokenInvalid() throws Exception {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn(null);
        cbPlanService.searchCbPlan(new SearchCriteria(), ORG_ID, TOKEN);
        verify(esUtilService, never()).searchDocuments(anyString(), any(), anyString());
    }

    @Test
    void testSearchCbPlanHandlesElasticSearchException() throws Exception {
        mockAuthenticatedUser();
        mockEsProperties();
        when(esUtilService.searchDocuments(anyString(), any(SearchCriteria.class), anyString()))
                .thenThrow(new RuntimeException("es down"));
        ApiResponse response = cbPlanService.searchCbPlan(new SearchCriteria(), ORG_ID, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    // ---------------- readCbPlan ----------------

    @Test
    void testReadCbPlanFailsWhenIdMissing() {
        ApiResponse response = cbPlanService.readCbPlan("", ORG_ID, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testReadCbPlanFailsWhenPlanNotFound() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(List.of());
        ApiResponse response = cbPlanService.readCbPlan(PLAN_ID, ORG_ID, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertTrue(response.getParams().getErr().contains(PLAN_ID));
    }

    @Test
    void testReadCbPlanReturnsDirectFieldsWhenNoDraftData() {
        Instant endDate = Instant.parse("2026-12-31T18:29:59Z");
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.NAME, "planName");
        plan.put(Constants.STATUS, Constants.DRAFT);
        plan.put(Constants.PLAN_YEAR, PLAN_YEAR);
        plan.put(Constants.END_DATE_REQUEST, endDate);
        plan.put(Constants.IS_APAR, true);
        plan.put(Constants.CONTENT_LIST, List.of("course1"));
        plan.put(Constants.CREATED_BY, USER_ID);
        mockExistingPlan(plan);
        ApiResponse response = cbPlanService.readCbPlan(PLAN_ID, ORG_ID, TOKEN);
        CbPlanReadResponseDto dto = (CbPlanReadResponseDto) response.getResult().get(Constants.CONTENT);
        assertEquals(PLAN_ID, dto.getId());
        assertEquals("planName", dto.getName());
        assertEquals(PLAN_YEAR, dto.getPlanYear());
        assertEquals(endDate, dto.getEndDate());
        assertTrue(dto.getIsApar());
        assertEquals(Constants.DRAFT, dto.getStatus());
        assertEquals(USER_ID, dto.getCreatedBy());
        assertEquals(List.of("course1"), dto.getContentList());
    }

    @Test
    void testReadCbPlanPrefersDraftDataForLivePlan() {
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.NAME, "liveName");
        plan.put(Constants.STATUS, Constants.LIVE);
        plan.put(Constants.PLAN_YEAR, PLAN_YEAR);
        plan.put(Constants.CONTENT_LIST, List.of("courseLive"));
        plan.put(Constants.DRAFT_DATA,
                "{\"name\":\"draftName\",\"contentList\":[\"courseDraft\"],\"isApar\":true,\"endDate\":\"2026-12-31\"}");
        mockExistingPlan(plan);
        ApiResponse response = cbPlanService.readCbPlan(PLAN_ID, ORG_ID, TOKEN);
        CbPlanReadResponseDto dto = (CbPlanReadResponseDto) response.getResult().get(Constants.CONTENT);
        assertEquals("draftName", dto.getName());
        assertTrue(dto.getIsApar());
        assertNotNull(dto.getEndDate());
        assertEquals(List.of("courseDraft"), dto.getContentList());
    }

    @Test
    void testReadCbPlanIgnoresEmptyDraftDataObject() {
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.NAME, "liveName");
        plan.put(Constants.STATUS, Constants.LIVE);
        plan.put(Constants.DRAFT_DATA, "{}");
        plan.put(Constants.CONTENT_LIST, List.of("courseLive"));
        mockExistingPlan(plan);
        ApiResponse response = cbPlanService.readCbPlan(PLAN_ID, ORG_ID, TOKEN);
        CbPlanReadResponseDto dto = (CbPlanReadResponseDto) response.getResult().get(Constants.CONTENT);
        assertEquals("liveName", dto.getName());
        assertEquals(List.of("courseLive"), dto.getContentList());
    }

    @Test
    void testReadCbPlanParsesContextDataIntoJsonNode() {
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.STATUS, Constants.DRAFT);
        plan.put(Constants.CONTEXT_DATA_REQUEST, "{\"accessControl\":{\"userGroups\":[]}}");
        mockExistingPlan(plan);
        ApiResponse response = cbPlanService.readCbPlan(PLAN_ID, ORG_ID, TOKEN);
        CbPlanReadResponseDto dto = (CbPlanReadResponseDto) response.getResult().get(Constants.CONTENT);
        assertNotNull(dto.getContextData());
        assertTrue(dto.getContextData().has("accessControl"));
    }

    @Test
    void testReadCbPlanReturnsNullContextDataForMalformedJson() {
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.STATUS, Constants.DRAFT);
        plan.put(Constants.CONTEXT_DATA_REQUEST, "not-valid-json");
        mockExistingPlan(plan);
        ApiResponse response = cbPlanService.readCbPlan(PLAN_ID, ORG_ID, TOKEN);
        CbPlanReadResponseDto dto = (CbPlanReadResponseDto) response.getResult().get(Constants.CONTENT);
        assertNotNull(dto);
        assertEquals(null, dto.getContextData());
    }

    @Test
    void testReadCbPlanHandlesUnexpectedException() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenThrow(new RuntimeException("db down"));
        ApiResponse response = cbPlanService.readCbPlan(PLAN_ID, ORG_ID, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    // ---------------- getCBPlanDictionaryForUser ----------------

    @Test
    void testGetCBPlanDictionaryFailsWhenTokenInvalid() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("");
        ApiResponse response = cbPlanService.getCBPlanDictionaryForUser(apiRequest(new HashMap<>()), TOKEN);
        assertEquals(HttpStatus.UNAUTHORIZED, response.getResponseCode());
    }

    @Test
    void testGetCBPlanDictionaryFailsForInvalidPlanYear() {
        mockAuthenticatedUser();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("planYear", "invalid-year");
        ApiResponse response = cbPlanService.getCBPlanDictionaryForUser(apiRequest(requestMap), TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testGetCBPlanDictionaryReturnsCachedEntryOnRedisHit() throws Exception {
        mockAuthenticatedUser();
        Map<String, List<CbPlanContentOccurrence>> aparMap = new LinkedHashMap<>();
        aparMap.put("course1", List.of(new CbPlanContentOccurrence(PLAN_ID, Instant.EPOCH, "Mandatory")));
        CbPlanDictionaryCacheEntry cacheEntry =
                new CbPlanDictionaryCacheEntry(aparMap, new LinkedHashMap<>(), 1, 0);
        when(redisCacheMgr.getFromCache(DICT_CACHE_KEY)).thenReturn(MAPPER.writeValueAsString(cacheEntry));
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("planYear", PLAN_YEAR);
        ApiResponse response = cbPlanService.getCBPlanDictionaryForUser(apiRequest(requestMap), TOKEN);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(1, response.getResult().get("aparCount"));
        verify(cbPlanCacheMgrV3, never()).getCbPlanForAllAndOrgId(anyString(), anyString(), any());
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    @Test
    void testGetCBPlanDictionaryFailsWhenUserNotFound() {
        mockAuthenticatedUser();
        when(redisCacheMgr.getFromCache(DICT_CACHE_KEY)).thenReturn(null);
        when(redisCacheMgr.getFromCache(BASIC_PROFILE_CACHE_KEY)).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(List.of());
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("planYear", PLAN_YEAR);
        ApiResponse response = cbPlanService.getCBPlanDictionaryForUser(apiRequest(requestMap), TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testGetCBPlanDictionaryUsesBasicProfileRedisCacheOnCacheMiss() {
        mockAuthenticatedUser();
        when(redisCacheMgr.getFromCache(DICT_CACHE_KEY)).thenReturn(null);
        when(redisCacheMgr.getFromCache(BASIC_PROFILE_CACHE_KEY))
                .thenReturn("{\"id\":\"" + USER_ID + "\",\"rootOrgId\":\"" + ORG_ID + "\"}");
        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(anyString(), anyString(), any())).thenReturn(List.of());
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("planYear", PLAN_YEAR);
        ApiResponse response = cbPlanService.getCBPlanDictionaryForUser(apiRequest(requestMap), TOKEN);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        verify(cassandraOperation, never()).getRecordsByProperties(
                anyString(), eq(Constants.USER), anyMap(), any(), any());
        verify(cbPlanCacheMgrV3).getCbPlanForAllAndOrgId(eq(ORG_ID), eq(PLAN_YEAR), any());
    }

    @Test
    void testGetCBPlanDictionaryReturnsEmptyWhenNoActivePlans() {
        mockAuthenticatedUser();
        when(redisCacheMgr.getFromCache(DICT_CACHE_KEY)).thenReturn(null);
        when(redisCacheMgr.getFromCache(BASIC_PROFILE_CACHE_KEY))
                .thenReturn("{\"id\":\"" + USER_ID + "\",\"rootOrgId\":\"" + ORG_ID + "\"}");
        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(anyString(), anyString(), any())).thenReturn(List.of());
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("planYear", PLAN_YEAR);
        ApiResponse response = cbPlanService.getCBPlanDictionaryForUser(apiRequest(requestMap), TOKEN);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(0, response.getResult().get("aparCount"));
        assertEquals(0, response.getResult().get("nonAparCount"));
        verify(redisCacheMgr).putInCache(eq(DICT_CACHE_KEY), anyString(), anyInt());
    }

    @Test
    void testGetCBPlanDictionaryGroupsAparContent() {
        mockAuthenticatedUser();
        when(redisCacheMgr.getFromCache(DICT_CACHE_KEY)).thenReturn(null);
        when(redisCacheMgr.getFromCache(BASIC_PROFILE_CACHE_KEY))
                .thenReturn("{\"id\":\"" + USER_ID + "\",\"rootOrgId\":\"" + ORG_ID + "\"}");
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.PLAN_ID, PLAN_ID);
        plan.put(Constants.END_DATE_KEY, Instant.now());
        plan.put(Constants.IS_APAR, true);
        plan.put(Constants.CONTENT_LIST, List.of("course1"));
        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(anyString(), anyString(), any())).thenReturn(List.of(plan));
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("planYear", PLAN_YEAR);
        ApiResponse response = cbPlanService.getCBPlanDictionaryForUser(apiRequest(requestMap), TOKEN);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(1, response.getResult().get("aparCount"));
        assertEquals(0, response.getResult().get("nonAparCount"));
        Map<String, Object> aparContentList = (Map<String, Object>) response.getResult().get("aparContentList");
        assertTrue(aparContentList.containsKey("course1"));
    }

    @Test
    void testGetCBPlanDictionaryGroupsNonAparContent() {
        mockAuthenticatedUser();
        when(redisCacheMgr.getFromCache(DICT_CACHE_KEY)).thenReturn(null);
        when(redisCacheMgr.getFromCache(BASIC_PROFILE_CACHE_KEY))
                .thenReturn("{\"id\":\"" + USER_ID + "\",\"rootOrgId\":\"" + ORG_ID + "\"}");
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.PLAN_ID, PLAN_ID);
        plan.put(Constants.END_DATE_KEY, Instant.now());
        plan.put(Constants.IS_APAR, false);
        plan.put(Constants.CONTENT_LIST, List.of("course1"));
        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(anyString(), anyString(), any())).thenReturn(List.of(plan));
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("planYear", PLAN_YEAR);
        ApiResponse response = cbPlanService.getCBPlanDictionaryForUser(apiRequest(requestMap), TOKEN);
        assertEquals(0, response.getResult().get("aparCount"));
        assertEquals(1, response.getResult().get("nonAparCount"));
    }

    @Test
    void testGetCBPlanDictionarySkipsPlansWithEmptyContentList() {
        mockAuthenticatedUser();
        when(redisCacheMgr.getFromCache(DICT_CACHE_KEY)).thenReturn(null);
        when(redisCacheMgr.getFromCache(BASIC_PROFILE_CACHE_KEY))
                .thenReturn("{\"id\":\"" + USER_ID + "\",\"rootOrgId\":\"" + ORG_ID + "\"}");
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.PLAN_ID, PLAN_ID);
        plan.put(Constants.IS_APAR, false);
        plan.put(Constants.CONTENT_LIST, List.of());
        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(anyString(), anyString(), any())).thenReturn(List.of(plan));
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("planYear", PLAN_YEAR);
        ApiResponse response = cbPlanService.getCBPlanDictionaryForUser(apiRequest(requestMap), TOKEN);
        assertEquals(0, response.getResult().get("aparCount"));
        assertEquals(0, response.getResult().get("nonAparCount"));
    }

    @Test
    void testGetCBPlanDictionaryDeniesAccessWhenContextCriteriaDoNotMatch() {
        mockAuthenticatedUser();
        when(redisCacheMgr.getFromCache(DICT_CACHE_KEY)).thenReturn(null);
        when(redisCacheMgr.getFromCache(BASIC_PROFILE_CACHE_KEY))
                .thenReturn("{\"id\":\"" + USER_ID + "\",\"rootOrgId\":\"" + ORG_ID + "\"}");
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.PLAN_ID, PLAN_ID);
        plan.put(Constants.IS_APAR, false);
        plan.put(Constants.CONTENT_LIST, List.of("course1"));
        plan.put(Constants.CONTEXT_DATA_REQUEST,
                "{\"accessControl\":{\"userGroups\":[{\"userGroupCriteriaList\":"
                        + "[{\"criteriaKey\":\"designation\",\"criteriaValue\":[\"Director\"]}]}]}}");
        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(anyString(), anyString(), any())).thenReturn(List.of(plan));
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("planYear", PLAN_YEAR);
        ApiResponse response = cbPlanService.getCBPlanDictionaryForUser(apiRequest(requestMap), TOKEN);
        assertEquals(0, response.getResult().get("nonAparCount"));
    }

    @Test
    void testGetCBPlanDictionaryEnrichesContentWhenRequested() throws Exception {
        mockAuthenticatedUser();
        Map<String, List<CbPlanContentOccurrence>> nonAparMap = new LinkedHashMap<>();
        nonAparMap.put("course1", List.of(new CbPlanContentOccurrence(PLAN_ID, Instant.EPOCH, "Mandatory")));
        CbPlanDictionaryCacheEntry cacheEntry =
                new CbPlanDictionaryCacheEntry(new LinkedHashMap<>(), nonAparMap, 0, 1);
        when(redisCacheMgr.getFromCache(DICT_CACHE_KEY)).thenReturn(MAPPER.writeValueAsString(cacheEntry));
        when(redisCacheMgr.getFromCache(Constants.EXTENDED_READ_CONTENT_CACHE_KEY_PREFIX + "course1"))
                .thenReturn("{\"identifier\":\"course1\",\"name\":\"Course 1\"}");
        when(serverProperties.getCbPlanEnrichedContentFieldsList()).thenReturn(List.of("identifier", "name"));
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("planYear", PLAN_YEAR);
        requestMap.put("enrichment", true);
        ApiResponse response = cbPlanService.getCBPlanDictionaryForUser(apiRequest(requestMap), TOKEN);
        Map<String, Object> enriched = (Map<String, Object>) response.getResult().get("enrichedContentList");
        assertNotNull(enriched);
        Map<String, Object> courseDetails = (Map<String, Object>) enriched.get("course1");
        assertEquals("Course 1", courseDetails.get("name"));
        assertFalse(courseDetails.containsKey("_fromCache"));
    }

    @Test
    void testGetCBPlanDictionaryOmitsEnrichedListWhenNotRequested() throws Exception {
        mockAuthenticatedUser();
        CbPlanDictionaryCacheEntry cacheEntry =
                new CbPlanDictionaryCacheEntry(new LinkedHashMap<>(), new LinkedHashMap<>(), 0, 0);
        when(redisCacheMgr.getFromCache(DICT_CACHE_KEY)).thenReturn(MAPPER.writeValueAsString(cacheEntry));
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("planYear", PLAN_YEAR);
        ApiResponse response = cbPlanService.getCBPlanDictionaryForUser(apiRequest(requestMap), TOKEN);
        assertFalse(response.getResult().containsKey("enrichedContentList"));
    }

    @Test
    void testGetCBPlanDictionaryHandlesNullRequestBody() {
        mockAuthenticatedUser();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(List.of());
        ApiResponse response = cbPlanService.getCBPlanDictionaryForUser(apiRequest(null), TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testGetCBPlanDictionaryHandlesUnexpectedException() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any()))
                .thenThrow(new RuntimeException("boom"));
        ApiResponse response = cbPlanService.getCBPlanDictionaryForUser(apiRequest(new HashMap<>()), TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testConstructor() {
        assertNotNull(new CbPlanServiceV3Impl(accessTokenValidator, cassandraOperation, serverProperties,
                userAndOrgService, esUtilService, contentService,
                outboundRequestHandlerService, cbPlanCacheMgrV3, redisCacheMgr));
    }

    @Test
    void testUpdateCbPlanSuccessWhenUserIsCreator() {
        mockValidUserAndOrg(false);
        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put(Constants.PLAN_ID, PLAN_ID);
        existingPlan.put(Constants.CREATED_BY, USER_ID);
        existingPlan.put(Constants.STATUS, Constants.DRAFT);
        existingPlan.put(Constants.PLAN_YEAR, PLAN_YEAR);
        mockExistingPlan(existingPlan);

        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));

        ApiResponse response = cbPlanService.updateCbPlan(validPlanRequestWithId(), ORG_ID, TOKEN, List.of("CONTENT_CREATOR"));
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void testUpdateCbPlanFailsWhenUserNotAuthorized() {
        mockValidUserAndOrg(false);
        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put(Constants.PLAN_ID, PLAN_ID);
        existingPlan.put(Constants.CREATED_BY, "differentUser");
        existingPlan.put(Constants.STATUS, Constants.DRAFT);
        mockExistingPlan(existingPlan);

        ApiResponse response = cbPlanService.updateCbPlan(validPlanRequestWithId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testUpdateCbPlanSuccessWhenUserHasAuthorizedRole() {
        mockValidUserAndOrg(false);
        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put(Constants.PLAN_ID, PLAN_ID);
        existingPlan.put(Constants.CREATED_BY, "differentUser");
        existingPlan.put(Constants.STATUS, Constants.DRAFT);
        existingPlan.put(Constants.PLAN_YEAR, PLAN_YEAR);
        mockExistingPlan(existingPlan);

        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));

        ApiResponse response = cbPlanService.updateCbPlan(validPlanRequestWithId(), ORG_ID, TOKEN, List.of("ADMIN"));
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void testPublishCbPlanFailsWhenUserNotAuthorized() {
        mockValidUserAndOrg(false);
        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put(Constants.PLAN_ID, PLAN_ID);
        existingPlan.put(Constants.CREATED_BY, "differentUser");
        existingPlan.put(Constants.STATUS, Constants.DRAFT);
        mockExistingPlan(existingPlan);

        ApiResponse response = cbPlanService.publishCbPlan(requestWithPlanId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testRetireCbPlanSuccessWhenUserIsCreator() {
        mockValidUserAndOrg(false);
        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put(Constants.PLAN_ID, PLAN_ID);
        existingPlan.put(Constants.CREATED_BY, USER_ID);
        existingPlan.put(Constants.STATUS, Constants.LIVE);
        existingPlan.put(Constants.PLAN_YEAR, PLAN_YEAR);
        mockExistingPlan(existingPlan);

        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));

        Map<String, Object> request = new HashMap<>();
        request.put(Constants.ID, PLAN_ID);
        ApiResponse response = cbPlanService.retireCbPlan(apiRequest(request), ORG_ID, TOKEN, List.of());
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void testRetireCbPlanFailsWhenUserNotAuthorized() {
        mockValidUserAndOrg(false);
        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put(Constants.PLAN_ID, PLAN_ID);
        existingPlan.put(Constants.CREATED_BY, "differentUser");
        existingPlan.put(Constants.STATUS, Constants.LIVE);
        mockExistingPlan(existingPlan);

        Map<String, Object> request = new HashMap<>();
        request.put(Constants.ID, PLAN_ID);
        ApiResponse response = cbPlanService.retireCbPlan(apiRequest(request), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }


    @Test
    void testReadCbPlanFailsWhenPlanIdNull() {
        ApiResponse response = cbPlanService.readCbPlan(null, ORG_ID, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testCreateCbPlanWithCCAOrganization() {
        mockValidUserAndOrg(true);
        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap())).thenReturn(cassandraInsertSuccess());

        ApiResponse response = cbPlanService.createCbPlan(validPlanRequest(), ORG_ID, TOKEN);
        assertEquals(HttpStatus.CREATED, response.getResponseCode());
    }

    @Test
    void testUpdateCbPlanFailsOnCassandraError() {
        mockValidUserAndOrg(false);
        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put(Constants.PLAN_ID, PLAN_ID);
        existingPlan.put(Constants.CREATED_BY, USER_ID);
        existingPlan.put(Constants.STATUS, Constants.DRAFT);
        existingPlan.put(Constants.PLAN_YEAR, PLAN_YEAR);
        mockExistingPlan(existingPlan);

        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.FAILED));

        ApiResponse response = cbPlanService.updateCbPlan(validPlanRequestWithId(), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testRetireCbPlanFailsOnCassandraError() {
        mockValidUserAndOrg(false);
        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put(Constants.PLAN_ID, PLAN_ID);
        existingPlan.put(Constants.CREATED_BY, USER_ID);
        existingPlan.put(Constants.STATUS, Constants.LIVE);
        existingPlan.put(Constants.PLAN_YEAR, PLAN_YEAR);
        mockExistingPlan(existingPlan);

        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.FAILED));

        Map<String, Object> request = new HashMap<>();
        request.put(Constants.ID, PLAN_ID);
        ApiResponse response = cbPlanService.retireCbPlan(apiRequest(request), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }


    @Test
    void testGetCBPlanDictionaryUsesCache() throws Exception {
        mockAuthenticatedUser();
        CbPlanDictionaryCacheEntry cacheEntry = new CbPlanDictionaryCacheEntry(
                new LinkedHashMap<>(), new LinkedHashMap<>(), 0, 0);
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(MAPPER.writeValueAsString(cacheEntry));

        Map<String, Object> request = new HashMap<>();
        request.put("planYear", PLAN_YEAR);
        ApiResponse response = cbPlanService.getCBPlanDictionaryForUser(apiRequest(request), TOKEN);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    @Test
    void testCreateCbPlanFailsWhenInsertReturnsNull() {
        mockValidUserAndOrg(false);
        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap())).thenReturn(null);

        ApiResponse response = cbPlanService.createCbPlan(validPlanRequest(), ORG_ID, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testCreateCbPlanWithCCAValidationFailure() {
        mockAuthenticatedUser();
        when(userAndOrgService.readOrgFromDB(eq(ORG_ID), anyList())).thenReturn(Collections.emptyMap());

        ApiResponse response = cbPlanService.createCbPlan(apiRequest(new HashMap<>()), ORG_ID, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testCreateCbPlanWithException() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any()))
                .thenThrow(new RuntimeException("Test exception"));

        ApiResponse response = cbPlanService.createCbPlan(apiRequest(new HashMap<>()), ORG_ID, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }



    @Test
    void testUpdateCbPlanUnauthorized() {
        mockAuthenticatedUser();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, PLAN_ID);
        ApiRequest request = apiRequest(requestMap);

        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put(Constants.CREATED_BY, "otherUser");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(List.of(existingPlan));

        ApiResponse response = cbPlanService.updateCbPlan(request, ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testUpdateCbPlanWithException() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any()))
                .thenThrow(new RuntimeException("Test exception"));

        ApiResponse response = cbPlanService.updateCbPlan(apiRequest(new HashMap<>()), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }


    @Test
    void testPublishCbPlanWithNonExistentPlan() {
        mockAuthenticatedUser();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, PLAN_ID);
        ApiRequest request = apiRequest(requestMap);

        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = cbPlanService.publishCbPlan(request, ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testPublishCbPlanUnauthorized() {
        mockAuthenticatedUser();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, PLAN_ID);
        ApiRequest request = apiRequest(requestMap);

        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put(Constants.CREATED_BY, "otherUser");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(List.of(existingPlan));

        ApiResponse response = cbPlanService.publishCbPlan(request, ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testPublishCbPlanWithException() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any()))
                .thenThrow(new RuntimeException("Test exception"));

        ApiResponse response = cbPlanService.publishCbPlan(apiRequest(new HashMap<>()), ORG_ID, TOKEN, List.of());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }







}
