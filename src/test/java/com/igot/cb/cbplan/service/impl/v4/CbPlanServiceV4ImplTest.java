package com.igot.cb.cbplan.service.impl.v4;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.igot.cb.cache.CbPlanCacheMgrV3;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.cbplan.dto.CbPlanReadResponseDto;
import com.igot.cb.cbplan.service.CbPlanServiceV3;
import com.igot.cb.cbplan.service.impl.CbPlanContentLookupServiceV3Impl;
import com.igot.cb.cbplan.service.impl.CbPlanDataTransformServiceV3Impl;
import com.igot.cb.cbplan.service.impl.CbPlanOrgLookupServiceV3Impl;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.UserProfileUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanServiceV4ImplTest {

    private static final String TOKEN = "token";
    private static final String USER_ID = "user1";
    private static final String ORG_ID = "org1";
    private static final String PLAN_ID = "plan1";
    private static final String PLAN_YEAR = "2026-27";

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private CbExtServerProperties serverProperties;

    @Mock
    private CbPlanValidationServiceV4Impl validationService;

    @Mock
    private CbPlanDataTransformServiceV3Impl dataTransformService;

    @Mock
    private CbPlanContentLookupServiceV3Impl contentLookupService;

    @Mock
    private CbPlanElasticSearchServiceV4Impl elasticSearchService;

    @Mock
    private CbPlanOrgLookupServiceV3Impl orgLookupService;

    @Mock
    private CbPlanReadServiceV4Impl readService;

    @Mock
    private CbPlanSearchServiceV4Impl searchService;

    @Mock
    private CbPlanServiceV3 cbPlanServiceV3;

    @Mock
    private EsUtilService esUtilService;

    @Mock
    private AccessTokenValidator accessTokenValidator;

    @Mock
    private UserProfileUtil userProfileUtil;

    @Mock
    private RedisCacheMgr redisCacheMgr;

    @Mock
    private CbPlanCacheMgrV3 cbPlanCacheMgrV3;

    @InjectMocks
    private CbPlanServiceV4Impl cbPlanService;

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

    private void mockAuthSuccess() {
        // For createCbPlan - uses validation service
        lenient().when(validationService.validateAndExtractUserId(eq(TOKEN), any())).thenReturn(USER_ID);
        lenient().when(validationService.validateUserOrganization(eq(USER_ID), any())).thenReturn(ORG_ID);
        lenient().when(validationService.validateOrgCCA(eq(ORG_ID), any())).thenReturn(false);

        // For updateCbPlan and publishCbPlan - extract from token
        lenient().when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TOKEN), any())).thenReturn(USER_ID);
        Map<String, String> userProfile = new HashMap<>();
        userProfile.put(Constants.USER_ROOT_ORG_ID, ORG_ID);
        userProfile.put(Constants.ROLES, "ADMIN,USER");
        lenient().when(userProfileUtil.buildUserProfile(eq(USER_ID), any())).thenReturn(userProfile);
    }

    private void mockExistingPlan(Map<String, Object> plan) {
        when(cassandraOperation.getRecordsByProperties(anyString(), eq(Constants.TABLE_CB_PLAN_V3), anyMap(), any(), any()))
                .thenReturn(List.of(plan));
    }

    private void mockNoExistingPlan() {
        when(cassandraOperation.getRecordsByProperties(anyString(), eq(Constants.TABLE_CB_PLAN_V3), anyMap(), any(), any()))
                .thenReturn(List.of());
    }

    /**
     * validationService is fully mocked in this class, so a boolean-returning stub alone
     * does not reproduce the response-mutating side effect the real CbPlanValidationServiceV4Impl
     * has on failure. This marks the response the way the real implementation would.
     */
    private static void markFailed(ApiResponse response, HttpStatus status) {
        response.getParams().setStatus(Constants.FAILED);
        response.setResponseCode(status);
    }

    // ---------------- createCbPlan ----------------

    @Test
    void createCbPlan_success_returnsCreatedWithId() throws JsonProcessingException {
        mockAuthSuccess();
        when(validationService.validateRequest(any(), anyBoolean(), anyString(), any())).thenReturn(true);
        Map<String, Object> planData = new HashMap<>();
        planData.put(Constants.PLAN_ID, PLAN_ID);
        when(dataTransformService.prepareCbPlanForInsert(any(), eq(USER_ID))).thenReturn(planData);
        Map<String, Object> insertResult = new HashMap<>();
        insertResult.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap(),
                any(BooleanSupplier.class), any(Runnable.class))).thenReturn(insertResult);

        ApiResponse response = cbPlanService.createCbPlan(apiRequest(new HashMap<>()), TOKEN);

        assertEquals(HttpStatus.CREATED, response.getResponseCode());
        assertEquals(Constants.CREATED, response.getResult().get(Constants.STATUS));
        assertEquals(PLAN_ID, response.getResult().get(Constants.ID));
        verify(contentLookupService).updateContentLookup(eq(PLAN_ID), anyMap());
        verify(elasticSearchService, never()).indexToElasticSearch(any(), any());
    }

    @Test
    void createCbPlan_tokenInvalid_failsWithoutInsert() {
        when(validationService.validateAndExtractUserId(anyString(), any())).thenReturn("");

        ApiResponse response = cbPlanService.createCbPlan(apiRequest(new HashMap<>()), TOKEN);

        assertNotNull(response);
        verify(cassandraOperation, never()).insertRecord(anyString(), anyString(), anyMap(),
                any(BooleanSupplier.class), any(Runnable.class));
    }

    @Test
    void createCbPlan_userOrgNotFound_returns400() {
        when(validationService.validateAndExtractUserId(anyString(), any())).thenReturn(USER_ID);
        when(validationService.validateUserOrganization(anyString(), any())).thenReturn("");

        ApiResponse response = cbPlanService.createCbPlan(apiRequest(new HashMap<>()), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        verify(cassandraOperation, never()).insertRecord(anyString(), anyString(), anyMap(),
                any(BooleanSupplier.class), any(Runnable.class));
    }

    @Test
    void createCbPlan_validationFails_returns400WithoutInsert() {
        mockAuthSuccess();
        when(validationService.validateRequest(any(), anyBoolean(), anyString(), any())).thenReturn(false);

        ApiResponse response = cbPlanService.createCbPlan(apiRequest(new HashMap<>()), TOKEN);

        verify(cassandraOperation, never()).insertRecord(anyString(), anyString(), anyMap(),
                any(BooleanSupplier.class), any(Runnable.class));
        assertNotEquals(HttpStatus.CREATED, response.getResponseCode());
    }

    @Test
    void createCbPlan_insertFails_returns500() throws JsonProcessingException {
        mockAuthSuccess();
        when(validationService.validateRequest(any(), anyBoolean(), anyString(), any())).thenReturn(true);
        Map<String, Object> planData = new HashMap<>();
        planData.put(Constants.PLAN_ID, PLAN_ID);
        when(dataTransformService.prepareCbPlanForInsert(any(), eq(USER_ID))).thenReturn(planData);
        Map<String, Object> insertFailedResult = new HashMap<>();
        insertFailedResult.put(Constants.RESPONSE, Constants.FAILED);
        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap(),
                any(BooleanSupplier.class), any(Runnable.class))).thenReturn(insertFailedResult);

        ApiResponse response = cbPlanService.createCbPlan(apiRequest(new HashMap<>()), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void createCbPlan_prepareThrowsJsonProcessingException_returns500() throws JsonProcessingException {
        mockAuthSuccess();
        when(validationService.validateRequest(any(), anyBoolean(), anyString(), any())).thenReturn(true);
        when(dataTransformService.prepareCbPlanForInsert(any(), eq(USER_ID)))
                .thenThrow(new JsonProcessingException("boom") {
                });

        ApiResponse response = cbPlanService.createCbPlan(apiRequest(new HashMap<>()), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void createCbPlan_unexpectedException_returns500() {
        when(validationService.validateAndExtractUserId(anyString(), any())).thenThrow(new RuntimeException("boom"));

        ApiResponse response = cbPlanService.createCbPlan(apiRequest(new HashMap<>()), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    // ---------------- updateCbPlan ----------------

    @Test
    void updateCbPlan_planIdMissing_returns400() {
        mockAuthSuccess();
        when(validationService.validatePlanIdExists(any(), any())).thenAnswer(invocation -> {
            markFailed(invocation.getArgument(1), HttpStatus.BAD_REQUEST);
            return false;
        });

        ApiResponse response = cbPlanService.updateCbPlan(apiRequest(new HashMap<>()), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void updateCbPlan_planNotFound_returns400() {
        mockAuthSuccess();
        when(validationService.validatePlanIdExists(any(), any())).thenReturn(true);
        mockNoExistingPlan();

        ApiResponse response = cbPlanService.updateCbPlan(requestWithPlanId(), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void updateCbPlan_unauthorized_returns403() {
        mockAuthSuccess();
        when(validationService.validatePlanIdExists(any(), any())).thenReturn(true);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, "otherUser");
        existingCbPlan.put(Constants.STATUS, Constants.DRAFT);
        mockExistingPlan(existingCbPlan);
        when(validationService.isUnauthorizedToUpdate(eq(USER_ID), anyMap(), any(), any())).thenAnswer(invocation -> {
            markFailed(invocation.getArgument(3), HttpStatus.FORBIDDEN);
            return true;
        });

        ApiResponse response = cbPlanService.updateCbPlan(requestWithPlanId(), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.FORBIDDEN, response.getResponseCode());
    }

    @Test
    void updateCbPlan_draftSuccess_appliesUpdateImmediately() throws JsonProcessingException {
        mockAuthSuccess();
        when(validationService.validatePlanIdExists(any(), any())).thenReturn(true);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.DRAFT);
        mockExistingPlan(existingCbPlan);
        when(validationService.isUnauthorizedToUpdate(eq(USER_ID), anyMap(), any(), any())).thenReturn(false);
        when(validationService.validateRequest(any(), anyBoolean(), anyString(), any())).thenReturn(true);
        Map<String, Object> updatedRequest = new HashMap<>();
        when(dataTransformService.prepareCbPlanForUpdate(anyMap(), eq(USER_ID))).thenReturn(updatedRequest);
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap(),
                any(Supplier.class), any(Runnable.class)))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));
        when(readService.extractIdentifiers(any())).thenReturn(List.of());

        ApiResponse response = cbPlanService.updateCbPlan(requestWithPlanId(), TOKEN);

        Map<String, Object> expectedUpdatedIds = new HashMap<>(updatedRequest);
        expectedUpdatedIds.put(Constants.CONTENT_LIST, List.of());
        Map<String, Object> expectedExistingIds = new HashMap<>(existingCbPlan);
        expectedExistingIds.put(Constants.CONTENT_LIST, List.of());
        assertEquals(Constants.UPDATED, response.getResult().get(Constants.STATUS));
        verify(contentLookupService).updateContentLookupForModifiedPlan(PLAN_ID, expectedUpdatedIds, expectedExistingIds);
        verify(elasticSearchService, never()).updateElasticSearchForPlan(any(), any());
    }

    @Test
    void updateCbPlan_draftValidationFails_returns400WithoutCassandraUpdate() {
        mockAuthSuccess();
        when(validationService.validatePlanIdExists(any(), any())).thenReturn(true);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.DRAFT);
        mockExistingPlan(existingCbPlan);
        when(validationService.isUnauthorizedToUpdate(eq(USER_ID), anyMap(), any(), any())).thenReturn(false);
        when(validationService.validateRequest(any(), anyBoolean(), anyString(), any())).thenReturn(false);

        cbPlanService.updateCbPlan(requestWithPlanId(), TOKEN);

        verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap(),
                any(Supplier.class), any(Runnable.class));
    }

    @Test
    void updateCbPlan_draftCassandraUpdateFails_returns400() throws JsonProcessingException {
        mockAuthSuccess();
        when(validationService.validatePlanIdExists(any(), any())).thenReturn(true);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.DRAFT);
        mockExistingPlan(existingCbPlan);
        when(validationService.isUnauthorizedToUpdate(eq(USER_ID), anyMap(), any(), any())).thenReturn(false);
        when(validationService.validateRequest(any(), anyBoolean(), anyString(), any())).thenReturn(true);
        when(dataTransformService.prepareCbPlanForUpdate(anyMap(), eq(USER_ID))).thenReturn(new HashMap<>());
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap(),
                any(Supplier.class), any(Runnable.class)))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.FAILED));

        ApiResponse response = cbPlanService.updateCbPlan(requestWithPlanId(), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void updateCbPlan_liveWithoutContentList_savesAsDraftAndSkipsNormalization() {
        mockAuthSuccess();
        when(validationService.validatePlanIdExists(any(), any())).thenReturn(true);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.LIVE);
        existingCbPlan.put(Constants.PLAN_ID, PLAN_ID);
        mockExistingPlan(existingCbPlan);
        when(validationService.isUnauthorizedToUpdate(eq(USER_ID), anyMap(), any(), any())).thenReturn(false);
        when(validationService.validateContextDataForLivePlanV4(anyMap(), anyBoolean(), anyString(), any(), any(), any()))
                .thenReturn(true);
        when(dataTransformService.buildUpdatedPlanForLive(anyMap(), anyMap(), eq(USER_ID), any(), any()))
                .thenReturn(Map.of(Constants.NAME, "updatedName"));
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, PLAN_ID);
        requestMap.put(Constants.NAME, "updatedName");

        ApiResponse response = cbPlanService.updateCbPlan(apiRequest(requestMap), TOKEN);

        assertEquals(Constants.UPDATED, response.getResult().get(Constants.STATUS));
        assertNotNull(response.getResult().get(Constants.MESSAGE));
        verify(validationService, never()).validateAndNormalizeContentList(any());
    }

    @Test
    void updateCbPlan_liveWithValidContentList_normalizesBeforeStagingDraft() {
        mockAuthSuccess();
        when(validationService.validatePlanIdExists(any(), any())).thenReturn(true);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.LIVE);
        existingCbPlan.put(Constants.PLAN_ID, PLAN_ID);
        mockExistingPlan(existingCbPlan);
        when(validationService.isUnauthorizedToUpdate(eq(USER_ID), anyMap(), any(), any())).thenReturn(false);
        when(validationService.validateAndNormalizeContentList(anyMap())).thenReturn(List.of());
        when(validationService.validateContextDataForLivePlanV4(anyMap(), anyBoolean(), anyString(), any(), any(), any()))
                .thenReturn(true);
        when(dataTransformService.buildUpdatedPlanForLive(anyMap(), anyMap(), eq(USER_ID), any(), any()))
                .thenReturn(Map.of(Constants.CONTENT_LIST, List.of("do_1")));
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.ID, "do_1");
        content.put(Constants.MANDATORY, "true");
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, PLAN_ID);
        requestMap.put(Constants.CONTENT_LIST, List.of(content));

        ApiResponse response = cbPlanService.updateCbPlan(apiRequest(requestMap), TOKEN);

        assertEquals(Constants.UPDATED, response.getResult().get(Constants.STATUS));
        verify(validationService).validateAndNormalizeContentList(requestMap);
    }

    @Test
    void updateCbPlan_liveWithInvalidContentList_returns400WithoutBuildingDraft() {
        // Regression test: contentList submitted as {id, mandatory} objects must be
        // rejected here rather than silently persisted unnormalized into draftData,
        // which previously broke the next read or republish.
        mockAuthSuccess();
        when(validationService.validatePlanIdExists(any(), any())).thenReturn(true);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.LIVE);
        mockExistingPlan(existingCbPlan);
        when(validationService.isUnauthorizedToUpdate(eq(USER_ID), anyMap(), any(), any())).thenReturn(false);
        when(validationService.validateAndNormalizeContentList(anyMap())).thenReturn(List.of("mandatory is required"));
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, PLAN_ID);
        requestMap.put(Constants.CONTENT_LIST, List.of(new HashMap<>()));

        ApiResponse response = cbPlanService.updateCbPlan(apiRequest(requestMap), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        verify(dataTransformService, never()).buildUpdatedPlanForLive(anyMap(), anyMap(), anyString(), any(), any());
        verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap());
    }

    @Test
    void updateCbPlan_liveContextDataValidationFails_returns400() {
        mockAuthSuccess();
        when(validationService.validatePlanIdExists(any(), any())).thenReturn(true);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.LIVE);
        mockExistingPlan(existingCbPlan);
        when(validationService.isUnauthorizedToUpdate(eq(USER_ID), anyMap(), any(), any())).thenReturn(false);
        when(validationService.validateContextDataForLivePlanV4(anyMap(), anyBoolean(), anyString(), any(), any(), any()))
                .thenAnswer(invocation -> {
                    markFailed(invocation.getArgument(5), HttpStatus.BAD_REQUEST);
                    return false;
                });

        ApiResponse response = cbPlanService.updateCbPlan(requestWithPlanId(), TOKEN);

        verify(dataTransformService, never()).buildUpdatedPlanForLive(anyMap(), anyMap(), anyString(), any(), any());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void updateCbPlan_unexpectedException_returns500() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenThrow(new RuntimeException("boom"));

        ApiResponse response = cbPlanService.updateCbPlan(requestWithPlanId(), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    // ---------------- publishCbPlan ----------------

    @Test
    void publishCbPlan_planIdMissing_returns400() {
        mockAuthSuccess();
        when(validationService.validateAndExtractPlanId(anyMap(), any())).thenAnswer(invocation -> {
            markFailed(invocation.getArgument(1), HttpStatus.BAD_REQUEST);
            return "";
        });

        ApiResponse response = cbPlanService.publishCbPlan(apiRequest(new HashMap<>()), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void publishCbPlan_planNotFound_returns400() {
        mockAuthSuccess();
        when(validationService.validateAndExtractPlanId(anyMap(), any())).thenReturn(PLAN_ID);
        mockNoExistingPlan();

        ApiResponse response = cbPlanService.publishCbPlan(requestWithPlanId(), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void publishCbPlan_unauthorized_returns403() {
        mockAuthSuccess();
        when(validationService.validateAndExtractPlanId(anyMap(), any())).thenReturn(PLAN_ID);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, "otherUser");
        mockExistingPlan(existingCbPlan);
        when(validationService.isUnauthorizedToUpdate(eq(USER_ID), anyMap(), any(), any())).thenAnswer(invocation -> {
            markFailed(invocation.getArgument(3), HttpStatus.FORBIDDEN);
            return true;
        });

        ApiResponse response = cbPlanService.publishCbPlan(requestWithPlanId(), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.FORBIDDEN, response.getResponseCode());
    }

    @Test
    void publishCbPlan_invalidStatus_returns400() {
        mockAuthSuccess();
        when(validationService.validateAndExtractPlanId(anyMap(), any())).thenReturn(PLAN_ID);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.CB_RETIRE);
        mockExistingPlan(existingCbPlan);
        when(validationService.isUnauthorizedToUpdate(eq(USER_ID), anyMap(), any(), any())).thenReturn(false);

        ApiResponse response = cbPlanService.publishCbPlan(requestWithPlanId(), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void publishCbPlan_draftContextDataValidationFails_returns400() {
        mockAuthSuccess();
        when(validationService.validateAndExtractPlanId(anyMap(), any())).thenReturn(PLAN_ID);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.DRAFT);
        mockExistingPlan(existingCbPlan);
        when(validationService.isUnauthorizedToUpdate(eq(USER_ID), anyMap(), any(), any())).thenReturn(false);
        when(validationService.validateContextDataForLivePlanV4(anyMap(), anyBoolean(), anyString(), any(), any(), any()))
                .thenAnswer(invocation -> {
                    markFailed(invocation.getArgument(5), HttpStatus.BAD_REQUEST);
                    return false;
                });

        ApiResponse response = cbPlanService.publishCbPlan(requestWithPlanId(), TOKEN);

        verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap(), any(), any());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void publishCbPlan_draftPublishSuccess_updatesOrgLookupAndReturnsSuccess() {
        mockAuthSuccess();
        when(validationService.validateAndExtractPlanId(anyMap(), any())).thenReturn(PLAN_ID);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.DRAFT);
        existingCbPlan.put(Constants.PLAN_YEAR, PLAN_YEAR);
        existingCbPlan.put(Constants.ORG_SCOPE, Constants.SINGLE);
        mockExistingPlan(existingCbPlan);
        when(validationService.isUnauthorizedToUpdate(eq(USER_ID), anyMap(), any(), any())).thenReturn(false);
        when(validationService.validateContextDataForLivePlanV4(anyMap(), anyBoolean(), anyString(), any(), any(), any()))
                .thenAnswer(invocation -> {
                    java.util.Set<String> rootOrgIdsOut = invocation.getArgument(3);
                    rootOrgIdsOut.add(ORG_ID);
                    return true;
                });
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap(), any(), any()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));
        when(orgLookupService.upsertCustomOrgLookup(eq(PLAN_ID), eq(PLAN_YEAR), any(), any(), eq(true)))
                .thenReturn(successApiResponse());

        ApiResponse response = cbPlanService.publishCbPlan(requestWithPlanId(), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
        verify(orgLookupService).upsertCustomOrgLookup(eq(PLAN_ID), eq(PLAN_YEAR), any(), any(), eq(true));
    }

    private static ApiResponse successApiResponse() {
        ApiResponse response = new ApiResponse();
        response.put(Constants.RESPONSE, Constants.SUCCESS);
        return response;
    }

    @Test
    void publishCbPlan_cassandraTransactionFails_returns500() {
        mockAuthSuccess();
        when(validationService.validateAndExtractPlanId(anyMap(), any())).thenReturn(PLAN_ID);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        existingCbPlan.put(Constants.STATUS, Constants.DRAFT);
        existingCbPlan.put(Constants.PLAN_YEAR, PLAN_YEAR);
        mockExistingPlan(existingCbPlan);
        when(validationService.isUnauthorizedToUpdate(eq(USER_ID), anyMap(), any(), any())).thenReturn(false);
        when(validationService.validateContextDataForLivePlanV4(anyMap(), anyBoolean(), anyString(), any(), any(), any()))
                .thenReturn(true);
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap(), any(), any()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.FAILED, Constants.ERROR_MESSAGE, "commit failed"));

        ApiResponse response = cbPlanService.publishCbPlan(requestWithPlanId(), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        verify(orgLookupService, never()).upsertCustomOrgLookup(anyString(), anyString(), any(), any(), anyBoolean());
    }

    @Test
    void publishCbPlan_unexpectedException_returns500() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenThrow(new RuntimeException("boom"));

        ApiResponse response = cbPlanService.publishCbPlan(requestWithPlanId(), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    // ---------------- readCbPlan ----------------

    @Test
    void readCbPlan_idMissing_returns400() {
        ApiResponse response = cbPlanService.readCbPlan("", TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void readCbPlan_planNotFound_returns400() {
        mockNoExistingPlan();

        ApiResponse response = cbPlanService.readCbPlan(PLAN_ID, TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertTrue(response.getParams().getErr().contains(PLAN_ID));
    }

    @Test
    void readCbPlan_livePlan_delegatesToReadServiceAndReturnsContent() throws JsonProcessingException {
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.STATUS, Constants.LIVE);
        mockExistingPlan(plan);
        CbPlanReadResponseDto dto = CbPlanReadResponseDto.builder().id(PLAN_ID).build();
        when(readService.buildEnrichedPlanData(plan, PLAN_ID)).thenReturn(dto);

        ApiResponse response = cbPlanService.readCbPlan(PLAN_ID, TOKEN);

        assertEquals(dto, response.getResult().get(Constants.CONTENT));
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void readCbPlan_jsonProcessingException_returns500() throws JsonProcessingException {
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.STATUS, Constants.LIVE);
        mockExistingPlan(plan);
        when(readService.buildEnrichedPlanData(anyMap(), eq(PLAN_ID))).thenThrow(new JsonProcessingException("boom") {
        });

        ApiResponse response = cbPlanService.readCbPlan(PLAN_ID, TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void readCbPlan_unexpectedException_returns500() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenThrow(new RuntimeException("db down"));

        ApiResponse response = cbPlanService.readCbPlan(PLAN_ID, TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void readCbPlan_draftPlan_returnsErrorMessage() {
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.STATUS, Constants.DRAFT);
        mockExistingPlan(plan);

        ApiResponse response = cbPlanService.readCbPlan(PLAN_ID, TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ERR_DRAFT_PLAN_NOT_ACCESSIBLE, response.getParams().getErr());
    }

    @Test
    void readCbPlanAdmin_draftPlan_delegatesToReadServiceAndReturnsContent() throws JsonProcessingException {
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.STATUS, Constants.DRAFT);
        mockExistingPlan(plan);
        CbPlanReadResponseDto dto = CbPlanReadResponseDto.builder().id(PLAN_ID).build();
        when(readService.buildEnrichedPlanData(plan, PLAN_ID)).thenReturn(dto);

        ApiResponse response = cbPlanService.readCbPlanAdmin(PLAN_ID, TOKEN);

        assertEquals(dto, response.getResult().get(Constants.CONTENT));
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void searchCbPlan_withValidRequest_delegatesToSearchService() {
        ApiRequest request = requestWithPlanId();
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TOKEN), any(ApiResponse.class))).thenReturn(USER_ID);
        Map<String, String> userProfile = new HashMap<>();
        userProfile.put(Constants.USER_ROOT_ORG_ID, ORG_ID);
        userProfile.put(Constants.ROLES, "MDO_LEADER");
        when(userProfileUtil.buildUserProfile(eq(USER_ID), any(ApiResponse.class))).thenReturn(userProfile);

        ApiResponse searchResponse = new ApiResponse();
        searchResponse.setResponseCode(HttpStatus.OK);
        when(searchService.searchCbPlan(request, ORG_ID, TOKEN)).thenReturn(searchResponse);

        ApiResponse response = cbPlanService.searchCbPlan(request, TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(searchService).searchCbPlan(request, ORG_ID, TOKEN);
    }

    @Test
    void searchCbPlan_missingOrgId_returnsError() {
        ApiRequest request = requestWithPlanId();
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TOKEN), any(ApiResponse.class))).thenReturn(USER_ID);
        Map<String, String> userProfile = new HashMap<>();
        userProfile.put(Constants.USER_ROOT_ORG_ID, null);
        when(userProfileUtil.buildUserProfile(eq(USER_ID), any(ApiResponse.class))).thenReturn(userProfile);

        ApiResponse response = cbPlanService.searchCbPlan(request, TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ERR_USER_ORG_NOT_FOUND, response.getParams().getErr());
        verify(searchService, never()).searchCbPlan(any(), anyString(), anyString());
    }

    @Test
    void retireCbPlan_withValidRequest_delegatesToV3Service() {
        ApiRequest request = requestWithPlanId();
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TOKEN), any(ApiResponse.class))).thenReturn(USER_ID);
        Map<String, String> userProfile = new HashMap<>();
        userProfile.put(Constants.USER_ROOT_ORG_ID, ORG_ID);
        userProfile.put(Constants.ROLES, "MDO_LEADER,USER");
        when(userProfileUtil.buildUserProfile(eq(USER_ID), any(ApiResponse.class))).thenReturn(userProfile);

        ApiResponse retireResponse = new ApiResponse();
        retireResponse.setResponseCode(HttpStatus.OK);
        when(cbPlanServiceV3.retireCbPlan(request, ORG_ID, TOKEN, List.of("MDO_LEADER", "USER")))
                .thenReturn(retireResponse);

        ApiResponse response = cbPlanService.retireCbPlan(request, TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(cbPlanServiceV3).retireCbPlan(request, ORG_ID, TOKEN, List.of("MDO_LEADER", "USER"));
    }

    @Test
    void retireCbPlan_withNoRoles_delegatesWithEmptyRolesList() {
        ApiRequest request = requestWithPlanId();
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TOKEN), any(ApiResponse.class))).thenReturn(USER_ID);
        Map<String, String> userProfile = new HashMap<>();
        userProfile.put(Constants.USER_ROOT_ORG_ID, ORG_ID);
        userProfile.put(Constants.ROLES, null);
        when(userProfileUtil.buildUserProfile(eq(USER_ID), any(ApiResponse.class))).thenReturn(userProfile);

        ApiResponse retireResponse = new ApiResponse();
        retireResponse.setResponseCode(HttpStatus.OK);
        when(cbPlanServiceV3.retireCbPlan(request, ORG_ID, TOKEN, List.of()))
                .thenReturn(retireResponse);

        ApiResponse response = cbPlanService.retireCbPlan(request, TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(cbPlanServiceV3).retireCbPlan(request, ORG_ID, TOKEN, List.of());
    }

    @Test
    void retireCbPlan_missingOrgId_returnsError() {
        ApiRequest request = requestWithPlanId();
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TOKEN), any(ApiResponse.class))).thenReturn(USER_ID);
        Map<String, String> userProfile = new HashMap<>();
        userProfile.put(Constants.USER_ROOT_ORG_ID, "");
        when(userProfileUtil.buildUserProfile(eq(USER_ID), any(ApiResponse.class))).thenReturn(userProfile);

        ApiResponse response = cbPlanService.retireCbPlan(request, TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ERR_USER_ORG_NOT_FOUND, response.getParams().getErr());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        verify(cbPlanServiceV3, never()).retireCbPlan(any(), anyString(), anyString(), any());
    }

    @Test
    void updateCbPlan_livePlanWithOnlyCaLinkedId_performsDirectUpdate() {
        mockAuthSuccess();
        when(validationService.validatePlanIdExists(any(), any())).thenReturn(true);
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, PLAN_ID);
        requestMap.put(Constants.CA_LINKED_ID, "ca-assessment-123");
        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put(Constants.STATUS, Constants.LIVE);
        existingPlan.put(Constants.CREATED_BY, USER_ID);
        mockExistingPlan(existingPlan);
        when(validationService.isUnauthorizedToUpdate(eq(USER_ID), anyMap(), any(), any())).thenReturn(false);
        when(validationService.validateUserOrganization(eq(USER_ID), any())).thenReturn(ORG_ID);
        when(validationService.validateOrgCCA(eq(ORG_ID), any())).thenReturn(false);
        when(cassandraOperation.updateRecord(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V3), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));
        ApiResponse response = cbPlanService.updateCbPlan(apiRequest(requestMap), TOKEN);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals("CB Plan caLinkedId updated successfully", response.getResult().get(Constants.RESPONSE));
        assertEquals(PLAN_ID, response.getResult().get(Constants.PLAN_ID));
        assertEquals("ca-assessment-123", response.getResult().get(Constants.CA_LINKED_ID));
        verify(elasticSearchService).updateElasticSearchForPlan(eq(PLAN_ID), anyMap());
    }

    @Test
    void updateCbPlan_draftPlanWithOnlyCaLinkedId_usesNormalDraftFlow() throws JsonProcessingException {
        mockAuthSuccess();
        when(validationService.validatePlanIdExists(any(), any())).thenReturn(true);
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, PLAN_ID);
        requestMap.put(Constants.CA_LINKED_ID, "ca-assessment-123");
        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put(Constants.STATUS, Constants.DRAFT);
        existingPlan.put(Constants.CREATED_BY, USER_ID);
        mockExistingPlan(existingPlan);
        when(validationService.isUnauthorizedToUpdate(eq(USER_ID), anyMap(), any(), any())).thenReturn(false);
        when(validationService.validateUserOrganization(eq(USER_ID), any())).thenReturn(ORG_ID);
        when(validationService.validateOrgCCA(eq(ORG_ID), any())).thenReturn(false);
        when(validationService.validateRequest(any(), anyBoolean(), anyString(), any())).thenReturn(true);
        when(dataTransformService.prepareCbPlanForUpdate(anyMap(), eq(USER_ID)))
                .thenReturn(new HashMap<>());
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap(),
                any(Supplier.class), any(Runnable.class)))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));

        ApiResponse response = cbPlanService.updateCbPlan(apiRequest(requestMap), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(dataTransformService).prepareCbPlanForUpdate(anyMap(), eq(USER_ID));
    }

    @Test
    void updateCaLinkedId_success_updatesCassandraEsAndInvalidatesCaches() {
        when(cassandraOperation.updateRecord(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V3), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));

        boolean result = cbPlanService.updateCaLinkedId(PLAN_ID, "ca-assessment-123", Constants.SYSTEM_USER);

        assertTrue(result);
        verify(cassandraOperation).updateRecord(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_CB_PLAN_V3),
                argThat(m -> "ca-assessment-123".equals(m.get(Constants.CA_LINKED_ID_DB))
                        && Constants.SYSTEM_USER.equals(m.get(Constants.UPDATED_BY))),
                eq(Map.of(Constants.PLAN_ID, PLAN_ID)));
        verify(elasticSearchService).updateElasticSearchForPlan(eq(PLAN_ID), anyMap());
        verify(cbPlanCacheMgrV3).invalidatePlan(PLAN_ID);
        verify(redisCacheMgr).deleteKeysByPatternAsync(Constants.CB_PLAN_V4_REDIS_KEY_PREFIX + "*");
    }

    @Test
    void updateCaLinkedId_nullValue_clearsLink() {
        when(cassandraOperation.updateRecord(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V3), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));

        boolean result = cbPlanService.updateCaLinkedId(PLAN_ID, null, Constants.SYSTEM_USER);

        assertTrue(result);
        verify(cassandraOperation).updateRecord(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_CB_PLAN_V3),
                argThat(m -> m.containsKey(Constants.CA_LINKED_ID_DB) && m.get(Constants.CA_LINKED_ID_DB) == null),
                anyMap());
        verify(elasticSearchService).updateElasticSearchForPlan(eq(PLAN_ID),
                argThat(m -> m.containsKey(Constants.CA_LINKED_ID_DB) && m.get(Constants.CA_LINKED_ID_DB) == null));
    }

    @Test
    void updateCaLinkedId_cassandraFailure_returnsFalseAndSkipsEsAndCaches() {
        when(cassandraOperation.updateRecord(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V3), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.FAILED));

        boolean result = cbPlanService.updateCaLinkedId(PLAN_ID, "ca-assessment-123", Constants.SYSTEM_USER);

        assertFalse(result);
        verify(elasticSearchService, never()).updateElasticSearchForPlan(anyString(), anyMap());
        verify(cbPlanCacheMgrV3, never()).invalidatePlan(anyString());
        verify(redisCacheMgr, never()).deleteKeysByPatternAsync(anyString());
    }

    @Test
    void updateCbPlan_directCaLinkedIdUpdateFails_returns500() {
        mockAuthSuccess();
        when(validationService.validatePlanIdExists(any(), any())).thenReturn(true);
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, PLAN_ID);
        requestMap.put(Constants.CA_LINKED_ID, "ca-assessment-123");
        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put(Constants.STATUS, Constants.LIVE);
        existingPlan.put(Constants.CREATED_BY, USER_ID);
        mockExistingPlan(existingPlan);
        when(validationService.isUnauthorizedToUpdate(eq(USER_ID), anyMap(), any(), any())).thenReturn(false);
        when(validationService.validateUserOrganization(eq(USER_ID), any())).thenReturn(ORG_ID);
        when(validationService.validateOrgCCA(eq(ORG_ID), any())).thenReturn(false);
        when(cassandraOperation.updateRecord(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V3), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.FAILED));
        ApiResponse response = cbPlanService.updateCbPlan(apiRequest(requestMap), TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("Failed to update caLinkedId", response.getParams().getErr());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void prepareDataForElasticsearch_withJsonStrings_deserializesToObjects() throws Exception {
        Map<String, Object> planData = new HashMap<>();
        planData.put(Constants.PLAN_ID, PLAN_ID);
        planData.put(Constants.NAME, "Test Plan");
        planData.put(Constants.CONTENT_LIST, List.of(
                "{\"identifier\":\"do_114467322178428928111\",\"mandatory\":true}",
                "{\"identifier\":\"do_114378386987417600180\",\"mandatory\":false}"
        ));

        java.lang.reflect.Method method = CbPlanServiceV4Impl.class.getDeclaredMethod(
                "prepareDataForElasticsearch", Map.class);
        method.setAccessible(true);
        Map<String, Object> result = (Map<String, Object>) method.invoke(cbPlanService, planData);

        assertNotNull(result);
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) result.get(Constants.CONTENT_LIST);
        assertNotNull(contentList);
        assertEquals(2, contentList.size());
        assertEquals("do_114467322178428928111", contentList.get(0).get(Constants.IDENTIFIER));
        assertEquals(true, contentList.get(0).get(Constants.MANDATORY));
        assertEquals("do_114378386987417600180", contentList.get(1).get(Constants.IDENTIFIER));
        assertEquals(false, contentList.get(1).get(Constants.MANDATORY));
    }

    @Test
    void prepareDataForElasticsearch_withObjects_keepsAsIs() throws Exception {
        Map<String, Object> content1 = new HashMap<>();
        content1.put(Constants.IDENTIFIER, "do_114467322178428928111");
        content1.put(Constants.MANDATORY, true);

        Map<String, Object> content2 = new HashMap<>();
        content2.put(Constants.IDENTIFIER, "do_114378386987417600180");
        content2.put(Constants.MANDATORY, false);

        Map<String, Object> planData = new HashMap<>();
        planData.put(Constants.PLAN_ID, PLAN_ID);
        planData.put(Constants.CONTENT_LIST, List.of(content1, content2));

        java.lang.reflect.Method method = CbPlanServiceV4Impl.class.getDeclaredMethod(
                "prepareDataForElasticsearch", Map.class);
        method.setAccessible(true);
        Map<String, Object> result = (Map<String, Object>) method.invoke(cbPlanService, planData);

        assertNotNull(result);
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) result.get(Constants.CONTENT_LIST);
        assertNotNull(contentList);
        assertEquals(2, contentList.size());
        assertEquals("do_114467322178428928111", contentList.get(0).get(Constants.IDENTIFIER));
        assertEquals(true, contentList.get(0).get(Constants.MANDATORY));
    }

    @Test
    void prepareDataForElasticsearch_withEmptyContentList_returnsEmpty() throws Exception {
        Map<String, Object> planData = new HashMap<>();
        planData.put(Constants.PLAN_ID, PLAN_ID);
        planData.put(Constants.CONTENT_LIST, List.of());

        java.lang.reflect.Method method = CbPlanServiceV4Impl.class.getDeclaredMethod(
                "prepareDataForElasticsearch", Map.class);
        method.setAccessible(true);
        Map<String, Object> result = (Map<String, Object>) method.invoke(cbPlanService, planData);

        assertNotNull(result);
        List<?> contentList = (List<?>) result.get(Constants.CONTENT_LIST);
        assertNotNull(contentList);
        assertTrue(contentList.isEmpty());
    }

    @Test
    void prepareDataForElasticsearch_withNullContentList_returnsNull() throws Exception {
        Map<String, Object> planData = new HashMap<>();
        planData.put(Constants.PLAN_ID, PLAN_ID);
        planData.put(Constants.CONTENT_LIST, null);

        java.lang.reflect.Method method = CbPlanServiceV4Impl.class.getDeclaredMethod(
                "prepareDataForElasticsearch", Map.class);
        method.setAccessible(true);
        Map<String, Object> result = (Map<String, Object>) method.invoke(cbPlanService, planData);

        assertNotNull(result);
        assertEquals(null, result.get(Constants.CONTENT_LIST));
    }

    @Test
    void prepareDataForElasticsearch_withNoContentList_returnsDataUnchanged() throws Exception {
        Map<String, Object> planData = new HashMap<>();
        planData.put(Constants.PLAN_ID, PLAN_ID);
        planData.put(Constants.NAME, "Test Plan");

        java.lang.reflect.Method method = CbPlanServiceV4Impl.class.getDeclaredMethod(
                "prepareDataForElasticsearch", Map.class);
        method.setAccessible(true);
        Map<String, Object> result = (Map<String, Object>) method.invoke(cbPlanService, planData);

        assertNotNull(result);
        assertEquals(PLAN_ID, result.get(Constants.PLAN_ID));
        assertEquals("Test Plan", result.get(Constants.NAME));
        assertEquals(null, result.get(Constants.CONTENT_LIST));
    }

    @Test
    void deserializeContentListFromJson_validJsonStrings_returnsObjects() throws Exception {
        List<String> jsonList = List.of(
                "{\"identifier\":\"do_114467322178428928111\",\"mandatory\":true}",
                "{\"identifier\":\"do_114378386987417600180\",\"mandatory\":false}"
        );

        java.lang.reflect.Method method = CbPlanServiceV4Impl.class.getDeclaredMethod(
                "deserializeContentListFromJson", List.class);
        method.setAccessible(true);
        List<Map<String, Object>> result = (List<Map<String, Object>>) method.invoke(cbPlanService, jsonList);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("do_114467322178428928111", result.get(0).get(Constants.IDENTIFIER));
        assertEquals(true, result.get(0).get(Constants.MANDATORY));
        assertEquals("do_114378386987417600180", result.get(1).get(Constants.IDENTIFIER));
        assertEquals(false, result.get(1).get(Constants.MANDATORY));
    }

    @Test
    void deserializeContentListFromJson_emptyList_returnsEmpty() throws Exception {
        java.lang.reflect.Method method = CbPlanServiceV4Impl.class.getDeclaredMethod(
                "deserializeContentListFromJson", List.class);
        method.setAccessible(true);
        List<Map<String, Object>> result = (List<Map<String, Object>>) method.invoke(cbPlanService, List.of());

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void deserializeContentListFromJson_nullList_returnsEmpty() throws Exception {
        java.lang.reflect.Method method = CbPlanServiceV4Impl.class.getDeclaredMethod(
                "deserializeContentListFromJson", List.class);
        method.setAccessible(true);
        List<Map<String, Object>> result = (List<Map<String, Object>>) method.invoke(cbPlanService,
                new Object[]{null});

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }
}
