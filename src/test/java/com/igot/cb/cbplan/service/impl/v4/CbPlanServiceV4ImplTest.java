package com.igot.cb.cbplan.service.impl.v4;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.cbplan.dto.CbPlanReadResponseDto;
import com.igot.cb.cbplan.service.impl.CbPlanContentLookupServiceV3Impl;
import com.igot.cb.cbplan.service.impl.CbPlanDataTransformServiceV3Impl;
import com.igot.cb.cbplan.service.impl.CbPlanElasticSearchServiceV3Impl;
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
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
    private CbPlanElasticSearchServiceV3Impl elasticSearchService;

    @Mock
    private CbPlanOrgLookupServiceV3Impl orgLookupService;

    @Mock
    private CbPlanReadServiceV4Impl readService;

    @Mock
    private EsUtilService esUtilService;

    @Mock
    private AccessTokenValidator accessTokenValidator;

    @Mock
    private UserProfileUtil userProfileUtil;

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
        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap())).thenReturn(successApiResponse());

        ApiResponse response = cbPlanService.createCbPlan(apiRequest(new HashMap<>()), TOKEN);

        assertEquals(HttpStatus.CREATED, response.getResponseCode());
        assertEquals(Constants.CREATED, response.getResult().get(Constants.STATUS));
        assertEquals(PLAN_ID, response.getResult().get(Constants.ID));
        verify(contentLookupService).updateContentLookup(eq(PLAN_ID), anyMap());
        verify(elasticSearchService).indexToElasticSearch(PLAN_ID, planData);
    }

    @Test
    void createCbPlan_tokenInvalid_failsWithoutInsert() {
        when(validationService.validateAndExtractUserId(anyString(), any())).thenReturn("");

        ApiResponse response = cbPlanService.createCbPlan(apiRequest(new HashMap<>()), TOKEN);

        assertNotNull(response);
        verify(cassandraOperation, never()).insertRecord(anyString(), anyString(), anyMap());
    }

    @Test
    void createCbPlan_userOrgNotFound_returns400() {
        when(validationService.validateAndExtractUserId(anyString(), any())).thenReturn(USER_ID);
        when(validationService.validateUserOrganization(anyString(), any())).thenReturn("");

        ApiResponse response = cbPlanService.createCbPlan(apiRequest(new HashMap<>()), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        verify(cassandraOperation, never()).insertRecord(anyString(), anyString(), anyMap());
    }

    @Test
    void createCbPlan_validationFails_returns400WithoutInsert() {
        mockAuthSuccess();
        when(validationService.validateRequest(any(), anyBoolean(), anyString(), any())).thenReturn(false);

        ApiResponse response = cbPlanService.createCbPlan(apiRequest(new HashMap<>()), TOKEN);

        verify(cassandraOperation, never()).insertRecord(anyString(), anyString(), anyMap());
        assertNotEquals(HttpStatus.CREATED, response.getResponseCode());
    }

    @Test
    void createCbPlan_insertFails_returns500() throws JsonProcessingException {
        mockAuthSuccess();
        when(validationService.validateRequest(any(), anyBoolean(), anyString(), any())).thenReturn(true);
        when(dataTransformService.prepareCbPlanForInsert(any(), eq(USER_ID))).thenReturn(new HashMap<>());
        ApiResponse insertFailed = new ApiResponse();
        insertFailed.put(Constants.RESPONSE, Constants.FAILED);
        insertFailed.getParams().setErr("insert error");
        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap())).thenReturn(insertFailed);

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
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));

        ApiResponse response = cbPlanService.updateCbPlan(requestWithPlanId(), TOKEN);

        assertEquals(Constants.UPDATED, response.getResult().get(Constants.STATUS));
        verify(contentLookupService).updateContentLookupForModifiedPlan(PLAN_ID, updatedRequest, existingCbPlan);
        verify(elasticSearchService).updateElasticSearchForPlan(PLAN_ID, updatedRequest);
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

        verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap());
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
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
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
}
