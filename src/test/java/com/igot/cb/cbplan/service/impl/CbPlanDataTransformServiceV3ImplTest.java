package com.igot.cb.cbplan.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanDataTransformServiceV3ImplTest {

    private static final String USER_ID = "user1";

    @Mock
    private CbExtServerProperties serverProperties;

    @InjectMocks
    private CbPlanDataTransformServiceV3Impl dataTransformService;

    private static ApiRequest apiRequest(Map<String, Object> requestMap) {
        ApiRequest request = new ApiRequest();
        request.setRequest(requestMap);
        return request;
    }

    @Test
    void testPrepareCbPlanForInsertPopulatesSystemFields() throws JsonProcessingException {
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.NAME, "planName");
        requestMap.put(Constants.CONTENT_LIST, List.of("content1"));
        requestMap.put(Constants.PLAN_YEAR, "2026-27");
        Map<String, Object> result = dataTransformService.prepareCbPlanForInsert(apiRequest(requestMap), USER_ID);
        assertNotNull(result.get(Constants.PLAN_ID));
        assertEquals(USER_ID, result.get(Constants.CREATED_BY));
        assertNotNull(result.get(Constants.CREATED_AT));
        assertEquals(Constants.DRAFT, result.get(Constants.STATUS));
        assertEquals("planName", result.get(Constants.NAME));
        assertEquals("2026-27", result.get(Constants.PLAN_YEAR));
        assertEquals(List.of("content1"), result.get(Constants.CONTENT_LIST));
    }

    @Test
    void testPrepareCbPlanForInsertDefaultsIsAparToFalse() throws JsonProcessingException {
        Map<String, Object> result = dataTransformService.prepareCbPlanForInsert(
                apiRequest(new HashMap<>()), USER_ID);
        assertEquals(false, result.get(Constants.IS_APAR));
    }

    @Test
    void testPrepareCbPlanForInsertOmitsPlanTypeWhenAbsent() throws JsonProcessingException {
        Map<String, Object> result = dataTransformService.prepareCbPlanForInsert(
                apiRequest(new HashMap<>()), USER_ID);
        assertFalse(result.containsKey(Constants.PLAN_TYPE));
    }

    @Test
    void testPrepareCbPlanForInsertIncludesPlanTypeWhenPresent() throws JsonProcessingException {
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.PLAN_TYPE, "AI CBP");
        Map<String, Object> result = dataTransformService.prepareCbPlanForInsert(apiRequest(requestMap), USER_ID);
        assertEquals("AI CBP", result.get(Constants.PLAN_TYPE));
    }

    @Test
    void testPrepareCbPlanForUpdateSetsAuditFields() throws JsonProcessingException {
        Map<String, Object> incomingRequest = new HashMap<>();
        incomingRequest.put(Constants.NAME, "updatedName");
        Map<String, Object> result = dataTransformService.prepareCbPlanForUpdate(incomingRequest, USER_ID);
        assertEquals(USER_ID, result.get(Constants.UPDATED_BY));
        assertNotNull(result.get(Constants.UPDATED_AT));
        assertEquals("updatedName", result.get(Constants.NAME));
    }

    @Test
    void testPrepareBasicPublishUpdate() {
        Map<String, Object> incomingRequest = new HashMap<>();
        incomingRequest.put(Constants.COMMENT, "publish comment");
        Map<String, Object> result = dataTransformService.prepareBasicPublishUpdate(incomingRequest, USER_ID);
        assertEquals(USER_ID, result.get(Constants.PUBLISHED_BY));
        assertEquals(USER_ID, result.get(Constants.UPDATED_BY));
        assertEquals("publish comment", result.get(Constants.COMMENT));
        assertNotNull(result.get(Constants.PUBLISHED_AT));
        assertNotNull(result.get(Constants.UPDATED_AT));
    }

    @Test
    void testPrepareArchiveUpdateWithComment() {
        Map<String, Object> result = dataTransformService.prepareArchiveUpdate("archive reason", USER_ID);
        assertEquals(Constants.CB_RETIRE, result.get(Constants.STATUS));
        assertEquals(USER_ID, result.get(Constants.UPDATED_BY));
        assertEquals("archive reason", result.get(Constants.COMMENT));
        assertNotNull(result.get(Constants.UPDATED_AT));
    }

    @Test
    void testPrepareArchiveUpdateOmitsNullComment() {
        Map<String, Object> result = dataTransformService.prepareArchiveUpdate(null, USER_ID);
        assertEquals(Constants.CB_RETIRE, result.get(Constants.STATUS));
        assertFalse(result.containsKey(Constants.COMMENT));
    }

    @Test
    void testParseEndDateReturnsNullForNull() {
        assertNull(dataTransformService.parseEndDate(null));
    }

    @Test
    void testParseEndDateFromDate() {
        Date date = new Date();
        assertEquals(date.toInstant(), dataTransformService.parseEndDate(date));
    }

    @Test
    void testParseEndDateFromInstant() {
        Instant instant = Instant.now();
        assertEquals(instant, dataTransformService.parseEndDate(instant));
    }

    @Test
    void testParseEndDateFromEpochMillis() {
        long epochMilli = 1700000000000L;
        assertEquals(Instant.ofEpochMilli(epochMilli), dataTransformService.parseEndDate(epochMilli));
    }

    @Test
    void testParseEndDateFromIsoString() {
        String isoString = "2026-08-12T10:15:30Z";
        assertEquals(Instant.parse(isoString), dataTransformService.parseEndDate(isoString));
    }

    @Test
    void testParseEndDateFromDateOnlyStringUsesEndOfDayKolkata() {
        Instant result = dataTransformService.parseEndDate("2026-08-12");
        assertEquals("2026-08-12T18:29:59Z", result.toString());
    }

    @Test
    void testParseEndDateThrowsForUnparseableString() {
        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> dataTransformService.parseEndDate("not-a-date"));
        assertTrue(exception.getMessage().contains("not-a-date"));
    }

    @Test
    void testParseEndDateReturnsNullForUnsupportedType() {
        assertNull(dataTransformService.parseEndDate(new Object()));
    }

    @Test
    void testBuildUpdatedPlanForLiveSuccess() {
        when(serverProperties.getCbPlanUpdateAllowedFields()).thenReturn(List.of(Constants.NAME));
        Map<String, Object> incomingRequest = new HashMap<>();
        incomingRequest.put(Constants.NAME, "updatedName");
        Set<String> rootOrgIds = Set.of("org1");
        Map<String, Object> result = dataTransformService.buildUpdatedPlanForLive(
                incomingRequest, new HashMap<>(), USER_ID, rootOrgIds, new ApiResponse());
        assertEquals("updatedName", result.get(Constants.NAME));
        assertEquals(USER_ID, result.get(Constants.UPDATED_BY));
        assertNotNull(result.get(Constants.UPDATED_AT));
        assertEquals(List.of("org1"), result.get(Constants.ROOT_ORG_IDS_IN_CONTEXT_DATA));
    }

    @Test
    void testBuildUpdatedPlanForLiveIgnoresFieldsNotInRequest() {
        when(serverProperties.getCbPlanUpdateAllowedFields())
                .thenReturn(List.of(Constants.NAME, Constants.COMMENT));
        Map<String, Object> incomingRequest = new HashMap<>();
        incomingRequest.put(Constants.NAME, "updatedName");
        Map<String, Object> result = dataTransformService.buildUpdatedPlanForLive(
                incomingRequest, new HashMap<>(), USER_ID, new HashSet<>(), new ApiResponse());
        assertEquals("updatedName", result.get(Constants.NAME));
        assertFalse(result.containsKey(Constants.COMMENT));
    }

    @Test
    void testBuildUpdatedPlanForLiveFailsWhenAllowedFieldIsNull() {
        when(serverProperties.getCbPlanUpdateAllowedFields()).thenReturn(List.of(Constants.NAME));
        Map<String, Object> incomingRequest = new HashMap<>();
        incomingRequest.put(Constants.NAME, null);
        ApiResponse response = new ApiResponse();
        Map<String, Object> result = dataTransformService.buildUpdatedPlanForLive(
                incomingRequest, new HashMap<>(), USER_ID, new HashSet<>(), response);
        assertTrue(result.isEmpty());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testBuildUpdatedPlanForLiveRejectsDowngradingIsApar() {
        when(serverProperties.getCbPlanUpdateAllowedFields()).thenReturn(List.of(Constants.IS_APAR));
        Map<String, Object> incomingRequest = new HashMap<>();
        incomingRequest.put(Constants.IS_APAR, false);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.IS_APAR, true);
        ApiResponse response = new ApiResponse();
        Map<String, Object> result = dataTransformService.buildUpdatedPlanForLive(
                incomingRequest, existingCbPlan, USER_ID, new HashSet<>(), response);
        assertTrue(result.isEmpty());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testBuildUpdatedPlanForLiveAllowsKeepingIsAparTrue() {
        when(serverProperties.getCbPlanUpdateAllowedFields()).thenReturn(List.of(Constants.IS_APAR));
        Map<String, Object> incomingRequest = new HashMap<>();
        incomingRequest.put(Constants.IS_APAR, true);
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.IS_APAR, true);
        Map<String, Object> result = dataTransformService.buildUpdatedPlanForLive(
                incomingRequest, existingCbPlan, USER_ID, new HashSet<>(), new ApiResponse());
        assertEquals(true, result.get(Constants.IS_APAR));
    }

    @Test
    void testBuildUpdatedPlanForLiveAllowsEnablingIsAparOnNonAparPlan() {
        when(serverProperties.getCbPlanUpdateAllowedFields()).thenReturn(List.of(Constants.IS_APAR));
        Map<String, Object> incomingRequest = new HashMap<>();
        incomingRequest.put(Constants.IS_APAR, true);
        Map<String, Object> result = dataTransformService.buildUpdatedPlanForLive(
                incomingRequest, new HashMap<>(), USER_ID, new HashSet<>(), new ApiResponse());
        assertEquals(true, result.get(Constants.IS_APAR));
    }

    @Test
    void testConstructor() {
        assertNotNull(new CbPlanDataTransformServiceV3Impl(serverProperties));
    }
}
