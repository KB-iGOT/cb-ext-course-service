package com.igot.cb.cbplan.service.impl;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanOrgLookupServiceV3ImplTest {

    private static final String PLAN_ID = "plan1";
    private static final String PLAN_YEAR = "2026-27";
    private static final String ORG_1 = "org1";
    private static final String ORG_2 = "org2";
    private static final String MINISTRY_ORG_001 = "ORG_001";
    private static final String MINISTRY_ORG_002 = "ORG_002";
    private static final String DB_ERROR_MSG = "db error";
    private static final String END_DATE_COLUMN = "enddate";
    private static final String PLAN_ID_COLUMN = "planid";
    private static final String MINISTRY_ID_COLUMN = "ministryorstateid";
    private static final String TEST_END_DATE_STR = "2026-12-31T18:29:59Z";
    private static final String INVALID_JSON = "not-valid-json";

    @Mock
    private CassandraOperation cassandraOperation;

    @InjectMocks
    private CbPlanOrgLookupServiceV3Impl orgLookupService;

    private static ApiResponse cassandraSuccess() {
        ApiResponse response = new ApiResponse();
        response.getParams().setStatus(Constants.SUCCESS);
        response.put(Constants.RESPONSE, Constants.SUCCESS);
        return response;
    }

    private static ApiResponse cassandraFailure() {
        ApiResponse response = new ApiResponse();
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr("insert failed");
        response.put(Constants.RESPONSE, Constants.FAILED);
        return response;
    }

    private static Map<String, Object> contextDataWithOrgs(String criteriaKey, List<String> orgIds) {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, criteriaKey);
        criteria.put(Constants.CRITERIA_VALUE, orgIds);
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, List.of(criteria));
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, List.of(userGroup));
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        return contextData;
    }

    @Test
    void testUpsertCustomOrgLookupSuccess() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());
        ApiResponse response = orgLookupService.upsertCustomOrgLookup(
                PLAN_ID, PLAN_YEAR, Set.of(ORG_1), null, true);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testUpsertCustomOrgLookupBuildsOneRowPerOrgWithEndDate() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());
        Instant endDate = Instant.parse(TEST_END_DATE_STR);
        orgLookupService.upsertCustomOrgLookup(PLAN_ID, PLAN_YEAR, Set.of(ORG_1, ORG_2), endDate, true);
        ArgumentCaptor<List<Map<String, Object>>> captor = ArgumentCaptor.forClass(List.class);
        verify(cassandraOperation).insertBulkRecord(
                eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_CB_PLAN_V3_LOOKUP_BY_ORG), captor.capture());
        List<Map<String, Object>> rows = captor.getValue();
        assertEquals(2, rows.size());
        assertEquals(PLAN_YEAR, rows.get(0).get("planyear"));
        assertEquals(PLAN_ID, rows.get(0).get(PLAN_ID_COLUMN));
        assertEquals(endDate, rows.get(0).get(END_DATE_COLUMN));
        assertEquals(true, rows.get(0).get("isactive"));
    }

    @Test
    void testUpsertCustomOrgLookupOmitsEndDateWhenNull() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());
        orgLookupService.upsertCustomOrgLookup(PLAN_ID, PLAN_YEAR, Set.of(ORG_1), null, false);
        ArgumentCaptor<List<Map<String, Object>>> captor = ArgumentCaptor.forClass(List.class);
        verify(cassandraOperation).insertBulkRecord(anyString(), anyString(), captor.capture());
        assertTrue(!captor.getValue().get(0).containsKey(END_DATE_COLUMN));
        assertEquals(false, captor.getValue().get(0).get("isactive"));
    }

    @Test
    void testUpsertCustomOrgLookupFailsForEmptyOrgList() {
        ApiResponse response = orgLookupService.upsertCustomOrgLookup(
                PLAN_ID, PLAN_YEAR, new HashSet<>(), null, true);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        verify(cassandraOperation, never()).insertBulkRecord(anyString(), anyString(), anyList());
    }

    @Test
    void testUpsertCustomOrgLookupPropagatesInsertFailure() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraFailure());
        ApiResponse response = orgLookupService.upsertCustomOrgLookup(
                PLAN_ID, PLAN_YEAR, Set.of(ORG_1), null, true);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testUpsertCustomOrgLookupHandlesException() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList()))
                .thenThrow(new RuntimeException(DB_ERROR_MSG));
        ApiResponse response = orgLookupService.upsertCustomOrgLookup(
                PLAN_ID, PLAN_YEAR, Set.of(ORG_1), null, true);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErr().contains(DB_ERROR_MSG));
    }

    @Test
    void testUpsertAllOrgLookupSuccess() {
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(cassandraSuccess());
        ApiResponse response = orgLookupService.upsertAllOrgLookup(PLAN_ID, PLAN_YEAR, null, true);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testUpsertAllOrgLookupHandlesException() {
        when(cassandraOperation.insertRecord(anyString(), anyString(), any()))
                .thenThrow(new RuntimeException(DB_ERROR_MSG));
        ApiResponse response = orgLookupService.upsertAllOrgLookup(PLAN_ID, PLAN_YEAR, null, true);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.FAILED, response.get(Constants.RESPONSE));
    }

    @Test
    void testHandleOrgLookupChangesNoopWhenExistingEmpty() {
        ApiResponse response = new ApiResponse();
        orgLookupService.handleOrgLookupChanges(PLAN_ID, PLAN_YEAR, new HashSet<>(), Set.of(ORG_1),
                Constants.CUSTOM, false, response);
        verify(cassandraOperation, never()).insertBulkRecord(anyString(), anyString(), anyList());
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testHandleOrgLookupChangesNoopWhenNewEmpty() {
        ApiResponse response = new ApiResponse();
        orgLookupService.handleOrgLookupChanges(PLAN_ID, PLAN_YEAR, Set.of(ORG_1), new HashSet<>(),
                Constants.CUSTOM, false, response);
        verify(cassandraOperation, never()).insertBulkRecord(anyString(), anyString(), anyList());
    }

    @Test
    void testHandleOrgLookupChangesDeactivatesRemovedOrgsForCustomScope() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());
        ApiResponse response = new ApiResponse();
        orgLookupService.handleOrgLookupChanges(PLAN_ID, PLAN_YEAR, Set.of(ORG_1, ORG_2), Set.of(ORG_1),
                Constants.CUSTOM, false, response);
        ArgumentCaptor<List<Map<String, Object>>> captor = ArgumentCaptor.forClass(List.class);
        verify(cassandraOperation).insertBulkRecord(anyString(), anyString(), captor.capture());
        assertEquals(1, captor.getValue().size());
        assertEquals(ORG_2, captor.getValue().get(0).get("orgid"));
        assertEquals(false, captor.getValue().get(0).get("isactive"));
    }

    @Test
    void testHandleOrgLookupChangesReportsFailureOnRemovalError() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraFailure());
        ApiResponse response = new ApiResponse();
        orgLookupService.handleOrgLookupChanges(PLAN_ID, PLAN_YEAR, Set.of(ORG_1, ORG_2), Set.of(ORG_1),
                Constants.SINGLE, false, response);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testHandleOrgLookupChangesDeactivatesAllScopeWhenOrgsAdded() {
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(cassandraSuccess());
        ApiResponse response = new ApiResponse();
        orgLookupService.handleOrgLookupChanges(PLAN_ID, PLAN_YEAR, Set.of(ORG_1), Set.of(ORG_1, ORG_2),
                Constants.ALL, false, response);
        verify(cassandraOperation).insertRecord(anyString(), anyString(), any());
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testHandleOrgLookupChangesSkippedWhenPlanUsesMinistryOrStateId() {
        ApiResponse response = new ApiResponse();
        orgLookupService.handleOrgLookupChanges(PLAN_ID, PLAN_YEAR, Set.of(ORG_1, ORG_2), Set.of(ORG_1),
                Constants.CUSTOM, true, response);
        verify(cassandraOperation, never()).insertBulkRecord(anyString(), anyString(), anyList());
        verify(cassandraOperation, never()).insertRecord(anyString(), anyString(), any());
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testDeactivateOrgLookupEntriesForSingleScope() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.ORG_SCOPE, Constants.SINGLE);
        existingCbPlan.put(Constants.CONTEXT_DATA_REQUEST,
                contextDataWithOrgs(Constants.ROOT_ORG_ID, List.of(ORG_1)));
        ApiResponse response = new ApiResponse();
        orgLookupService.deactivateOrgLookupEntries(PLAN_ID, PLAN_YEAR, existingCbPlan, response);
        verify(cassandraOperation).insertBulkRecord(anyString(), anyString(), anyList());
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testDeactivateOrgLookupEntriesForAllScope() {
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(cassandraSuccess());
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.ORG_SCOPE, Constants.ALL);
        ApiResponse response = new ApiResponse();
        orgLookupService.deactivateOrgLookupEntries(PLAN_ID, PLAN_YEAR, existingCbPlan, response);
        verify(cassandraOperation).insertRecord(anyString(), anyString(), any());
    }

    @Test
    void testDeactivateOrgLookupEntriesSkipsUnknownScope() {
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.ORG_SCOPE, "UnknownScope");
        ApiResponse response = new ApiResponse();
        orgLookupService.deactivateOrgLookupEntries(PLAN_ID, PLAN_YEAR, existingCbPlan, response);
        verify(cassandraOperation, never()).insertBulkRecord(anyString(), anyString(), anyList());
        verify(cassandraOperation, never()).insertRecord(anyString(), anyString(), any());
    }

    @Test
    void testExtractUniqueRootOrgIdsReturnsEmptyForMissingContextData() {
        assertTrue(orgLookupService.extractUniqueRootOrgIds(new HashMap<>()).isEmpty());
    }

    @Test
    void testExtractUniqueRootOrgIdsFromMapContextData() {
        Map<String, Object> rawRequest = new HashMap<>();
        rawRequest.put(Constants.CONTEXT_DATA_REQUEST,
                contextDataWithOrgs(Constants.TARGETED_ORGANISATION, List.of(ORG_1, ORG_2)));
        assertEquals(Set.of(ORG_1, ORG_2), orgLookupService.extractUniqueRootOrgIds(rawRequest));
    }

    @Test
    void testExtractUniqueRootOrgIdsFromStringContextData() {
        Map<String, Object> rawRequest = new HashMap<>();
        rawRequest.put(Constants.CONTEXT_DATA_REQUEST,
                "{\"accessControl\":{\"userGroups\":[{\"userGroupCriteriaList\":"
                        + "[{\"criteriaKey\":\"rootOrgId\",\"criteriaValue\":[\"org1\"]}]}]}}");
        assertEquals(Set.of(ORG_1), orgLookupService.extractUniqueRootOrgIds(rawRequest));
    }

    @Test
    void testExtractUniqueRootOrgIdsReturnsEmptyForMalformedJson() {
        Map<String, Object> rawRequest = new HashMap<>();
        rawRequest.put(Constants.CONTEXT_DATA_REQUEST, INVALID_JSON);
        assertTrue(orgLookupService.extractUniqueRootOrgIds(rawRequest).isEmpty());
    }

    @Test
    void testExtractUniqueRootOrgIdsReturnsEmptyForUnsupportedType() {
        Map<String, Object> rawRequest = new HashMap<>();
        rawRequest.put(Constants.CONTEXT_DATA_REQUEST, 12345);
        assertTrue(orgLookupService.extractUniqueRootOrgIds(rawRequest).isEmpty());
    }

    @Test
    void testExtractOrgIdsFromCriteriaCollectsRootOrgId() {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID);
        criteria.put(Constants.CRITERIA_VALUE, List.of(ORG_1));
        Set<String> orgIdSet = new HashSet<>();
        orgLookupService.extractOrgIdsFromCriteria(List.of(criteria), orgIdSet);
        assertEquals(Set.of(ORG_1), orgIdSet);
    }

    @Test
    void testExtractOrgIdsFromCriteriaIgnoresUnrelatedKeys() {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, Constants.DESIGNATION);
        criteria.put(Constants.CRITERIA_VALUE, List.of("Manager"));
        Set<String> orgIdSet = new HashSet<>();
        orgLookupService.extractOrgIdsFromCriteria(List.of(criteria), orgIdSet);
        assertTrue(orgIdSet.isEmpty());
    }

    @Test
    void testExtractOrgIdsFromCriteriaIgnoresEmptyValues() {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID);
        criteria.put(Constants.CRITERIA_VALUE, List.of());
        Set<String> orgIdSet = new HashSet<>();
        orgLookupService.extractOrgIdsFromCriteria(List.of(criteria), orgIdSet);
        assertTrue(orgIdSet.isEmpty());
    }

    @Test
    void testConstructor() {
        assertNotNull(new CbPlanOrgLookupServiceV3Impl(cassandraOperation));
    }

    @Test
    void testExtractMinistryOrStateIdsReturnsEmptyForMissingContextData() {
        assertTrue(orgLookupService.extractMinistryOrStateIds(new HashMap<>()).isEmpty());
    }

    @Test
    void testExtractMinistryOrStateIdsFromMapContextData() {
        Map<String, Object> rawRequest = new HashMap<>();
        rawRequest.put(Constants.CONTEXT_DATA_REQUEST,
                contextDataWithOrgs(Constants.MINISTRY_OR_STATEID, List.of(MINISTRY_ORG_001, MINISTRY_ORG_002)));
        assertEquals(Set.of(MINISTRY_ORG_001, MINISTRY_ORG_002), orgLookupService.extractMinistryOrStateIds(rawRequest));
    }

    @Test
    void testExtractMinistryOrStateIdsFromStringContextData() {
        Map<String, Object> rawRequest = new HashMap<>();
        rawRequest.put(Constants.CONTEXT_DATA_REQUEST,
                "{\"accessControl\":{\"userGroups\":[{\"userGroupCriteriaList\":"
                        + "[{\"criteriaKey\":\"ministryOrStateId\",\"criteriaValue\":[\"ORG_001\"]}]}]}}");
        assertEquals(Set.of(MINISTRY_ORG_001), orgLookupService.extractMinistryOrStateIds(rawRequest));
    }

    @Test
    void testExtractMinistryOrStateIdsReturnsEmptyForMalformedJson() {
        Map<String, Object> rawRequest = new HashMap<>();
        rawRequest.put(Constants.CONTEXT_DATA_REQUEST, INVALID_JSON);
        assertTrue(orgLookupService.extractMinistryOrStateIds(rawRequest).isEmpty());
    }

    @Test
    void testExtractMinistryOrStateIdsIgnoresOtherCriteria() {
        Map<String, Object> rawRequest = new HashMap<>();
        rawRequest.put(Constants.CONTEXT_DATA_REQUEST,
                contextDataWithOrgs(Constants.ROOT_ORG_ID, List.of(ORG_1)));
        assertTrue(orgLookupService.extractMinistryOrStateIds(rawRequest).isEmpty());
    }

    @Test
    void testUpsertMinistryOrStateIdLookupSuccess() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());
        ApiResponse response = orgLookupService.upsertMinistryOrStateIdLookup(
                PLAN_ID, PLAN_YEAR, Set.of(MINISTRY_ORG_001), null, true);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testUpsertMinistryOrStateIdLookupBuildsOneRowPerMinistryId() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());
        Instant endDate = Instant.parse(TEST_END_DATE_STR);
        orgLookupService.upsertMinistryOrStateIdLookup(
                PLAN_ID, PLAN_YEAR, Set.of(MINISTRY_ORG_001, MINISTRY_ORG_002), endDate, true);
        ArgumentCaptor<List<Map<String, Object>>> captor = ArgumentCaptor.forClass(List.class);
        verify(cassandraOperation).insertBulkRecord(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_CB_PLAN_V3_LOOKUP_BY_MINISTRY_OR_STATE_ID),
                captor.capture());
        List<Map<String, Object>> rows = captor.getValue();
        assertEquals(2, rows.size());
        assertEquals(PLAN_YEAR, rows.get(0).get(Constants.PLAN_YEAR));
        assertEquals(PLAN_ID, rows.get(0).get(PLAN_ID_COLUMN));
        assertEquals(endDate, rows.get(0).get(END_DATE_COLUMN));
        assertEquals(true, rows.get(0).get(Constants.IS_ACTIVE));
        assertTrue(Set.of(MINISTRY_ORG_001, MINISTRY_ORG_002).contains(rows.get(0).get(MINISTRY_ID_COLUMN)));
    }

    @Test
    void testUpsertMinistryOrStateIdLookupSucceedsForEmptySet() {
        ApiResponse response = orgLookupService.upsertMinistryOrStateIdLookup(
                PLAN_ID, PLAN_YEAR, new HashSet<>(), null, true);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        verify(cassandraOperation, never()).insertBulkRecord(anyString(), anyString(), anyList());
    }

    @Test
    void testUpsertMinistryOrStateIdLookupHandlesException() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList()))
                .thenThrow(new RuntimeException(DB_ERROR_MSG));
        ApiResponse response = orgLookupService.upsertMinistryOrStateIdLookup(
                PLAN_ID, PLAN_YEAR, Set.of(MINISTRY_ORG_001), null, true);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErr().contains(DB_ERROR_MSG));
    }

    @Test
    void testHandleMinistryOrStateIdLookupChangesNoopWhenExistingEmpty() {
        ApiResponse response = new ApiResponse();
        orgLookupService.handleMinistryOrStateIdLookupChanges(
                PLAN_ID, PLAN_YEAR, new HashSet<>(), Set.of(MINISTRY_ORG_001), null, response);
        verify(cassandraOperation, never()).insertBulkRecord(anyString(), anyString(), anyList());
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testHandleMinistryOrStateIdLookupChangesDeactivatesWhenNewEmpty() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());
        ApiResponse response = new ApiResponse();
        orgLookupService.handleMinistryOrStateIdLookupChanges(
                PLAN_ID, PLAN_YEAR, Set.of(MINISTRY_ORG_001), new HashSet<>(), null, response);
        verify(cassandraOperation).insertBulkRecord(anyString(), anyString(), anyList());
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testHandleMinistryOrStateIdLookupChangesDeactivatesRemovedIds() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());
        Instant endDate = Instant.parse(TEST_END_DATE_STR);
        ApiResponse response = new ApiResponse();
        orgLookupService.handleMinistryOrStateIdLookupChanges(
                PLAN_ID, PLAN_YEAR, Set.of(MINISTRY_ORG_001, MINISTRY_ORG_002), Set.of(MINISTRY_ORG_001), endDate, response);
        ArgumentCaptor<List<Map<String, Object>>> captor = ArgumentCaptor.forClass(List.class);
        verify(cassandraOperation).insertBulkRecord(anyString(), anyString(), captor.capture());
        assertEquals(1, captor.getValue().size());
        assertEquals(MINISTRY_ORG_002, captor.getValue().get(0).get(MINISTRY_ID_COLUMN));
        assertEquals(false, captor.getValue().get(0).get(Constants.IS_ACTIVE));
        assertEquals(endDate, captor.getValue().get(0).get(END_DATE_COLUMN));
    }

    @Test
    void testHandleMinistryOrStateIdLookupChangesReportsFailureOnDeactivationError() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraFailure());
        ApiResponse response = new ApiResponse();
        orgLookupService.handleMinistryOrStateIdLookupChanges(
                PLAN_ID, PLAN_YEAR, Set.of(MINISTRY_ORG_001, MINISTRY_ORG_002), Set.of(MINISTRY_ORG_001), null, response);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }
}
