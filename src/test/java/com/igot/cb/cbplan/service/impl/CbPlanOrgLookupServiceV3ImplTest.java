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
                PLAN_ID, PLAN_YEAR, Set.of("org1"), null, true);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testUpsertCustomOrgLookupBuildsOneRowPerOrgWithEndDate() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());
        Instant endDate = Instant.parse("2026-12-31T18:29:59Z");
        orgLookupService.upsertCustomOrgLookup(PLAN_ID, PLAN_YEAR, Set.of("org1", "org2"), endDate, true);
        ArgumentCaptor<List<Map<String, Object>>> captor = ArgumentCaptor.forClass(List.class);
        verify(cassandraOperation).insertBulkRecord(
                eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_CB_PLAN_V3_LOOKUP_BY_ORG), captor.capture());
        List<Map<String, Object>> rows = captor.getValue();
        assertEquals(2, rows.size());
        assertEquals(PLAN_YEAR, rows.get(0).get("planyear"));
        assertEquals(PLAN_ID, rows.get(0).get("planid"));
        assertEquals(endDate, rows.get(0).get("enddate"));
        assertEquals(true, rows.get(0).get("isactive"));
    }

    @Test
    void testUpsertCustomOrgLookupOmitsEndDateWhenNull() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());
        orgLookupService.upsertCustomOrgLookup(PLAN_ID, PLAN_YEAR, Set.of("org1"), null, false);
        ArgumentCaptor<List<Map<String, Object>>> captor = ArgumentCaptor.forClass(List.class);
        verify(cassandraOperation).insertBulkRecord(anyString(), anyString(), captor.capture());
        assertTrue(!captor.getValue().get(0).containsKey("enddate"));
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
                PLAN_ID, PLAN_YEAR, Set.of("org1"), null, true);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testUpsertCustomOrgLookupHandlesException() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList()))
                .thenThrow(new RuntimeException("db error"));
        ApiResponse response = orgLookupService.upsertCustomOrgLookup(
                PLAN_ID, PLAN_YEAR, Set.of("org1"), null, true);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErr().contains("db error"));
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
                .thenThrow(new RuntimeException("db error"));
        ApiResponse response = orgLookupService.upsertAllOrgLookup(PLAN_ID, PLAN_YEAR, null, true);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.FAILED, response.get(Constants.RESPONSE));
    }

    @Test
    void testHandleOrgLookupChangesNoopWhenExistingEmpty() {
        ApiResponse response = new ApiResponse();
        orgLookupService.handleOrgLookupChanges(PLAN_ID, PLAN_YEAR, new HashSet<>(), Set.of("org1"),
                Constants.CUSTOM, response);
        verify(cassandraOperation, never()).insertBulkRecord(anyString(), anyString(), anyList());
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testHandleOrgLookupChangesNoopWhenNewEmpty() {
        ApiResponse response = new ApiResponse();
        orgLookupService.handleOrgLookupChanges(PLAN_ID, PLAN_YEAR, Set.of("org1"), new HashSet<>(),
                Constants.CUSTOM, response);
        verify(cassandraOperation, never()).insertBulkRecord(anyString(), anyString(), anyList());
    }

    @Test
    void testHandleOrgLookupChangesDeactivatesRemovedOrgsForCustomScope() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());
        ApiResponse response = new ApiResponse();
        orgLookupService.handleOrgLookupChanges(PLAN_ID, PLAN_YEAR, Set.of("org1", "org2"), Set.of("org1"),
                Constants.CUSTOM, response);
        ArgumentCaptor<List<Map<String, Object>>> captor = ArgumentCaptor.forClass(List.class);
        verify(cassandraOperation).insertBulkRecord(anyString(), anyString(), captor.capture());
        assertEquals(1, captor.getValue().size());
        assertEquals("org2", captor.getValue().get(0).get("orgid"));
        assertEquals(false, captor.getValue().get(0).get("isactive"));
    }

    @Test
    void testHandleOrgLookupChangesReportsFailureOnRemovalError() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraFailure());
        ApiResponse response = new ApiResponse();
        orgLookupService.handleOrgLookupChanges(PLAN_ID, PLAN_YEAR, Set.of("org1", "org2"), Set.of("org1"),
                Constants.SINGLE, response);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testHandleOrgLookupChangesDeactivatesAllScopeWhenOrgsAdded() {
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(cassandraSuccess());
        ApiResponse response = new ApiResponse();
        orgLookupService.handleOrgLookupChanges(PLAN_ID, PLAN_YEAR, Set.of("org1"), Set.of("org1", "org2"),
                Constants.ALL, response);
        verify(cassandraOperation).insertRecord(anyString(), anyString(), any());
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testDeactivateOrgLookupEntriesForSingleScope() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.ORG_SCOPE, Constants.SINGLE);
        existingCbPlan.put(Constants.CONTEXT_DATA_REQUEST,
                contextDataWithOrgs(Constants.ROOT_ORG_ID, List.of("org1")));
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
                contextDataWithOrgs(Constants.TARGETED_ORGANISATION, List.of("org1", "org2")));
        assertEquals(Set.of("org1", "org2"), orgLookupService.extractUniqueRootOrgIds(rawRequest));
    }

    @Test
    void testExtractUniqueRootOrgIdsFromStringContextData() {
        Map<String, Object> rawRequest = new HashMap<>();
        rawRequest.put(Constants.CONTEXT_DATA_REQUEST,
                "{\"accessControl\":{\"userGroups\":[{\"userGroupCriteriaList\":"
                        + "[{\"criteriaKey\":\"rootOrgId\",\"criteriaValue\":[\"org1\"]}]}]}}");
        assertEquals(Set.of("org1"), orgLookupService.extractUniqueRootOrgIds(rawRequest));
    }

    @Test
    void testExtractUniqueRootOrgIdsReturnsEmptyForMalformedJson() {
        Map<String, Object> rawRequest = new HashMap<>();
        rawRequest.put(Constants.CONTEXT_DATA_REQUEST, "not-valid-json");
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
        criteria.put(Constants.CRITERIA_VALUE, List.of("org1"));
        Set<String> orgIdSet = new HashSet<>();
        orgLookupService.extractOrgIdsFromCriteria(List.of(criteria), orgIdSet);
        assertEquals(Set.of("org1"), orgIdSet);
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
}
