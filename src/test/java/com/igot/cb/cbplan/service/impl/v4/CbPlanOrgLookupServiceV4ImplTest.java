package com.igot.cb.cbplan.service.impl.v4;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanOrgLookupServiceV4ImplTest {

    private static final String PLAN_ID = "plan-v4-001";
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
    private static final String V4_KEYSPACE = "sunbird_v4_test";
    private static final String V4_ORG_TABLE = "v4_cb_plan_lookup_by_org";
    private static final String V4_ALL_ORG_TABLE = "v4_cb_plan_lookup_by_all_org";
    private static final String V4_MINISTRY_TABLE = "v4_cb_plan_lookup_by_ministry";

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private CbExtServerProperties serverProperties;

    @InjectMocks
    private CbPlanOrgLookupServiceV4Impl orgLookupService;

    @BeforeEach
    void setUp() {
        lenient().when(serverProperties.getCbPlanV4Keyspace()).thenReturn(V4_KEYSPACE);
        lenient().when(serverProperties.getCbPlanV4LookupByOrgTable()).thenReturn(V4_ORG_TABLE);
        lenient().when(serverProperties.getCbPlanV4LookupByAllOrgTable()).thenReturn(V4_ALL_ORG_TABLE);
        lenient().when(serverProperties.getCbPlanV4LookupByMinistryOrStateIdTable()).thenReturn(V4_MINISTRY_TABLE);
    }

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

    // ── upsertCustomOrgLookup ──────────────────────────────────────────────────

    @Test
    void upsertCustomOrgLookup_singleOrg_returnsSuccess() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());

        ApiResponse response = orgLookupService.upsertCustomOrgLookup(
                PLAN_ID, PLAN_YEAR, Set.of(ORG_1), null, true);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void upsertCustomOrgLookup_twoOrgs_buildsOneRowPerOrgWithEndDate() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());
        Instant endDate = Instant.parse(TEST_END_DATE_STR);

        orgLookupService.upsertCustomOrgLookup(PLAN_ID, PLAN_YEAR, Set.of(ORG_1, ORG_2), endDate, true);

        ArgumentCaptor<List<Map<String, Object>>> captor = ArgumentCaptor.forClass(List.class);
        verify(cassandraOperation).insertBulkRecord(eq(V4_KEYSPACE), eq(V4_ORG_TABLE), captor.capture());
        List<Map<String, Object>> rows = captor.getValue();
        assertEquals(2, rows.size());
        assertEquals(PLAN_YEAR, rows.get(0).get(Constants.PLAN_YEAR));
        assertEquals(PLAN_ID, rows.get(0).get(PLAN_ID_COLUMN));
        assertEquals(endDate, rows.get(0).get(END_DATE_COLUMN));
        assertEquals(true, rows.get(0).get(Constants.IS_ACTIVE));
    }

    @Test
    void upsertCustomOrgLookup_nullEndDate_endDateColumnAbsent() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());

        orgLookupService.upsertCustomOrgLookup(PLAN_ID, PLAN_YEAR, Set.of(ORG_1), null, false);

        ArgumentCaptor<List<Map<String, Object>>> captor = ArgumentCaptor.forClass(List.class);
        verify(cassandraOperation).insertBulkRecord(anyString(), anyString(), captor.capture());
        assertThat(captor.getValue().get(0)).doesNotContainKey(END_DATE_COLUMN);
        assertEquals(false, captor.getValue().get(0).get(Constants.IS_ACTIVE));
    }

    @Test
    void upsertCustomOrgLookup_emptyOrgList_failsWithoutCallingCassandra() {
        ApiResponse response = orgLookupService.upsertCustomOrgLookup(
                PLAN_ID, PLAN_YEAR, new HashSet<>(), null, true);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        verify(cassandraOperation, never()).insertBulkRecord(anyString(), anyString(), anyList());
    }

    @Test
    void upsertCustomOrgLookup_cassandraInsertFails_propagatesFailure() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraFailure());

        ApiResponse response = orgLookupService.upsertCustomOrgLookup(
                PLAN_ID, PLAN_YEAR, Set.of(ORG_1), null, true);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void upsertCustomOrgLookup_exceptionThrown_returnsFailedWithMessage() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList()))
                .thenThrow(new RuntimeException(DB_ERROR_MSG));

        ApiResponse response = orgLookupService.upsertCustomOrgLookup(
                PLAN_ID, PLAN_YEAR, Set.of(ORG_1), null, true);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErr().contains(DB_ERROR_MSG));
    }

    // ── upsertAllOrgLookup ────────────────────────────────────────────────────

    @Test
    void upsertAllOrgLookup_success_returnsSuccess() {
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(cassandraSuccess());

        ApiResponse response = orgLookupService.upsertAllOrgLookup(PLAN_ID, PLAN_YEAR, null, true);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        verify(cassandraOperation).insertRecord(eq(V4_KEYSPACE), eq(V4_ALL_ORG_TABLE), any());
    }

    @Test
    void upsertAllOrgLookup_exceptionThrown_returnsFailedResponse() {
        when(cassandraOperation.insertRecord(anyString(), anyString(), any()))
                .thenThrow(new RuntimeException(DB_ERROR_MSG));

        ApiResponse response = orgLookupService.upsertAllOrgLookup(PLAN_ID, PLAN_YEAR, null, true);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.FAILED, response.get(Constants.RESPONSE));
    }

    // ── handleOrgLookupChanges ────────────────────────────────────────────────

    @Test
    void handleOrgLookupChanges_existingOrgListEmpty_noopNoInsert() {
        ApiResponse response = new ApiResponse();

        orgLookupService.handleOrgLookupChanges(
                PLAN_ID, PLAN_YEAR, new HashSet<>(), Set.of(ORG_1), Constants.CUSTOM, false, response);

        verify(cassandraOperation, never()).insertBulkRecord(anyString(), anyString(), anyList());
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void handleOrgLookupChanges_newOrgListEmpty_noopNoInsert() {
        ApiResponse response = new ApiResponse();

        orgLookupService.handleOrgLookupChanges(
                PLAN_ID, PLAN_YEAR, Set.of(ORG_1), new HashSet<>(), Constants.CUSTOM, false, response);

        verify(cassandraOperation, never()).insertBulkRecord(anyString(), anyString(), anyList());
    }

    @Test
    void handleOrgLookupChanges_customScope_deactivatesRemovedOrg() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());
        ApiResponse response = new ApiResponse();

        orgLookupService.handleOrgLookupChanges(
                PLAN_ID, PLAN_YEAR, Set.of(ORG_1, ORG_2), Set.of(ORG_1), Constants.CUSTOM, false, response);

        ArgumentCaptor<List<Map<String, Object>>> captor = ArgumentCaptor.forClass(List.class);
        verify(cassandraOperation).insertBulkRecord(anyString(), anyString(), captor.capture());
        assertEquals(1, captor.getValue().size());
        assertEquals(ORG_2, captor.getValue().get(0).get("orgid"));
        assertEquals(false, captor.getValue().get(0).get(Constants.IS_ACTIVE));
    }

    @Test
    void handleOrgLookupChanges_singleScope_deactivatesRemovedOrg_setsFailedOnInsertError() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraFailure());
        ApiResponse response = new ApiResponse();

        orgLookupService.handleOrgLookupChanges(
                PLAN_ID, PLAN_YEAR, Set.of(ORG_1, ORG_2), Set.of(ORG_1), Constants.SINGLE, false, response);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void handleOrgLookupChanges_allScope_deactivatesAllOrgWhenOrgsAdded() {
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(cassandraSuccess());
        ApiResponse response = new ApiResponse();

        orgLookupService.handleOrgLookupChanges(
                PLAN_ID, PLAN_YEAR, Set.of(ORG_1), Set.of(ORG_1, ORG_2), Constants.ALL, false, response);

        verify(cassandraOperation).insertRecord(anyString(), anyString(), any());
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void handleOrgLookupChanges_hasMinistryOrStateId_skipsAllLookupUpdates() {
        ApiResponse response = new ApiResponse();

        orgLookupService.handleOrgLookupChanges(
                PLAN_ID, PLAN_YEAR, Set.of(ORG_1, ORG_2), Set.of(ORG_1), Constants.CUSTOM, true, response);

        verify(cassandraOperation, never()).insertBulkRecord(anyString(), anyString(), anyList());
        verify(cassandraOperation, never()).insertRecord(anyString(), anyString(), any());
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    // ── deactivateOrgLookupEntries ────────────────────────────────────────────

    @Test
    void deactivateOrgLookupEntries_singleScope_callsInsertBulkRecord() {
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
    void deactivateOrgLookupEntries_allScope_callsInsertRecord() {
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(cassandraSuccess());
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.ORG_SCOPE, Constants.ALL);
        ApiResponse response = new ApiResponse();

        orgLookupService.deactivateOrgLookupEntries(PLAN_ID, PLAN_YEAR, existingCbPlan, response);

        verify(cassandraOperation).insertRecord(anyString(), anyString(), any());
    }

    @Test
    void deactivateOrgLookupEntries_unknownScope_noCassandraCalls() {
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.ORG_SCOPE, "UnknownScope");
        ApiResponse response = new ApiResponse();

        orgLookupService.deactivateOrgLookupEntries(PLAN_ID, PLAN_YEAR, existingCbPlan, response);

        verify(cassandraOperation, never()).insertBulkRecord(anyString(), anyString(), anyList());
        verify(cassandraOperation, never()).insertRecord(anyString(), anyString(), any());
    }

    // ── extractUniqueRootOrgIds ───────────────────────────────────────────────

    @Test
    void extractUniqueRootOrgIds_noContextData_returnsEmpty() {
        assertTrue(orgLookupService.extractUniqueRootOrgIds(new HashMap<>()).isEmpty());
    }

    @Test
    void extractUniqueRootOrgIds_mapContextData_returnsOrgIds() {
        Map<String, Object> rawRequest = new HashMap<>();
        rawRequest.put(Constants.CONTEXT_DATA_REQUEST,
                contextDataWithOrgs(Constants.TARGETED_ORGANISATION, List.of(ORG_1, ORG_2)));

        assertEquals(Set.of(ORG_1, ORG_2), orgLookupService.extractUniqueRootOrgIds(rawRequest));
    }

    @Test
    void extractUniqueRootOrgIds_stringContextData_parsesAndReturnsOrgIds() {
        Map<String, Object> rawRequest = new HashMap<>();
        rawRequest.put(Constants.CONTEXT_DATA_REQUEST,
                "{\"accessControl\":{\"userGroups\":[{\"userGroupCriteriaList\":"
                        + "[{\"criteriaKey\":\"rootOrgId\",\"criteriaValue\":[\"org1\"]}]}]}}");

        assertEquals(Set.of(ORG_1), orgLookupService.extractUniqueRootOrgIds(rawRequest));
    }

    @Test
    void extractUniqueRootOrgIds_malformedJsonString_returnsEmpty() {
        Map<String, Object> rawRequest = new HashMap<>();
        rawRequest.put(Constants.CONTEXT_DATA_REQUEST, INVALID_JSON);

        assertTrue(orgLookupService.extractUniqueRootOrgIds(rawRequest).isEmpty());
    }

    @Test
    void extractUniqueRootOrgIds_unsupportedType_returnsEmpty() {
        Map<String, Object> rawRequest = new HashMap<>();
        rawRequest.put(Constants.CONTEXT_DATA_REQUEST, 12345);

        assertTrue(orgLookupService.extractUniqueRootOrgIds(rawRequest).isEmpty());
    }

    // ── extractOrgIdsFromCriteria ─────────────────────────────────────────────

    @Test
    void extractOrgIdsFromCriteria_rootOrgIdKey_collectsIds() {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID);
        criteria.put(Constants.CRITERIA_VALUE, List.of(ORG_1));
        Set<String> orgIdSet = new HashSet<>();

        orgLookupService.extractOrgIdsFromCriteria(List.of(criteria), orgIdSet);

        assertEquals(Set.of(ORG_1), orgIdSet);
    }

    @Test
    void extractOrgIdsFromCriteria_unrelatedKey_doesNotCollect() {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, Constants.DESIGNATION);
        criteria.put(Constants.CRITERIA_VALUE, List.of("Manager"));
        Set<String> orgIdSet = new HashSet<>();

        orgLookupService.extractOrgIdsFromCriteria(List.of(criteria), orgIdSet);

        assertTrue(orgIdSet.isEmpty());
    }

    @Test
    void extractOrgIdsFromCriteria_emptyValues_doesNotCollect() {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID);
        criteria.put(Constants.CRITERIA_VALUE, List.of());
        Set<String> orgIdSet = new HashSet<>();

        orgLookupService.extractOrgIdsFromCriteria(List.of(criteria), orgIdSet);

        assertTrue(orgIdSet.isEmpty());
    }

    // ── extractMinistryOrStateIds ─────────────────────────────────────────────

    @Test
    void extractMinistryOrStateIds_noContextData_returnsEmpty() {
        assertTrue(orgLookupService.extractMinistryOrStateIds(new HashMap<>()).isEmpty());
    }

    @Test
    void extractMinistryOrStateIds_mapContextData_returnsMinistryIds() {
        Map<String, Object> rawRequest = new HashMap<>();
        rawRequest.put(Constants.CONTEXT_DATA_REQUEST,
                contextDataWithOrgs(Constants.MINISTRY_OR_STATEID, List.of(MINISTRY_ORG_001, MINISTRY_ORG_002)));

        assertEquals(Set.of(MINISTRY_ORG_001, MINISTRY_ORG_002),
                orgLookupService.extractMinistryOrStateIds(rawRequest));
    }

    @Test
    void extractMinistryOrStateIds_stringContextData_parsesAndReturnsMinistryIds() {
        Map<String, Object> rawRequest = new HashMap<>();
        rawRequest.put(Constants.CONTEXT_DATA_REQUEST,
                "{\"accessControl\":{\"userGroups\":[{\"userGroupCriteriaList\":"
                        + "[{\"criteriaKey\":\"ministryOrStateId\",\"criteriaValue\":[\"ORG_001\"]}]}]}}");

        assertEquals(Set.of(MINISTRY_ORG_001), orgLookupService.extractMinistryOrStateIds(rawRequest));
    }

    @Test
    void extractMinistryOrStateIds_malformedJsonString_returnsEmpty() {
        Map<String, Object> rawRequest = new HashMap<>();
        rawRequest.put(Constants.CONTEXT_DATA_REQUEST, INVALID_JSON);

        assertTrue(orgLookupService.extractMinistryOrStateIds(rawRequest).isEmpty());
    }

    @Test
    void extractMinistryOrStateIds_rootOrgIdCriteria_returnsEmpty() {
        Map<String, Object> rawRequest = new HashMap<>();
        rawRequest.put(Constants.CONTEXT_DATA_REQUEST,
                contextDataWithOrgs(Constants.ROOT_ORG_ID, List.of(ORG_1)));

        assertTrue(orgLookupService.extractMinistryOrStateIds(rawRequest).isEmpty());
    }

    // ── upsertMinistryOrStateIdLookup ─────────────────────────────────────────

    @Test
    void upsertMinistryOrStateIdLookup_singleId_returnsSuccess() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());

        ApiResponse response = orgLookupService.upsertMinistryOrStateIdLookup(
                PLAN_ID, PLAN_YEAR, Set.of(MINISTRY_ORG_001), null, true);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void upsertMinistryOrStateIdLookup_twoIds_buildsOneRowPerIdWithEndDate() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());
        Instant endDate = Instant.parse(TEST_END_DATE_STR);

        orgLookupService.upsertMinistryOrStateIdLookup(
                PLAN_ID, PLAN_YEAR, Set.of(MINISTRY_ORG_001, MINISTRY_ORG_002), endDate, true);

        ArgumentCaptor<List<Map<String, Object>>> captor = ArgumentCaptor.forClass(List.class);
        verify(cassandraOperation).insertBulkRecord(eq(V4_KEYSPACE), eq(V4_MINISTRY_TABLE), captor.capture());
        List<Map<String, Object>> rows = captor.getValue();
        assertEquals(2, rows.size());
        assertEquals(PLAN_YEAR, rows.get(0).get(Constants.PLAN_YEAR));
        assertEquals(PLAN_ID, rows.get(0).get(PLAN_ID_COLUMN));
        assertEquals(endDate, rows.get(0).get(END_DATE_COLUMN));
        assertEquals(true, rows.get(0).get(Constants.IS_ACTIVE));
        assertThat(Set.of(MINISTRY_ORG_001, MINISTRY_ORG_002)).contains((String) rows.get(0).get(MINISTRY_ID_COLUMN));
    }

    @Test
    void upsertMinistryOrStateIdLookup_emptySet_returnsSuccessWithoutInsert() {
        ApiResponse response = orgLookupService.upsertMinistryOrStateIdLookup(
                PLAN_ID, PLAN_YEAR, new HashSet<>(), null, true);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        verify(cassandraOperation, never()).insertBulkRecord(anyString(), anyString(), anyList());
    }

    @Test
    void upsertMinistryOrStateIdLookup_cassandraInsertFails_returnsFailure() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraFailure());

        ApiResponse response = orgLookupService.upsertMinistryOrStateIdLookup(
                PLAN_ID, PLAN_YEAR, Set.of(MINISTRY_ORG_001), null, true);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void upsertMinistryOrStateIdLookup_exceptionThrown_returnsFailedWithMessage() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList()))
                .thenThrow(new RuntimeException(DB_ERROR_MSG));

        ApiResponse response = orgLookupService.upsertMinistryOrStateIdLookup(
                PLAN_ID, PLAN_YEAR, Set.of(MINISTRY_ORG_001), null, true);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErr().contains(DB_ERROR_MSG));
    }

    // ── handleMinistryOrStateIdLookupChanges ──────────────────────────────────

    @Test
    void handleMinistryOrStateIdLookupChanges_bothSetsEmpty_noop() {
        ApiResponse response = new ApiResponse();

        orgLookupService.handleMinistryOrStateIdLookupChanges(
                PLAN_ID, PLAN_YEAR, new HashSet<>(), new HashSet<>(), null, response);

        verify(cassandraOperation, never()).insertBulkRecord(anyString(), anyString(), anyList());
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void handleMinistryOrStateIdLookupChanges_existingOnlyPresent_deactivatesAll() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());
        ApiResponse response = new ApiResponse();

        orgLookupService.handleMinistryOrStateIdLookupChanges(
                PLAN_ID, PLAN_YEAR, Set.of(MINISTRY_ORG_001), new HashSet<>(), null, response);

        verify(cassandraOperation).insertBulkRecord(anyString(), anyString(), anyList());
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void handleMinistryOrStateIdLookupChanges_removedId_deactivatedWithEndDate() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraSuccess());
        Instant endDate = Instant.parse(TEST_END_DATE_STR);
        ApiResponse response = new ApiResponse();

        orgLookupService.handleMinistryOrStateIdLookupChanges(
                PLAN_ID, PLAN_YEAR, Set.of(MINISTRY_ORG_001, MINISTRY_ORG_002),
                Set.of(MINISTRY_ORG_001), endDate, response);

        ArgumentCaptor<List<Map<String, Object>>> captor = ArgumentCaptor.forClass(List.class);
        verify(cassandraOperation).insertBulkRecord(anyString(), anyString(), captor.capture());
        assertEquals(1, captor.getValue().size());
        assertEquals(MINISTRY_ORG_002, captor.getValue().get(0).get(MINISTRY_ID_COLUMN));
        assertEquals(false, captor.getValue().get(0).get(Constants.IS_ACTIVE));
        assertEquals(endDate, captor.getValue().get(0).get(END_DATE_COLUMN));
    }

    @Test
    void handleMinistryOrStateIdLookupChanges_deactivationFails_setsFailedOnResponse() {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList())).thenReturn(cassandraFailure());
        ApiResponse response = new ApiResponse();

        orgLookupService.handleMinistryOrStateIdLookupChanges(
                PLAN_ID, PLAN_YEAR, Set.of(MINISTRY_ORG_001, MINISTRY_ORG_002),
                Set.of(MINISTRY_ORG_001), null, response);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void handleMinistryOrStateIdLookupChanges_newOnlyPresent_noDeactivation() {
        ApiResponse response = new ApiResponse();

        orgLookupService.handleMinistryOrStateIdLookupChanges(
                PLAN_ID, PLAN_YEAR, new HashSet<>(), Set.of(MINISTRY_ORG_001), null, response);

        verify(cassandraOperation, never()).insertBulkRecord(anyString(), anyString(), anyList());
        assertNotEquals(Constants.FAILED, response.getParams().getStatus());
    }

    // ── constructor ───────────────────────────────────────────────────────────

    @Test
    void constructor_validDeps_createsInstance() {
        assertNotNull(new CbPlanOrgLookupServiceV4Impl(cassandraOperation, serverProperties));
    }
}
