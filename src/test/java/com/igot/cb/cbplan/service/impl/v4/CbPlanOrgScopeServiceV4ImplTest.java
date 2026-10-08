package com.igot.cb.cbplan.service.impl.v4;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.cbplan.service.impl.CbPlanRequestValidatorImpl;
import com.igot.cb.service.UserAndOrgServiceImpl;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanOrgScopeServiceV4ImplTest {

    private static final String ORG_ID = "org1";
    private static final String OTHER_ORG_ID = "org2";
    private static final String VALID_GROUP_ID = "3fa85f64-5717-4562-b3fc-2c963f66afa6";
    private static final String OTHER_GROUP_ID = "3fa85f64-5717-4562-b3fc-2c963f66afa7";

    @Mock
    private CbPlanUserGroupLookupServiceV4Impl userGroupLookupService;

    @Mock
    private UserAndOrgServiceImpl userAndOrgService;

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private CbExtServerProperties serverProperties;

    @Mock
    private CbPlanRequestValidatorImpl cbPlanRequestValidator;

    @InjectMocks
    private CbPlanOrgScopeServiceV4Impl orgScopeService;

    private final Map<String, Map<String, Object>> mockedGroups = new HashMap<>();
    private final Map<String, Map<String, Object>> mockedOrgs = new HashMap<>();

    @BeforeEach
    void setUp() {
        mockedGroups.clear();
        mockedOrgs.clear();
        setupBatchOrgFetchStub();
    }

    private void setupBatchOrgFetchStub() {
        lenient().when(cassandraOperation.getRecordsByProperties(
            eq(Constants.KEYSPACE_SUNBIRD),
            eq(Constants.ORG_TABLE),
            any(),
            any(),
            any()
        )).thenAnswer(invocation -> {
            Map<String, Object> queryMap = invocation.getArgument(2);
            if (queryMap == null || !queryMap.containsKey(Constants.ID)) {
                return List.of();
            }
            Object idValue = queryMap.get(Constants.ID);
            List<String> requestedIds = (idValue instanceof List)
                ? (List<String>) idValue
                : List.of((String) idValue);

            List<Map<String, Object>> results = new ArrayList<>();
            for (String orgId : requestedIds) {
                if (mockedOrgs.containsKey(orgId)) {
                    results.add(mockedOrgs.get(orgId));
                }
            }
            return results;
        });
    }

    private static Map<String, Object> userGroupRef(String userGroupId) {
        Map<String, Object> group = new HashMap<>();
        group.put(Constants.USER_GROUP_ID, userGroupId);
        return group;
    }

    private static Map<String, Object> requestWithGroups(List<Map<String, Object>> groups) {
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, groups);
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.CONTEXT_DATA_REQUEST, contextData);
        return request;
    }

    private static Map<String, Object> activeGroupEntity(List<Map<String, List<String>>> criteria) {
        Map<String, Object> entity = new HashMap<>();
        entity.put(Constants.COL_STATUS, Constants.ACTIVE);
        entity.put(Constants.COL_CRITERIA, criteria);
        return entity;
    }

    private void mockNonL0Org(String orgId) {
        Map<String, Object> orgMap = new HashMap<>();
        orgMap.put(Constants.ID, orgId);
        orgMap.put(Constants.MINISTRY_OR_STATETYPE_DB, "NON_SPV");
        mockedOrgs.put(orgId, orgMap);
        lenient().when(userAndOrgService.readOrgFromDB(eq(orgId), any())).thenReturn(orgMap);
    }

    private void mockL0Org(String orgId) {
        Map<String, Object> orgMap = new HashMap<>();
        orgMap.put(Constants.ID, orgId);
        orgMap.put(Constants.MINISTRY_OR_STATETYPE_DB, Constants.SPV);
        mockedOrgs.put(orgId, orgMap);
        lenient().when(userAndOrgService.readOrgFromDB(eq(orgId), any())).thenReturn(orgMap);
    }

    /**
     * Builds a group entity map keyed uniquely by groupId. Mockito matches stub
     * arguments by equals(); two entities with identical content (e.g. both built
     * from an empty criteria list) would otherwise collide and silently return
     * the wrong group's stubbed extraction result in multi-group tests.
     */
    private static Map<String, Object> uniqueGroupEntity(String groupId) {
        Map<String, Object> entity = activeGroupEntity(List.of());
        entity.put(Constants.COL_USERGROUPID, groupId);
        return entity;
    }

    private void mockGroupWithRootOrgIds(String groupId, String... rootOrgIds) {
        Map<String, Object> entity = uniqueGroupEntity(groupId);
        mockedGroups.put(groupId, entity);
        when(userGroupLookupService.extractRootOrgIds(entity)).thenReturn(new HashSet<>(List.of(rootOrgIds)));
        when(userGroupLookupService.extractMinistryOrStateIds(entity)).thenReturn(new HashSet<>());
        setupBatchFetchStub();
    }

    private void mockGroupWithMinistryOrStateIds(String groupId, String... ministryOrStateIds) {
        Map<String, Object> entity = uniqueGroupEntity(groupId);
        mockedGroups.put(groupId, entity);
        when(userGroupLookupService.extractRootOrgIds(entity)).thenReturn(new HashSet<>());
        when(userGroupLookupService.extractMinistryOrStateIds(entity)).thenReturn(new HashSet<>(List.of(ministryOrStateIds)));
        setupBatchFetchStub();
    }

    private void mockGroupWithNoCriteria(String groupId) {
        Map<String, Object> entity = uniqueGroupEntity(groupId);
        mockedGroups.put(groupId, entity);
        when(userGroupLookupService.extractRootOrgIds(entity)).thenReturn(new HashSet<>());
        when(userGroupLookupService.extractMinistryOrStateIds(entity)).thenReturn(new HashSet<>());
        setupBatchFetchStub();
    }

    private void setupBatchFetchStub() {
        lenient().when(userGroupLookupService.fetchUserGroupsByIds(any(), eq(ORG_ID)))
            .thenAnswer(invocation -> {
                List<String> requestedIds = invocation.getArgument(0);
                Map<String, Map<String, Object>> result = new HashMap<>();
                for (String id : requestedIds) {
                    if (mockedGroups.containsKey(id)) {
                        result.put(id, mockedGroups.get(id));
                    }
                }
                return result;
            });
    }

    // ---------------- structural validation ----------------

    @Test
    void resolveOrgScope_contextDataKeyMissing_returnsError() {
        List<String> errors = orgScopeService.resolveOrgScope(new HashMap<>(), false, ORG_ID, null, null);

        assertEquals(List.of(Constants.ERR_CONTEXT_DATA_MISSING), errors);
    }

    @Test
    void resolveOrgScope_contextDataMalformedJsonString_returnsError() {
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.CONTEXT_DATA_REQUEST, "not-valid-json");

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertEquals(List.of(Constants.ERR_CONTEXT_DATA_UNPARSEABLE), errors);
    }

    @Test
    void resolveOrgScope_contextDataWrongType_returnsError() {
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.CONTEXT_DATA_REQUEST, 12345);

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertEquals(List.of(Constants.ERR_CONTEXT_DATA_INVALID_TYPE), errors);
    }

    @Test
    void resolveOrgScope_accessControlMissing_returnsError() {
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.CONTEXT_DATA_REQUEST, new HashMap<>());

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertEquals(List.of(Constants.ERR_ACCESS_CONTROL_MISSING), errors);
    }

    @Test
    void resolveOrgScope_userGroupsMissing_returnsError() {
        Map<String, Object> request = requestWithGroups(new ArrayList<>());

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertEquals(List.of(Constants.ERR_USER_GROUPS_MISSING), errors);
    }

    @Test
    void resolveOrgScope_userGroupIdMissing_returnsError() {
        Map<String, Object> request = requestWithGroups(List.of(new HashMap<>()));

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertEquals(List.of(Constants.ERR_USER_GROUP_NO_FORMAT), errors);
    }

    @Test
    void resolveOrgScope_userGroupIdBlank_returnsError() {
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef("  ")));

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertEquals(List.of(Constants.ERR_USER_GROUP_ID_REQUIRED), errors);
    }

    @Test
    void resolveOrgScope_mixedV3AndV4Format_returnsError() {
        // Mixing V3 format (inline criteria) with V4 format (userGroupId) is not allowed
        Map<String, Object> v4Group = userGroupRef(VALID_GROUP_ID);
        Map<String, Object> v3Group = new HashMap<>();
        v3Group.put(Constants.USER_GROUP_CRITERIA_LIST, List.of());
        v3Group.put(Constants.USER_GROUP_NAME, "V3 Group");
        Map<String, Object> request = requestWithGroups(List.of(v4Group, v3Group));

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertTrue(errors.contains(Constants.ERR_USER_GROUP_MIXED_FORMAT));
    }

    @Test
    void resolveOrgScope_v3FormatOnly_delegatesToV3Validator() {
        // Pure V3 format (no userGroupId) should be accepted and delegated to V3 validator
        Map<String, Object> v3Group = new HashMap<>();
        v3Group.put(Constants.USER_GROUP_CRITERIA_LIST, List.of(
                Map.of("criteriaKey", "rootOrgId", "criteriaValue", List.of(ORG_ID))
        ));
        v3Group.put(Constants.USER_GROUP_NAME, "V3 Group");
        Map<String, Object> request = requestWithGroups(List.of(v3Group));

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        // V3 validator will process it - exact validation depends on V3 implementation
        // This test just verifies no format mixing error occurs
        assertFalse(errors.contains(Constants.ERR_USER_GROUP_MIXED_FORMAT));
    }

    @Test
    void resolveOrgScope_userGroupIdNotValidUuid_returnsError() {
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef("not-a-uuid")));

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertEquals(List.of(Constants.ERR_USER_GROUP_ID_INVALID_FORMAT), errors);
    }

    @Test
    void resolveOrgScope_userGroupNotFound_returnsError() {
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));
        when(userGroupLookupService.fetchUserGroupsByIds(any(), eq(ORG_ID)))
            .thenReturn(new HashMap<>());

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertEquals(List.of(Constants.ERR_USER_GROUP_NOT_FOUND + VALID_GROUP_ID), errors);
    }

    @Test
    void resolveOrgScope_userGroupInactive_returnsError() {
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));
        Map<String, Object> inactiveEntity = new HashMap<>();
        inactiveEntity.put(Constants.COL_STATUS, "INACTIVE");
        when(userGroupLookupService.fetchUserGroupsByIds(any(), eq(ORG_ID)))
            .thenReturn(Map.of(VALID_GROUP_ID, inactiveEntity));

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertEquals(List.of(Constants.ERR_USER_GROUP_NOT_ACTIVE + VALID_GROUP_ID), errors);
    }

    @Test
    void resolveOrgScope_contextDataAsJsonString_isParsedAndResolved() {
        String contextDataJson = "{\"accessControl\":{\"userGroups\":[{\"userGroupId\":\"" + VALID_GROUP_ID + "\"}]}}";
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.CONTEXT_DATA_REQUEST, contextDataJson);
        mockGroupWithRootOrgIds(VALID_GROUP_ID, ORG_ID);
        mockNonL0Org(ORG_ID);

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertTrue(errors.isEmpty());
        assertEquals(Constants.SINGLE, request.get(Constants.ORG_SCOPE));
    }

    // ---------------- non-CCA, non-L0 caller ----------------

    @Test
    void resolveOrgScope_nonCcaNonL0_rootOrgMatchesCaller_resolvesSingle() {
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));
        mockGroupWithRootOrgIds(VALID_GROUP_ID, ORG_ID);
        mockNonL0Org(ORG_ID);

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertTrue(errors.isEmpty());
        assertEquals(Constants.SINGLE, request.get(Constants.ORG_SCOPE));
        assertEquals(List.of(ORG_ID), request.get(Constants.ORG_ID_LIST));
    }

    @Test
    void resolveOrgScope_nonCcaNonL0_rootOrgMismatch_returnsError() {
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));
        mockGroupWithRootOrgIds(VALID_GROUP_ID, OTHER_ORG_ID);
        mockNonL0Org(ORG_ID);

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertEquals(List.of(Constants.ERR_ROOT_ORG_ID_MISMATCH), errors);
    }

    @Test
    void resolveOrgScope_nonCcaNonL0_multipleRootOrgIds_returnsError() {
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID), userGroupRef(OTHER_GROUP_ID)));
        mockGroupWithRootOrgIds(VALID_GROUP_ID, ORG_ID);
        mockGroupWithRootOrgIds(OTHER_GROUP_ID, OTHER_ORG_ID);
        mockNonL0Org(ORG_ID);

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertEquals(List.of(Constants.ERR_MULTIPLE_ROOT_ORG_IDS), errors);
    }

    @Test
    void resolveOrgScope_nonCcaNonL0_noCriteriaOnGroup_returnsError() {
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));
        mockGroupWithNoCriteria(VALID_GROUP_ID);
        mockNonL0Org(ORG_ID);

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertEquals(List.of(Constants.ERR_ROOT_ORG_CRITERIA_MISSING), errors);
    }

    // ---------------- CCA caller ----------------

    @Test
    void resolveOrgScope_cca_noCriteriaOnAnyGroup_resolvesAll() {
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));
        mockGroupWithNoCriteria(VALID_GROUP_ID);
        mockNonL0Org(ORG_ID);

        List<String> errors = orgScopeService.resolveOrgScope(request, true, ORG_ID, null, null);

        assertTrue(errors.isEmpty());
        assertEquals(Constants.ALL, request.get(Constants.ORG_SCOPE));
    }

    @Test
    void resolveOrgScope_cca_oneRootOrgId_resolvesSingle() {
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));
        mockGroupWithRootOrgIds(VALID_GROUP_ID, OTHER_ORG_ID);
        mockNonL0Org(ORG_ID);

        List<String> errors = orgScopeService.resolveOrgScope(request, true, ORG_ID, null, null);

        assertTrue(errors.isEmpty());
        assertEquals(Constants.SINGLE, request.get(Constants.ORG_SCOPE));
    }

    @Test
    void resolveOrgScope_cca_multipleRootOrgIds_resolvesCustom() {
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID), userGroupRef(OTHER_GROUP_ID)));
        mockGroupWithRootOrgIds(VALID_GROUP_ID, ORG_ID);
        mockGroupWithRootOrgIds(OTHER_GROUP_ID, OTHER_ORG_ID);
        mockNonL0Org(ORG_ID);

        List<String> errors = orgScopeService.resolveOrgScope(request, true, ORG_ID, null, null);

        assertTrue(errors.isEmpty());
        assertEquals(Constants.CUSTOM, request.get(Constants.ORG_SCOPE));
    }

    @Test
    void resolveOrgScope_cca_mixedGroupsSomeMissingCriteria_returnsRestrictionError() {
        when(serverProperties.getMsgOnUserGroupRestrictionForAllOrg()).thenReturn("restricted");
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID), userGroupRef(OTHER_GROUP_ID)));
        mockGroupWithRootOrgIds(VALID_GROUP_ID, ORG_ID);
        mockGroupWithNoCriteria(OTHER_GROUP_ID);
        mockNonL0Org(ORG_ID);

        List<String> errors = orgScopeService.resolveOrgScope(request, true, ORG_ID, null, null);

        assertEquals(List.of("restricted"), errors);
    }

    // ---------------- L0 caller ----------------

    @Test
    void resolveOrgScope_nonCcaL0_oneRootOrgId_resolvesSingle() {
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));
        mockGroupWithRootOrgIds(VALID_GROUP_ID, OTHER_ORG_ID);
        mockL0Org(ORG_ID);

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertTrue(errors.isEmpty());
        assertEquals(Constants.SINGLE, request.get(Constants.ORG_SCOPE));
    }

    @Test
    void resolveOrgScope_nonCcaL0_multipleRootOrgIds_resolvesCustom() {
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID), userGroupRef(OTHER_GROUP_ID)));
        mockGroupWithRootOrgIds(VALID_GROUP_ID, ORG_ID);
        mockGroupWithRootOrgIds(OTHER_GROUP_ID, OTHER_ORG_ID);
        mockL0Org(ORG_ID);

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertTrue(errors.isEmpty());
        assertEquals(Constants.CUSTOM, request.get(Constants.ORG_SCOPE));
    }

    @Test
    void resolveOrgScope_nonCcaL0_noCriteriaOnGroup_returnsRootOrgCriteriaMissingError() {
        // collectCriteria rejects a criteria-less group for any non-CCA caller (L0 or not)
        // before org-scope application ever runs, so this never reaches applyL0OrgScope's
        // own ERR_NO_ROOT_ORG_ID check.
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));
        mockGroupWithNoCriteria(VALID_GROUP_ID);

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertEquals(List.of(Constants.ERR_ROOT_ORG_CRITERIA_MISSING), errors);
    }

    // ---------------- ministryOrStateId usage ----------------

    @Test
    void resolveOrgScope_ministryOrStateIdOnL0Org_resolvesAllAndPopulatesMinistryIds() {
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));
        mockGroupWithMinistryOrStateIds(VALID_GROUP_ID, OTHER_ORG_ID);
        mockL0Org(OTHER_ORG_ID);
        mockNonL0Org(ORG_ID);
        Set<String> ministryOrStateIdsOut = new HashSet<>();

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, new HashSet<>(), ministryOrStateIdsOut);

        assertTrue(errors.isEmpty());
        assertEquals(Constants.ALL, request.get(Constants.ORG_SCOPE));
        assertEquals(Set.of(OTHER_ORG_ID), ministryOrStateIdsOut);
    }

    @Test
    void resolveOrgScope_ministryOrStateIdTargetNotL0_returnsError() {
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));
        mockGroupWithMinistryOrStateIds(VALID_GROUP_ID, OTHER_ORG_ID);
        mockNonL0Org(OTHER_ORG_ID);

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertEquals(List.of(String.format(Constants.ERR_ORG_NOT_L0, OTHER_ORG_ID)), errors);
    }

    @Test
    void resolveOrgScope_ministryOrStateIdTargetNotFound_returnsError() {
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));
        mockGroupWithMinistryOrStateIds(VALID_GROUP_ID, OTHER_ORG_ID);
        // No org mock - OTHER_ORG_ID will not be found

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertEquals(List.of(String.format(Constants.ERR_ORG_NOT_FOUND_FOR_L0_VALIDATION, OTHER_ORG_ID)), errors);
    }

    @Test
    void resolveOrgScope_bothRootOrgAndMinistryUsedByL0Caller_returnsError() {
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID), userGroupRef(OTHER_GROUP_ID)));
        mockGroupWithRootOrgIds(VALID_GROUP_ID, ORG_ID);
        mockGroupWithMinistryOrStateIds(OTHER_GROUP_ID, OTHER_ORG_ID);
        mockL0Org(OTHER_ORG_ID);
        mockL0Org(ORG_ID);

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertEquals(List.of(Constants.ERR_BOTH_ROOT_ORG_AND_MINISTRY_USED), errors);
    }

    @Test
    void resolveOrgScope_nullOutputSets_doesNotThrow() {
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));
        mockGroupWithRootOrgIds(VALID_GROUP_ID, ORG_ID);
        mockNonL0Org(ORG_ID);

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertTrue(errors.isEmpty());
    }

    @Test
    void resolveOrgScope_nonCcaNonL0_flagEnabled_multipleRootOrgIds_resolvesCustom() {
        when(serverProperties.isUserGroupAllowMultipleRootOrgIds()).thenReturn(true);
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID), userGroupRef(OTHER_GROUP_ID)));
        mockGroupWithRootOrgIds(VALID_GROUP_ID, ORG_ID);
        mockGroupWithRootOrgIds(OTHER_GROUP_ID, OTHER_ORG_ID);
        mockNonL0Org(ORG_ID);

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertTrue(errors.isEmpty());
        assertEquals(Constants.CUSTOM, request.get(Constants.ORG_SCOPE));
    }

    @Test
    void resolveOrgScope_nonCcaNonL0_flagEnabled_singleRootOrgIdMismatch_resolvesSingle() {
        when(serverProperties.isUserGroupAllowMultipleRootOrgIds()).thenReturn(true);
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));
        mockGroupWithRootOrgIds(VALID_GROUP_ID, OTHER_ORG_ID);
        mockNonL0Org(ORG_ID);

        List<String> errors = orgScopeService.resolveOrgScope(request, false, ORG_ID, null, null);

        assertTrue(errors.isEmpty());
        assertEquals(Constants.SINGLE, request.get(Constants.ORG_SCOPE));
    }

    // ---------------- resolveOrgScopeForTargetedOrg (AI CBP admin flow) ----------------
    // ORG_ID plays the targetedOrganisation; groups are stubbed under ORG_ID's partition only.

    @Test
    void targetedOrg_userGroupsReadFromTargetedOrgPartition() {
        mockNonL0Org(ORG_ID);
        mockGroupWithRootOrgIds(VALID_GROUP_ID, ORG_ID);
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));

        orgScopeService.resolveOrgScopeForTargetedOrg(request, false, ORG_ID, null, null);

        verify(userGroupLookupService).fetchUserGroupsByIds(List.of(VALID_GROUP_ID), ORG_ID);
    }

    @Test
    void targetedOrg_groupOwnedByAnotherOrg_notFound() {
        mockedGroups.put(VALID_GROUP_ID, uniqueGroupEntity(VALID_GROUP_ID));
        setupBatchFetchStub();
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));

        List<String> errors = orgScopeService.resolveOrgScopeForTargetedOrg(request, false, OTHER_ORG_ID, null, null);

        assertEquals(List.of(Constants.ERR_USER_GROUP_NOT_FOUND + VALID_GROUP_ID), errors);
        assertFalse(request.containsKey(Constants.ORG_ID_LIST));
    }

    @Test
    void targetedOrg_inlineCriteriaFormat_rejected() {
        Map<String, Object> inlineGroup = new HashMap<>();
        inlineGroup.put(Constants.USER_GROUP_NAME, "group");
        Map<String, Object> request = requestWithGroups(List.of(inlineGroup));

        List<String> errors = orgScopeService.resolveOrgScopeForTargetedOrg(request, false, ORG_ID, null, null);

        assertEquals(List.of(Constants.ERR_AICBP_USER_GROUP_ID_REQUIRED), errors);
        verifyNoInteractions(cbPlanRequestValidator);
    }

    // Scenario 1: single MDO
    @Test
    void targetedOrg_scenario1_mdoWithOwnRootOrgId_single() {
        mockNonL0Org(ORG_ID);
        mockGroupWithRootOrgIds(VALID_GROUP_ID, ORG_ID);
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));
        Set<String> rootOrgIdsOut = new HashSet<>();

        List<String> errors = orgScopeService.resolveOrgScopeForTargetedOrg(request, false, ORG_ID, rootOrgIdsOut, null);

        assertTrue(errors.isEmpty());
        assertEquals(Constants.SINGLE, request.get(Constants.ORG_SCOPE));
        assertEquals(List.of(ORG_ID), request.get(Constants.ORG_ID_LIST));
        assertEquals(Set.of(ORG_ID), rootOrgIdsOut);
    }

    @Test
    void targetedOrg_scenario1_mdoWithOtherRootOrgId_rejectedEvenWhenMultipleRootOrgIdsAllowed() {
        lenient().when(serverProperties.isUserGroupAllowMultipleRootOrgIds()).thenReturn(true);
        mockNonL0Org(ORG_ID);
        mockGroupWithRootOrgIds(VALID_GROUP_ID, OTHER_ORG_ID);
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));

        List<String> errors = orgScopeService.resolveOrgScopeForTargetedOrg(request, false, ORG_ID, null, null);

        assertEquals(List.of(String.format(Constants.ERR_AICBP_ROOT_ORG_ID_MISMATCH, ORG_ID)), errors);
        assertFalse(request.containsKey(Constants.ORG_ID_LIST));
    }

    @Test
    void targetedOrg_scenario1_mdoWithMultipleRootOrgIds_rejected() {
        mockNonL0Org(ORG_ID);
        mockGroupWithRootOrgIds(VALID_GROUP_ID, ORG_ID, OTHER_ORG_ID);
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));

        List<String> errors = orgScopeService.resolveOrgScopeForTargetedOrg(request, false, ORG_ID, null, null);

        assertEquals(List.of(String.format(Constants.ERR_AICBP_ROOT_ORG_ID_MISMATCH, ORG_ID)), errors);
    }

    @Test
    void targetedOrg_mdoWithoutCriteria_rejected() {
        mockGroupWithNoCriteria(VALID_GROUP_ID);
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));

        List<String> errors = orgScopeService.resolveOrgScopeForTargetedOrg(request, false, ORG_ID, null, null);

        assertEquals(List.of(Constants.ERR_ROOT_ORG_CRITERIA_MISSING), errors);
    }

    // Scenario 2: all users of a ministry/state
    @Test
    void targetedOrg_scenario2_l0WithOwnMinistryOrStateId_all() {
        mockL0Org(ORG_ID);
        mockGroupWithMinistryOrStateIds(VALID_GROUP_ID, ORG_ID);
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));
        Set<String> ministryOut = new HashSet<>();

        List<String> errors = orgScopeService.resolveOrgScopeForTargetedOrg(request, false, ORG_ID, null, ministryOut);

        assertTrue(errors.isEmpty());
        assertEquals(Constants.ALL, request.get(Constants.ORG_SCOPE));
        assertEquals(List.of(ORG_ID), request.get(Constants.ORG_ID_LIST));
        assertEquals(Set.of(ORG_ID), ministryOut);
    }

    @Test
    void targetedOrg_scenario2_l0WithAnotherMinistryOrStateId_rejected() {
        mockL0Org(ORG_ID);
        mockL0Org(OTHER_ORG_ID);
        mockGroupWithMinistryOrStateIds(VALID_GROUP_ID, OTHER_ORG_ID);
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));

        List<String> errors = orgScopeService.resolveOrgScopeForTargetedOrg(request, false, ORG_ID, null, null);

        assertEquals(List.of(String.format(Constants.ERR_AICBP_MINISTRY_OR_STATE_ID_MISMATCH, ORG_ID)), errors);
        assertFalse(request.containsKey(Constants.ORG_SCOPE));
    }

    @Test
    void targetedOrg_scenario2_nonL0TargetWithMinistryOrStateId_rejected() {
        mockNonL0Org(ORG_ID);
        mockL0Org(OTHER_ORG_ID);
        mockGroupWithMinistryOrStateIds(VALID_GROUP_ID, OTHER_ORG_ID);
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));

        List<String> errors = orgScopeService.resolveOrgScopeForTargetedOrg(request, false, ORG_ID, null, null);

        assertEquals(List.of(String.format(Constants.ERR_AICBP_TARGET_NOT_MINISTRY_OR_STATE, ORG_ID)), errors);
    }

    // Scenario 3: ministry/state with a specific list of orgs
    @Test
    void targetedOrg_scenario3_l0WithRootOrgIdList_custom() {
        mockL0Org(ORG_ID);
        mockGroupWithRootOrgIds(VALID_GROUP_ID, ORG_ID, OTHER_ORG_ID);
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));

        List<String> errors = orgScopeService.resolveOrgScopeForTargetedOrg(request, false, ORG_ID, null, null);

        assertTrue(errors.isEmpty());
        assertEquals(Constants.CUSTOM, request.get(Constants.ORG_SCOPE));
        assertEquals(List.of(ORG_ID), request.get(Constants.ORG_ID_LIST));
    }

    @Test
    void targetedOrg_l0MixingRootOrgIdAndMinistryOrStateId_rejected() {
        mockL0Org(ORG_ID);
        mockGroupWithRootOrgIds(VALID_GROUP_ID, OTHER_ORG_ID);
        mockGroupWithMinistryOrStateIds(OTHER_GROUP_ID, ORG_ID);
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID), userGroupRef(OTHER_GROUP_ID)));

        List<String> errors = orgScopeService.resolveOrgScopeForTargetedOrg(request, false, ORG_ID, null, null);

        assertEquals(List.of(Constants.ERR_BOTH_ROOT_ORG_AND_MINISTRY_USED), errors);
    }

    // Scenario 4: CCA, all orgs
    @Test
    void targetedOrg_scenario4_ccaWithEmptyRootOrgId_all() {
        mockGroupWithNoCriteria(VALID_GROUP_ID);
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));

        List<String> errors = orgScopeService.resolveOrgScopeForTargetedOrg(request, true, ORG_ID, null, null);

        assertTrue(errors.isEmpty());
        assertEquals(Constants.ALL, request.get(Constants.ORG_SCOPE));
        assertEquals(List.of(ORG_ID), request.get(Constants.ORG_ID_LIST));
        verifyNoInteractions(userAndOrgService);
    }

    // Scenario 5: CCA with a specific list of orgs
    @Test
    void targetedOrg_scenario5_ccaWithRootOrgIdList_custom() {
        mockGroupWithRootOrgIds(VALID_GROUP_ID, ORG_ID, OTHER_ORG_ID);
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));

        List<String> errors = orgScopeService.resolveOrgScopeForTargetedOrg(request, true, ORG_ID, null, null);

        assertTrue(errors.isEmpty());
        assertEquals(Constants.CUSTOM, request.get(Constants.ORG_SCOPE));
        assertEquals(List.of(ORG_ID), request.get(Constants.ORG_ID_LIST));
    }

    @Test
    void targetedOrg_scenario5_ccaWithSingleOtherOrg_single() {
        mockGroupWithRootOrgIds(VALID_GROUP_ID, OTHER_ORG_ID);
        Map<String, Object> request = requestWithGroups(List.of(userGroupRef(VALID_GROUP_ID)));

        List<String> errors = orgScopeService.resolveOrgScopeForTargetedOrg(request, true, ORG_ID, null, null);

        assertTrue(errors.isEmpty());
        assertEquals(Constants.SINGLE, request.get(Constants.ORG_SCOPE));
        assertEquals(List.of(ORG_ID), request.get(Constants.ORG_ID_LIST));
    }
}
