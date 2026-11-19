package com.igot.cb.cbplan.service.impl.v4;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.CbExtRedisCacheMgr;
import com.igot.cb.cache.CbPlanCacheMgrV4;
import com.igot.cb.cache.CbPlanCacheVersions;
import com.igot.cb.cache.CbPlanVersionMgr;
import com.igot.cb.cache.UserGroupCacheMgrV4;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.cbplan.service.impl.CbPlanContentLookupServiceV3Impl;
import com.igot.cb.cbplan.service.impl.CbPlanDataTransformServiceV3Impl;
import com.igot.cb.cbplan.service.impl.CbPlanEnrichmentServiceV3Impl;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Test class for CbPlanDictionaryPreviousPostingServiceV4Impl.
 * Covers the supplied-posting evaluation semantics, the candidate fetch across the user's own and
 * the supplied org, the dedicated prevdict cache, the previous-year fallback and access control.
 */
@ExtendWith(MockitoExtension.class)
class CbPlanDictionaryPreviousPostingServiceV4ImplTest {

    private static final String TEST_USER_ID = "user_123";
    private static final String TEST_ORG_ID = "org_current_001";
    private static final String PREVIOUS_ORG_ID = "org_previous_002";
    private static final String TEST_PLAN_YEAR = "2026-27";
    private static final String PREVIOUS_PLAN_YEAR = "2025-26";
    private static final String TEST_AUTH_TOKEN = "valid_token";
    private static final String TEST_PLAN_ID = "plan_001";
    private static final String SECOND_PLAN_ID = "plan_002";
    private static final String TEST_USER_GROUP_ID = "ug_001";
    private static final String TEST_CA_ID = "ca_do_live_001";
    private static final String CURRENT_DESIGNATION = "Manager";
    private static final String PREVIOUS_DESIGNATION = "Director";
    private static final String CURRENT_SERVICE = "IAS";
    private static final String PREVIOUS_DICT_KEY_SUFFIX = ":prevdict";
    private static final String STANDARD_DICT_KEY_SUFFIX = ":dict";
    private static final String INVALID_PLAN_YEAR = "20xx";
    private static final String RETIRED_STATUS = "Retired";
    private static final String TEST_MINISTRY_ID = "ministry_001";
    private static final String CONTENT_ID = "do_content_001";
    private static final String TRUE_VALUE = "true";
    private static final String FALSE_VALUE = "false";

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private CbPlanCacheMgrV4 cbPlanCacheMgrV4;

    @Mock
    private UserGroupCacheMgrV4 userGroupCacheMgrV4;

    @Mock
    private AccessTokenValidator accessTokenValidator;

    @Mock
    private CbExtRedisCacheMgr redisCacheMgr;

    @Mock
    private CbExtRedisCacheMgr userProfileRedisCacheMgr;

    @Mock
    private CbExtServerProperties serverProperties;

    @Mock
    private CbPlanEnrichmentServiceV3Impl enrichmentService;

    @Mock
    private CbPlanDataTransformServiceV3Impl dataTransformService;

    @Mock
    private CbPlanContentLookupServiceV3Impl contentLookupService;

    @Spy
    private ObjectMapper mapper = new ObjectMapper();

    private CbPlanVersionMgr versionMgr;

    private CbPlanDictionaryPreviousPostingServiceV4Impl previousPostingService;

    @BeforeEach
    void setUp() throws Exception {
        versionMgr = new CbPlanVersionMgr(redisCacheMgr);
        previousPostingService = new CbPlanDictionaryPreviousPostingServiceV4Impl(cassandraOperation,
                cbPlanCacheMgrV4, versionMgr, userGroupCacheMgrV4, accessTokenValidator, userProfileRedisCacheMgr,
                redisCacheMgr, serverProperties, enrichmentService, dataTransformService, contentLookupService);

        lenient().when(serverProperties.getCbPlanV4DictionaryRedisCacheTtlSeconds()).thenReturn(3600);
        lenient().when(serverProperties.getCassandraQueryLimitPrimaryKey()).thenReturn(1);
        lenient().when(serverProperties.isUserGroupAllowEmptyRootOrgIds()).thenReturn(false);
        lenient().when(contentLookupService.getContentMetadata(anyString())).thenReturn(buildLiveMetadata());
        lenient().when(dataTransformService.mergePlanLists(anyList(), anyList()))
                .thenAnswer(invocation -> {
                    List<Map<String, Object>> merged = new ArrayList<>(invocation.getArgument(0));
                    merged.addAll(invocation.getArgument(1));
                    return merged;
                });
        lenient().when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(Collections.emptyList());
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_nullToken_returnsUnauthorized() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(null);

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(response.getParams().getErr()).contains("Invalid or missing authentication token");
        verifyNoInteractions(redisCacheMgr, cbPlanCacheMgrV4);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_blankToken_returnsUnauthorized() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn("");

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        verifyNoInteractions(cbPlanCacheMgrV4);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_invalidPlanYear_returnsBadRequest() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(TEST_USER_ID);

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(INVALID_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getParams().getStatus()).isEqualTo(Constants.FAILED);
        verifyNoInteractions(cbPlanCacheMgrV4);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_userNotFound_returnsBadRequest() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(TEST_USER_ID);
        lenient().when(userProfileRedisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER),
                anyMap(), anyList(), anyInt())).thenReturn(Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getParams().getErr()).contains("User does not exist");
        verifyNoInteractions(cbPlanCacheMgrV4);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_planTargetsPreviousOrg_isListed() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_planTargetsCurrentOrg_isNotListed() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, TEST_ORG_ID, false);
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(TEST_ORG_ID, Constants.ROOT_ORG_ID, List.of(TEST_ORG_ID));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_planTargetsPreviousDesignation_isListed() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, TEST_ORG_ID, false);
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubUserGroup(TEST_ORG_ID, Constants.DESIGNATION, List.of(PREVIOUS_DESIGNATION));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, null, PREVIOUS_DESIGNATION), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_planTargetsCurrentDesignation_isNotListed() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, TEST_ORG_ID, false);
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubUserGroup(TEST_ORG_ID, Constants.DESIGNATION, List.of(CURRENT_DESIGNATION));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, null, PREVIOUS_DESIGNATION), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_onlyOrgSupplied_designationStaysCurrent() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroupWithCriteria(PREVIOUS_ORG_ID, Map.of(
                Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID),
                Constants.DESIGNATION, List.of(CURRENT_DESIGNATION)));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_nonOverlaidField_matchedAgainstCurrentProfile() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroupWithCriteria(PREVIOUS_ORG_ID, Map.of(
                Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID),
                Constants.SERVICE, List.of(CURRENT_SERVICE)));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_noPreviousPostingSupplied_evaluatesCurrentProfile() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, TEST_ORG_ID, false);
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubUserGroup(TEST_ORG_ID, Constants.ROOT_ORG_ID, List.of(TEST_ORG_ID));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, null, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
        verify(cbPlanCacheMgrV4, times(1))
                .getCbPlanForAllAndOrgId(anyString(), anyString(), any(AtomicBoolean.class), any());
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_previousOrgDiffersFromCurrent_fetchesBothOrgs() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> currentOrgPlan = createV4Plan(TEST_PLAN_ID, TEST_ORG_ID, false);
        Map<String, Object> previousOrgPlan = createV4Plan(SECOND_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, List.of(currentOrgPlan));
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(previousOrgPlan));
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID));
        stubUserGroup(TEST_ORG_ID, Constants.ROOT_ORG_ID, List.of(TEST_ORG_ID));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        verify(cbPlanCacheMgrV4).getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR),
                any(AtomicBoolean.class), any());
        verify(cbPlanCacheMgrV4).getCbPlanForAllAndOrgId(eq(PREVIOUS_ORG_ID), eq(TEST_PLAN_YEAR),
                any(AtomicBoolean.class), any());
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(SECOND_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_previousOrgSameAsCurrent_fetchesOnlyOnce() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, TEST_ORG_ID, false);
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubUserGroup(TEST_ORG_ID, Constants.ROOT_ORG_ID, List.of(TEST_ORG_ID));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, TEST_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        verify(cbPlanCacheMgrV4, times(1))
                .getCbPlanForAllAndOrgId(anyString(), anyString(), any(AtomicBoolean.class), any());
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_planWithoutContextData_isAccessible() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createBasePlan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_unparseableContextData_isNotAccessible() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createBasePlan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        plan.put(Constants.CONTEXT_DATA_REQUEST, "{not-valid-json");
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_emptyUserGroups_isNotAccessible() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createBasePlan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        plan.put(Constants.CONTEXT_DATA_REQUEST, writeJson(Map.of(Constants.ACCESS_CONTROL,
                Map.of(Constants.USER_GROUPS, Collections.emptyList()))));
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_userGroupNotResolved_isNotAccessible() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        when(userGroupCacheMgrV4.fetchUserGroupsByIds(anyList(), eq(PREVIOUS_ORG_ID)))
                .thenReturn(Collections.emptyMap());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_emptyCriteriaValuesWithFlagOn_isAccessible() {
        stubUserProfile(CURRENT_DESIGNATION);
        when(serverProperties.isUserGroupAllowEmptyRootOrgIds()).thenReturn(true);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_emptyCriteriaValuesWithFlagOff_isNotAccessible() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_v3InlineCriteriaMatching_isListed() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV3Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID,
                Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID));
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_v3InlineCriteriaNotMatching_isNotListed() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV3Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID,
                Constants.ROOT_ORG_ID, List.of(TEST_ORG_ID));
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_aparPlan_goesToAparList() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, true);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(aparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).isEmpty();
        assertThat(yearResult(response, TEST_PLAN_YEAR))
                .containsEntry(Constants.RESPONSE_KEY_APAR_PLAN_COUNT, 1);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_nonAparPlan_goesToNonAparList() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
        assertThat(aparPlans(response, TEST_PLAN_YEAR)).isEmpty();
        assertThat(yearResult(response, TEST_PLAN_YEAR))
                .containsEntry(Constants.RESPONSE_KEY_NON_APAR_PLAN_COUNT, 1);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_cacheHit_returnsCachedResultWithoutFetching() {
        stubUserProfileWithoutToken(CURRENT_DESIGNATION);
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(TEST_USER_ID);
        Map<String, Object> cachedPayload = new HashMap<>();
        cachedPayload.put(TEST_PLAN_YEAR, Map.of(Constants.RESPONSE_KEY_APAR_PLAN_COUNT, 0));
        CbPlanCacheVersions versions = versionMgr.getVersions(TEST_ORG_ID, null, TEST_PLAN_YEAR);
        String stampedJson = writeJson(versionMgr.stamp(cachedPayload, versions));
        when(redisCacheMgr.getFromCache(expectedCacheKey())).thenReturn(stampedJson);

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getResult()).containsKey(TEST_PLAN_YEAR);
        verify(cbPlanCacheMgrV4, never())
                .getCbPlanForAllAndOrgId(anyString(), anyString(), any(AtomicBoolean.class), any());
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_cacheKeyUsesPrevdictSuffix_notStandardDictKey() {
        stubUserProfile(CURRENT_DESIGNATION);
        stubOrgPlansWithCacheEnabled(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());

        previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        verify(redisCacheMgr).getFromCache(expectedCacheKey());
        assertThat(expectedCacheKey()).endsWith(PREVIOUS_DICT_KEY_SUFFIX)
                .doesNotEndWith(STANDARD_DICT_KEY_SUFFIX);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_lookupCacheEnabled_writesResultToPrevdictKey() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlansWithCacheEnabled(TEST_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID));

        previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        verify(redisCacheMgr).putInCache(eq(expectedCacheKey()), anyString(), eq(3600));
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_lookupCacheDisabled_doesNotWriteResult() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID));

        previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        verify(redisCacheMgr, never()).putInCache(anyString(), anyString(), anyInt());
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_noPlansAndPlanYearRequested_appendsPreviousYear() {
        stubUserProfile(CURRENT_DESIGNATION);
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubOrgPlans(TEST_ORG_ID, PREVIOUS_PLAN_YEAR, Collections.emptyList());
        stubOrgPlans(PREVIOUS_ORG_ID, PREVIOUS_PLAN_YEAR, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getResult()).containsKey(TEST_PLAN_YEAR).containsKey(PREVIOUS_PLAN_YEAR);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_noPlansAndNoPlanYearRequested_returnsSingleYearOnly() {
        stubUserProfile(CURRENT_DESIGNATION);
        lenient().when(cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(anyString(), anyString(),
                any(AtomicBoolean.class), any())).thenReturn(Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(null, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getResult()).hasSize(1);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_plansFoundButNoneAccessible_appendsPreviousYear() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubOrgPlans(TEST_ORG_ID, PREVIOUS_PLAN_YEAR, Collections.emptyList());
        stubOrgPlans(PREVIOUS_ORG_ID, PREVIOUS_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, List.of("org_unrelated"));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).isEmpty();
        assertThat(response.getResult()).containsKey(PREVIOUS_PLAN_YEAR);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_previousYearHasAccessiblePlan_isListed() {
        stubUserProfile(CURRENT_DESIGNATION);
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        Map<String, Object> plan = createV4Plan(SECOND_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(TEST_ORG_ID, PREVIOUS_PLAN_YEAR, Collections.emptyList());
        stubOrgPlans(PREVIOUS_ORG_ID, PREVIOUS_PLAN_YEAR, List.of(plan));
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, PREVIOUS_PLAN_YEAR)).containsKey(SECOND_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_liveCaLinkedId_isKept() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(planEntry(response, TEST_PLAN_YEAR, TEST_PLAN_ID))
                .containsEntry(Constants.CA_LINKED_ID, TEST_CA_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_nonLiveCaLinkedId_strippedButPlanKept() throws Exception {
        stubUserProfile(CURRENT_DESIGNATION);
        when(contentLookupService.getContentMetadata(TEST_CA_ID)).thenReturn(buildMetadata(RETIRED_STATUS));
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
        assertThat(planEntry(response, TEST_PLAN_YEAR, TEST_PLAN_ID))
                .doesNotContainKey(Constants.CA_LINKED_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_orgDetailsResolved_enrichesCreatedByOrgName() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID));
        Map<String, Object> orgRecord = new HashMap<>();
        orgRecord.put(Constants.ID, PREVIOUS_ORG_ID);
        orgRecord.put(Constants.ORG_NAME, "Previous Org");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.ORG_TABLE),
                anyMap(), anyList(), isNull())).thenReturn(List.of(orgRecord));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(planEntry(response, TEST_PLAN_YEAR, TEST_PLAN_ID))
                .containsEntry(Constants.CREATED_BY_ORG_NAME, "Previous Org");
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_unexpectedError_returnsInternalServerError() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(TEST_USER_ID);
        lenient().when(userProfileRedisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER),
                anyMap(), anyList(), anyInt())).thenThrow(new IllegalStateException("boom"));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getParams().getStatus()).isEqualTo(Constants.FAILED);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_illegalArgumentFromFetch_returnsBadRequest() {
        stubUserProfile(CURRENT_DESIGNATION);
        lenient().when(cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(anyString(), anyString(),
                any(AtomicBoolean.class), any())).thenThrow(new IllegalArgumentException("bad"));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getParams().getErr()).contains("Invalid request parameters");
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_profileFromRedisCache_skipsCassandraLookup() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(TEST_USER_ID);
        Map<String, Object> professionalDetail = new HashMap<>();
        professionalDetail.put(Constants.DESIGNATION, CURRENT_DESIGNATION);
        Map<String, Object> profileDetails = new HashMap<>();
        profileDetails.put(Constants.PROFESSIONAL_DETAILS, List.of(professionalDetail));
        Map<String, Object> cachedProfile = new HashMap<>();
        cachedProfile.put(Constants.ID, TEST_USER_ID);
        cachedProfile.put(Constants.ROOT_ORG_ID, TEST_ORG_ID);
        cachedProfile.put(Constants.PROFILE_DETAILS, profileDetails);
        String cachedProfileJson = writeJson(cachedProfile);
        when(userProfileRedisCacheMgr.getFromCache(anyString())).thenReturn(cachedProfileJson);
        lenient().doAnswer(invocation -> null).when(enrichmentService)
                .extractMinistryOrStateDetails(anyMap(), anyMap());
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.DESIGNATION, List.of(CURRENT_DESIGNATION));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
        verify(cassandraOperation, never()).getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER), anyMap(), anyList(), anyInt());
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_malformedCachedProfile_returnsInternalServerError() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(TEST_USER_ID);
        when(userProfileRedisCacheMgr.getFromCache(anyString())).thenReturn("{not-valid-json");

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getParams().getErr()).contains("Error parsing user profile data");
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_userOnMinistry_fetchesMinistryPlans() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(TEST_USER_ID);
        stubUserProfileWithMinistry();
        Map<String, Object> ministryPlan = createV4Plan(SECOND_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        when(cbPlanCacheMgrV4.getCbPlanForMinistryOrStateId(eq(TEST_MINISTRY_ID), eq(TEST_PLAN_YEAR), anyString()))
                .thenReturn(List.of(ministryPlan));
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(SECOND_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_contextDataAsMap_isEvaluated() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createBasePlan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        plan.put(Constants.CONTEXT_DATA_REQUEST, new HashMap<>(Map.of(Constants.ACCESS_CONTROL,
                new HashMap<>(Map.of(Constants.USER_GROUPS,
                        List.of(new HashMap<>(Map.of(Constants.USER_GROUP_ID, TEST_USER_GROUP_ID))))))));
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_contextDataWithoutAccessControl_isNotAccessible() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createBasePlan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        plan.put(Constants.CONTEXT_DATA_REQUEST, writeJson(Map.of(Constants.NAME, "no-access-control")));
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_accessControlWithoutUserGroups_isNotAccessible() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createBasePlan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        plan.put(Constants.CONTEXT_DATA_REQUEST, writeJson(Map.of(Constants.ACCESS_CONTROL,
                Map.of(Constants.NAME, "no-groups"))));
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_centralDeputationV4Matching_isListed() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.CENTRAL_DEPUTATION_LOWER_KEY, List.of(FALSE_VALUE));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_centralDeputationV4NotMatching_isNotListed() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.CENTRAL_DEPUTATION_LOWER_KEY, List.of(TRUE_VALUE));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_criteriaKeyMissingFromProfile_isNotListed() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.BATCH, List.of("2011"));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_userGroupWithNonMapCriteria_isSkipped() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.COL_USERGROUPID, TEST_USER_GROUP_ID);
        userGroup.put(Constants.COL_ORGID, PREVIOUS_ORG_ID);
        userGroup.put(Constants.COL_CRITERIA, List.of("not-a-map"));
        lenient().when(userGroupCacheMgrV4.fetchUserGroupsByIds(anyList(), eq(PREVIOUS_ORG_ID)))
                .thenReturn(Map.of(TEST_USER_GROUP_ID, userGroup));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_v3CentralDeputationMatching_isListed() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV3Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID,
                Constants.CENTRAL_DEPUTATION_LOWER_KEY, List.of(FALSE_VALUE));
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_v3CriteriaKeyMissingFromProfile_isNotListed() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV3Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID,
                Constants.BATCH, List.of("2011"));
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_v3CriteriaWithoutKey_isNotListed() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createBasePlan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        Map<String, Object> criteria = Map.of(Constants.CRITERIA_VALUE, List.of(PREVIOUS_ORG_ID));
        Map<String, Object> userGroup = Map.of(Constants.USER_GROUP_CRITERIA_LIST, List.of(criteria));
        plan.put(Constants.CONTEXT_DATA_REQUEST, writeJson(Map.of(Constants.ACCESS_CONTROL,
                Map.of(Constants.USER_GROUPS, List.of(userGroup)))));
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_v3EmptyCriteriaList_isNotListed() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createBasePlan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        Map<String, Object> userGroup = Map.of(Constants.USER_GROUP_CRITERIA_LIST, Collections.emptyList());
        plan.put(Constants.CONTEXT_DATA_REQUEST, writeJson(Map.of(Constants.ACCESS_CONTROL,
                Map.of(Constants.USER_GROUPS, List.of(userGroup)))));
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_contentListAsJsonStrings_isParsedToObjects() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        plan.put(Constants.CONTENT_LIST, List.of(
                writeJson(Map.of(Constants.IDENTIFIER, CONTENT_ID, Constants.MANDATORY, true))));
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(contentList(response, TEST_PLAN_ID)).hasSize(1);
        assertThat(asMap(contentList(response, TEST_PLAN_ID).get(0)))
                .containsEntry(Constants.IDENTIFIER, CONTENT_ID)
                .containsEntry(Constants.MANDATORY, true);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_contentListAsPlainIds_isConvertedWithMandatoryFalse() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        List<Object> rawContentList = new ArrayList<>();
        rawContentList.add(CONTENT_ID);
        rawContentList.add(null);
        plan.put(Constants.CONTENT_LIST, rawContentList);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(contentList(response, TEST_PLAN_ID)).hasSize(1);
        assertThat(asMap(contentList(response, TEST_PLAN_ID).get(0)))
                .containsEntry(Constants.IDENTIFIER, CONTENT_ID)
                .containsEntry(Constants.MANDATORY, false);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_contentListAlreadyObjects_isReturnedAsIs() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        plan.put(Constants.CONTENT_LIST, List.of(Map.of(Constants.IDENTIFIER, CONTENT_ID,
                Constants.MANDATORY, true)));
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(asMap(contentList(response, TEST_PLAN_ID).get(0)))
                .containsEntry(Constants.IDENTIFIER, CONTENT_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_contentListWithNonIdentifierJson_fallsBackToPlainId() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        String jsonWithoutIdentifier = writeJson(Map.of(Constants.NAME, "no-identifier"));
        plan.put(Constants.CONTENT_LIST, List.of(jsonWithoutIdentifier));
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(asMap(contentList(response, TEST_PLAN_ID).get(0)))
                .containsEntry(Constants.IDENTIFIER, jsonWithoutIdentifier)
                .containsEntry(Constants.MANDATORY, false);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_planWithoutOrgIdList_hasNullCreatedByOrgId() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createBasePlan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        plan.put(Constants.ORG_ID_LIST, Collections.emptyList());
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(planEntry(response, TEST_PLAN_YEAR, TEST_PLAN_ID))
                .containsEntry(Constants.CREATED_BY_ORG_ID, null);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_planWithoutCaLinkedId_skipsContentLookup() throws Exception {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createBasePlan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        plan.remove(Constants.CA_LINKED_ID_DB);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
        verify(contentLookupService, never()).getContentMetadata(anyString());
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_contentLookupThrows_caLinkedIdStripped() throws Exception {
        stubUserProfile(CURRENT_DESIGNATION);
        when(contentLookupService.getContentMetadata(TEST_CA_ID)).thenThrow(new IllegalStateException("down"));
        Map<String, Object> plan = createBasePlan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(planEntry(response, TEST_PLAN_YEAR, TEST_PLAN_ID))
                .doesNotContainKey(Constants.CA_LINKED_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_orgLookupThrows_plansStillReturned() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createBasePlan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.ORG_TABLE),
                anyMap(), anyList(), isNull())).thenThrow(new IllegalStateException("org lookup down"));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(planEntry(response, TEST_PLAN_YEAR, TEST_PLAN_ID))
                .containsEntry(Constants.CREATED_BY_ORG_NAME, null);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_planUserGroupsWithNonMapEntry_isSkipped() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createBasePlan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        plan.put(Constants.CONTEXT_DATA_REQUEST, writeJson(Map.of(Constants.ACCESS_CONTROL,
                Map.of(Constants.USER_GROUPS, List.of("not-a-map")))));
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_blankPreviousValues_evaluatesCurrentProfile() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, TEST_ORG_ID, false);
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubUserGroup(TEST_ORG_ID, Constants.ROOT_ORG_ID, List.of(TEST_ORG_ID));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, "   ", "  "), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_nonStringPreviousOrgId_evaluatesCurrentProfile() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, TEST_ORG_ID, false);
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubUserGroup(TEST_ORG_ID, Constants.ROOT_ORG_ID, List.of(TEST_ORG_ID));
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.REQUEST_PARAM_PLAN_YEAR, TEST_PLAN_YEAR);
        requestMap.put(Constants.REQUEST_PARAM_PREVIOUS_ROOT_ORG_ID, 42);
        request.setRequest(requestMap);

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                request, TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_requestNotAMap_usesDefaultPlanYear() {
        stubUserProfile(CURRENT_DESIGNATION);
        lenient().when(cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(anyString(), anyString(),
                any(AtomicBoolean.class), any())).thenReturn(Collections.emptyList());
        ApiRequest request = new ApiRequest();
        request.setRequest(null);

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                request, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getResult()).hasSize(1);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_criteriaValueListWithNullEntry_ignoresNull() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        List<String> valuesWithNull = new ArrayList<>();
        valuesWithNull.add(null);
        valuesWithNull.add(PREVIOUS_ORG_ID);
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.ROOT_ORG_ID, valuesWithNull);
        stubUserGroupWithRawCriteria(PREVIOUS_ORG_ID, criteria);

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_criteriaValueNull_treatedAsEmpty() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.DESIGNATION, null);
        stubUserGroupWithRawCriteria(PREVIOUS_ORG_ID, criteria);

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_criteriaValueScalar_isNormalisedToSingleton() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.ROOT_ORG_ID, PREVIOUS_ORG_ID);
        stubUserGroupWithRawCriteria(PREVIOUS_ORG_ID, criteria);

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_professionalDetailsNotAList_profileStillBuilt() {
        stubUserProfileWithRawDetails(Map.of(Constants.PROFESSIONAL_DETAILS, "not-a-list"));
        stubNoPlansForBothOrgs();

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_professionalDetailsEmptyList_profileStillBuilt() {
        stubUserProfileWithRawDetails(Map.of(Constants.PROFESSIONAL_DETAILS, Collections.emptyList()));
        stubNoPlansForBothOrgs();

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_professionalDetailsEntryNotAMap_profileStillBuilt() {
        stubUserProfileWithRawDetails(Map.of(Constants.PROFESSIONAL_DETAILS, List.of("not-a-map")));
        stubNoPlansForBothOrgs();

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_cadreBatchPresent_isMatchable() {
        stubUserProfileWithRawDetails(Map.of(Constants.CADRE_DETAILS,
                Map.of(Constants.CADRE_BATCH, 2011, Constants.CENTRAL_DEPUTATION, true)));
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.BATCH, List.of("2011"));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_noCadreDetails_defaultsCentralDeputationFalse() {
        stubUserProfileWithRawDetails(Map.of(Constants.PROFILE_STATUS_KEY, "VERIFIED"));
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.CENTRAL_DEPUTATION_LOWER_KEY, List.of(FALSE_VALUE));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_blankProfileDetails_profileStillBuilt() {
        stubUserRecordWithProfileDetails("   ");
        stubNoPlansForBothOrgs();

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_profileDetailsUnsupportedType_profileStillBuilt() {
        stubUserRecordWithProfileDetails(42);
        stubNoPlansForBothOrgs();

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_v3CentralDeputationEmptyValue_isNotListed() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV3Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID,
                Constants.CENTRAL_DEPUTATION_LOWER_KEY, Collections.emptyList());
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).containsKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_v3EmptyCriteriaValue_isNotListed() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV3Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID,
                Constants.ROOT_ORG_ID, Collections.emptyList());
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_userGroupWithoutCriteria_isNotListed() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.COL_USERGROUPID, TEST_USER_GROUP_ID);
        userGroup.put(Constants.COL_ORGID, PREVIOUS_ORG_ID);
        lenient().when(userGroupCacheMgrV4.fetchUserGroupsByIds(anyList(), eq(PREVIOUS_ORG_ID)))
                .thenReturn(Map.of(TEST_USER_GROUP_ID, userGroup));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_planWithBlankPlanId_isSkippedFromContextParsing() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        plan.put(Constants.PLAN_ID, "");
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(nonAparPlans(response, TEST_PLAN_YEAR)).doesNotContainKey(TEST_PLAN_ID);
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_emptyContentList_returnsEmptyList() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        plan.put(Constants.CONTENT_LIST, Collections.emptyList());
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(contentList(response, TEST_PLAN_ID)).isEmpty();
    }

    @Test
    void getDictionaryForUserWithPreviousPosting_contentListFirstItemMapWithoutIdentifier_isTransformed() {
        stubUserProfile(CURRENT_DESIGNATION);
        Map<String, Object> plan = createV4Plan(TEST_PLAN_ID, PREVIOUS_ORG_ID, false);
        plan.put(Constants.CONTENT_LIST, List.of(Map.of(Constants.NAME, "no-identifier-key")));
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, List.of(plan));
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubUserGroup(PREVIOUS_ORG_ID, Constants.ROOT_ORG_ID, List.of(PREVIOUS_ORG_ID));

        ApiResponse response = previousPostingService.getDictionaryForUserWithPreviousPosting(
                buildRequest(TEST_PLAN_YEAR, PREVIOUS_ORG_ID, null), TEST_AUTH_TOKEN);

        assertThat(asMap(contentList(response, TEST_PLAN_ID).get(0)))
                .containsEntry(Constants.MANDATORY, false);
    }

    private void stubNoPlansForBothOrgs() {
        stubOrgPlans(TEST_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubOrgPlans(PREVIOUS_ORG_ID, TEST_PLAN_YEAR, Collections.emptyList());
        stubOrgPlans(TEST_ORG_ID, PREVIOUS_PLAN_YEAR, Collections.emptyList());
        stubOrgPlans(PREVIOUS_ORG_ID, PREVIOUS_PLAN_YEAR, Collections.emptyList());
    }

    private void stubUserProfileWithRawDetails(Map<String, Object> profileDetails) {
        stubUserRecordWithProfileDetails(writeJson(profileDetails));
    }

    private void stubUserRecordWithProfileDetails(Object profileDetailsValue) {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(TEST_USER_ID);
        lenient().when(userProfileRedisCacheMgr.getFromCache(anyString())).thenReturn(null);
        Map<String, Object> userRecord = new HashMap<>();
        userRecord.put(Constants.ID, TEST_USER_ID);
        userRecord.put(Constants.ROOT_ORG_ID, TEST_ORG_ID);
        userRecord.put(Constants.PROFILE_DETAILS.toLowerCase(), profileDetailsValue);
        lenient().when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER),
                anyMap(), anyList(), anyInt())).thenReturn(List.of(userRecord));
        lenient().doAnswer(invocation -> null).when(enrichmentService)
                .extractMinistryOrStateDetails(anyMap(), anyMap());
    }

    private void stubUserGroupWithRawCriteria(String planOrgId, Map<String, Object> criteria) {
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.COL_USERGROUPID, TEST_USER_GROUP_ID);
        userGroup.put(Constants.COL_ORGID, planOrgId);
        userGroup.put(Constants.COL_CRITERIA, List.of(criteria));
        lenient().when(userGroupCacheMgrV4.fetchUserGroupsByIds(anyList(), eq(planOrgId)))
                .thenReturn(Map.of(TEST_USER_GROUP_ID, userGroup));
    }

    private List<Object> contentList(ApiResponse response, String planId) {
        Object value = planEntry(response, TEST_PLAN_YEAR, planId).get(Constants.CONTENT_LIST);
        assertThat(value).isInstanceOf(List.class);
        return new ArrayList<>((List<?>) value);
    }

    private void stubUserProfileWithMinistry() {
        lenient().when(userProfileRedisCacheMgr.getFromCache(anyString())).thenReturn(null);
        Map<String, Object> professionalDetail = new HashMap<>();
        professionalDetail.put(Constants.DESIGNATION, CURRENT_DESIGNATION);
        Map<String, Object> profileDetails = new HashMap<>();
        profileDetails.put(Constants.PROFESSIONAL_DETAILS, List.of(professionalDetail));
        Map<String, Object> userRecord = new HashMap<>();
        userRecord.put(Constants.ID, TEST_USER_ID);
        userRecord.put(Constants.ROOT_ORG_ID, TEST_ORG_ID);
        userRecord.put(Constants.PROFILE_DETAILS.toLowerCase(), writeJson(profileDetails));
        lenient().when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER),
                anyMap(), anyList(), anyInt())).thenReturn(List.of(userRecord));
        lenient().doAnswer(invocation -> {
            Map<String, String> profile = invocation.getArgument(0);
            profile.put(Constants.MINISTRY_OR_STATE_ID_RQST, TEST_MINISTRY_ID);
            return null;
        }).when(enrichmentService).extractMinistryOrStateDetails(anyMap(), anyMap());
    }

    private ApiRequest buildRequest(String planYear, String previousRootOrgId, String previousDesignation) {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        if (planYear != null) {
            requestMap.put(Constants.REQUEST_PARAM_PLAN_YEAR, planYear);
        }
        if (previousRootOrgId != null) {
            requestMap.put(Constants.REQUEST_PARAM_PREVIOUS_ROOT_ORG_ID, previousRootOrgId);
        }
        if (previousDesignation != null) {
            requestMap.put(Constants.REQUEST_PARAM_PREVIOUS_DESIGNATION, previousDesignation);
        }
        request.setRequest(requestMap);
        return request;
    }

    private String expectedCacheKey() {
        return Constants.CB_PLAN_V4_REDIS_KEY_PREFIX + TEST_ORG_ID + ":" + TEST_USER_ID + ":" + TEST_PLAN_YEAR
                + Constants.CB_PLAN_V4_PREV_POSTING_DICT_KEY_SUFFIX;
    }

    private void stubUserProfile(String designation) {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(TEST_USER_ID);
        stubUserProfileWithoutToken(designation);
    }

    private void stubUserProfileWithoutToken(String designation) {
        lenient().when(userProfileRedisCacheMgr.getFromCache(anyString())).thenReturn(null);
        Map<String, Object> professionalDetail = new HashMap<>();
        professionalDetail.put(Constants.DESIGNATION, designation);
        Map<String, Object> cadreDetails = new HashMap<>();
        cadreDetails.put(Constants.CIVIL_SERVICE_NAME, CURRENT_SERVICE);
        Map<String, Object> profileDetails = new HashMap<>();
        profileDetails.put(Constants.PROFESSIONAL_DETAILS, List.of(professionalDetail));
        profileDetails.put(Constants.CADRE_DETAILS, cadreDetails);
        Map<String, Object> userRecord = new HashMap<>();
        userRecord.put(Constants.ID, TEST_USER_ID);
        userRecord.put(Constants.ROOT_ORG_ID, TEST_ORG_ID);
        userRecord.put(Constants.PROFILE_DETAILS.toLowerCase(), writeJson(profileDetails));
        lenient().when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER),
                anyMap(), anyList(), anyInt())).thenReturn(List.of(userRecord));
        lenient().doAnswer(invocation -> null).when(enrichmentService)
                .extractMinistryOrStateDetails(anyMap(), anyMap());
    }

    private void stubOrgPlans(String orgId, String planYear, List<Map<String, Object>> plans) {
        lenient().when(cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(eq(orgId), eq(planYear),
                any(AtomicBoolean.class), any())).thenReturn(plans);
    }

    private void stubOrgPlansWithCacheEnabled(String orgId, String planYear, List<Map<String, Object>> plans) {
        lenient().when(cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(eq(orgId), eq(planYear),
                any(AtomicBoolean.class), any())).thenAnswer(invocation -> {
                    AtomicBoolean flag = invocation.getArgument(2);
                    flag.set(true);
                    return plans;
                });
    }

    private void stubUserGroup(String planOrgId, String criteriaKey, List<String> criteriaValues) {
        stubUserGroupWithCriteria(planOrgId, Map.of(criteriaKey, criteriaValues));
    }

    private void stubUserGroupWithCriteria(String planOrgId, Map<String, List<String>> criteria) {
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.COL_USERGROUPID, TEST_USER_GROUP_ID);
        userGroup.put(Constants.COL_ORGID, planOrgId);
        userGroup.put(Constants.COL_CRITERIA, List.of(criteria));
        lenient().when(userGroupCacheMgrV4.fetchUserGroupsByIds(anyList(), eq(planOrgId)))
                .thenReturn(Map.of(TEST_USER_GROUP_ID, userGroup));
    }

    private Map<String, Object> createBasePlan(String planId, String creatorOrgId, boolean isApar) {
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.PLAN_ID, planId);
        plan.put(Constants.NAME, "Test Plan " + planId);
        plan.put(Constants.IS_APAR, isApar);
        plan.put(Constants.ORG_ID_LIST, List.of(creatorOrgId));
        plan.put(Constants.CA_LINKED_ID_DB, TEST_CA_ID);
        return plan;
    }

    private Map<String, Object> createV4Plan(String planId, String creatorOrgId, boolean isApar) {
        Map<String, Object> plan = createBasePlan(planId, creatorOrgId, isApar);
        Map<String, Object> accessControl = Map.of(Constants.USER_GROUPS,
                List.of(Map.of(Constants.USER_GROUP_ID, TEST_USER_GROUP_ID)));
        plan.put(Constants.CONTEXT_DATA_REQUEST, writeJson(Map.of(Constants.ACCESS_CONTROL, accessControl)));
        return plan;
    }

    private Map<String, Object> createV3Plan(String planId, String creatorOrgId, String criteriaKey,
                                             List<String> criteriaValues) {
        Map<String, Object> plan = createBasePlan(planId, creatorOrgId, false);
        Map<String, Object> criteria = Map.of(Constants.CRITERIA_KEY, criteriaKey,
                Constants.CRITERIA_VALUE, criteriaValues);
        Map<String, Object> userGroup = Map.of(Constants.USER_GROUP_CRITERIA_LIST, List.of(criteria));
        Map<String, Object> accessControl = Map.of(Constants.USER_GROUPS, List.of(userGroup));
        plan.put(Constants.CONTEXT_DATA_REQUEST, writeJson(Map.of(Constants.ACCESS_CONTROL, accessControl)));
        return plan;
    }

    private Map<String, Object> buildLiveMetadata() {
        return buildMetadata(Constants.LIVE);
    }

    private Map<String, Object> buildMetadata(String status) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(Constants.STATUS, status);
        return metadata;
    }

    private String writeJson(Map<String, Object> value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize test fixture", e);
        }
    }

    private Map<String, Object> yearResult(ApiResponse response, String planYear) {
        return asMap(response.getResult().get(planYear));
    }

    private Map<String, Object> aparPlans(ApiResponse response, String planYear) {
        return asMap(yearResult(response, planYear).get(Constants.RESPONSE_KEY_APAR_PLAN_LIST));
    }

    private Map<String, Object> nonAparPlans(ApiResponse response, String planYear) {
        return asMap(yearResult(response, planYear).get(Constants.RESPONSE_KEY_NON_APAR_PLAN_LIST));
    }

    private Map<String, Object> planEntry(ApiResponse response, String planYear, String planId) {
        return asMap(nonAparPlans(response, planYear).get(planId));
    }

    /*
     * The service returns nested Map<String, Object> structures by contract; centralising the cast
     * here keeps the unchecked conversion to a single documented place instead of every assertion.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Object value) {
        assertThat(value).isInstanceOf(Map.class);
        return (Map<String, Object>) value;
    }

}
