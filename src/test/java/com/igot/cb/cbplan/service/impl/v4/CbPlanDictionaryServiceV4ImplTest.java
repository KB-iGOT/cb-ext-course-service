package com.igot.cb.cbplan.service.impl.v4;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.CbPlanCacheMgrV3;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cbplan.service.impl.CbPlanDataTransformServiceV3Impl;
import com.igot.cb.cbplan.service.impl.CbPlanEnrichmentServiceV3Impl;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Test class for CbPlanDictionaryServiceV4Impl.
 * Tests user dictionary API, access control evaluation, caching, and V3/V4 compatibility.
 */
@ExtendWith(MockitoExtension.class)
class CbPlanDictionaryServiceV4ImplTest {

    private static final String TEST_USER_ID = "user_123";
    private static final String TEST_ORG_ID = "org_001";
    private static final String TEST_PLAN_YEAR = "2026-27";
    private static final String TEST_AUTH_TOKEN = "valid_token";
    private static final String TEST_PLAN_ID = "plan_001";

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private CbPlanCacheMgrV3 cbPlanCacheMgrV3;

    @Mock
    private CbPlanUserGroupLookupServiceV4Impl userGroupLookupService;

    @Mock
    private AccessTokenValidator accessTokenValidator;

    @Mock
    private RedisCacheMgr redisCacheMgr;

    @Mock
    private CbExtServerProperties serverProperties;

    @Mock
    private CbPlanEnrichmentServiceV3Impl enrichmentService;

    @Mock
    private CbPlanDataTransformServiceV3Impl dataTransformService;

    @Spy
    private ObjectMapper mapper = new ObjectMapper();

    @InjectMocks
    private CbPlanDictionaryServiceV4Impl dictionaryService;

    private ApiRequest testRequest;

    @BeforeEach
    void setUp() {
        testRequest = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.REQUEST_PARAM_PLAN_YEAR, TEST_PLAN_YEAR);
        testRequest.setRequest(requestMap);

        lenient().when(serverProperties.getCbPlanV3RedisCacheTtlSeconds()).thenReturn(3600);
        lenient().when(serverProperties.getCassandraQueryLimitPrimaryKey()).thenReturn(1);
    }

    @Test
    void getCBPlanDictionaryForUser_invalidToken_returnsUnauthorized() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(null);

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(response.getParams().getErr()).contains("Invalid or missing authentication token");
        verifyNoInteractions(redisCacheMgr, cbPlanCacheMgrV3);
    }

    @Test
    void getCBPlanDictionaryForUser_blankToken_returnsUnauthorized() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn("");

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        verifyNoInteractions(redisCacheMgr);
    }

    @Test
    void getCBPlanDictionaryForUser_invalidPlanYearFormat_returnsBadRequest() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(TEST_USER_ID);

        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.REQUEST_PARAM_PLAN_YEAR, "invalid-year");
        testRequest.setRequest(requestMap);

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getParams().getErr()).contains("Invalid planYear format");
        verifyNoInteractions(cbPlanCacheMgrV3);
    }

    @Test
    void getCBPlanDictionaryForUser_cacheHit_returnsCachedResponse() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(TEST_USER_ID);

        String cacheKey = Constants.CB_PLAN_V4_REDIS_KEY_PREFIX + TEST_USER_ID + ":" + TEST_PLAN_YEAR + ":dict";
        String cachedJson = "{\"" + TEST_PLAN_YEAR + "\":{\"aparPlanList\":{},\"nonAparPlanList\":{}}}";
        when(redisCacheMgr.getFromCache(cacheKey)).thenReturn(cachedJson);

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getResult()).containsKey(TEST_PLAN_YEAR);
        verify(redisCacheMgr, times(1)).getFromCache(cacheKey);
        verifyNoInteractions(cassandraOperation, cbPlanCacheMgrV3);
    }

    @Test
    void getCBPlanDictionaryForUser_userNotFound_returnsBadRequest() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(TEST_USER_ID);
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER),
                any(Map.class),
                any(List.class),
                anyInt()
        )).thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getParams().getErr()).contains("User does not exist");
    }

    @Test
    void getCBPlanDictionaryForUser_noPlans_returnsEmptyLists() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> yearResult = (Map<String, Object>) response.getResult().get(TEST_PLAN_YEAR);
        assertThat(yearResult.get(Constants.RESPONSE_KEY_APAR_PLAN_LIST)).isEqualTo(Collections.emptyMap());
        assertThat(yearResult.get(Constants.RESPONSE_KEY_NON_APAR_PLAN_LIST)).isEqualTo(Collections.emptyMap());
    }

    @Test
    void getCBPlanDictionaryForUser_withAparAndNonAparPlans_partitionsCorrectly() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> aparPlan = createMockPlan("plan_apar", true, null);
        Map<String, Object> nonAparPlan = createMockPlan("plan_non_apar", false, null);

        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(List.of(aparPlan, nonAparPlan));
        lenient().when(userGroupLookupService.fetchUserGroupsByIds(anyList(), eq(TEST_ORG_ID)))
                .thenReturn(Collections.emptyMap());
        lenient().when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> yearResult = (Map<String, Object>) response.getResult().get(TEST_PLAN_YEAR);
        Map<String, Map<String, Object>> aparList = (Map<String, Map<String, Object>>) yearResult.get(Constants.RESPONSE_KEY_APAR_PLAN_LIST);
        Map<String, Map<String, Object>> nonAparList = (Map<String, Map<String, Object>>) yearResult.get(Constants.RESPONSE_KEY_NON_APAR_PLAN_LIST);

        assertThat(aparList).hasSize(1);
        assertThat(nonAparList).hasSize(1);
        assertThat(aparList).containsKey("plan_apar");
        assertThat(nonAparList).containsKey("plan_non_apar");
        assertThat(aparList.get("plan_apar").get(Constants.PLAN_ID)).isEqualTo("plan_apar");
        assertThat(nonAparList.get("plan_non_apar").get(Constants.PLAN_ID)).isEqualTo("plan_non_apar");
    }

    @Test
    void getCBPlanDictionaryForUser_v4AccessControl_evaluatesUserGroups() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> v4Plan = createV4PlanWithUserGroups("plan_v4", List.of("ug_123"));
        Map<String, Object> userGroup = createMockUserGroup("ug_123", List.of(
                Map.of("department", List.of("HR")),
                Map.of("designation", List.of("Manager"))
        ));

        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(List.of(v4Plan));
        when(userGroupLookupService.fetchUserGroupsByIds(eq(List.of("ug_123")), eq(TEST_ORG_ID)))
                .thenReturn(Map.of("ug_123", userGroup));
        lenient().when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        verify(userGroupLookupService, times(1)).fetchUserGroupsByIds(eq(List.of("ug_123")), eq(TEST_ORG_ID));
    }

    @Test
    void getCBPlanDictionaryForUser_v3AccessControl_evaluatesInlineCriteria() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> v3Plan = createV3PlanWithInlineCriteria("plan_v3", List.of(
                Map.of(
                        Constants.CRITERIA_KEY, "department",
                        Constants.CRITERIA_VALUE, List.of("HR")
                )
        ));

        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(List.of(v3Plan));
        lenient().when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        verifyNoInteractions(userGroupLookupService);
    }

    @Test
    void getCBPlanDictionaryForUser_noAccessControl_grantsAccessToAll() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> openPlan = createMockPlan("plan_open", false, null);
        openPlan.remove(Constants.CONTEXT_DATA_REQUEST);

        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(List.of(openPlan));
        lenient().when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> yearResult = (Map<String, Object>) response.getResult().get(TEST_PLAN_YEAR);
        Map<String, Map<String, Object>> nonAparList = (Map<String, Map<String, Object>>) yearResult.get(Constants.RESPONSE_KEY_NON_APAR_PLAN_LIST);
        assertThat(nonAparList).hasSize(1);
    }

    @Test
    void getCBPlanDictionaryForUser_ministryPlans_mergesWithOrgPlans() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(TEST_USER_ID);
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> userRecord = new HashMap<>();
        userRecord.put(Constants.ID, TEST_USER_ID);
        userRecord.put(Constants.ROOT_ORG_ID, TEST_ORG_ID);

        Map<String, Object> profileDetails = new HashMap<>();
        profileDetails.put(Constants.ROOT_ORG_ID, TEST_ORG_ID);
        try {
            userRecord.put(Constants.PROFILE_DETAILS.toLowerCase(), mapper.writeValueAsString(profileDetails));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER),
                any(Map.class),
                any(List.class),
                anyInt()
        )).thenReturn(List.of(userRecord));

        doAnswer(invocation -> {
            Map<String, String> profile = invocation.getArgument(0);
            profile.put(Constants.MINISTRY_OR_STATE_ID_RQST, "ministry_001");
            return null;
        }).when(enrichmentService).extractMinistryOrStateDetails(any(Map.class), any(Map.class));

        Map<String, Object> orgPlan = createMockPlan("plan_org", false, null);
        Map<String, Object> ministryPlan = createMockPlan("plan_ministry", false, null);

        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(List.of(orgPlan));
        when(cbPlanCacheMgrV3.getCbPlanForMinistryOrStateId(eq("ministry_001"), eq(TEST_PLAN_YEAR)))
                .thenReturn(List.of(ministryPlan));
        when(dataTransformService.mergePlanLists(anyList(), anyList()))
                .thenReturn(List.of(orgPlan, ministryPlan));
        lenient().when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        verify(cbPlanCacheMgrV3, times(1)).getCbPlanForMinistryOrStateId(eq("ministry_001"), eq(TEST_PLAN_YEAR));
        verify(dataTransformService, times(1)).mergePlanLists(anyList(), anyList());
    }

    @Test
    void getCBPlanDictionaryForUser_orgDetailsEnrichment_populatesOrgNamesAndLogos() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> plan = createMockPlan("plan_001", false, null);
        plan.put(Constants.ORG_ID_LIST, List.of("org_creator"));

        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(List.of(plan));

        Map<String, Object> orgRecord = new HashMap<>();
        orgRecord.put(Constants.ID, "org_creator");
        orgRecord.put(Constants.ORG_NAME, "Creator Organization");
        orgRecord.put(Constants.LOGO, "https://example.com/logo.png");

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.ORG_TABLE),
                any(Map.class),
                eq(List.of(Constants.ID, Constants.ORG_NAME, Constants.LOGO)),
                eq(null)
        )).thenReturn(List.of(orgRecord));

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> yearResult = (Map<String, Object>) response.getResult().get(TEST_PLAN_YEAR);
        Map<String, Map<String, Object>> nonAparList = (Map<String, Map<String, Object>>) yearResult.get(Constants.RESPONSE_KEY_NON_APAR_PLAN_LIST);
        Map<String, Object> planEntry = nonAparList.values().iterator().next();
        assertThat(planEntry).containsEntry(Constants.CREATED_BY_ORG_NAME, "Creator Organization");
        assertThat(planEntry).containsEntry(Constants.CREATED_BY_ORG_LOGO, "https://example.com/logo.png");
    }

    @Test
    void getCBPlanDictionaryForUser_cachesResult_afterSuccessfulFetch() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        doAnswer(invocation -> {
            AtomicBoolean atomicBoolean = invocation.getArgument(2);
            atomicBoolean.set(true);
            return Collections.emptyList();
        }).when(cbPlanCacheMgrV3).getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class));

        dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        String expectedCacheKey = Constants.CB_PLAN_V4_REDIS_KEY_PREFIX + TEST_USER_ID + ":" + TEST_PLAN_YEAR + ":dict";
        verify(redisCacheMgr, times(1)).putInCache(eq(expectedCacheKey), anyString(), anyInt());
    }

    @Test
    void getCBPlanDictionaryForUser_exception_returnsInternalServerError() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(TEST_USER_ID);
        when(redisCacheMgr.getFromCache(anyString())).thenThrow(new RuntimeException("Redis error"));

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getParams().getErr()).contains("Failed to fetch CB Plan dictionary");
    }

    @Test
    void getCBPlanDictionaryForUser_blankPlanYear_usesCurrentFinancialYear() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(TEST_USER_ID);

        Map<String, Object> requestMap = new HashMap<>();
        testRequest.setRequest(requestMap);

        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> userRecord = new HashMap<>();
        userRecord.put(Constants.ID, TEST_USER_ID);
        userRecord.put(Constants.ROOT_ORG_ID, TEST_ORG_ID);

        Map<String, Object> profileDetails = new HashMap<>();
        profileDetails.put(Constants.ROOT_ORG_ID, TEST_ORG_ID);
        try {
            userRecord.put(Constants.PROFILE_DETAILS.toLowerCase(), mapper.writeValueAsString(profileDetails));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER),
                any(Map.class),
                any(List.class),
                anyInt()
        )).thenReturn(List.of(userRecord));

        doAnswer(invocation -> null).when(enrichmentService).extractMinistryOrStateDetails(any(Map.class), any(Map.class));
        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), anyString(), any(AtomicBoolean.class)))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getResult()).isNotEmpty();
    }

    @Test
    void getCBPlanDictionaryForUser_userProfileFromRedis_usesCache() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(TEST_USER_ID);

        String userCacheKey = Constants.USER + ":basicProfile:" + TEST_USER_ID;
        String dictCacheKey = Constants.CB_PLAN_V4_REDIS_KEY_PREFIX + TEST_USER_ID + ":" + TEST_PLAN_YEAR + ":dict";
        String cachedUserProfile = "{\"id\":\"" + TEST_USER_ID + "\",\"rootOrgId\":\"" + TEST_ORG_ID + "\"}";

        when(redisCacheMgr.getFromCache(eq(userCacheKey))).thenReturn(cachedUserProfile);
        when(redisCacheMgr.getFromCache(eq(dictCacheKey))).thenReturn(null);

        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(Collections.emptyList());
        lenient().doAnswer(invocation -> null).when(enrichmentService).extractMinistryOrStateDetails(any(Map.class), any(Map.class));

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        verify(redisCacheMgr, times(1)).getFromCache(eq(userCacheKey));
        verify(cassandraOperation, never()).getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER),
                any(),
                any(),
                anyInt()
        );
    }

    @Test
    void getCBPlanDictionaryForUser_multipleUserGroups_batchFetchesAll() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> plan1 = createV4PlanWithUserGroups("plan_1", List.of("ug_1", "ug_2"));
        Map<String, Object> plan2 = createV4PlanWithUserGroups("plan_2", List.of("ug_2", "ug_3"));

        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(List.of(plan1, plan2));
        when(userGroupLookupService.fetchUserGroupsByIds(anyList(), eq(TEST_ORG_ID)))
                .thenReturn(Collections.emptyMap());
        lenient().when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(Collections.emptyList());

        dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        verify(userGroupLookupService, times(1)).fetchUserGroupsByIds(
                argThat(list -> list.size() == 3 && list.containsAll(List.of("ug_1", "ug_2", "ug_3"))),
                eq(TEST_ORG_ID)
        );
    }

    @Test
    void getCBPlanDictionaryForUser_accessControlDenied_excludesPlan() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(TEST_USER_ID);
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> userRecord = new HashMap<>();
        userRecord.put(Constants.ID, TEST_USER_ID);
        userRecord.put(Constants.ROOT_ORG_ID, TEST_ORG_ID);

        Map<String, Object> profileDetails = new HashMap<>();
        profileDetails.put("department", "Finance");
        profileDetails.put(Constants.ROOT_ORG_ID, TEST_ORG_ID);
        try {
            userRecord.put(Constants.PROFILE_DETAILS.toLowerCase(), mapper.writeValueAsString(profileDetails));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER),
                any(Map.class),
                any(List.class),
                anyInt()
        )).thenReturn(List.of(userRecord));

        doAnswer(invocation -> null).when(enrichmentService).extractMinistryOrStateDetails(any(Map.class), any(Map.class));

        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> hrOnlyPlan = createV4PlanWithUserGroups("plan_hr", List.of("ug_hr"));
        Map<String, Object> hrGroup = createMockUserGroup("ug_hr", List.of(
                Map.of("department", List.of("HR"))
        ));

        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(List.of(hrOnlyPlan));
        when(userGroupLookupService.fetchUserGroupsByIds(anyList(), eq(TEST_ORG_ID)))
                .thenReturn(Map.of("ug_hr", hrGroup));
        lenient().when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> yearResult = (Map<String, Object>) response.getResult().get(TEST_PLAN_YEAR);
        Map<String, Map<String, Object>> nonAparList = (Map<String, Map<String, Object>>) yearResult.get(Constants.RESPONSE_KEY_NON_APAR_PLAN_LIST);
        assertThat(nonAparList).isEmpty();
    }

    private void setupValidUserProfileMocks() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(TEST_USER_ID);
        lenient().when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> userRecord = new HashMap<>();
        userRecord.put(Constants.ID, TEST_USER_ID);
        userRecord.put(Constants.ROOT_ORG_ID, TEST_ORG_ID);

        Map<String, Object> profileDetails = new HashMap<>();
        profileDetails.put("department", "Engineering");
        profileDetails.put("designation", "Manager");
        profileDetails.put(Constants.ROOT_ORG_ID, TEST_ORG_ID);

        try {
            userRecord.put(Constants.PROFILE_DETAILS.toLowerCase(), mapper.writeValueAsString(profileDetails));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        lenient().when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER),
                any(Map.class),
                any(List.class),
                anyInt()
        )).thenReturn(List.of(userRecord));

        lenient().doAnswer(invocation -> null).when(enrichmentService).extractMinistryOrStateDetails(any(Map.class), any(Map.class));
    }

    private Map<String, Object> createMockPlan(String planId, boolean isApar, String contextData) {
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.PLAN_ID, planId);
        plan.put(Constants.NAME, "Test Plan " + planId);
        plan.put(Constants.IS_APAR, isApar);
        plan.put(Constants.ORG_ID_LIST, List.of(TEST_ORG_ID));

        if (contextData != null) {
            plan.put(Constants.CONTEXT_DATA_REQUEST, contextData);
        }

        return plan;
    }

    private Map<String, Object> createV4PlanWithUserGroups(String planId, List<String> userGroupIds) {
        Map<String, Object> plan = createMockPlan(planId, false, null);

        List<Map<String, Object>> userGroups = new ArrayList<>();
        for (String ugId : userGroupIds) {
            userGroups.add(Map.of(Constants.USER_GROUP_ID, ugId));
        }

        Map<String, Object> accessControl = Map.of(Constants.USER_GROUPS, userGroups);
        Map<String, Object> contextData = Map.of(Constants.ACCESS_CONTROL, accessControl);

        try {
            plan.put(Constants.CONTEXT_DATA_REQUEST, mapper.writeValueAsString(contextData));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return plan;
    }

    private Map<String, Object> createV3PlanWithInlineCriteria(String planId, List<Map<String, Object>> criteriaList) {
        Map<String, Object> plan = createMockPlan(planId, false, null);

        Map<String, Object> userGroup = Map.of(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        Map<String, Object> accessControl = Map.of(Constants.USER_GROUPS, List.of(userGroup));
        Map<String, Object> contextData = Map.of(Constants.ACCESS_CONTROL, accessControl);

        try {
            plan.put(Constants.CONTEXT_DATA_REQUEST, mapper.writeValueAsString(contextData));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return plan;
    }

    private Map<String, Object> createMockUserGroup(String userGroupId, List<Map<String, List<String>>> criteria) {
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.COL_USERGROUPID, userGroupId);
        userGroup.put(Constants.COL_ORGID, TEST_ORG_ID);
        userGroup.put("criteria", criteria);
        return userGroup;
    }

    @Test
    void getCBPlanDictionaryForUser_v3ContentList_addsMandatoryFalse() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> planWithV3ContentList = createMockPlan("plan_v3_content", false, null);
        planWithV3ContentList.put(Constants.CONTENT_LIST, List.of(
                "do_114376977434968064182",
                "do_114378386987417600180"
        ));

        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(List.of(planWithV3ContentList));
        lenient().when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> yearResult = (Map<String, Object>) response.getResult().get(TEST_PLAN_YEAR);
        Map<String, Map<String, Object>> nonAparList = (Map<String, Map<String, Object>>) yearResult.get(Constants.RESPONSE_KEY_NON_APAR_PLAN_LIST);

        Map<String, Object> plan = nonAparList.get("plan_v3_content");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) plan.get(Constants.CONTENT_LIST);

        assertThat(contentList).hasSize(2);
        assertThat(contentList.get(0)).containsEntry(Constants.IDENTIFIER, "do_114376977434968064182");
        assertThat(contentList.get(0)).containsEntry(Constants.MANDATORY, false);
        assertThat(contentList.get(1)).containsEntry(Constants.IDENTIFIER, "do_114378386987417600180");
        assertThat(contentList.get(1)).containsEntry(Constants.MANDATORY, false);
    }

    @Test
    void getCBPlanDictionaryForUser_v4ContentList_preservesMandatoryField() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> planWithV4ContentList = createMockPlan("plan_v4_content", false, null);
        planWithV4ContentList.put(Constants.CONTENT_LIST, List.of(
                "{\"identifier\":\"do_114467322178428928111\",\"mandatory\":true}",
                "{\"identifier\":\"do_114378386987417600180\",\"mandatory\":false}"
        ));

        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(List.of(planWithV4ContentList));
        lenient().when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> yearResult = (Map<String, Object>) response.getResult().get(TEST_PLAN_YEAR);
        Map<String, Map<String, Object>> nonAparList = (Map<String, Map<String, Object>>) yearResult.get(Constants.RESPONSE_KEY_NON_APAR_PLAN_LIST);

        Map<String, Object> plan = nonAparList.get("plan_v4_content");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) plan.get(Constants.CONTENT_LIST);

        assertThat(contentList).hasSize(2);
        assertThat(contentList.get(0)).containsEntry(Constants.IDENTIFIER, "do_114467322178428928111");
        assertThat(contentList.get(0)).containsEntry(Constants.MANDATORY, true);
        assertThat(contentList.get(1)).containsEntry(Constants.IDENTIFIER, "do_114378386987417600180");
        assertThat(contentList.get(1)).containsEntry(Constants.MANDATORY, false);
    }

    @Test
    void getCBPlanDictionaryForUser_emptyContentList_returnsEmptyList() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> planWithEmptyContentList = createMockPlan("plan_empty_content", false, null);
        planWithEmptyContentList.put(Constants.CONTENT_LIST, Collections.emptyList());

        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(List.of(planWithEmptyContentList));
        lenient().when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> yearResult = (Map<String, Object>) response.getResult().get(TEST_PLAN_YEAR);
        Map<String, Map<String, Object>> nonAparList = (Map<String, Map<String, Object>>) yearResult.get(Constants.RESPONSE_KEY_NON_APAR_PLAN_LIST);

        Map<String, Object> plan = nonAparList.get("plan_empty_content");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) plan.get(Constants.CONTENT_LIST);

        assertThat(contentList).isEmpty();
    }

    @Test
    void getCBPlanDictionaryForUser_nullContentList_returnsEmptyList() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> planWithNullContentList = createMockPlan("plan_null_content", false, null);
        planWithNullContentList.put(Constants.CONTENT_LIST, null);

        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(List.of(planWithNullContentList));
        lenient().when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> yearResult = (Map<String, Object>) response.getResult().get(TEST_PLAN_YEAR);
        Map<String, Map<String, Object>> nonAparList = (Map<String, Map<String, Object>>) yearResult.get(Constants.RESPONSE_KEY_NON_APAR_PLAN_LIST);

        Map<String, Object> plan = nonAparList.get("plan_null_content");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) plan.get(Constants.CONTENT_LIST);

        assertThat(contentList).isEmpty();
    }

    @Test
    void getCBPlanDictionaryForUser_invalidJsonContentList_treatsAsV3WithMandatoryFalse() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> planWithInvalidJson = createMockPlan("plan_invalid_json", false, null);
        planWithInvalidJson.put(Constants.CONTENT_LIST, List.of(
                "{\"id\":\"do_123\"}",
                "plain_string_do_456"
        ));

        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(List.of(planWithInvalidJson));
        lenient().when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> yearResult = (Map<String, Object>) response.getResult().get(TEST_PLAN_YEAR);
        Map<String, Map<String, Object>> nonAparList = (Map<String, Map<String, Object>>) yearResult.get(Constants.RESPONSE_KEY_NON_APAR_PLAN_LIST);

        Map<String, Object> plan = nonAparList.get("plan_invalid_json");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) plan.get(Constants.CONTENT_LIST);

        assertThat(contentList).hasSize(2);
        assertThat(contentList.get(0)).containsEntry(Constants.IDENTIFIER, "{\"id\":\"do_123\"}");
        assertThat(contentList.get(0)).containsEntry(Constants.MANDATORY, false);
        assertThat(contentList.get(1)).containsEntry(Constants.IDENTIFIER, "plain_string_do_456");
        assertThat(contentList.get(1)).containsEntry(Constants.MANDATORY, false);
    }

    @Test
    void getCBPlanDictionaryForUser_singleItemV3ContentList_addsMandatoryFalse() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> planWithSingleItem = createMockPlan("plan_single_item", false, null);
        planWithSingleItem.put(Constants.CONTENT_LIST, List.of("do_1143558909548953601106"));

        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(List.of(planWithSingleItem));
        lenient().when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> yearResult = (Map<String, Object>) response.getResult().get(TEST_PLAN_YEAR);
        Map<String, Map<String, Object>> nonAparList = (Map<String, Map<String, Object>>) yearResult.get(Constants.RESPONSE_KEY_NON_APAR_PLAN_LIST);

        Map<String, Object> plan = nonAparList.get("plan_single_item");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) plan.get(Constants.CONTENT_LIST);

        assertThat(contentList).hasSize(1);
        assertThat(contentList.get(0)).containsEntry(Constants.IDENTIFIER, "do_1143558909548953601106");
        assertThat(contentList.get(0)).containsEntry(Constants.MANDATORY, false);
    }

    @Test
    void getCBPlanDictionaryForUser_aparPlanWithV4ContentList_preservesMandatory() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> aparPlanWithContentList = createMockPlan("apar_plan_content", true, null);
        aparPlanWithContentList.put(Constants.CONTENT_LIST, List.of(
                "{\"identifier\":\"do_apar_123\",\"mandatory\":true}"
        ));

        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(List.of(aparPlanWithContentList));
        lenient().when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> yearResult = (Map<String, Object>) response.getResult().get(TEST_PLAN_YEAR);
        Map<String, Map<String, Object>> aparList = (Map<String, Map<String, Object>>) yearResult.get(Constants.RESPONSE_KEY_APAR_PLAN_LIST);

        Map<String, Object> plan = aparList.get("apar_plan_content");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) plan.get(Constants.CONTENT_LIST);

        assertThat(contentList).hasSize(1);
        assertThat(contentList.get(0)).containsEntry(Constants.IDENTIFIER, "do_apar_123");
        assertThat(contentList.get(0)).containsEntry(Constants.MANDATORY, true);
    }

    @Test
    void getCBPlanDictionaryForUser_nestedObjectContentList_skipsTransformation() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> planWithNestedObjects = createMockPlan("plan_nested_obj", false, null);
        Map<String, Object> content1 = new HashMap<>();
        content1.put(Constants.IDENTIFIER, "do_114467322178428928111");
        content1.put(Constants.MANDATORY, true);

        Map<String, Object> content2 = new HashMap<>();
        content2.put(Constants.IDENTIFIER, "do_114378386987417600180");
        content2.put(Constants.MANDATORY, false);

        planWithNestedObjects.put(Constants.CONTENT_LIST, List.of(content1, content2));

        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(List.of(planWithNestedObjects));
        lenient().when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> yearResult = (Map<String, Object>) response.getResult().get(TEST_PLAN_YEAR);
        Map<String, Map<String, Object>> nonAparList = (Map<String, Map<String, Object>>) yearResult.get(Constants.RESPONSE_KEY_NON_APAR_PLAN_LIST);

        Map<String, Object> plan = nonAparList.get("plan_nested_obj");
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) plan.get(Constants.CONTENT_LIST);

        assertThat(contentList).hasSize(2);
        assertThat(contentList.get(0)).containsEntry(Constants.IDENTIFIER, "do_114467322178428928111");
        assertThat(contentList.get(0)).containsEntry(Constants.MANDATORY, true);
        assertThat(contentList.get(1)).containsEntry(Constants.IDENTIFIER, "do_114378386987417600180");
        assertThat(contentList.get(1)).containsEntry(Constants.MANDATORY, false);
    }

    @Test
    void getCBPlanDictionaryForUser_mixedContentListFormats_handlesAll() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> v3Plan = createMockPlan("plan_v3_mix", false, null);
        v3Plan.put(Constants.CONTENT_LIST, List.of("do_v3_plain"));

        Map<String, Object> v4Plan = createMockPlan("plan_v4_mix", false, null);
        v4Plan.put(Constants.CONTENT_LIST, List.of("{\"identifier\":\"do_v4_json\",\"mandatory\":true}"));

        Map<String, Object> nestedPlan = createMockPlan("plan_nested_mix", false, null);
        Map<String, Object> nestedContent = new HashMap<>();
        nestedContent.put(Constants.IDENTIFIER, "do_nested_obj");
        nestedContent.put(Constants.MANDATORY, false);
        nestedPlan.put(Constants.CONTENT_LIST, List.of(nestedContent));

        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(List.of(v3Plan, v4Plan, nestedPlan));
        lenient().when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> yearResult = (Map<String, Object>) response.getResult().get(TEST_PLAN_YEAR);
        Map<String, Map<String, Object>> nonAparList = (Map<String, Map<String, Object>>) yearResult.get(Constants.RESPONSE_KEY_NON_APAR_PLAN_LIST);

        assertThat(nonAparList).hasSize(3);

        List<Map<String, Object>> v3ContentList = (List<Map<String, Object>>) nonAparList.get("plan_v3_mix").get(Constants.CONTENT_LIST);
        assertThat(v3ContentList.get(0)).containsEntry(Constants.IDENTIFIER, "do_v3_plain");
        assertThat(v3ContentList.get(0)).containsEntry(Constants.MANDATORY, false);

        List<Map<String, Object>> v4ContentList = (List<Map<String, Object>>) nonAparList.get("plan_v4_mix").get(Constants.CONTENT_LIST);
        assertThat(v4ContentList.get(0)).containsEntry(Constants.IDENTIFIER, "do_v4_json");
        assertThat(v4ContentList.get(0)).containsEntry(Constants.MANDATORY, true);

        List<Map<String, Object>> nestedContentList = (List<Map<String, Object>>) nonAparList.get("plan_nested_mix").get(Constants.CONTENT_LIST);
        assertThat(nestedContentList.get(0)).containsEntry(Constants.IDENTIFIER, "do_nested_obj");
        assertThat(nestedContentList.get(0)).containsEntry(Constants.MANDATORY, false);
    }

    @Test
    @SuppressWarnings("unchecked")
    void getCBPlanDictionaryForUser_planYearProvidedNoCurrentData_fetchesPreviousYearData() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        Map<String, Object> prevYearPlan = createMockPlan("plan_prev_001", false, null);
        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(Collections.emptyList());
        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq("2025-26"), any(AtomicBoolean.class)))
                .thenReturn(List.of(prevYearPlan));
        lenient().when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getResult()).containsKeys(TEST_PLAN_YEAR, "2025-26");
        Map<String, Object> currentYearResult = (Map<String, Object>) response.getResult().get(TEST_PLAN_YEAR);
        assertThat(currentYearResult.get(Constants.RESPONSE_KEY_APAR_PLAN_COUNT)).isEqualTo(0);
        assertThat(currentYearResult.get(Constants.RESPONSE_KEY_NON_APAR_PLAN_COUNT)).isEqualTo(0);
        Map<String, Object> prevYearResult = (Map<String, Object>) response.getResult().get("2025-26");
        Map<String, Map<String, Object>> prevNonAparList =
                (Map<String, Map<String, Object>>) prevYearResult.get(Constants.RESPONSE_KEY_NON_APAR_PLAN_LIST);
        assertThat(prevNonAparList).containsKey("plan_prev_001");
    }

    @Test
    @SuppressWarnings("unchecked")
    void getCBPlanDictionaryForUser_planYearProvidedNoPlansBothYears_returnsBothYearsEmpty() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(Collections.emptyList());
        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq("2025-26"), any(AtomicBoolean.class)))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getResult()).containsKeys(TEST_PLAN_YEAR, "2025-26");
        Map<String, Object> currentYearResult = (Map<String, Object>) response.getResult().get(TEST_PLAN_YEAR);
        Map<String, Object> prevYearResult = (Map<String, Object>) response.getResult().get("2025-26");
        assertThat(currentYearResult.get(Constants.RESPONSE_KEY_APAR_PLAN_COUNT)).isEqualTo(0);
        assertThat(currentYearResult.get(Constants.RESPONSE_KEY_NON_APAR_PLAN_COUNT)).isEqualTo(0);
        assertThat(prevYearResult.get(Constants.RESPONSE_KEY_APAR_PLAN_COUNT)).isEqualTo(0);
        assertThat(prevYearResult.get(Constants.RESPONSE_KEY_NON_APAR_PLAN_COUNT)).isEqualTo(0);
    }

    @Test
    void getCBPlanDictionaryForUser_planYearNotProvided_noPlans_noFallbackTriggered() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(TEST_USER_ID);
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        Map<String, Object> userRecord = new HashMap<>();
        userRecord.put(Constants.ID, TEST_USER_ID);
        userRecord.put(Constants.ROOT_ORG_ID, TEST_ORG_ID);
        try {
            userRecord.put(Constants.PROFILE_DETAILS.toLowerCase(),
                    mapper.writeValueAsString(Map.of(Constants.ROOT_ORG_ID, TEST_ORG_ID)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER),
                any(Map.class), any(List.class), anyInt()))
                .thenReturn(List.of(userRecord));
        doAnswer(invocation -> null).when(enrichmentService)
                .extractMinistryOrStateDetails(any(Map.class), any(Map.class));
        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), anyString(), any(AtomicBoolean.class)))
                .thenReturn(Collections.emptyList());

        ApiRequest noYearRequest = new ApiRequest();
        noYearRequest.setRequest(new HashMap<>());
        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(noYearRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getResult()).hasSize(1);
        verify(cbPlanCacheMgrV3, times(1)).getCbPlanForAllAndOrgId(any(), anyString(), any());
    }

    @Test
    void getCBPlanDictionaryForUser_planYearProvidedNoCurrentData_previousYearKeyIsCorrect() {
        setupValidUserProfileMocks();
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq(TEST_PLAN_YEAR), any(AtomicBoolean.class)))
                .thenReturn(Collections.emptyList());
        lenient().when(cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(eq(TEST_ORG_ID), eq("2025-26"), any(AtomicBoolean.class)))
                .thenReturn(Collections.emptyList());

        ApiResponse response = dictionaryService.getCBPlanDictionaryForUser(testRequest, TEST_AUTH_TOKEN);

        assertThat(response.getResult()).containsKey("2025-26");
        assertThat(response.getResult()).doesNotContainKey("2024-25");
    }
}
