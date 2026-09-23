package com.igot.cb.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cbplan.service.CbPlanServiceV4;
import com.igot.cb.cbplan.util.CbPlanYearUtil;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.CourseAccessServiceImpl;
import com.igot.cb.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.service.UserAndOrgServiceImpl;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ContentInfoUtilCbPlanV4Test {

    private static final String AUTH_TOKEN = "Bearer test-token";
    private static final String YEAR_KEY = "2024-25";
    private static final String PLAN_ID_APAR = "plan-apar-1";
    private static final String PLAN_ID_NON_APAR = "plan-non-apar-1";
    private static final String CONTENT_ID_1 = "do_content_1";
    private static final String CONTENT_ID_2 = "do_content_2";
    private static final String CONTENT_ID_3 = "do_content_3";

    @Mock
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    @Mock
    private UserAndOrgServiceImpl userAndOrgService;
    @Mock
    private RedisCacheMgr redisCacheMgr;
    @Mock
    private CourseAccessServiceImpl courseAccessService;
    @Mock
    private CbExtServerProperties serverProperties;
    @Mock
    private CbPlanServiceV4 cbPlanServiceV4;

    private ContentInfoUtil contentInfoUtil;

    @BeforeEach
    void setUp() {
        contentInfoUtil = new ContentInfoUtil(
                outboundRequestHandlerService,
                userAndOrgService,
                redisCacheMgr,
                new ObjectMapper(),
                courseAccessService,
                serverProperties,
                cbPlanServiceV4);
    }

    // ──────────────────────────────────────────────────────────────────────
    // null / empty response
    // ──────────────────────────────────────────────────────────────────────

    /** Verifies that a null ApiResponse from the CB plan service returns all three lists empty without throwing. */
    @Test
    void getCbPlanV4ContentIds_nullResponse_returnsEmptyLists() {
        when(cbPlanServiceV4.getCBPlanDictionaryForUser(any(ApiRequest.class), anyString()))
                .thenReturn(null);

        Map<String, List<String>> result = contentInfoUtil.getCbPlanV4ContentIds(AUTH_TOKEN);

        assertThat(result.get(Constants.APAR)).isEmpty();
        assertThat(result.get(Constants.TRAINING_PLAN)).isEmpty();
        assertThat(result.get(Constants.AI_CBP)).isEmpty();
    }

    /** Verifies that an ApiResponse whose result map is empty returns all three lists empty. */
    @Test
    void getCbPlanV4ContentIds_emptyResult_returnsEmptyLists() {
        ApiResponse response = new ApiResponse();
        response.setResult(Collections.emptyMap());
        when(cbPlanServiceV4.getCBPlanDictionaryForUser(any(ApiRequest.class), anyString()))
                .thenReturn(response);

        Map<String, List<String>> result = contentInfoUtil.getCbPlanV4ContentIds(AUTH_TOKEN);

        assertThat(result.get(Constants.APAR)).isEmpty();
        assertThat(result.get(Constants.TRAINING_PLAN)).isEmpty();
        assertThat(result.get(Constants.AI_CBP)).isEmpty();
    }

    // ──────────────────────────────────────────────────────────────────────
    // APAR plan – non-AI-CBP → goes to aparIds
    // ──────────────────────────────────────────────────────────────────────

    /** Verifies that a non-AI-CBP plan in aparPlanList routes its content identifiers to the apar bucket only. */
    @Test
    void getCbPlanV4ContentIds_aparPlanNonAiCbp_contentGoesToApar() {
        when(cbPlanServiceV4.getCBPlanDictionaryForUser(any(ApiRequest.class), anyString()))
                .thenReturn(buildResponse(YEAR_KEY,
                        planList(PLAN_ID_APAR, "APAR", List.of(CONTENT_ID_1)),
                        Collections.emptyMap()));

        Map<String, List<String>> result = contentInfoUtil.getCbPlanV4ContentIds(AUTH_TOKEN);

        assertThat(result.get(Constants.APAR)).containsExactly(CONTENT_ID_1);
        assertThat(result.get(Constants.TRAINING_PLAN)).isEmpty();
        assertThat(result.get(Constants.AI_CBP)).isEmpty();
    }

    // ──────────────────────────────────────────────────────────────────────
    // non-APAR plan – non-AI-CBP → goes to trainingPlanIds
    // ──────────────────────────────────────────────────────────────────────

    /** Verifies that a non-AI-CBP plan in nonAparPlanList routes its content identifiers to the trainingPlan bucket only. */
    @Test
    void getCbPlanV4ContentIds_nonAparPlanNonAiCbp_contentGoesToTrainingPlan() {
        when(cbPlanServiceV4.getCBPlanDictionaryForUser(any(ApiRequest.class), anyString()))
                .thenReturn(buildResponse(YEAR_KEY,
                        Collections.emptyMap(),
                        planList(PLAN_ID_NON_APAR, "trainingPlan", List.of(CONTENT_ID_2))));

        Map<String, List<String>> result = contentInfoUtil.getCbPlanV4ContentIds(AUTH_TOKEN);

        assertThat(result.get(Constants.APAR)).isEmpty();
        assertThat(result.get(Constants.TRAINING_PLAN)).containsExactly(CONTENT_ID_2);
        assertThat(result.get(Constants.AI_CBP)).isEmpty();
    }

    // ──────────────────────────────────────────────────────────────────────
    // AI-CBP plan → goes to aiCbpIds (regardless of aparPlanList bucket)
    // ──────────────────────────────────────────────────────────────────────

    /** Verifies that an AICBP-typed plan inside aparPlanList routes all its content to the aiCbp bucket, leaving apar empty. */
    @Test
    void getCbPlanV4ContentIds_aiCbpPlanInAparList_contentGoesToAiCbp() {
        when(cbPlanServiceV4.getCBPlanDictionaryForUser(any(ApiRequest.class), anyString()))
                .thenReturn(buildResponse(YEAR_KEY,
                        planList(PLAN_ID_APAR, Constants.PLAN_TYPE_AI_CBP, List.of(CONTENT_ID_1)),
                        Collections.emptyMap()));

        Map<String, List<String>> result = contentInfoUtil.getCbPlanV4ContentIds(AUTH_TOKEN);

        assertThat(result.get(Constants.AI_CBP)).containsExactly(CONTENT_ID_1);
        assertThat(result.get(Constants.APAR)).isEmpty();
        assertThat(result.get(Constants.TRAINING_PLAN)).isEmpty();
    }

    /** Verifies that an AICBP-typed plan inside nonAparPlanList routes all its content to the aiCbp bucket, leaving trainingPlan empty. */
    @Test
    void getCbPlanV4ContentIds_aiCbpPlanInNonAparList_contentGoesToAiCbp() {
        when(cbPlanServiceV4.getCBPlanDictionaryForUser(any(ApiRequest.class), anyString()))
                .thenReturn(buildResponse(YEAR_KEY,
                        Collections.emptyMap(),
                        planList(PLAN_ID_NON_APAR, Constants.PLAN_TYPE_AI_CBP, List.of(CONTENT_ID_2))));

        Map<String, List<String>> result = contentInfoUtil.getCbPlanV4ContentIds(AUTH_TOKEN);

        assertThat(result.get(Constants.AI_CBP)).containsExactly(CONTENT_ID_2);
        assertThat(result.get(Constants.APAR)).isEmpty();
        assertThat(result.get(Constants.TRAINING_PLAN)).isEmpty();
    }

    // ──────────────────────────────────────────────────────────────────────
    // AI-CBP IDs must be excluded from apar and trainingPlan lists
    // ──────────────────────────────────────────────────────────────────────

    /** Verifies the two-pass exclusion logic: an ID claimed by any AI-CBP plan must not appear in apar or trainingPlan, even if it also appears in a non-AI-CBP plan. */
    @Test
    void getCbPlanV4ContentIds_aiCbpContentExcludedFromAparAndTrainingPlan() {
        Map<String, Map<String, Object>> aparPlanList = new HashMap<>();
        aparPlanList.put(PLAN_ID_APAR, plan(Constants.PLAN_TYPE_AI_CBP, List.of(CONTENT_ID_1)));
        aparPlanList.put("plan-apar-2", plan("APAR", List.of(CONTENT_ID_1, CONTENT_ID_2)));

        Map<String, Map<String, Object>> nonAparPlanList = new HashMap<>();
        nonAparPlanList.put(PLAN_ID_NON_APAR, plan("trainingPlan", List.of(CONTENT_ID_1, CONTENT_ID_3)));

        when(cbPlanServiceV4.getCBPlanDictionaryForUser(any(ApiRequest.class), anyString()))
                .thenReturn(buildResponse(YEAR_KEY, aparPlanList, nonAparPlanList));

        Map<String, List<String>> result = contentInfoUtil.getCbPlanV4ContentIds(AUTH_TOKEN);

        assertThat(result.get(Constants.AI_CBP)).containsExactly(CONTENT_ID_1);
        assertThat(result.get(Constants.APAR)).containsExactly(CONTENT_ID_2);
        assertThat(result.get(Constants.TRAINING_PLAN)).containsExactly(CONTENT_ID_3);
    }

    // ──────────────────────────────────────────────────────────────────────
    // multiple year keys – content aggregated across all years
    // ──────────────────────────────────────────────────────────────────────

    /** Verifies that content from multiple year keys in the result map is aggregated into a single output list per bucket. */
    @Test
    void getCbPlanV4ContentIds_multipleYears_aggregatesContentAcrossAllYears() {
        ApiResponse response = new ApiResponse();
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("2023-24", yearEntry(
                planList("plan-a", "APAR", List.of(CONTENT_ID_1)),
                Collections.emptyMap()));
        resultMap.put("2024-25", yearEntry(
                planList("plan-b", "APAR", List.of(CONTENT_ID_2)),
                Collections.emptyMap()));
        response.setResult(resultMap);
        when(cbPlanServiceV4.getCBPlanDictionaryForUser(any(ApiRequest.class), anyString()))
                .thenReturn(response);

        Map<String, List<String>> result = contentInfoUtil.getCbPlanV4ContentIds(AUTH_TOKEN);

        assertThat(result.get(Constants.APAR)).containsExactlyInAnyOrder(CONTENT_ID_1, CONTENT_ID_2);
        assertThat(result.get(Constants.TRAINING_PLAN)).isEmpty();
        assertThat(result.get(Constants.AI_CBP)).isEmpty();
    }

    // ──────────────────────────────────────────────────────────────────────
    // empty contentList in a plan → no IDs added
    // ──────────────────────────────────────────────────────────────────────

    /** Verifies that plans with an empty contentList contribute no identifiers to any output bucket. */
    @Test
    void getCbPlanV4ContentIds_emptyContentListInPlan_noIdsAdded() {
        when(cbPlanServiceV4.getCBPlanDictionaryForUser(any(ApiRequest.class), anyString()))
                .thenReturn(buildResponse(YEAR_KEY,
                        planList(PLAN_ID_APAR, "APAR", Collections.emptyList()),
                        planList(PLAN_ID_NON_APAR, "trainingPlan", Collections.emptyList())));

        Map<String, List<String>> result = contentInfoUtil.getCbPlanV4ContentIds(AUTH_TOKEN);

        assertThat(result.get(Constants.APAR)).isEmpty();
        assertThat(result.get(Constants.TRAINING_PLAN)).isEmpty();
        assertThat(result.get(Constants.AI_CBP)).isEmpty();
    }

    // ──────────────────────────────────────────────────────────────────────
    // null planType → treated as non-AI-CBP (goes to apar or trainingPlan)
    // ──────────────────────────────────────────────────────────────────────

    /** Verifies that a null planType field is safely handled — equalsIgnoreCase(null) returns false, so the plan is treated as non-AI-CBP. */
    @Test
    void getCbPlanV4ContentIds_nullPlanType_treatedAsNonAiCbp() {
        when(cbPlanServiceV4.getCBPlanDictionaryForUser(any(ApiRequest.class), anyString()))
                .thenReturn(buildResponse(YEAR_KEY,
                        planList(PLAN_ID_APAR, null, List.of(CONTENT_ID_1)),
                        Collections.emptyMap()));

        Map<String, List<String>> result = contentInfoUtil.getCbPlanV4ContentIds(AUTH_TOKEN);

        assertThat(result.get(Constants.APAR)).containsExactly(CONTENT_ID_1);
        assertThat(result.get(Constants.AI_CBP)).isEmpty();
    }

    // ──────────────────────────────────────────────────────────────────────
    // exception from service → returns empty lists (no exception propagated)
    // ──────────────────────────────────────────────────────────────────────

    /** Verifies that a RuntimeException from the CB plan service is caught internally and all three output lists are returned empty. */
    @Test
    void getCbPlanV4ContentIds_serviceThrowsException_returnsEmptyListsSafely() {
        when(cbPlanServiceV4.getCBPlanDictionaryForUser(any(ApiRequest.class), anyString()))
                .thenThrow(new RuntimeException("downstream failure"));

        Map<String, List<String>> result = contentInfoUtil.getCbPlanV4ContentIds(AUTH_TOKEN);

        assertThat(result.get(Constants.APAR)).isEmpty();
        assertThat(result.get(Constants.TRAINING_PLAN)).isEmpty();
        assertThat(result.get(Constants.AI_CBP)).isEmpty();
    }

    // ──────────────────────────────────────────────────────────────────────
    // AICBP case-insensitive match
    // ──────────────────────────────────────────────────────────────────────

    /** Verifies that the AICBP planType match is case-insensitive — lowercase "aicbp" must route content to the aiCbp bucket. */
    @Test
    void getCbPlanV4ContentIds_aiCbpPlanTypeCaseInsensitive_recognisedAsAiCbp() {
        when(cbPlanServiceV4.getCBPlanDictionaryForUser(any(ApiRequest.class), anyString()))
                .thenReturn(buildResponse(YEAR_KEY,
                        planList(PLAN_ID_APAR, "aicbp", List.of(CONTENT_ID_1)),
                        Collections.emptyMap()));

        Map<String, List<String>> result = contentInfoUtil.getCbPlanV4ContentIds(AUTH_TOKEN);

        assertThat(result.get(Constants.AI_CBP)).containsExactly(CONTENT_ID_1);
        assertThat(result.get(Constants.APAR)).isEmpty();
    }

    // ──────────────────────────────────────────────────────────────────────
    // builder helpers
    // ──────────────────────────────────────────────────────────────────────

    private ApiResponse buildResponse(String yearKey,
                                      Map<String, Map<String, Object>> aparPlanList,
                                      Map<String, Map<String, Object>> nonAparPlanList) {
        ApiResponse response = new ApiResponse();
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put(yearKey, yearEntry(aparPlanList, nonAparPlanList));
        response.setResult(resultMap);
        return response;
    }

    private Map<String, Object> yearEntry(Map<String, Map<String, Object>> aparPlanList,
                                           Map<String, Map<String, Object>> nonAparPlanList) {
        Map<String, Object> entry = new HashMap<>();
        entry.put(Constants.RESPONSE_KEY_APAR_PLAN_LIST, aparPlanList);
        entry.put(Constants.RESPONSE_KEY_NON_APAR_PLAN_LIST, nonAparPlanList);
        return entry;
    }

    private Map<String, Map<String, Object>> planList(String planId,
                                                       String planType,
                                                       List<String> contentIds) {
        Map<String, Map<String, Object>> list = new HashMap<>();
        list.put(planId, plan(planType, contentIds));
        return list;
    }

    private Map<String, Object> plan(String planType, List<String> contentIds) {
        Map<String, Object> planMap = new HashMap<>();
        planMap.put(Constants.PLAN_TYPE, planType);
        List<Map<String, Object>> contentList = new ArrayList<>();
        for (String id : contentIds) {
            Map<String, Object> item = new HashMap<>();
            item.put(Constants.IDENTIFIER, id);
            contentList.add(item);
        }
        planMap.put(Constants.CONTENT_LIST, contentList);
        return planMap;
    }

    @Test
    void getCbPlanV4ContentIds_buildsRequestWithCurrentFinancialYear() {
        when(cbPlanServiceV4.getCBPlanDictionaryForUser(any(ApiRequest.class), anyString()))
                .thenReturn(buildResponse(YEAR_KEY, Collections.emptyMap(), Collections.emptyMap()));
        ArgumentCaptor<ApiRequest> requestCaptor = ArgumentCaptor.forClass(ApiRequest.class);

        contentInfoUtil.getCbPlanV4ContentIds(AUTH_TOKEN);

        verify(cbPlanServiceV4).getCBPlanDictionaryForUser(requestCaptor.capture(), eq(AUTH_TOKEN));
        assertThat(requestCaptor.getValue().getRequest())
                .asInstanceOf(InstanceOfAssertFactories.MAP)
                .containsEntry(Constants.REQUEST_PARAM_PLAN_YEAR, CbPlanYearUtil.resolveCurrentFinancialYear());
    }

    @Test
    void getCbPlanV4ContentIds_currentYearEmptyPreviousYearHasData_aggregatesPreviousYearContent() {
        ApiResponse response = new ApiResponse();
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("2026-27", yearEntry(Collections.emptyMap(), Collections.emptyMap()));
        resultMap.put("2025-26", yearEntry(
                planList(PLAN_ID_APAR, "APAR", List.of(CONTENT_ID_1)),
                planList(PLAN_ID_NON_APAR, "trainingPlan", List.of(CONTENT_ID_2))));
        response.setResult(resultMap);
        when(cbPlanServiceV4.getCBPlanDictionaryForUser(any(ApiRequest.class), anyString()))
                .thenReturn(response);

        Map<String, List<String>> result = contentInfoUtil.getCbPlanV4ContentIds(AUTH_TOKEN);

        assertThat(result.get(Constants.APAR)).containsExactly(CONTENT_ID_1);
        assertThat(result.get(Constants.TRAINING_PLAN)).containsExactly(CONTENT_ID_2);
        assertThat(result.get(Constants.AI_CBP)).isEmpty();
    }

    @Test
    void getCbPlanV4ContentIds_emptyYearEntryAmongMultipleYears_skippedWithoutError() {
        ApiResponse response = new ApiResponse();
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("2026-27", yearEntry(Collections.emptyMap(), Collections.emptyMap()));
        resultMap.put("2025-26", yearEntry(
                planList(PLAN_ID_APAR, "APAR", List.of(CONTENT_ID_1)),
                Collections.emptyMap()));
        response.setResult(resultMap);
        when(cbPlanServiceV4.getCBPlanDictionaryForUser(any(ApiRequest.class), anyString()))
                .thenReturn(response);

        Map<String, List<String>> result = contentInfoUtil.getCbPlanV4ContentIds(AUTH_TOKEN);

        assertThat(result.get(Constants.APAR)).containsExactly(CONTENT_ID_1);
        assertThat(result.get(Constants.TRAINING_PLAN)).isEmpty();
        assertThat(result.get(Constants.AI_CBP)).isEmpty();
    }

    @Test
    void getCbPlanV4ContentIds_yearEntryWithNullPlanLists_skippedWithoutError() {
        ApiResponse response = new ApiResponse();
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("2026-27", new HashMap<>());
        response.setResult(resultMap);
        when(cbPlanServiceV4.getCBPlanDictionaryForUser(any(ApiRequest.class), anyString()))
                .thenReturn(response);

        Map<String, List<String>> result = contentInfoUtil.getCbPlanV4ContentIds(AUTH_TOKEN);

        assertThat(result.get(Constants.APAR)).isEmpty();
        assertThat(result.get(Constants.TRAINING_PLAN)).isEmpty();
        assertThat(result.get(Constants.AI_CBP)).isEmpty();
    }

}
