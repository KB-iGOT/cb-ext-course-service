package com.igot.cb.cbplan.service.impl.v4;

import com.igot.cb.cache.CbExtRedisCacheMgr;
import com.igot.cb.cache.CbPlanVersionMgr;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanLookupServiceV4ImplTest {

    private static final String USER_ID = "user-1";
    private static final String USER_ORG_ID = "org-1";
    private static final String COURSE_1 = "do_111";
    private static final String COURSE_2 = "do_222";
    private static final String END_DATE_1 = "2027-03-31T18:29:59Z";
    private static final String END_DATE_2 = "2026-12-15T18:29:59Z";
    private static final String MINISTRY_ID = "ministry-1";
    /** Version stamp matching the counters an un-stubbed (null-returning) MGET yields. */
    private static final String CV_ZERO = "\"_cv\":{\"orgVer\":0,\"ministryVer\":0,\"allVer\":0}";

    @Mock
    private CbPlanDictionaryServiceV4Impl dictionaryService;

    @Mock
    private CbExtRedisCacheMgr redisCacheMgr;

    private CbPlanLookupServiceV4Impl lookupService;

    @BeforeEach
    void setUp() {
        lookupService = new CbPlanLookupServiceV4Impl(dictionaryService, new CbPlanVersionMgr(redisCacheMgr));
    }

    private static Map<String, Object> aparPlan(String endDate, Object contentList) {
        Map<String, Object> plan = new LinkedHashMap<>();
        plan.put(Constants.END_DATE_REQUEST, endDate);
        plan.put(Constants.CONTENT_LIST, contentList);
        return plan;
    }

    private static Map<String, Object> contentItem(String identifier) {
        Map<String, Object> item = new HashMap<>();
        item.put(Constants.IDENTIFIER, identifier);
        return item;
    }

    @Test
    void getCBPlanCourseListForUser_blankUserId_returnsBadRequest() {
        ApiResponse response = lookupService.getCBPlanCourseListForUser("", USER_ORG_ID);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("UserId is blank", response.getParams().getErr());
        verify(redisCacheMgr, never()).getFromCache(anyString());
    }

    @Test
    void getCBPlanCourseListForUser_blankUserOrgId_returnsBadRequest() {
        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, "");

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("UserOrgId is blank", response.getParams().getErr());
        verify(redisCacheMgr, never()).getFromCache(anyString());
    }

    @Test
    void getCBPlanCourseListForUser_cacheHit_returnsCoursesFromCache() {
        String cachedJson = buildCachedJson(List.of(
                aparPlan(END_DATE_1, List.of(contentItem(COURSE_1), contentItem(COURSE_2)))));
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(cachedJson);

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        Map<String, String> contents = castContents(response);
        assertEquals(2, contents.size());
        assertEquals(END_DATE_1, contents.get(COURSE_1));
        assertEquals(END_DATE_1, contents.get(COURSE_2));
        verify(dictionaryService, never())
                .resolveAccessibleAparPlansByUserId(anyString(), anyString(), anyString(), any());
    }

    @Test
    void getCBPlanCourseListForUser_cacheHitStringContentListEntries_flattensPlainIds() {
        String cachedJson = buildCachedJson(List.of(
                aparPlan(END_DATE_1, List.of(COURSE_1, COURSE_2))));
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(cachedJson);

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        Map<String, String> contents = castContents(response);
        assertEquals(2, contents.size());
        assertEquals(END_DATE_1, contents.get(COURSE_1));
        assertEquals(END_DATE_1, contents.get(COURSE_2));
    }

    @Test
    void getCBPlanCourseListForUser_cacheHitDuplicateCourseIds_firstEndDateWins() {
        String cachedJson = buildCachedJson(List.of(
                aparPlan(END_DATE_1, List.of(contentItem(COURSE_1))),
                aparPlan(END_DATE_2, List.of(contentItem(COURSE_1), contentItem(COURSE_2)))));
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(cachedJson);

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        Map<String, String> contents = castContents(response);
        assertEquals(2, contents.size());
        assertEquals(END_DATE_1, contents.get(COURSE_1));
        assertEquals(END_DATE_2, contents.get(COURSE_2));
    }

    @Test
    void getCBPlanCourseListForUser_cacheHitContentListNotList_skipsPlan() {
        String cachedJson = buildCachedJson(List.of(
                aparPlan(END_DATE_1, "not-a-list"),
                aparPlan(END_DATE_1, List.of(contentItem(COURSE_1)))));
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(cachedJson);

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        Map<String, String> contents = castContents(response);
        assertEquals(1, contents.size());
        assertEquals(END_DATE_1, contents.get(COURSE_1));
    }

    @Test
    void getCBPlanCourseListForUser_cacheHitBlankOrNonStringIdentifier_skipped() {
        Map<String, Object> blankItem = new HashMap<>();
        blankItem.put(Constants.IDENTIFIER, "   ");
        Map<String, Object> nonStringItem = new HashMap<>();
        nonStringItem.put(Constants.IDENTIFIER, 42);
        List<Object> items = new ArrayList<>();
        items.add(contentItem(COURSE_1));
        items.add(blankItem);
        items.add(nonStringItem);
        items.add(999);
        String cachedJson = buildCachedJson(List.of(aparPlan(END_DATE_1, items)));
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(cachedJson);

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        Map<String, String> contents = castContents(response);
        assertEquals(1, contents.size());
        assertEquals(END_DATE_1, contents.get(COURSE_1));
    }

    @Test
    void getCBPlanCourseListForUser_cacheBlank_fallsBackToPipeline() {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn("");
        stubPipelineReturning(List.of(aparPlan(END_DATE_1, List.of(contentItem(COURSE_1)))));

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        Map<String, String> contents = castContents(response);
        assertEquals(1, contents.size());
        assertEquals(END_DATE_1, contents.get(COURSE_1));
        verify(dictionaryService)
                .resolveAccessibleAparPlansByUserId(eq(USER_ID), eq(USER_ORG_ID), anyString(), any());
    }

    @Test
    void getCBPlanCourseListForUser_cacheMalformedJson_fallsBackToPipeline() {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn("not-json{");
        stubPipelineReturning(List.of(aparPlan(END_DATE_1, List.of(contentItem(COURSE_1)))));

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(1, castContents(response).size());
    }

    @Test
    void getCBPlanCourseListForUser_cacheMissingYearResult_fallsBackToPipeline() {
        String cachedJson = "{" + CV_ZERO + ",\"otherYear\":{\"aparPlanList\":{}}}";
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(cachedJson);
        stubPipelineReturning(List.of(aparPlan(END_DATE_1, List.of(contentItem(COURSE_1)))));

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(1, castContents(response).size());
    }

    @Test
    void getCBPlanCourseListForUser_cacheYearResultWrongType_fallsBackToPipeline() {
        String cachedJson = "{" + CV_ZERO + ",\"" + currentPlanYear() + "\":\"not-a-map\"}";
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(cachedJson);
        stubPipelineReturning(List.of(aparPlan(END_DATE_1, List.of(contentItem(COURSE_1)))));

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(1, castContents(response).size());
    }

    @Test
    void getCBPlanCourseListForUser_cacheMissingAparList_fallsBackToPipeline() {
        String cachedJson = "{" + CV_ZERO + ",\"" + currentPlanYear() + "\":{\"nonAparPlanList\":{}}}";
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(cachedJson);
        stubPipelineReturning(List.of());

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertTrue(castContents(response).isEmpty());
    }

    @Test
    void getCBPlanCourseListForUser_cacheAparListWrongType_fallsBackToPipeline() {
        String cachedJson = "{" + CV_ZERO + ",\"" + currentPlanYear() + "\":{\"aparPlanList\":[]}}";
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(cachedJson);
        stubPipelineReturning(List.of(aparPlan(END_DATE_1, List.of(contentItem(COURSE_1)))));

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(1, castContents(response).size());
    }

    @Test
    void getCBPlanCourseListForUser_cacheAparListEmpty_returnsEmptyContents() {
        String cachedJson = "{" + CV_ZERO + ",\"" + currentPlanYear() + "\":{\"aparPlanList\":{}}}";
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(cachedJson);

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertTrue(castContents(response).isEmpty());
        verify(dictionaryService, never())
                .resolveAccessibleAparPlansByUserId(anyString(), anyString(), anyString(), any());
    }

    @Test
    void getCBPlanCourseListForUser_pipelineInstantEndDate_normalizedToIso() {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        Instant instant = Instant.parse(END_DATE_1);
        Map<String, Object> plan = new LinkedHashMap<>();
        plan.put(Constants.END_DATE_REQUEST, instant);
        plan.put(Constants.CONTENT_LIST, List.of(contentItem(COURSE_1)));
        stubPipelineReturning(List.of(plan));

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        assertEquals(END_DATE_1, castContents(response).get(COURSE_1));
    }

    @Test
    void getCBPlanCourseListForUser_pipelineNullEndDate_mapsToNullValue() {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        Map<String, Object> plan = new LinkedHashMap<>();
        plan.put(Constants.END_DATE_REQUEST, null);
        plan.put(Constants.CONTENT_LIST, List.of(contentItem(COURSE_1)));
        stubPipelineReturning(List.of(plan));

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        Map<String, String> contents = castContents(response);
        assertTrue(contents.containsKey(COURSE_1));
        assertNull(contents.get(COURSE_1));
    }

    @Test
    void getCBPlanCourseListForUser_pipelineReturnsEmpty_returnsEmptyContents() {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        stubPipelineReturning(List.of());

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertTrue(castContents(response).isEmpty());
    }

    @Test
    void getCBPlanCourseListForUser_pipelineFailsWithFailedStatus_returnsError() {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(dictionaryService.resolveAccessibleAparPlansByUserId(anyString(), anyString(), anyString(), any()))
                .thenAnswer(invocation -> {
                    ApiResponse response = invocation.getArgument(3);
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErr("User does not exist");
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return List.of();
                });

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("User does not exist", response.getParams().getErr());
    }

    @Test
    void getCBPlanCourseListForUser_dictionaryThrows_returnsInternalServerError() {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(dictionaryService.resolveAccessibleAparPlansByUserId(anyString(), anyString(), anyString(), any()))
                .thenThrow(new RuntimeException("boom"));

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("Failed to lookup user CB plan courses", response.getParams().getErr());
    }

    @Test
    void getCBPlanCourseListForUser_cacheKeyUsesExpectedFormat() {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn("");
        stubPipelineReturning(List.of());

        lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        String expectedKey = Constants.CB_PLAN_V4_REDIS_KEY_PREFIX
                + USER_ORG_ID + ":" + USER_ID + ":" + currentPlanYear() + ":dict";
        verify(redisCacheMgr).getFromCache(expectedKey);
    }

    @Test
    void getCBPlanCourseListForUser_cacheUnstamped_fallsBackToPipeline() {
        String cachedJson = buildCachedJson(List.of(aparPlan(END_DATE_1, List.of(contentItem(COURSE_1)))), null);
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(cachedJson);
        stubPipelineReturning(List.of(aparPlan(END_DATE_2, List.of(contentItem(COURSE_2)))));

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        Map<String, String> contents = castContents(response);
        assertEquals(1, contents.size());
        assertEquals(END_DATE_2, contents.get(COURSE_2));
        verify(redisCacheMgr, never()).mgetFromCache(any(String[].class));
    }

    @Test
    void getCBPlanCourseListForUser_cacheStaleOrgVersion_fallsBackToPipeline() {
        String cachedJson = buildCachedJson(List.of(aparPlan(END_DATE_1, List.of(contentItem(COURSE_1)))));
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(cachedJson);
        when(redisCacheMgr.mgetFromCache(any(String[].class))).thenReturn(java.util.Arrays.asList("1", null));
        stubPipelineReturning(List.of(aparPlan(END_DATE_2, List.of(contentItem(COURSE_2)))));

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        Map<String, String> contents = castContents(response);
        assertEquals(1, contents.size());
        assertEquals(END_DATE_2, contents.get(COURSE_2));
        verify(dictionaryService).resolveAccessibleAparPlansByUserId(eq(USER_ID), eq(USER_ORG_ID), anyString(), any());
    }

    @Test
    void getCBPlanCourseListForUser_cacheStaleAllVersion_fallsBackToPipeline() {
        String cachedJson = buildCachedJson(List.of(aparPlan(END_DATE_1, List.of(contentItem(COURSE_1)))));
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(cachedJson);
        when(redisCacheMgr.mgetFromCache(any(String[].class))).thenReturn(java.util.Arrays.asList(null, "3"));
        stubPipelineReturning(List.of());

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        assertTrue(castContents(response).isEmpty());
        verify(dictionaryService).resolveAccessibleAparPlansByUserId(eq(USER_ID), eq(USER_ORG_ID), anyString(), any());
    }

    @Test
    void getCBPlanCourseListForUser_cacheMatchingVersions_servedWithoutPipeline() {
        String cv = "\"_cv\":{\"orgVer\":7,\"ministryVer\":0,\"allVer\":3}";
        String cachedJson = buildCachedJson(List.of(aparPlan(END_DATE_1, List.of(contentItem(COURSE_1)))), cv);
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(cachedJson);
        when(redisCacheMgr.mgetFromCache(any(String[].class))).thenReturn(List.of("7", "3"));

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        assertEquals(END_DATE_1, castContents(response).get(COURSE_1));
        verify(dictionaryService, never())
                .resolveAccessibleAparPlansByUserId(anyString(), anyString(), anyString(), any());
    }

    @Test
    void getCBPlanCourseListForUser_cacheStampedWithMinistry_checksMinistryCounterFromStamp() {
        String cv = "\"_cv\":{\"orgVer\":1,\"ministryVer\":2,\"allVer\":0,\"ministryId\":\"" + MINISTRY_ID + "\"}";
        String cachedJson = buildCachedJson(List.of(aparPlan(END_DATE_1, List.of(contentItem(COURSE_1)))), cv);
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(cachedJson);
        when(redisCacheMgr.mgetFromCache(any(String[].class))).thenReturn(java.util.Arrays.asList("1", null, "2"));

        ApiResponse response = lookupService.getCBPlanCourseListForUser(USER_ID, USER_ORG_ID);

        assertEquals(END_DATE_1, castContents(response).get(COURSE_1));
        String year = currentPlanYear();
        verify(redisCacheMgr).mgetFromCache(
                Constants.CB_PLAN_V4_VERSION_KEY_PREFIX + "org:" + USER_ORG_ID + ":" + year,
                Constants.CB_PLAN_V4_VERSION_KEY_PREFIX + "all:" + year,
                Constants.CB_PLAN_V4_VERSION_KEY_PREFIX + "ministry:" + MINISTRY_ID + ":" + year);
        verify(dictionaryService, never())
                .resolveAccessibleAparPlansByUserId(anyString(), anyString(), anyString(), any());
    }

    private void stubPipelineReturning(List<Map<String, Object>> plans) {
        when(dictionaryService.resolveAccessibleAparPlansByUserId(anyString(), anyString(), anyString(), any()))
                .thenReturn(plans);
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> castContents(ApiResponse response) {
        Object contents = response.getResult().get(Constants.CONTENTS);
        assertNotNull(contents);
        return (Map<String, String>) contents;
    }

    private String buildCachedJson(List<Map<String, Object>> plans) {
        return buildCachedJson(plans, CV_ZERO);
    }

    private String buildCachedJson(List<Map<String, Object>> plans, String cvBlock) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        if (cvBlock != null) {
            sb.append(cvBlock).append(',');
        }
        sb.append('"').append(currentPlanYear()).append("\":{\"")
                .append(Constants.RESPONSE_KEY_APAR_PLAN_LIST).append("\":{");
        for (int i = 0; i < plans.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append("\"plan").append(i).append("\":");
            sb.append(toJsonMap(plans.get(i)));
        }
        sb.append("}}}");
        return sb.toString();
    }

    private String toJsonMap(Map<String, Object> plan) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean first = true;
        for (Map.Entry<String, Object> entry : plan.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            sb.append('"').append(entry.getKey()).append("\":");
            sb.append(toJsonValue(entry.getValue()));
        }
        sb.append('}');
        return sb.toString();
    }

    private String toJsonValue(Object value) {
        if (value == null) return "null";
        if (value instanceof String s) return "\"" + s + "\"";
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        if (value instanceof Map<?, ?> m) {
            @SuppressWarnings("unchecked")
            Map<String, Object> mm = (Map<String, Object>) m;
            return toJsonMap(mm);
        }
        if (value instanceof List<?> list) {
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append(toJsonValue(list.get(i)));
            }
            return sb.append(']').toString();
        }
        return "\"" + value + "\"";
    }

    private String currentPlanYear() {
        return com.igot.cb.cbplan.util.CbPlanYearUtil.resolveCurrentFinancialYear();
    }
}
