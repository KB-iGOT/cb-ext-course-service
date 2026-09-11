package com.igot.cb.cbplan.service.impl.v4;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.igot.cb.cbplan.dto.CbPlanReadResponseDto;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CbPlanReadServiceV4ImplTest {

    private static final String PLAN_ID = "plan1";
    private static final String PLAN_YEAR = "2026-27";

    private final CbPlanReadServiceV4Impl readService = new CbPlanReadServiceV4Impl();

    private static Map<String, Object> basePlan(String status) {
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.STATUS, status);
        plan.put(Constants.NAME, "planName");
        plan.put(Constants.PLAN_YEAR, PLAN_YEAR);
        plan.put(Constants.CONTENT_TYPE, "Course");
        plan.put(Constants.CREATED_BY, "user1");
        return plan;
    }

    @Test
    void buildEnrichedPlanData_draftStatusWithNoDraftData_returnsDirectFields() throws JsonProcessingException {
        Instant endDate = Instant.parse("2026-12-31T18:29:59Z");
        Map<String, Object> plan = basePlan(Constants.DRAFT);
        plan.put(Constants.END_DATE_REQUEST, endDate);
        plan.put(Constants.IS_APAR, true);
        plan.put(Constants.CONTENT_LIST, List.of("do_1", "do_2"));

        CbPlanReadResponseDto dto = readService.buildEnrichedPlanData(plan, PLAN_ID);

        assertEquals(PLAN_ID, dto.getId());
        assertEquals("planName", dto.getName());
        assertEquals(PLAN_YEAR, dto.getPlanYear());
        assertEquals(endDate, dto.getEndDate());
        assertTrue(dto.getIsApar());
        assertEquals(Constants.DRAFT, dto.getStatus());
        assertEquals("user1", dto.getCreatedBy());
        assertEquals("", dto.getCreatedByName());
        assertEquals(List.of("do_1", "do_2"), dto.getContentList());
    }

    @Test
    void buildEnrichedPlanData_liveStatusWithNoDraftData_returnsDirectFields() throws JsonProcessingException {
        Map<String, Object> plan = basePlan(Constants.LIVE);
        plan.put(Constants.CONTENT_LIST, List.of("do_1"));

        CbPlanReadResponseDto dto = readService.buildEnrichedPlanData(plan, PLAN_ID);

        assertEquals(Constants.LIVE, dto.getStatus());
        assertEquals("planName", dto.getName());
        assertEquals(List.of("do_1"), dto.getContentList());
    }

    @Test
    void buildEnrichedPlanData_liveStatusWithEmptyDraftDataObject_ignoresDraftAndUsesRecord() throws JsonProcessingException {
        Map<String, Object> plan = basePlan(Constants.LIVE);
        plan.put(Constants.DRAFT_DATA, Constants.EMPTY_JSON);
        plan.put(Constants.CONTENT_LIST, List.of("do_record"));

        CbPlanReadResponseDto dto = readService.buildEnrichedPlanData(plan, PLAN_ID);

        assertEquals("planName", dto.getName());
        assertEquals(List.of("do_record"), dto.getContentList());
    }

    @Test
    void buildEnrichedPlanData_liveStatusWithPendingDraftData_prefersDraftFields() throws JsonProcessingException {
        Map<String, Object> plan = basePlan(Constants.LIVE);
        plan.put(Constants.CONTENT_LIST, List.of("do_record"));
        plan.put(Constants.DRAFT_DATA,
                "{\"name\":\"draftName\",\"contentList\":[\"do_draft\"],\"isApar\":true,\"endDate\":\"2026-12-31\"}");

        CbPlanReadResponseDto dto = readService.buildEnrichedPlanData(plan, PLAN_ID);

        assertEquals("draftName", dto.getName());
        assertTrue(dto.getIsApar());
        assertNotNull(dto.getEndDate());
        assertEquals(List.of("do_draft"), dto.getContentList());
        assertEquals(Constants.LIVE, dto.getStatus());
    }

    @Test
    void buildEnrichedPlanData_draftDataPresentButStatusNotLive_usesRecordNotDraft() throws JsonProcessingException {
        Map<String, Object> plan = basePlan(Constants.DRAFT);
        plan.put(Constants.CONTENT_LIST, List.of("do_record"));
        plan.put(Constants.DRAFT_DATA, "{\"name\":\"draftName\",\"contentList\":[\"do_draft\"]}");

        CbPlanReadResponseDto dto = readService.buildEnrichedPlanData(plan, PLAN_ID);

        assertEquals("planName", dto.getName());
        assertEquals(List.of("do_record"), dto.getContentList());
    }

    @Test
    void buildEnrichedPlanData_malformedDraftData_throwsJsonProcessingException() {
        Map<String, Object> plan = basePlan(Constants.LIVE);
        plan.put(Constants.DRAFT_DATA, "not-valid-json");

        assertThrows(JsonProcessingException.class, () -> readService.buildEnrichedPlanData(plan, PLAN_ID));
    }

    @Test
    void buildEnrichedPlanData_missingIsApar_defaultsToFalse() throws JsonProcessingException {
        Map<String, Object> plan = basePlan(Constants.DRAFT);

        CbPlanReadResponseDto dto = readService.buildEnrichedPlanData(plan, PLAN_ID);

        assertFalse(dto.getIsApar());
    }

    @Test
    void buildEnrichedPlanData_missingContentList_returnsEmptyList() throws JsonProcessingException {
        Map<String, Object> plan = basePlan(Constants.DRAFT);

        CbPlanReadResponseDto dto = readService.buildEnrichedPlanData(plan, PLAN_ID);

        assertNotNull(dto.getContentList());
        assertTrue(dto.getContentList().isEmpty());
    }

    @Test
    void buildEnrichedPlanData_contentListWithNullEntry_preservesNull() throws JsonProcessingException {
        Map<String, Object> plan = basePlan(Constants.DRAFT);
        plan.put(Constants.CONTENT_LIST, Arrays.asList("do_1", null));

        CbPlanReadResponseDto dto = readService.buildEnrichedPlanData(plan, PLAN_ID);

        assertEquals(Arrays.asList("do_1", null), dto.getContentList());
    }

    @Test
    void buildEnrichedPlanData_nonListContentListValue_returnsEmptyList() throws JsonProcessingException {
        Map<String, Object> plan = basePlan(Constants.DRAFT);
        plan.put(Constants.CONTENT_LIST, "not-a-list");

        CbPlanReadResponseDto dto = readService.buildEnrichedPlanData(plan, PLAN_ID);

        assertTrue(dto.getContentList().isEmpty());
    }

    @Test
    void buildEnrichedPlanData_contextDataAbsent_returnsNullContextData() throws JsonProcessingException {
        Map<String, Object> plan = basePlan(Constants.DRAFT);

        CbPlanReadResponseDto dto = readService.buildEnrichedPlanData(plan, PLAN_ID);

        assertNull(dto.getContextData());
    }

    @Test
    void buildEnrichedPlanData_contextDataValidJsonString_parsesToJsonNode() throws JsonProcessingException {
        Map<String, Object> plan = basePlan(Constants.DRAFT);
        plan.put(Constants.CONTEXT_DATA_REQUEST, "{\"accessControl\":{\"userGroups\":[{\"userGroupId\":\"g1\"}]}}");

        CbPlanReadResponseDto dto = readService.buildEnrichedPlanData(plan, PLAN_ID);

        assertNotNull(dto.getContextData());
        assertTrue(dto.getContextData().has("accessControl"));
        assertEquals("g1", dto.getContextData().at("/accessControl/userGroups/0/userGroupId").asText());
    }

    @Test
    void buildEnrichedPlanData_contextDataLegacyShapeWithInlineCriteria_passesThroughUnchanged() throws JsonProcessingException {
        Map<String, Object> plan = basePlan(Constants.DRAFT);
        plan.put(Constants.CONTEXT_DATA_REQUEST,
                "{\"accessControl\":{\"userGroups\":[{\"userGroupName\":\"HR\",\"userGroupCriteriaList\":"
                        + "[{\"criteriaKey\":\"rootOrgId\",\"criteriaValue\":[\"org1\"]}]}]}}");

        CbPlanReadResponseDto dto = readService.buildEnrichedPlanData(plan, PLAN_ID);

        assertEquals("HR", dto.getContextData().at("/accessControl/userGroups/0/userGroupName").asText());
        assertTrue(dto.getContextData().at("/accessControl/userGroups/0/userGroupId").isMissingNode());
    }

    @Test
    void buildEnrichedPlanData_contextDataMalformedJsonString_returnsNullContextData() throws JsonProcessingException {
        Map<String, Object> plan = basePlan(Constants.DRAFT);
        plan.put(Constants.CONTEXT_DATA_REQUEST, "not-valid-json");

        CbPlanReadResponseDto dto = readService.buildEnrichedPlanData(plan, PLAN_ID);

        assertNull(dto.getContextData());
    }

    @Test
    void buildEnrichedPlanData_planTypeAndCbPublishedAt_arePassedThrough() throws JsonProcessingException {
        Instant publishedAt = Instant.parse("2026-05-01T00:00:00Z");
        Map<String, Object> plan = basePlan(Constants.LIVE);
        plan.put(Constants.PLAN_TYPE, "STANDARD");
        plan.put(Constants.CB_PUBLISHED_AT, publishedAt);

        CbPlanReadResponseDto dto = readService.buildEnrichedPlanData(plan, PLAN_ID);

        assertEquals("STANDARD", dto.getPlanType());
        assertEquals(publishedAt, dto.getCbPublishedAt());
    }

    @Test
    void extractContentList_emptyListInput_returnsEmptyList() throws JsonProcessingException {
        Map<String, Object> plan = basePlan(Constants.DRAFT);
        plan.put(Constants.CONTENT_LIST, new ArrayList<>());

        CbPlanReadResponseDto dto = readService.buildEnrichedPlanData(plan, PLAN_ID);

        assertNotNull(dto.getContentList());
        assertTrue(dto.getContentList().isEmpty());
    }
}
