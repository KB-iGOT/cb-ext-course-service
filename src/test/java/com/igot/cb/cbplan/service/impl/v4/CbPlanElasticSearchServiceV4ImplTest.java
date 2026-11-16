package com.igot.cb.cbplan.service.impl.v4;

import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanElasticSearchServiceV4ImplTest {

    private static final String ES_INDEX = "cbplan-index";
    private static final String JSON_PATH = "path.json";
    private static final String PLAN_ID = "plan1";

    @Mock
    private EsUtilService esUtilService;

    @Mock
    private CbExtServerProperties serverProperties;

    @InjectMocks
    private CbPlanElasticSearchServiceV4Impl elasticSearchService;

    private void stubEsProperties() {
        when(serverProperties.getCpPlanIndex()).thenReturn(ES_INDEX);
        when(serverProperties.getElasticCbPlanJsonPath()).thenReturn(JSON_PATH);
    }

    @Test
    void indexToElasticSearch_withValidData_addsIdAndSanitizes() {
        stubEsProperties();
        Instant createdAt = Instant.parse("2026-08-12T10:15:30Z");
        Map<String, Object> planData = new HashMap<>();
        planData.put(Constants.NAME, "planName");
        planData.put(Constants.CREATED_AT, createdAt);
        elasticSearchService.indexToElasticSearch(PLAN_ID, planData);
        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(esUtilService).addDocument(eq(ES_INDEX), eq(Constants.INDEX_TYPE), eq(PLAN_ID),
                captor.capture(), eq(JSON_PATH));
        Map<String, Object> indexed = captor.getValue();
        assertEquals(PLAN_ID, indexed.get(Constants.ID));
        assertEquals(DateTimeFormatter.ISO_INSTANT.format(createdAt), indexed.get(Constants.CREATED_AT));
        assertEquals("planName", indexed.get(Constants.NAME));
    }

    @Test
    void updateElasticSearchForPlan_withValidData_updatesDocument() {
        stubEsProperties();
        Map<String, Object> updatedRequest = new HashMap<>();
        updatedRequest.put(Constants.NAME, "updatedName");
        elasticSearchService.updateElasticSearchForPlan(PLAN_ID, updatedRequest);
        verify(esUtilService).updateDocument(eq(ES_INDEX), eq(Constants.INDEX_TYPE), eq(PLAN_ID),
                anyMap(), eq(JSON_PATH));
    }

    @Test
    void sanitizeForElastic_withInstant_convertsToIsoString() {
        Instant now = Instant.parse("2026-08-12T10:15:30Z");
        Map<String, Object> input = new HashMap<>();
        input.put(Constants.CREATED_AT, now);
        input.put(Constants.NAME, "planName");
        input.put(Constants.CONTENT_LIST, List.of("course1"));
        Map<String, Object> sanitized = elasticSearchService.sanitizeForElastic(input);
        assertEquals("2026-08-12T10:15:30Z", sanitized.get(Constants.CREATED_AT));
        assertEquals("planName", sanitized.get(Constants.NAME));
        assertEquals(List.of("course1"), sanitized.get(Constants.CONTENT_LIST));
    }

    @Test
    void sanitizeForElastic_withEmptyAndNullValues_handlesCorrectly() {
        Map<String, Object> input = new HashMap<>();
        input.put(Constants.NAME, null);
        Map<String, Object> sanitized = elasticSearchService.sanitizeForElastic(input);
        assertTrue(sanitized.containsKey(Constants.NAME));
        assertTrue(elasticSearchService.sanitizeForElastic(new HashMap<>()).isEmpty());
    }

    @Test
    void sanitizeForElastic_withCaLinkedIdDb_renamesToCaLinkedId() {
        Map<String, Object> input = new HashMap<>();
        input.put(Constants.CA_LINKED_ID_DB, "caId123");
        input.put(Constants.NAME, "planName");
        Map<String, Object> sanitized = elasticSearchService.sanitizeForElastic(input);
        assertEquals("caId123", sanitized.get(Constants.CA_LINKED_ID));
        assertFalse(sanitized.containsKey(Constants.CA_LINKED_ID_DB));
        assertEquals("planName", sanitized.get(Constants.NAME));
    }

    @Test
    void sanitizeForElastic_withPlanYear_renamesToPlanYear() {
        Map<String, Object> input = new HashMap<>();
        input.put(Constants.PLAN_YEAR, "2026");
        input.put(Constants.NAME, "planName");
        Map<String, Object> sanitized = elasticSearchService.sanitizeForElastic(input);
        assertEquals("2026", sanitized.get(Constants.REQUEST_PARAM_PLAN_YEAR));
        assertFalse(sanitized.containsKey(Constants.PLAN_YEAR));
        assertEquals("planName", sanitized.get(Constants.NAME));
    }

    @Test
    void sanitizeForElastic_withBothCaLinkedIdAndPlanYear_renamesBoth() {
        Map<String, Object> input = new HashMap<>();
        input.put(Constants.CA_LINKED_ID_DB, "caId123");
        input.put(Constants.PLAN_YEAR, "2026");
        input.put(Constants.NAME, "planName");
        Map<String, Object> sanitized = elasticSearchService.sanitizeForElastic(input);
        assertEquals("caId123", sanitized.get(Constants.CA_LINKED_ID));
        assertEquals("2026", sanitized.get(Constants.REQUEST_PARAM_PLAN_YEAR));
        assertFalse(sanitized.containsKey(Constants.CA_LINKED_ID_DB));
        assertFalse(sanitized.containsKey(Constants.PLAN_YEAR));
        assertEquals("planName", sanitized.get(Constants.NAME));
    }

    @Test
    void sanitizeForElastic_withCaLinkedIdDbAndInstant_appliesBothTransformations() {
        Instant now = Instant.parse("2026-08-12T10:15:30Z");
        Map<String, Object> input = new HashMap<>();
        input.put(Constants.CA_LINKED_ID_DB, "caId123");
        input.put(Constants.CREATED_AT, now);
        input.put(Constants.NAME, "planName");
        Map<String, Object> sanitized = elasticSearchService.sanitizeForElastic(input);
        assertEquals("caId123", sanitized.get(Constants.CA_LINKED_ID));
        assertFalse(sanitized.containsKey(Constants.CA_LINKED_ID_DB));
        assertEquals("2026-08-12T10:15:30Z", sanitized.get(Constants.CREATED_AT));
        assertEquals("planName", sanitized.get(Constants.NAME));
    }

    @Test
    void sanitizeForElastic_withoutCaLinkedIdDb_doesNotAddCaLinkedId() {
        Map<String, Object> input = new HashMap<>();
        input.put(Constants.NAME, "planName");
        Map<String, Object> sanitized = elasticSearchService.sanitizeForElastic(input);
        assertFalse(sanitized.containsKey(Constants.CA_LINKED_ID));
        assertFalse(sanitized.containsKey(Constants.CA_LINKED_ID_DB));
        assertEquals("planName", sanitized.get(Constants.NAME));
    }

    @Test
    void indexToElasticSearch_withCaLinkedIdDb_alignsFieldName() {
        stubEsProperties();
        Map<String, Object> planData = new HashMap<>();
        planData.put(Constants.NAME, "planName");
        planData.put(Constants.CA_LINKED_ID_DB, "caId123");
        elasticSearchService.indexToElasticSearch(PLAN_ID, planData);
        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(esUtilService).addDocument(eq(ES_INDEX), eq(Constants.INDEX_TYPE), eq(PLAN_ID),
                captor.capture(), eq(JSON_PATH));
        Map<String, Object> indexed = captor.getValue();
        assertEquals("caId123", indexed.get(Constants.CA_LINKED_ID));
        assertFalse(indexed.containsKey(Constants.CA_LINKED_ID_DB));
    }

    @Test
    void updateElasticSearchForPlan_withCaLinkedIdDb_alignsFieldName() {
        stubEsProperties();
        Map<String, Object> updatedRequest = new HashMap<>();
        updatedRequest.put(Constants.NAME, "updatedName");
        updatedRequest.put(Constants.CA_LINKED_ID_DB, "caId123");
        elasticSearchService.updateElasticSearchForPlan(PLAN_ID, updatedRequest);
        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(esUtilService).updateDocument(eq(ES_INDEX), eq(Constants.INDEX_TYPE), eq(PLAN_ID),
                captor.capture(), eq(JSON_PATH));
        Map<String, Object> updated = captor.getValue();
        assertEquals("caId123", updated.get(Constants.CA_LINKED_ID));
        assertFalse(updated.containsKey(Constants.CA_LINKED_ID_DB));
    }

    @Test
    void constructor_withValidDependencies_createsInstance() {
        assertNotNull(new CbPlanElasticSearchServiceV4Impl(esUtilService, serverProperties));
    }

    @Test
    void tryIndexPlan_whenEsAddReturnsNonNull_returnsTrue() {
        stubEsProperties();
        Map<String, Object> planData = new HashMap<>();
        planData.put(Constants.NAME, "planName");
        when(esUtilService.addDocument(anyString(), anyString(), anyString(), anyMap(), anyString())).thenReturn("created");

        boolean result = elasticSearchService.tryIndexPlan(PLAN_ID, planData);

        assertTrue(result);
        verify(esUtilService, times(1)).addDocument(
                eq(ES_INDEX), eq(Constants.INDEX_TYPE), eq(PLAN_ID), anyMap(), eq(JSON_PATH));
    }

    @Test
    void tryIndexPlan_whenEsAddReturnsNull_returnsFalse() {
        stubEsProperties();
        Map<String, Object> planData = new HashMap<>();
        when(esUtilService.addDocument(anyString(), anyString(), anyString(), anyMap(), anyString())).thenReturn(null);

        boolean result = elasticSearchService.tryIndexPlan(PLAN_ID, planData);

        assertFalse(result);
    }

    @Test
    void tryIndexPlan_whenEsAddThrows_returnsFalse() {
        stubEsProperties();
        Map<String, Object> planData = new HashMap<>();
        when(esUtilService.addDocument(anyString(), anyString(), anyString(), anyMap(), anyString()))
                .thenThrow(new RuntimeException("ES unavailable"));

        boolean result = elasticSearchService.tryIndexPlan(PLAN_ID, planData);

        assertFalse(result);
    }

    @Test
    void rollbackCreate_whenDeleteSucceeds_completesNormally() {
        when(serverProperties.getCpPlanIndex()).thenReturn(ES_INDEX);
        when(esUtilService.deleteDocument(eq(ES_INDEX), eq(PLAN_ID))).thenReturn(true);

        assertDoesNotThrow(() -> elasticSearchService.rollbackCreate(PLAN_ID));
        verify(esUtilService, times(1)).deleteDocument(eq(ES_INDEX), eq(PLAN_ID));
    }

    @Test
    void rollbackCreate_whenDeleteFails_logsDivergenceWithoutThrowing() {
        when(serverProperties.getCpPlanIndex()).thenReturn(ES_INDEX);
        when(esUtilService.deleteDocument(eq(ES_INDEX), eq(PLAN_ID))).thenReturn(false);

        assertDoesNotThrow(() -> elasticSearchService.rollbackCreate(PLAN_ID));
    }

    @Test
    void tryUpdatePlan_whenEsUpdateReturnsNonNull_returnsTrue() {
        stubEsProperties();
        Map<String, Object> updateData = new HashMap<>();
        updateData.put(Constants.NAME, "updatedName");
        when(esUtilService.updateDocument(anyString(), anyString(), anyString(), anyMap(), anyString())).thenReturn("updated");

        boolean result = elasticSearchService.tryUpdatePlan(PLAN_ID, updateData);

        assertTrue(result);
        verify(esUtilService, times(1)).updateDocument(
                eq(ES_INDEX), eq(Constants.INDEX_TYPE), eq(PLAN_ID), anyMap(), eq(JSON_PATH));
    }

    @Test
    void tryUpdatePlan_whenEsUpdateReturnsNull_returnsFalse() {
        stubEsProperties();
        Map<String, Object> updateData = new HashMap<>();
        when(esUtilService.updateDocument(anyString(), anyString(), anyString(), anyMap(), anyString())).thenReturn(null);

        boolean result = elasticSearchService.tryUpdatePlan(PLAN_ID, updateData);

        assertFalse(result);
    }

    @Test
    void tryUpdatePlan_whenEsUpdateThrows_returnsFalse() {
        stubEsProperties();
        Map<String, Object> updateData = new HashMap<>();
        when(esUtilService.updateDocument(anyString(), anyString(), anyString(), anyMap(), anyString()))
                .thenThrow(new RuntimeException("ES down"));

        boolean result = elasticSearchService.tryUpdatePlan(PLAN_ID, updateData);

        assertFalse(result);
    }

    @Test
    void rollbackUpdate_whenTryUpdateSucceeds_completesNormally() {
        stubEsProperties();
        Map<String, Object> previousState = new HashMap<>();
        previousState.put(Constants.NAME, "originalName");
        when(esUtilService.updateDocument(anyString(), anyString(), anyString(), anyMap(), anyString())).thenReturn("updated");

        assertDoesNotThrow(() -> elasticSearchService.rollbackUpdate(PLAN_ID, previousState));
    }

    @Test
    void rollbackUpdate_whenTryUpdateFails_logsDivergenceWithoutThrowing() {
        stubEsProperties();
        Map<String, Object> previousState = new HashMap<>();
        when(esUtilService.updateDocument(anyString(), anyString(), anyString(), anyMap(), anyString())).thenReturn(null);

        assertDoesNotThrow(() -> elasticSearchService.rollbackUpdate(PLAN_ID, previousState));
    }

    @Test
    void sanitizeForElastic_withV4FormatContextData_extractsUserGroupIds() {
        String contextDataJson = "{\"accessControl\":{\"userGroups\":[{\"userGroupId\":\"id1\"},{\"userGroupId\":\"id2\"}]}}";
        Map<String, Object> input = new HashMap<>();
        input.put(Constants.CONTEXT_DATA_REQUEST, contextDataJson);
        input.put(Constants.NAME, "planName");

        Map<String, Object> sanitized = elasticSearchService.sanitizeForElastic(input);

        assertEquals(List.of("id1", "id2"), sanitized.get(Constants.CONTEXT_DATA_ES_FIELD_V4));
    }

    @Test
    void sanitizeForElastic_withV3FormatContextData_doesNotAddContextDataV4() {
        String contextDataJson = "{\"accessControl\":{\"userGroups\":[{\"userGroupName\":\"Group A\","
                + "\"userGroupCriteriaList\":[{\"criteriaKey\":\"rootOrgId\",\"criteriaValue\":[\"org1\"]}]}]}}";
        Map<String, Object> input = new HashMap<>();
        input.put(Constants.CONTEXT_DATA_REQUEST, contextDataJson);

        Map<String, Object> sanitized = elasticSearchService.sanitizeForElastic(input);

        assertFalse(sanitized.containsKey(Constants.CONTEXT_DATA_ES_FIELD_V4));
    }

    @Test
    void sanitizeForElastic_withoutContextData_doesNotAddContextDataV4() {
        Map<String, Object> input = new HashMap<>();
        input.put(Constants.NAME, "planName");

        Map<String, Object> sanitized = elasticSearchService.sanitizeForElastic(input);

        assertFalse(sanitized.containsKey(Constants.CONTEXT_DATA_ES_FIELD_V4));
    }

    @Test
    void sanitizeForElastic_withMalformedContextDataJson_doesNotThrowAndSkipsContextDataV4() {
        Map<String, Object> input = new HashMap<>();
        input.put(Constants.CONTEXT_DATA_REQUEST, "not-valid-json");

        Map<String, Object> sanitized = assertDoesNotThrow(() -> elasticSearchService.sanitizeForElastic(input));

        assertFalse(sanitized.containsKey(Constants.CONTEXT_DATA_ES_FIELD_V4));
    }
}
