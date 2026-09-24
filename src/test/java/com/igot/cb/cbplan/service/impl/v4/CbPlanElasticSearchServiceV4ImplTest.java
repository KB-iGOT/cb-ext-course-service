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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
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
}
