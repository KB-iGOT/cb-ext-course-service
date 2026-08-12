package com.igot.cb.cbplan.service.impl;

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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanElasticSearchServiceV3ImplTest {

    private static final String ES_INDEX = "cbplan-index";
    private static final String JSON_PATH = "path.json";
    private static final String PLAN_ID = "plan1";

    @Mock
    private EsUtilService esUtilService;

    @Mock
    private CbExtServerProperties serverProperties;

    @InjectMocks
    private CbPlanElasticSearchServiceV3Impl elasticSearchService;

    private void stubEsProperties() {
        when(serverProperties.getCpPlanIndex()).thenReturn(ES_INDEX);
        when(serverProperties.getElasticCbPlanJsonPath()).thenReturn(JSON_PATH);
    }

    @Test
    void testIndexToElasticSearchAddsIdAndSanitizes() {
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
    void testUpdateElasticSearchForPlan() {
        stubEsProperties();
        Map<String, Object> updatedRequest = new HashMap<>();
        updatedRequest.put(Constants.NAME, "updatedName");
        elasticSearchService.updateElasticSearchForPlan(PLAN_ID, updatedRequest);
        verify(esUtilService).updateDocument(eq(ES_INDEX), eq(Constants.INDEX_TYPE), eq(PLAN_ID),
                anyMap(), eq(JSON_PATH));
    }

    @Test
    void testUpdateElasticSearchForArchiveMergesAndForcesRetireStatus() {
        stubEsProperties();
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.NAME, "planName");
        existingCbPlan.put(Constants.STATUS, Constants.LIVE);
        Map<String, Object> updateData = new HashMap<>();
        updateData.put(Constants.COMMENT, "archived");
        elasticSearchService.updateElasticSearchForArchive(PLAN_ID, existingCbPlan, updateData);
        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(esUtilService).addDocument(eq(ES_INDEX), eq(Constants.INDEX_TYPE), eq(PLAN_ID),
                captor.capture(), eq(JSON_PATH));
        Map<String, Object> indexed = captor.getValue();
        assertEquals(Constants.CB_RETIRE, indexed.get(Constants.STATUS));
        assertEquals(PLAN_ID, indexed.get(Constants.ID));
        assertEquals("planName", indexed.get(Constants.NAME));
        assertEquals("archived", indexed.get(Constants.COMMENT));
    }

    @Test
    void testUpdateElasticSearchForArchiveDoesNotMutateExistingPlan() {
        stubEsProperties();
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.STATUS, Constants.LIVE);
        elasticSearchService.updateElasticSearchForArchive(PLAN_ID, existingCbPlan, new HashMap<>());
        assertEquals(Constants.LIVE, existingCbPlan.get(Constants.STATUS));
    }

    @Test
    void testSanitizeForElasticConvertsInstantToIsoString() {
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
    void testSanitizeForElasticHandlesEmptyAndNullValues() {
        Map<String, Object> input = new HashMap<>();
        input.put(Constants.NAME, null);
        Map<String, Object> sanitized = elasticSearchService.sanitizeForElastic(input);
        assertTrue(sanitized.containsKey(Constants.NAME));
        assertTrue(elasticSearchService.sanitizeForElastic(new HashMap<>()).isEmpty());
    }

    @Test
    void testConstructor() {
        assertNotNull(new CbPlanElasticSearchServiceV3Impl(esUtilService, serverProperties));
    }
}
