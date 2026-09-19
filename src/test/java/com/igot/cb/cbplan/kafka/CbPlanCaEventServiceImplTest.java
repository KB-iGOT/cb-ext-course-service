package com.igot.cb.cbplan.kafka;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.cbplan.service.impl.CbPlanElasticSearchServiceV3Impl;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanCaEventServiceImplTest {

    private static final String PLAN_ID = "plan-001";
    private static final String CA_ID   = "do_123";

    @Mock
    private CassandraOperation cassandraOperation;
    @Mock
    private CbPlanElasticSearchServiceV3Impl elasticSearchService;

    private CbPlanCaEventServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new CbPlanCaEventServiceImpl(cassandraOperation, elasticSearchService);
    }


    @Test
    void processEvent_nullEvent_skipsProcessing() {
        service.processEvent(null);

        verify(cassandraOperation, never()).getRecordsByProperties(any(), any(), any(), any(), any());
    }

    @Test
    void processEvent_blankEventType_skipsProcessing() {
        service.processEvent(buildEvent("", PLAN_ID, CA_ID));

        verify(cassandraOperation, never()).getRecordsByProperties(any(), any(), any(), any(), any());
    }

    @Test
    void processEvent_blankTrainingPlanId_skipsProcessing() {
        service.processEvent(buildEvent(Constants.CA_EVENT_TYPE_ADD, "", CA_ID));

        verify(cassandraOperation, never()).getRecordsByProperties(any(), any(), any(), any(), any());
    }

    @Test
    void processEvent_blankCaIdentifier_skipsProcessing() {
        service.processEvent(buildEvent(Constants.CA_EVENT_TYPE_ADD, PLAN_ID, ""));

        verify(cassandraOperation, never()).getRecordsByProperties(any(), any(), any(), any(), any());
    }

    @Test
    void processEvent_unknownEventType_skipsProcessing() {
        service.processEvent(buildEvent("UNKNOWN", PLAN_ID, CA_ID));

        verify(cassandraOperation, never()).getRecordsByProperties(any(), any(), any(), any(), any());
    }


    @Test
    void processEvent_planNotFound_skipsUpdate() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), eq(1)))
                .thenReturn(Collections.emptyList());

        service.processEvent(buildEvent(Constants.CA_EVENT_TYPE_ADD, PLAN_ID, CA_ID));

        verify(cassandraOperation, never()).updateRecord(any(), any(), any(), any());
        verify(elasticSearchService, never()).updateElasticSearchForPlan(any(), any());
    }


    @Test
    void processEvent_addEvent_caLinkedIdAlreadySet_skipsUpdate() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), eq(1)))
                .thenReturn(List.of(planWith(CA_ID)));

        service.processEvent(buildEvent(Constants.CA_EVENT_TYPE_ADD, PLAN_ID, CA_ID));

        verify(cassandraOperation, never()).updateRecord(any(), any(), any(), any());
        verify(elasticSearchService, never()).updateElasticSearchForPlan(any(), any());
    }

    @Test
    void processEvent_removeEvent_caLinkedIdAlreadyBlank_skipsUpdate() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), eq(1)))
                .thenReturn(List.of(planWith("")));

        service.processEvent(buildEvent(Constants.CA_EVENT_TYPE_REMOVE, PLAN_ID, CA_ID));

        verify(cassandraOperation, never()).updateRecord(any(), any(), any(), any());
        verify(elasticSearchService, never()).updateElasticSearchForPlan(any(), any());
    }


    @Test
    void processEvent_addEvent_setsCaLinkedIdInCassandra() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), eq(1)))
                .thenReturn(List.of(planWith("old-ca")));
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));

        service.processEvent(buildEvent(Constants.CA_EVENT_TYPE_ADD, PLAN_ID, CA_ID));

        ArgumentCaptor<Map<String, Object>> attrsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(cassandraOperation).updateRecord(
                eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_CB_PLAN_V3),
                attrsCaptor.capture(), anyMap());
        assertThat(attrsCaptor.getValue()).containsEntry(Constants.CA_LINKED_ID_DB, CA_ID);
    }

    @Test
    void processEvent_addEvent_syncsToElasticSearch() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), eq(1)))
                .thenReturn(List.of(planWith("old-ca")));
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));

        service.processEvent(buildEvent(Constants.CA_EVENT_TYPE_ADD, PLAN_ID, CA_ID));

        ArgumentCaptor<Map<String, Object>> esCaptor = ArgumentCaptor.forClass(Map.class);
        verify(elasticSearchService).updateElasticSearchForPlan(eq(PLAN_ID), esCaptor.capture());
        assertThat(esCaptor.getValue()).containsEntry(Constants.CA_LINKED_ID, CA_ID);
    }


    @Test
    void processEvent_removeEvent_clearsCaLinkedIdInCassandra() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), eq(1)))
                .thenReturn(List.of(planWith(CA_ID)));
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));

        service.processEvent(buildEvent(Constants.CA_EVENT_TYPE_REMOVE, PLAN_ID, CA_ID));

        ArgumentCaptor<Map<String, Object>> attrsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(cassandraOperation).updateRecord(
                eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_CB_PLAN_V3),
                attrsCaptor.capture(), anyMap());
        assertThat(attrsCaptor.getValue()).containsEntry(Constants.CA_LINKED_ID_DB, "");
    }

    @Test
    void processEvent_removeEvent_syncsEmptyValueToElasticSearch() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), eq(1)))
                .thenReturn(List.of(planWith(CA_ID)));
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));

        service.processEvent(buildEvent(Constants.CA_EVENT_TYPE_REMOVE, PLAN_ID, CA_ID));

        ArgumentCaptor<Map<String, Object>> esCaptor = ArgumentCaptor.forClass(Map.class);
        verify(elasticSearchService).updateElasticSearchForPlan(eq(PLAN_ID), esCaptor.capture());
        assertThat(esCaptor.getValue()).containsEntry(Constants.CA_LINKED_ID, "");
    }


    @Test
    void processEvent_cassandraUpdateFails_stillTriesEsSync() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), eq(1)))
                .thenReturn(List.of(planWith("old-ca")));
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.FAILED));

        service.processEvent(buildEvent(Constants.CA_EVENT_TYPE_ADD, PLAN_ID, CA_ID));

        verify(elasticSearchService).updateElasticSearchForPlan(eq(PLAN_ID), anyMap());
    }

    @Test
    void processEvent_esSyncThrows_doesNotPropagateException() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), eq(1)))
                .thenReturn(List.of(planWith("old-ca")));
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));
        doThrow(new RuntimeException("ES down"))
                .when(elasticSearchService).updateElasticSearchForPlan(anyString(), anyMap());

        assertDoesNotThrow(
                () -> service.processEvent(buildEvent(Constants.CA_EVENT_TYPE_ADD, PLAN_ID, CA_ID)));
    }


    private CbPlanCaEvent buildEvent(String eventType, String trainingPlanId, String caIdentifier) {
        CbPlanCaEvent event = new CbPlanCaEvent();
        event.setEventType(eventType);
        event.setTrainingPlanId(trainingPlanId);
        event.setCaIdentifier(caIdentifier);
        return event;
    }

    private Map<String, Object> planWith(String caLinkedId) {
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.PLAN_ID, PLAN_ID);
        plan.put(Constants.CA_LINKED_ID, caLinkedId);
        return plan;
    }
}
