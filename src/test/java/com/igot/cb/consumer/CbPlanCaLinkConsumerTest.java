package com.igot.cb.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.cbplan.service.CbPlanServiceV4;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanCaLinkConsumerTest {

    private static final String PLAN_ID = "e39f6cf0-b314-11f1-9299-21ed79c92209";
    private static final String CA_ID = "do_11466125660417228812";
    private static final String OTHER_CA_ID = "do_other";

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private CbExtServerProperties serverProperties;

    @Mock
    private CbPlanServiceV4 cbPlanServiceV4;

    private CbPlanCaLinkConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new CbPlanCaLinkConsumer(new ObjectMapper(), cassandraOperation, serverProperties, cbPlanServiceV4);
        lenient().when(serverProperties.getCassandraQueryLimitPrimaryKey()).thenReturn(1);
    }

    private static ConsumerRecord<String, String> record(String value) {
        return new ConsumerRecord<>("topic", 0, 0L, null, value);
    }

    private static String event(String type, String planId, String caId) {
        return "{\"eventType\":\"" + type + "\",\"trainingPlanId\":\"" + planId + "\",\"caIdentifier\":\"" + caId + "\"}";
    }

    private void mockPlan(String currentCaLinkedId) {
        Map<String, Object> plan = new HashMap<>();
        plan.put(Constants.PLAN_ID, PLAN_ID);
        plan.put(Constants.CA_LINKED_ID_DB, currentCaLinkedId);
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_CB_PLAN_V3),
                eq(Map.of(Constants.PLAN_ID, PLAN_ID)), isNull(), anyInt())).thenReturn(List.of(plan));
    }

    @Test
    void add_whenNotLinked_setsCaLinkedId() {
        mockPlan(null);
        when(cbPlanServiceV4.updateCaLinkedId(PLAN_ID, CA_ID, Constants.SYSTEM_USER)).thenReturn(true);

        consumer.consumeCaLinkEvent(record(event("ADD", PLAN_ID, CA_ID)));

        verify(cbPlanServiceV4).updateCaLinkedId(PLAN_ID, CA_ID, Constants.SYSTEM_USER);
    }

    @Test
    void add_whenAlreadyLinkedToSameCa_isNoOp() {
        mockPlan(CA_ID);

        consumer.consumeCaLinkEvent(record(event("ADD", PLAN_ID, CA_ID)));

        verify(cbPlanServiceV4, never()).updateCaLinkedId(anyString(), any(), anyString());
    }

    @Test
    void add_whenLinkedToDifferentCa_overwrites() {
        mockPlan(OTHER_CA_ID);
        when(cbPlanServiceV4.updateCaLinkedId(PLAN_ID, CA_ID, Constants.SYSTEM_USER)).thenReturn(true);

        consumer.consumeCaLinkEvent(record(event("ADD", PLAN_ID, CA_ID)));

        verify(cbPlanServiceV4).updateCaLinkedId(PLAN_ID, CA_ID, Constants.SYSTEM_USER);
    }

    @Test
    void remove_whenLinkedToSameCa_clearsCaLinkedId() {
        mockPlan(CA_ID);
        when(cbPlanServiceV4.updateCaLinkedId(PLAN_ID, null, Constants.SYSTEM_USER)).thenReturn(true);

        consumer.consumeCaLinkEvent(record(event("REMOVE", PLAN_ID, CA_ID)));

        verify(cbPlanServiceV4).updateCaLinkedId(PLAN_ID, null, Constants.SYSTEM_USER);
    }

    @Test
    void remove_whenLinkedToDifferentCa_isSkipped() {
        mockPlan(OTHER_CA_ID);

        consumer.consumeCaLinkEvent(record(event("REMOVE", PLAN_ID, CA_ID)));

        verify(cbPlanServiceV4, never()).updateCaLinkedId(anyString(), any(), anyString());
    }

    @Test
    void remove_whenNotLinked_isSkipped() {
        mockPlan(null);

        consumer.consumeCaLinkEvent(record(event("REMOVE", PLAN_ID, CA_ID)));

        verify(cbPlanServiceV4, never()).updateCaLinkedId(anyString(), any(), anyString());
    }

    @Test
    void planNotFound_isSkipped() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), anyInt()))
                .thenReturn(Collections.emptyList());

        consumer.consumeCaLinkEvent(record(event("ADD", PLAN_ID, CA_ID)));

        verify(cbPlanServiceV4, never()).updateCaLinkedId(anyString(), any(), anyString());
    }

    @Test
    void unknownEventType_isSkippedWithoutLookup() {
        consumer.consumeCaLinkEvent(record(event("UPSERT", PLAN_ID, CA_ID)));

        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), anyInt());
        verify(cbPlanServiceV4, never()).updateCaLinkedId(anyString(), any(), anyString());
    }

    @Test
    void missingFields_areSkipped() {
        consumer.consumeCaLinkEvent(record("{\"eventType\":\"ADD\",\"trainingPlanId\":\"" + PLAN_ID + "\"}"));
        consumer.consumeCaLinkEvent(record("{\"eventType\":\"ADD\",\"caIdentifier\":\"" + CA_ID + "\"}"));

        verify(cbPlanServiceV4, never()).updateCaLinkedId(anyString(), any(), anyString());
    }

    @Test
    void blankOrInvalidJson_doesNotThrow() {
        consumer.consumeCaLinkEvent(record(""));
        consumer.consumeCaLinkEvent(record(null));
        consumer.consumeCaLinkEvent(record("not-json"));

        verify(cbPlanServiceV4, never()).updateCaLinkedId(anyString(), any(), anyString());
    }

    @Test
    void updateFailure_isLoggedNotThrown() {
        mockPlan(null);
        when(cbPlanServiceV4.updateCaLinkedId(PLAN_ID, CA_ID, Constants.SYSTEM_USER)).thenReturn(false);

        consumer.consumeCaLinkEvent(record(event("ADD", PLAN_ID, CA_ID)));

        verify(cbPlanServiceV4).updateCaLinkedId(PLAN_ID, CA_ID, Constants.SYSTEM_USER);
    }
}
