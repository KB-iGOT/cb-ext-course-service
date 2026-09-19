package com.igot.cb.cbplan.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class CbPlanCaEventConsumerTest {

    private static final String TOPIC     = "sunbirddev.trainingplan.ca.events";
    private static final String PLAN_ID   = "plan-001";
    private static final String CA_ID     = "do_123";
    private static final String VALID_JSON =
            "{\"eventType\":\"ADD\",\"trainingPlanId\":\"" + PLAN_ID + "\",\"caIdentifier\":\"" + CA_ID + "\"}";

    @Mock
    private CbPlanCaEventService cbPlanCaEventService;

    private CbPlanCaEventConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new CbPlanCaEventConsumer(cbPlanCaEventService, new ObjectMapper());
    }

    @Test
    void consume_blankMessage_skipsProcessing() {
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>(TOPIC, 0, 0L, PLAN_ID, "");

        consumer.consume(consumerRecord);

        verify(cbPlanCaEventService, never()).processEvent(any());
    }

    @Test
    void consume_nullMessage_skipsProcessing() {
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>(TOPIC, 0, 0L, PLAN_ID, null);

        consumer.consume(consumerRecord);

        verify(cbPlanCaEventService, never()).processEvent(any());
    }

    @Test
    void consume_validMessage_delegatesToServiceAsync() {
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>(TOPIC, 0, 0L, PLAN_ID, VALID_JSON);

        consumer.consume(consumerRecord);

        verify(cbPlanCaEventService, timeout(1000)).processEvent(any(CbPlanCaEvent.class));
    }

    @Test
    void consume_invalidJson_skipsProcessing() {
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>(TOPIC, 0, 0L, PLAN_ID, "not-json");

        consumer.consume(consumerRecord);

        verify(cbPlanCaEventService, timeout(1000).times(0)).processEvent(any());
    }
}
