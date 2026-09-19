package com.igot.cb.cbplan.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Kafka consumer for CB Plan Content Association (CA) events.
 * Delegates processing to {@link CbPlanCaEventService}.
 */
@Service
public class CbPlanCaEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(CbPlanCaEventConsumer.class);

    private final CbPlanCaEventService cbPlanCaEventService;
    private final ObjectMapper objectMapper;

    public CbPlanCaEventConsumer(CbPlanCaEventService cbPlanCaEventService,
                                 ObjectMapper objectMapper) {
        this.cbPlanCaEventService = cbPlanCaEventService;
        this.objectMapper = objectMapper;
    }

    /**
     * Entry point for Kafka CA events. Rejects blank payloads immediately;
     * valid messages are handed off to {@link CompletableFuture#runAsync} so the
     * Kafka polling thread is never blocked by downstream I/O.
     *
     * @param consumerRecord Kafka consumer record carrying the raw JSON payload
     */
    @KafkaListener(
            topics = "${kafka.topics.cbplan.ca.events}",
            groupId = "${kafka.topics.cbplan.ca.events.group}"
    )
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        if (StringUtils.isBlank(consumerRecord.value())) {
            log.error("consume: Invalid Kafka message - topic={}, partition={}, offset={}",
                    consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
            return;
        }
        log.debug("consume: Received message - topic={}, partition={}, offset={}",
                consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
        CompletableFuture.runAsync(() -> processAsync(consumerRecord));
    }

    /**
     * Deserializes the raw JSON payload and delegates to the service layer.
     * Runs on the common fork-join pool via {@link CompletableFuture#runAsync}.
     * Any exception is caught and logged; the message is not retried.
     *
     * @param consumerRecord Kafka consumer record to process asynchronously
     */
    private void processAsync(ConsumerRecord<String, String> consumerRecord) {
        try {
            CbPlanCaEvent event = objectMapper.readValue(consumerRecord.value(), CbPlanCaEvent.class);
            cbPlanCaEventService.processEvent(event);
        } catch (Exception e) {
            log.error("processAsync: Failed to process message - topic={}, partition={}, offset={}",
                    consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), e);
        }
    }
}
