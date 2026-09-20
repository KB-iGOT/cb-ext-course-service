package com.igot.cb.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.cbplan.service.CbPlanServiceV4;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Consumes training-plan CA-link events published by the search-indexer Flink job when a
 * "Comprehensive Assessment" collection's trainingPlan_v2 changes, and mirrors the link onto the
 * CB Plan's calinkedid column.
 * <p>
 * Message shape: {"eventType":"ADD|REMOVE","trainingPlanId":"<cb plan id>","caIdentifier":"do_..."}
 * <p>
 * Processing is synchronous and compare-then-write, so redeliveries and replays are idempotent.
 */
@Component
public class CbPlanCaLinkConsumer {

    private static final Logger logger = LoggerFactory.getLogger(CbPlanCaLinkConsumer.class);

    private final ObjectMapper mapper;
    private final CassandraOperation cassandraOperation;
    private final CbExtServerProperties serverProperties;
    private final CbPlanServiceV4 cbPlanServiceV4;

    public CbPlanCaLinkConsumer(ObjectMapper mapper, CassandraOperation cassandraOperation,
                                CbExtServerProperties serverProperties, CbPlanServiceV4 cbPlanServiceV4) {
        this.mapper = mapper;
        this.cassandraOperation = cassandraOperation;
        this.serverProperties = serverProperties;
        this.cbPlanServiceV4 = cbPlanServiceV4;
    }

    @KafkaListener(topics = "${spring.kafka.cbplan.ca.link.topic.name}", groupId = "${spring.kafka.cbplan.ca.link.consumer.group.id}")
    public void consumeCaLinkEvent(ConsumerRecord<String, String> record) {
        String message = record.value();
        if (StringUtils.isBlank(message)) {
            logger.warn("CbPlanCaLinkConsumer: Received empty message from topic: {}", record.topic());
            return;
        }
        try {
            Map<String, String> event = mapper.readValue(message, new TypeReference<Map<String, String>>() {
            });
            processEvent(event);
        } catch (JsonProcessingException e) {
            logger.error("CbPlanCaLinkConsumer: Failed to parse message: {}", message, e);
        } catch (Exception e) {
            logger.error("CbPlanCaLinkConsumer: Unexpected error processing message: {}", message, e);
        }
    }

    private void processEvent(Map<String, String> event) {
        String eventType = event.get(Constants.EVENT_TYPE);
        String planId = event.get(Constants.TRAINING_PLAN_ID);
        String caIdentifier = event.get(Constants.CA_IDENTIFIER);
        if (StringUtils.isBlank(eventType) || StringUtils.isBlank(planId) || StringUtils.isBlank(caIdentifier)) {
            logger.warn("CbPlanCaLinkConsumer: Invalid event, missing eventType/trainingPlanId/caIdentifier: {}", event);
            return;
        }
        boolean isAdd = Constants.CA_LINK_EVENT_ADD.equalsIgnoreCase(eventType);
        boolean isRemove = Constants.CA_LINK_EVENT_REMOVE.equalsIgnoreCase(eventType);
        if (!isAdd && !isRemove) {
            logger.warn("CbPlanCaLinkConsumer: Unknown eventType '{}' for planId={}, caIdentifier={}", eventType, planId, caIdentifier);
            return;
        }

        List<Map<String, Object>> plans = cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD, Constants.TABLE_CB_PLAN_V3,
                Map.of(Constants.PLAN_ID, planId), null, serverProperties.getCassandraQueryLimitPrimaryKey());
        if (CollectionUtils.isEmpty(plans)) {
            logger.error("CbPlanCaLinkConsumer: CB Plan not found - planId={}, eventType={}, caIdentifier={}", planId, eventType, caIdentifier);
            return;
        }
        String currentCaLinkedId = (String) plans.get(0).get(Constants.CA_LINKED_ID_DB);

        if (isAdd) {
            handleAdd(planId, caIdentifier, currentCaLinkedId);
        } else {
            handleRemove(planId, caIdentifier, currentCaLinkedId);
        }
    }

    private void handleAdd(String planId, String caIdentifier, String currentCaLinkedId) {
        if (caIdentifier.equals(currentCaLinkedId)) {
            logger.info("CbPlanCaLinkConsumer: ADD skipped, already linked - planId={}, caIdentifier={}", planId, caIdentifier);
            return;
        }
        if (StringUtils.isNotBlank(currentCaLinkedId)) {
            logger.warn("CbPlanCaLinkConsumer: ADD replacing existing link - planId={}, existing={}, new={}",
                    planId, currentCaLinkedId, caIdentifier);
        }
        boolean updated = cbPlanServiceV4.updateCaLinkedId(planId, caIdentifier, Constants.SYSTEM_USER);
        logResult("ADD", planId, caIdentifier, updated);
    }

    private void handleRemove(String planId, String caIdentifier, String currentCaLinkedId) {
        if (!caIdentifier.equals(currentCaLinkedId)) {
            logger.warn("CbPlanCaLinkConsumer: REMOVE skipped, plan is not linked to this CA - planId={}, current={}, requested={}",
                    planId, currentCaLinkedId, caIdentifier);
            return;
        }
        boolean updated = cbPlanServiceV4.updateCaLinkedId(planId, null, Constants.SYSTEM_USER);
        logResult("REMOVE", planId, caIdentifier, updated);
    }

    private void logResult(String eventType, String planId, String caIdentifier, boolean updated) {
        if (updated) {
            logger.info("CbPlanCaLinkConsumer: {} applied - planId={}, caIdentifier={}", eventType, planId, caIdentifier);
        } else {
            logger.error("CbPlanCaLinkConsumer: {} failed - planId={}, caIdentifier={}", eventType, planId, caIdentifier);
        }
    }
}
