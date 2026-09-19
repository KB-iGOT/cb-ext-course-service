package com.igot.cb.cbplan.kafka;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.cbplan.service.impl.CbPlanElasticSearchServiceV3Impl;
import com.igot.cb.util.Constants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Processes CB Plan CA events received from Kafka.
 * Applies ADD or REMOVE operations to the plan's caLinkedId field in Cassandra
 * and propagates the change to ElasticSearch.
 */
@Service
public class CbPlanCaEventServiceImpl implements CbPlanCaEventService {

    private static final Logger log = LoggerFactory.getLogger(CbPlanCaEventServiceImpl.class);

    private final CassandraOperation cassandraOperation;
    private final CbPlanElasticSearchServiceV3Impl elasticSearchService;

    public CbPlanCaEventServiceImpl(CassandraOperation cassandraOperation,
                                    CbPlanElasticSearchServiceV3Impl elasticSearchService) {
        this.cassandraOperation = cassandraOperation;
        this.elasticSearchService = elasticSearchService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processEvent(CbPlanCaEvent event) {
        if (!isValidEvent(event)) {
            return;
        }
        log.info("processEvent: eventType={}, trainingPlanId={}, caIdentifier={}",
                event.getEventType(), event.getTrainingPlanId(), event.getCaIdentifier());
        Map<String, Object> plan = fetchPlan(event.getTrainingPlanId());
        if (plan.isEmpty()) {
            return;
        }
        if (!hasChanged(event, plan)) {
            log.info("processEvent: No change required - eventType={}, caIdentifier={}",
                    event.getEventType(), event.getCaIdentifier());
            return;
        }
        String newValue = resolveNewValue(event);
        persistCaLinkedId(event.getTrainingPlanId(), newValue);
        syncToElasticSearch(event.getTrainingPlanId(), newValue);
    }

    /**
     * Validates that all required fields are present and the eventType is
     * a known value (ADD or REMOVE). Returns false and logs a warning on any violation.
     *
     * @param event the incoming CA event
     * @return true if the event is safe to process
     */
    private boolean isValidEvent(CbPlanCaEvent event) {
        if (event == null
                || StringUtils.isBlank(event.getEventType())
                || StringUtils.isBlank(event.getTrainingPlanId())
                || StringUtils.isBlank(event.getCaIdentifier())) {
            log.warn("isValidEvent: Skipping invalid event - {}", event);
            return false;
        }
        if (!Constants.CA_EVENT_TYPE_ADD.equals(event.getEventType())
                && !Constants.CA_EVENT_TYPE_REMOVE.equals(event.getEventType())) {
            log.warn("isValidEvent: Unknown eventType={}", event.getEventType());
            return false;
        }
        return true;
    }

    /**
     * Fetches the CB Plan record from Cassandra by planId.
     * Returns an empty map and logs a warning if the plan does not exist.
     *
     * @param planId the training plan ID to look up
     * @return plan data map, or empty map if not found
     */
    private Map<String, Object> fetchPlan(String planId) {
        List<Map<String, Object>> results = cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD, Constants.TABLE_CB_PLAN_V3,
                Map.of(Constants.PLAN_ID, planId), null, 1);
        if (CollectionUtils.isEmpty(results)) {
            log.warn("fetchPlan: Plan not found - planId={}", planId);
            return Collections.emptyMap();
        }
        return results.get(0);
    }

    /**
     * Determines whether the event would actually change the stored caLinkedId value.
     * Prevents redundant Cassandra writes and ES syncs for duplicate or already-applied events.
     * ADD: true if the current value differs from caIdentifier.
     * REMOVE: true if the current value is non-blank (i.e. there is something to clear).
     *
     * @param event the CA event to evaluate
     * @param plan  the current plan data from Cassandra
     * @return true if the caLinkedId value would change
     */
    private boolean hasChanged(CbPlanCaEvent event, Map<String, Object> plan) {
        String currentValue = (String) plan.get(Constants.CA_LINKED_ID);
        if (Constants.CA_EVENT_TYPE_ADD.equals(event.getEventType())) {
            return !event.getCaIdentifier().equals(currentValue);
        }
        return StringUtils.isNotBlank(currentValue);
    }

    /**
     * Resolves the value to persist for caLinkedId based on the event type.
     * ADD  → caIdentifier from the event.
     * REMOVE → empty string (clears the field without a DELETE operation).
     *
     * @param event the CA event
     * @return the new caLinkedId value to store
     */
    private String resolveNewValue(CbPlanCaEvent event) {
        if (Constants.CA_EVENT_TYPE_ADD.equals(event.getEventType())) {
            return event.getCaIdentifier();
        }
        if (Constants.CA_EVENT_TYPE_REMOVE.equals(event.getEventType())) {
            return StringUtils.EMPTY;
        }
        return StringUtils.EMPTY;
    }

    /**
     * Writes the updated caLinkedId and current timestamp to the cb_plan_v3 table.
     * Logs an error if the Cassandra driver reports a non-success response.
     *
     * @param planId   the plan to update
     * @param newValue the new caLinkedId value (caIdentifier for ADD, empty for REMOVE)
     */
    private void persistCaLinkedId(String planId, String newValue) {
        Map<String, Object> updateAttributes = new HashMap<>();
        updateAttributes.put(Constants.CA_LINKED_ID_DB, newValue);
        updateAttributes.put(Constants.COL_UPDATEDAT, Instant.now());
        Map<String, Object> result = cassandraOperation.updateRecord(
                Constants.KEYSPACE_SUNBIRD, Constants.TABLE_CB_PLAN_V3,
                updateAttributes, Map.of(Constants.PLAN_ID, planId));
        if (!Constants.SUCCESS.equals(result.get(Constants.RESPONSE))) {
            log.error("persistCaLinkedId: Cassandra update failed - planId={}", planId);
        } else {
            log.info("persistCaLinkedId: Updated calinkedid in Cassandra - planId={}, value={}",
                    planId, newValue);
        }
    }

    /**
     * Propagates the updated caLinkedId to ElasticSearch via a partial document update.
     * Exceptions are caught and logged; Cassandra is the source of truth and is already
     * updated at this point — ES staleness is temporary and non-fatal.
     *
     * @param planId   the plan whose ES document to update
     * @param newValue the new caLinkedId value to index
     */
    private void syncToElasticSearch(String planId, String newValue) {
        try {
            Map<String, Object> esUpdate = new HashMap<>();
            esUpdate.put(Constants.CA_LINKED_ID, newValue);
            esUpdate.put(Constants.UPDATED_AT, Instant.now());
            elasticSearchService.updateElasticSearchForPlan(planId, esUpdate);
            log.info("syncToElasticSearch: ES synced - planId={}", planId);
        } catch (Exception e) {
            log.error("syncToElasticSearch: ES sync failed - planId={}", planId, e);
        }
    }
}
