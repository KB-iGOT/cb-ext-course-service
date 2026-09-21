package com.igot.cb.cbplan.service.impl.v4;

import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Service for managing CB Plan V4 ElasticSearch operations.
 * Extends V3 behavior with V4-specific field alignments.
 *
 * @version 4.0
 */
@Service
@Slf4j
public class CbPlanElasticSearchServiceV4Impl {
    private final EsUtilService esUtilService;
    private final CbExtServerProperties serverProperties;

    public CbPlanElasticSearchServiceV4Impl(EsUtilService esUtilService, CbExtServerProperties serverProperties) {
        this.esUtilService = esUtilService;
        this.serverProperties = serverProperties;
    }

    /**
     * Indexes CB Plan to ElasticSearch.
     *
     * @param planId   CB Plan ID
     * @param planData CB Plan data to index
     */
    public void indexToElasticSearch(String planId, Map<String, Object> planData) {
        planData.put(Constants.ID, planId);
        Map<String, Object> sanitizedMap = sanitizeForElastic(planData);
        esUtilService.addDocument(
                serverProperties.getCpPlanIndex(),
                Constants.INDEX_TYPE,
                planId,
                sanitizedMap,
                serverProperties.getElasticCbPlanJsonPath());
    }

    /**
     * Updates ElasticSearch for a modified CB Plan.
     *
     * @param cbPlanId       CB Plan ID
     * @param updatedRequest updated request data
     */
    public void updateElasticSearchForPlan(String cbPlanId, Map<String, Object> updatedRequest) {
        Map<String, Object> sanitizedMap = sanitizeForElastic(updatedRequest);
        esUtilService.updateDocument(serverProperties.getCpPlanIndex(), Constants.INDEX_TYPE,
                cbPlanId, sanitizedMap, serverProperties.getElasticCbPlanJsonPath());
    }

    /**
     * Sanitizes CB Plan data for ElasticSearch indexing.
     * Converts Instant objects to ISO string format.
     *
     * @param input raw plan data
     * @return sanitized data ready for ES indexing
     */
    public Map<String, Object> sanitizeForElastic(Map<String, Object> input) {
        Map<String, Object> sanitized = new HashMap<>();
        for (Map.Entry<String, Object> entry : input.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Instant instant) {
                sanitized.put(entry.getKey(), DateTimeFormatter.ISO_INSTANT.format(instant));
            } else {
                sanitized.put(entry.getKey(), value);
            }
        }
        alignKeysWithEsSchema(sanitized);
        return sanitized;
    }

    /**
     * Renames Cassandra column keys to the camelCase field names declared in the
     * ElasticSearch required-fields schema.
     * EsUtilService drops any document key absent from that schema, so a key whose
     * case differs from the schema is silently discarded instead of being indexed.
     * V4-specific alignments:
     * - "planyear" (Cassandra) → "planYear" (ES schema)
     * - "calinkedid" (Cassandra) → "caLinkedId" (ES schema)
     *
     * @param sanitized document being prepared for indexing, mutated in place
     */
    private void alignKeysWithEsSchema(Map<String, Object> sanitized) {
        if (sanitized.containsKey(Constants.PLAN_YEAR)) {
            sanitized.put(Constants.REQUEST_PARAM_PLAN_YEAR, sanitized.remove(Constants.PLAN_YEAR));
        }
        if (sanitized.containsKey(Constants.CA_LINKED_ID_DB)) {
            sanitized.put(Constants.CA_LINKED_ID, sanitized.remove(Constants.CA_LINKED_ID_DB));
        }
    }

    /**
     * Attempts to index a CB Plan in Elasticsearch.
     * Used as {@code preCommitValidator} in the transactional {@code insertRecord} overload.
     *
     * @param planId        CB Plan ID
     * @param planDataForEs plan data already prepared for ES (deserialized contentList, etc.)
     * @return true if ES indexing succeeded
     */
    public boolean tryIndexPlan(String planId, Map<String, Object> planDataForEs) {
        try {
            planDataForEs.put(Constants.ID, planId);
            Map<String, Object> sanitizedMap = sanitizeForElastic(planDataForEs);
            String result = esUtilService.addDocument(
                    serverProperties.getCpPlanIndex(),
                    Constants.INDEX_TYPE,
                    planId,
                    sanitizedMap,
                    serverProperties.getElasticCbPlanJsonPath());
            if (result == null) {
                log.error("Failed to index CB Plan in ES (null result): planId={}", planId);
                return false;
            }
            log.info("Indexed CB Plan in ES: planId={}", planId);
            return true;
        } catch (Exception e) {
            log.error("Failed to index CB Plan in ES: planId={}", planId, e);
            return false;
        }
    }

    /**
     * Best-effort rollback for create: deletes the ES document when the subsequent
     * Cassandra insert fails.
     * Called as {@code onCommitFailureRollback} when Cassandra fails after ES already succeeded.
     *
     * @param planId CB Plan ID to remove from ES
     */
    public void rollbackCreate(String planId) {
        if (!esUtilService.deleteDocument(serverProperties.getCpPlanIndex(), planId)) {
            log.error("ES_CASSANDRA_DIVERGENCE: ES rollback for create failed for planId={} — manual reconciliation required", planId);
        }
    }

    /**
     * Attempts to update a CB Plan in Elasticsearch.
     * Used as {@code preCommitValidator} in the transactional {@code updateRecord} overload.
     *
     * @param cbPlanId       CB Plan ID
     * @param updatedRequest update properties (sanitized internally)
     * @return true if ES update succeeded
     */
    public boolean tryUpdatePlan(String cbPlanId, Map<String, Object> updatedRequest) {
        try {
            Map<String, Object> sanitizedMap = sanitizeForElastic(updatedRequest);
            String result = esUtilService.updateDocument(
                    serverProperties.getCpPlanIndex(),
                    Constants.INDEX_TYPE,
                    cbPlanId,
                    sanitizedMap,
                    serverProperties.getElasticCbPlanJsonPath());
            if (result == null) {
                log.error("Failed to update CB Plan in ES (null result): planId={}", cbPlanId);
                return false;
            }
            log.info("Updated CB Plan in ES: planId={}", cbPlanId);
            return true;
        } catch (Exception e) {
            log.error("Failed to update CB Plan in ES: planId={}", cbPlanId, e);
            return false;
        }
    }

    /**
     * Best-effort rollback for update: restores the previous ES document state.
     * Called as {@code onCommitFailureRollback} when Cassandra fails after ES already succeeded.
     *
     * @param planId        CB Plan ID
     * @param previousState full document state before the attempted update
     */
    public void rollbackUpdate(String planId, Map<String, Object> previousState) {
        if (!tryUpdatePlan(planId, previousState)) {
            log.error("ES_CASSANDRA_DIVERGENCE: ES rollback for update failed for planId={} — manual reconciliation required", planId);
        }
    }
}
