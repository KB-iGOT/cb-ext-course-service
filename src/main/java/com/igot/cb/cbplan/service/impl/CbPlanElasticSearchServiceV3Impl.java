package com.igot.cb.cbplan.service.impl;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;

import lombok.extern.slf4j.Slf4j;

/**
 * Service for managing CB Plan ElasticSearch operations.
 *
 * @version 3.0
 */
@Service
@Slf4j
public class CbPlanElasticSearchServiceV3Impl {
    private final EsUtilService esUtilService;
    private final CbExtServerProperties serverProperties;

    public CbPlanElasticSearchServiceV3Impl(EsUtilService esUtilService, CbExtServerProperties serverProperties) {
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
     * Updates ElasticSearch when archiving a CB Plan.
     *
     * @param cbPlanId       CB Plan ID
     * @param existingCbPlan existing CB Plan data
     * @param updateData     update data (comment, updatedBy, updatedAt)
     */
    public void updateElasticSearchForArchive(String cbPlanId, Map<String, Object> existingCbPlan,
                                              Map<String, Object> updateData) {
        Map<String, Object> esDocument = new HashMap<>(existingCbPlan);
        esDocument.putAll(updateData);
        esDocument.put(Constants.ID, cbPlanId);
        esDocument.put(Constants.STATUS, Constants.CB_RETIRE);
        Map<String, Object> sanitizedMap = sanitizeForElastic(esDocument);
        esUtilService.addDocument(
                serverProperties.getCpPlanIndex(),
                Constants.INDEX_TYPE,
                cbPlanId,
                sanitizedMap,
                serverProperties.getElasticCbPlanJsonPath());
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
     * The plan year is stored in Cassandra as "planyear" but declared in the schema
     * and index mapping as "planYear";
     *
     * @param sanitized document being prepared for indexing, mutated in place
     */
    private void alignKeysWithEsSchema(Map<String, Object> sanitized) {
        if (sanitized.containsKey(Constants.PLAN_YEAR)) {
            sanitized.put(Constants.REQUEST_PARAM_PLAN_YEAR, sanitized.remove(Constants.PLAN_YEAR));
        }
    }
}
