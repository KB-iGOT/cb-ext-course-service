package com.igot.cb.cbplan.service.impl;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;

import lombok.extern.slf4j.Slf4j;

/**
 * Service for CB Plan data transformation operations.
 *
 * @version 3.0
 */
@Service
@Slf4j
public class CbPlanDataTransformServiceV3Impl {
    private final CbExtServerProperties serverProperties;
    private final ObjectMapper mapper;

    public CbPlanDataTransformServiceV3Impl(CbExtServerProperties serverProperties) {
        this.serverProperties = serverProperties;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Prepares CB Plan data for insert (CREATE operation).
     *
     * @param request API request
     * @param userId  user ID
     * @return prepared CB Plan map
     * @throws JsonProcessingException if JSON processing fails
     */
    public Map<String, Object> prepareCbPlanForInsert(ApiRequest request, String userId)
            throws JsonProcessingException {
        Map<String, Object> requestMap = (Map<String, Object>) request.getRequest();
        Map<String, Object> cbPlan = new HashMap<>();
        cbPlan.put(Constants.PLAN_ID, String.valueOf(Uuids.timeBased()));
        cbPlan.put(Constants.CREATED_BY, userId);
        cbPlan.put(Constants.CREATED_AT, Instant.now());
        cbPlan.put(Constants.STATUS, Constants.DRAFT);
        populatePlanFields(cbPlan, requestMap);
        return cbPlan;
    }

    /**
     * Prepares CB Plan data for update (UPDATE operation on DRAFT).
     *
     * @param incomingRequest incoming request map
     * @param userId          user ID
     * @return prepared update map
     * @throws JsonProcessingException if JSON processing fails
     */
    public Map<String, Object> prepareCbPlanForUpdate(Map<String, Object> incomingRequest, String userId)
            throws JsonProcessingException {
        Map<String, Object> updatedRequest = new HashMap<>();
        updatedRequest.put(Constants.UPDATED_BY, userId);
        updatedRequest.put(Constants.UPDATED_AT, Instant.now());
        populateUpdateFields(updatedRequest, incomingRequest);
        return updatedRequest;
    }

    /**
     * Prepares basic publish update data.
     *
     * @param incomingRequest incoming request map
     * @param userId          user ID
     * @return prepared publish update map
     */
    public Map<String, Object> prepareBasicPublishUpdate(Map<String, Object> incomingRequest, String userId) {
        String comment = (String) incomingRequest.get(Constants.COMMENT);
        Map<String, Object> updatedRequest = new HashMap<>();
        updatedRequest.put(Constants.PUBLISHED_AT, Instant.now());
        updatedRequest.put(Constants.PUBLISHED_BY, userId);
        updatedRequest.put(Constants.UPDATED_AT, Instant.now());
        updatedRequest.put(Constants.COMMENT, comment);
        updatedRequest.put(Constants.UPDATED_BY, userId);
        return updatedRequest;
    }

    /**
     * Prepares CB Plan for re-publish (when updating a LIVE plan).
     * <p>
     * <p>
     * /**
     * Builds updated plan data for LIVE plan modifications.
     *
     * @param incomingRequest         incoming request map
     * @param existingCbPlan          existing CB Plan data
     * @param userId                  user ID
     * @param rootOrgIdsInContextData org IDs extracted from context
     * @param response                API response object
     * @return built update map or empty map if validation fails
     */
    public Map<String, Object> buildUpdatedPlanForLive(Map<String, Object> incomingRequest,
                                                       Map<String, Object> existingCbPlan,
                                                       String userId, Set<String> rootOrgIdsInContextData,
                                                       ApiResponse response) {
        List<String> allowedFields = serverProperties.getCbPlanUpdateAllowedFields();
        Map<String, Object> updatedCbPlan = new HashMap<>();
        for (String field : allowedFields) {
            if (incomingRequest.containsKey(field)
                    && !processLivePlanField(field, incomingRequest, existingCbPlan, updatedCbPlan, response)) {
                return Collections.emptyMap();
            }
        }
        updatedCbPlan.put(Constants.UPDATED_AT, Instant.now());
        updatedCbPlan.put(Constants.UPDATED_BY, userId);
        updatedCbPlan.put(Constants.ROOT_ORG_IDS_IN_CONTEXT_DATA, new ArrayList<>(rootOrgIdsInContextData));
        return updatedCbPlan;
    }

    /**
     * Prepares archive update data.
     *
     * @param comment archive comment
     * @param userId  user ID
     * @return prepared archive update map
     */
    public Map<String, Object> prepareArchiveUpdate(String comment, String userId) {
        Map<String, Object> updateData = new HashMap<>();
        updateData.put(Constants.STATUS, Constants.CB_RETIRE);
        updateData.put(Constants.UPDATED_BY, userId);
        updateData.put(Constants.UPDATED_AT, Instant.now());
        if (Objects.nonNull(comment)) {
            updateData.put(Constants.COMMENT, comment);
        }
        return updateData;
    }

    /**
     * Parses end date from various formats.
     *
     * @param endDateObj end date object
     * @return parsed Instant or null
     */
    public Instant parseEndDate(Object endDateObj) {
        if (Objects.isNull(endDateObj)) {
            return null;
        }
        try {
            if (endDateObj instanceof Date date) {
                return date.toInstant();
            }
            if (endDateObj instanceof Instant instant) {
                return instant;
            }
            if (endDateObj instanceof Long longValue) {
                return Instant.ofEpochMilli(longValue);
            }
            if (endDateObj instanceof String stringValue) {
                return parseEndDateFromString(stringValue);
            }
        } catch (Exception e) {
            throw new RuntimeException(Constants.ERR_INVALID_END_DATE_FORMAT + endDateObj, e);
        }
        return null;
    }

    private void populatePlanFields(Map<String, Object> cbPlan, Map<String, Object> requestMap)
            throws JsonProcessingException {
        cbPlan.put(Constants.IS_APAR, requestMap.getOrDefault(Constants.IS_APAR, false));
        cbPlan.put(Constants.ORG_ID_LIST, requestMap.get(Constants.ORG_ID_LIST));
        cbPlan.put(Constants.ORG_SCOPE, requestMap.get(Constants.ORG_SCOPE));
        cbPlan.put(Constants.CONTENT_LIST, requestMap.get(Constants.CONTENT_LIST));
        cbPlan.put(Constants.NAME, requestMap.get(Constants.NAME));
        cbPlan.put(Constants.COMMENT, requestMap.get(Constants.COMMENT));
        cbPlan.put(Constants.CONTENT_TYPE, requestMap.get(Constants.CONTENT_TYPE));
        cbPlan.put(Constants.END_DATE_REQUEST, parseEndDate(requestMap.get(Constants.END_DATE_REQUEST)));
        cbPlan.put(Constants.CONTEXT_DATA_REQUEST, mapper.writeValueAsString(requestMap.get(Constants.CONTEXT_DATA_REQUEST)));
        cbPlan.put(Constants.PLAN_YEAR, requestMap.get(Constants.REQUEST_PARAM_PLAN_YEAR));
        if (requestMap.containsKey(Constants.PLAN_TYPE)) {
            cbPlan.put(Constants.PLAN_TYPE, requestMap.get(Constants.PLAN_TYPE));
        }
    }

    private void populateUpdateFields(Map<String, Object> updatedRequest, Map<String, Object> incomingRequest)
            throws JsonProcessingException {
        updatedRequest.put(Constants.IS_APAR, incomingRequest.getOrDefault(Constants.IS_APAR, false));
        updatedRequest.put(Constants.ORG_ID_LIST, incomingRequest.get(Constants.ORG_ID_LIST));
        updatedRequest.put(Constants.ORG_SCOPE, incomingRequest.get(Constants.ORG_SCOPE));
        updatedRequest.put(Constants.CONTENT_LIST, incomingRequest.get(Constants.CONTENT_LIST));
        updatedRequest.put(Constants.NAME, incomingRequest.get(Constants.NAME));
        updatedRequest.put(Constants.COMMENT, incomingRequest.get(Constants.COMMENT));
        updatedRequest.put(Constants.CONTENT_TYPE, incomingRequest.get(Constants.CONTENT_TYPE));
        updatedRequest.put(Constants.END_DATE_REQUEST, parseEndDate(incomingRequest.get(Constants.END_DATE_REQUEST)));
        updatedRequest.put(Constants.CONTEXT_DATA_REQUEST,
                mapper.writeValueAsString(incomingRequest.get(Constants.CONTEXT_DATA_REQUEST)));
        updatedRequest.put(Constants.PLAN_YEAR, incomingRequest.get(Constants.REQUEST_PARAM_PLAN_YEAR));
        if (incomingRequest.containsKey(Constants.PLAN_TYPE)) {
            updatedRequest.put(Constants.PLAN_TYPE, incomingRequest.get(Constants.PLAN_TYPE));
        }
    }

    private boolean processLivePlanField(String field, Map<String, Object> incomingRequest,
                                         Map<String, Object> existingCbPlan, Map<String, Object> updatedCbPlan,
                                         ApiResponse response) {
        if (Constants.IS_APAR.equalsIgnoreCase(field)) {
            return validateIsAparUpdate(incomingRequest, existingCbPlan, field, updatedCbPlan, response);
        }
        Object value = incomingRequest.get(field);
        if (Objects.nonNull(value)) {
            updatedCbPlan.put(field, value);
            return true;
        } else {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(String.format(Constants.ERR_FIELD_CANNOT_BE_NULL, field));
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return false;
        }
    }

    private boolean validateIsAparUpdate(Map<String, Object> incomingRequest, Map<String, Object> existingCbPlan,
                                         String field, Map<String, Object> updatedCbPlan, ApiResponse response) {
        boolean existingIsApar = Objects.nonNull(existingCbPlan.get(Constants.IS_APAR))
                && (Boolean) existingCbPlan.get(Constants.IS_APAR);
        if (existingIsApar) {
            boolean newIsApar = Objects.nonNull(incomingRequest.get(field))
                    && (Boolean) incomingRequest.get(field);
            if (!newIsApar) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.ERR_CANNOT_CHANGE_ISAPAR);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return false;
            }
        }
        updatedCbPlan.put(field, incomingRequest.get(field));
        return true;
    }

    private Instant parseEndDateFromString(String endDateStr) {
        try {
            return Instant.parse(endDateStr);
        } catch (DateTimeParseException e) {
            LocalDate localDate = LocalDate.parse(endDateStr, DateTimeFormatter.ofPattern(Constants.DATE_FORMAT_YYYY_MM_DD));
            ZoneId kolkata = ZoneId.of(Constants.TIMEZONE_ASIA_KOLKATA);
            return localDate.atTime(23, 59, 59).atZone(kolkata).toInstant();
        }
    }

    /**
     * Merges and deduplicates plan lists from org and ministry lookup tables.
     * Org plans take precedence over ministry plans when planId duplicates exist.
     *
     * @param orgPlans      plans from org/all_org tables
     * @param ministryPlans plans from ministry lookup table
     * @return merged, deduplicated plan list
     */
    public List<Map<String, Object>> mergePlanLists(List<Map<String, Object>> orgPlans,
                                                    List<Map<String, Object>> ministryPlans) {
        if (CollectionUtils.isEmpty(ministryPlans)) {
            return orgPlans;
        }
        if (CollectionUtils.isEmpty(orgPlans)) {
            return ministryPlans;
        }
        Map<String, Map<String, Object>> planMap = new java.util.LinkedHashMap<>();
        for (Map<String, Object> plan : orgPlans) {
            String planId = (String) plan.get(Constants.PLAN_ID);
            if (org.apache.commons.lang3.StringUtils.isNotBlank(planId)) {
                planMap.put(planId, plan);
            }
        }
        for (Map<String, Object> plan : ministryPlans) {
            String planId = (String) plan.get(Constants.PLAN_ID);
            if (org.apache.commons.lang3.StringUtils.isNotBlank(planId)) {
                planMap.putIfAbsent(planId, plan);
            }
        }
        return new ArrayList<>(planMap.values());
    }

    /**
     * Determines which map contains contextData for extraction.
     * Draft publish flow → contextData in updatedRequest
     * Live republish flow → contextData in existingCbPlan
     *
     * @param updatedRequest updated request map
     * @param existingCbPlan existing CB Plan data
     * @return map containing contextData
     */
    public Map<String, Object> determineContextDataSource(Map<String, Object> updatedRequest,
                                                          Map<String, Object> existingCbPlan) {
        if (updatedRequest.containsKey(Constants.CONTEXT_DATA_REQUEST)) {
            log.debug("determineContextDataSource: Using updatedRequest (draft publish flow)");
            return updatedRequest;
        }
        log.debug("determineContextDataSource: Using existingCbPlan (live republish flow)");
        return existingCbPlan;
    }
}
