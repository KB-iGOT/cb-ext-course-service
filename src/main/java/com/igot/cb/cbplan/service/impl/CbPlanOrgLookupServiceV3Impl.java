package com.igot.cb.cbplan.service.impl;

import java.time.Instant;
import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;

import lombok.extern.slf4j.Slf4j;

/**
 * Service for managing CB Plan organization lookup table operations.
 *
 * @version 3.0
 */
@Service
@Slf4j
public class CbPlanOrgLookupServiceV3Impl {
    private final CassandraOperation cassandraOperation;
    private final ObjectMapper mapper;

    public CbPlanOrgLookupServiceV3Impl(CassandraOperation cassandraOperation) {
        this.cassandraOperation = cassandraOperation;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Handles organization lookup changes when updating a CB Plan.
     *
     * @param cbPlanId           CB Plan ID
     * @param planYear           plan year (e.g., "2026-27")
     * @param existingRootOrgIds existing organization IDs
     * @param newRootOrgIds      new organization IDs
     * @param existingOrgScope   existing organization scope
     * @param response           API response object
     */
    public void handleOrgLookupChanges(String cbPlanId, String planYear, Set<String> existingRootOrgIds,
                                       Set<String> newRootOrgIds, String existingOrgScope,
                                       ApiResponse response) {
        if (CollectionUtils.isEmpty(existingRootOrgIds) || CollectionUtils.isEmpty(newRootOrgIds)) {
            return;
        }
        Set<String> removed = new HashSet<>(existingRootOrgIds);
        removed.removeAll(newRootOrgIds);
        if (CollectionUtils.isNotEmpty(removed) && (Constants.CUSTOM.equalsIgnoreCase(existingOrgScope)
                || Constants.SINGLE.equalsIgnoreCase(existingOrgScope))) {
            ApiResponse removeResp = upsertCustomOrgLookup(cbPlanId, planYear, removed, null, false);
            if (!Constants.SUCCESS.equals(removeResp.get(Constants.RESPONSE))) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(removeResp.getParams().getErr());
                response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                return;
            }
        }
        if (Constants.ALL.equalsIgnoreCase(existingOrgScope)) {
            Set<String> newlyAdded = new HashSet<>(newRootOrgIds);
            newlyAdded.removeAll(existingRootOrgIds);
            if (CollectionUtils.isNotEmpty(newlyAdded)) {
                ApiResponse removeResp = upsertAllOrgLookup(cbPlanId, planYear, null, false);
                if (!Constants.SUCCESS.equals(removeResp.get(Constants.RESPONSE))) {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErr(removeResp.getParams().getErr());
                    response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                }
            }
        }
    }

    /**
     * Upserts custom organization lookup entries.
     *
     * @param cbPlanId  CB Plan ID
     * @param planYear  plan year
     * @param orgIdList list of organization IDs
     * @param endDate   end date (can be null)
     * @param isActive  active status
     * @return API response
     */
    public ApiResponse upsertCustomOrgLookup(String cbPlanId, String planYear, Set<String> orgIdList,
                                             Instant endDate, boolean isActive) {
        ApiResponse response = new ApiResponse();
        try {
            if (CollectionUtils.isEmpty(orgIdList)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("orgIdList is empty. Cannot create lookup entries.");
                return response;
            }
            List<Map<String, Object>> lookupMaps = new ArrayList<>();
            for (String orgId : orgIdList) {
                Map<String, Object> lookupMap = new HashMap<>();
                lookupMap.put("planyear", planYear);
                lookupMap.put("planid", cbPlanId);
                lookupMap.put("orgid", orgId);
                if (Objects.nonNull(endDate)) {
                    lookupMap.put("enddate", endDate);
                }
                lookupMap.put("isactive", isActive);
                lookupMaps.add(lookupMap);
            }
            response = cassandraOperation.insertBulkRecord(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_CB_PLAN_V3_LOOKUP_BY_ORG,
                    lookupMaps);
            if (!Constants.SUCCESS.equals(response.getParams().getStatus())) {
                return response;
            }
            response.getParams().setStatus(Constants.SUCCESS);
            response.getResult().put("message", "Lookup entries created successfully for all orgIds");
        } catch (Exception e) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Exception while creating org lookup entries: " + e.getMessage());
            log.error("CbPlanOrgLookupService.upsertCustomOrgLookup: Error inserting org lookup entries for CB Plan: {}",
                    cbPlanId, e);
        }
        return response;
    }

    /**
     * Upserts ALL organization lookup entry.
     *
     * @param cbPlanId CB Plan ID
     * @param planYear plan year
     * @param endDate  end date (can be null)
     * @param isActive active status
     * @return API response
     */
    public ApiResponse upsertAllOrgLookup(String cbPlanId, String planYear, Instant endDate, boolean isActive) {
        ApiResponse response = new ApiResponse();
        try {
            Map<String, Object> allOrgMap = new HashMap<>();
            allOrgMap.put("planyear", planYear);
            allOrgMap.put(Constants.PLAN_ID, cbPlanId);
            if (Objects.nonNull(endDate)) {
                allOrgMap.put(Constants.END_DATE, endDate);
            }
            allOrgMap.put("isactive", isActive);
            response = (ApiResponse) cassandraOperation.insertRecord(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_CB_PLAN_V3_LOOKUP_BY_ALL_ORG,
                    allOrgMap);
        } catch (Exception e) {
            response.getParams().setStatus(Constants.FAILED);
            response.put(Constants.RESPONSE, Constants.FAILED);
            response.getParams().setErr("Exception while inserting ALL org lookup: " + e.getMessage());
            log.error("CbPlanOrgLookupService.upsertAllOrgLookup: Error inserting ALL org lookup for CB Plan: {}",
                    cbPlanId, e);
        }
        return response;
    }

    /**
     * Deactivates organization lookup entries for archived CB Plans.
     *
     * @param cbPlanId       CB Plan ID
     * @param planYear       plan year
     * @param existingCbPlan existing CB Plan data
     * @param response       API response object
     */
    public void deactivateOrgLookupEntries(String cbPlanId, String planYear, Map<String, Object> existingCbPlan,
                                           ApiResponse response) {
        String orgScope = (String) existingCbPlan.get(Constants.ORG_SCOPE);
        Set<String> rootOrgIds = extractUniqueRootOrgIds(existingCbPlan);
        ApiResponse lookupResp = null;
        if (Constants.SINGLE.equalsIgnoreCase(orgScope) || Constants.CUSTOM.equalsIgnoreCase(orgScope)) {
            lookupResp = upsertCustomOrgLookup(cbPlanId, planYear, rootOrgIds, null, false);
        } else if (Constants.ALL.equalsIgnoreCase(orgScope)) {
            lookupResp = upsertAllOrgLookup(cbPlanId, planYear, null, false);
        }
        if (Objects.nonNull(lookupResp) && !Constants.SUCCESS.equals(lookupResp.get(Constants.RESPONSE))) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(lookupResp.getParams().getErr());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Extracts unique root organization IDs from contextData.
     *
     * @param rawRequest raw request map containing contextData
     * @return set of unique root organization IDs
     */
    public Set<String> extractUniqueRootOrgIds(Map<String, Object> rawRequest) {
        Set<String> orgIdSet = new HashSet<>();
        Object contextDataObj = rawRequest.get(Constants.CONTEXT_DATA_REQUEST);
        if (Objects.isNull(contextDataObj)) {
            return orgIdSet;
        }
        Map<String, Object> contextData = new HashMap<>();
        try {
            if (contextDataObj instanceof String stringData) {
                contextData = mapper.readValue(stringData, new TypeReference<Map<String, Object>>() {
                });
            } else if (contextDataObj instanceof Map) {
                contextData = (Map<String, Object>) contextDataObj;
            } else {
                return orgIdSet;
            }
        } catch (Exception e) {
            log.warn("CbPlanOrgLookupService.extractUniqueRootOrgIds: Failed to parse contextData", e);
            return orgIdSet;
        }
        Map<String, Object> accessControl = (Map<String, Object>) contextData.getOrDefault(
                Constants.ACCESS_CONTROL, new HashMap<>());
        List<Map<String, Object>> userGroups = (List<Map<String, Object>>) accessControl.getOrDefault(
                Constants.USER_GROUPS, new ArrayList<>());
        for (Map<String, Object> userGroup : userGroups) {
            List<Map<String, Object>> criteriaList = (List<Map<String, Object>>) userGroup.get(
                    Constants.USER_GROUP_CRITERIA_LIST);
            if (CollectionUtils.isNotEmpty(criteriaList)) {
                extractOrgIdsFromCriteria(criteriaList, orgIdSet);
            }
        }
        return orgIdSet;
    }

    /**
     * Extracts organization IDs from criteria list.
     *
     * @param criteriaList list of criteria maps
     * @param orgIdSet     set to populate with extracted org IDs
     */
    public void extractOrgIdsFromCriteria(List<Map<String, Object>> criteriaList, Set<String> orgIdSet) {
        for (Map<String, Object> criteria : criteriaList) {
            String criteriaKey = (String) criteria.get(Constants.CRITERIA_KEY);
            if (Constants.ROOT_ORG_ID.equalsIgnoreCase(criteriaKey)
                    || Constants.TARGETED_ORGANISATION.equalsIgnoreCase(criteriaKey)) {
                List<String> values = (List<String>) criteria.get(Constants.CRITERIA_VALUE);
                if (CollectionUtils.isNotEmpty(values)) {
                    orgIdSet.addAll(values);
                }
            }
        }
    }
}
