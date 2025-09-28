package com.igot.cb.service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.cassandra.exceptions.CustomException;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiRespParam;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.model.CbPlanDto;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import com.igot.cb.util.RequestValidator;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class CbPlanServiceImpl {

    private final AccessTokenValidator accessTokenValidator;

    private final ObjectMapper mapper = new ObjectMapper();

    private final CassandraOperation cassandraOperation;

    private final CbExtServerProperties serverProperties;

    private final UserAndOrgServiceImpl userAndOrgService;

    private final ContentInfoServiceImpl contentService;

    private final EsUtilService esUtilService;

    private final RequestValidator requestValidator;

    public CbPlanServiceImpl(AccessTokenValidator accessTokenValidator, CassandraOperation cassandraOperation,
            CbExtServerProperties serverProperties, UserAndOrgServiceImpl userAndOrgService,
            ContentInfoServiceImpl contentService, EsUtilService esUtilService, RequestValidator requestValidator) {
        this.accessTokenValidator = accessTokenValidator;
        this.cassandraOperation = cassandraOperation;
        this.serverProperties = serverProperties;
        this.userAndOrgService = userAndOrgService;
        this.contentService = contentService;
        this.requestValidator = requestValidator;
        this.esUtilService = esUtilService;
    }

    public ApiResponse createCbPlan(ApiRequest request, String userOrgId, String authUserToken) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_CREATE);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authUserToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }

            String rootOrgId = getRootOrgFromUser(userId, response);
            if (Constants.FAILED.equalsIgnoreCase(response.getParams().getStatus())) {
                return response;
            }

            boolean isCCA = getCCAFromOrg(rootOrgId, response);
            if (Constants.FAILED.equalsIgnoreCase(response.getParams().getStatus())) {
                return response;
            }

            List<String> validations = requestValidator.validateCbPlanCreateRequest(request, isCCA, userOrgId);
            if (CollectionUtils.isNotEmpty(validations)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(mapper.writeValueAsString(validations));
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            try {
                Map<String, Object> requestMap = prepareCbPlanForInsert((Map<String, Object>) request.getRequest(),
                        userId);

                ApiResponse resp = (ApiResponse) cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD,
                        Constants.TABLE_CB_PLAN_V2, requestMap);
                if (Constants.SUCCESS.equals(resp.get(Constants.RESPONSE))) {
                    requestMap.put(Constants.ID, String.valueOf(requestMap.get(Constants.PLAN_ID)));
                    Map<String, Object> sanitizedMap = sanitizeForElastic(requestMap);
                    esUtilService.addDocument(serverProperties.getCpPlanIndex(), Constants.INDEX_TYPE,
                            String.valueOf(requestMap.get(Constants.PLAN_ID)),
                            sanitizedMap, serverProperties.getElasticCbPlanJsonPath());
                    response.getResult().put(Constants.ID, String.valueOf(requestMap.get(Constants.PLAN_ID)));
                    response.getResult().put(Constants.STATUS, Constants.CREATED);
                } else {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErr("Failed to Create CB Plan for OrgId: " + userOrgId + " message: "
                            + resp.getParams().getErr());
                    response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                }
            } catch (JsonProcessingException e) {
                log.error("Failed to Create CB Plan for OrgId: " + userOrgId, e);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(e.getMessage());
                response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            }
        } catch (Exception e) {
            log.error("Failed to Create CB Plan for OrgId: " + userOrgId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    public ApiResponse updateCbPlan(ApiRequest request, String userOrgId, String token, List<String> userRoles) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_UPDATE);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(token, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }
            Map<String, Object> updatedCbPlan = (Map<String, Object>) request.getRequest();
            String cbPlanId = (String) updatedCbPlan.get(Constants.ID);
            if (StringUtils.isBlank(cbPlanId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("Required Param id is missing");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            List<Map<String, Object>> cbPlanMapInfo = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_CB_PLAN_V2, Map.of(Constants.PLAN_ID, cbPlanId), null,
                    null);
            Map<String, Object> existingCbPlan = cbPlanMapInfo.get(0);
            if (MapUtils.isEmpty(existingCbPlan)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("cbPlan is not found for id: " + cbPlanId);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            if (!(userId.equals(existingCbPlan.get(Constants.CREATED_BY)) ||
                    serverProperties.getCbPlanUpdatePublishAuthorizedRoles().stream().anyMatch(
                            roles -> CollectionUtils.isNotEmpty(userRoles) && userRoles.contains(roles)))) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("Not Authorized to update cbp Plan");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            String rootOrgId = getRootOrgFromUser(userId, response);
            if (Constants.FAILED.equalsIgnoreCase(response.getParams().getStatus())) {
                return response;
            }

            boolean isCCA = getCCAFromOrg(rootOrgId, response);
            if (Constants.FAILED.equalsIgnoreCase(response.getParams().getStatus())) {
                return response;
            }

            String existingPlanStatus = (String) existingCbPlan.get(Constants.STATUS);
            if (Constants.LIVE.equalsIgnoreCase(existingPlanStatus)) {
                // Any changes to live cb plan will make it to draft status
                // Nothing else can be changed
                // Verify only allowed fields are being changed
                handleUpdateOfLiveCbPlan(response, updatedCbPlan, existingCbPlan, userId, rootOrgId, isCCA);
                if (Constants.FAILED.equalsIgnoreCase(response.getParams().getStatus())) {
                    return response;
                }
            } else if (Constants.DRAFT.equalsIgnoreCase(existingPlanStatus)) {
                List<String> validations = requestValidator.validateCbPlanCreateRequest(request, isCCA, userOrgId);
                if (CollectionUtils.isNotEmpty(validations)) {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErr(mapper.writeValueAsString(validations));
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return response;
                }
                Map<String, Object> updatedRequest = prepareCbPlanForUpdate(updatedCbPlan, existingCbPlan, userId);
                Map<String, Object> resp = cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD,
                        Constants.TABLE_CB_PLAN_V2, updatedRequest, Map.of(Constants.PLAN_ID, cbPlanId));
                if (resp.get(Constants.RESPONSE).equals(Constants.SUCCESS)) {
                    Map<String, Object> sanitizedMap = sanitizeForElastic(updatedRequest);
                    esUtilService.updateDocument(serverProperties.getCpPlanIndex(), Constants.INDEX_TYPE, cbPlanId,
                            sanitizedMap, serverProperties.getElasticCbPlanJsonPath());
                    response.getResult().put(Constants.STATUS, Constants.UPDATED);
                } else {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErr("cbPlan is not found for id: " + cbPlanId);
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                }
            }

            return response;

        } catch (Exception e) {
            log.error("Failed to Update CB Plan for OrgId: " + userOrgId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return response;
    }

    public ApiResponse publishCbPlan(ApiRequest request, String userOrgId, String authUserToken,
            List<String> userRoles) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_PUBLISH);
        Map<String, Object> incomingRequest = (Map<String, Object>) request.getRequest();
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authUserToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }
            String cbPlanId = (String) incomingRequest.get(Constants.ID);
            if (StringUtils.isBlank(cbPlanId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("Required Param id is missing");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            List<Map<String, Object>> cbPlanMapInfo = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_CB_PLAN_V2, Map.of(Constants.PLAN_ID, cbPlanId), null,
                    null);
            Map<String, Object> existingCbPlan = cbPlanMapInfo.get(0);
            if (MapUtils.isEmpty(existingCbPlan)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("cbPlan is not found for id: " + cbPlanId);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            if (!(userId.equals(existingCbPlan.get(Constants.CREATED_BY)) ||
                    serverProperties.getCbPlanUpdatePublishAuthorizedRoles().stream().anyMatch(
                            roles -> CollectionUtils.isNotEmpty(userRoles) && userRoles.contains(roles)))) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("Not Authorized to update cbp Plan");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            String rootOrgId = getRootOrgFromUser(userId, response);
            if (Constants.FAILED.equalsIgnoreCase(response.getParams().getStatus())) {
                return response;
            }

            boolean isCCA = getCCAFromOrg(rootOrgId, response);
            if (Constants.FAILED.equalsIgnoreCase(response.getParams().getStatus())) {
                return response;
            }

            String comment = (String) incomingRequest.get(Constants.COMMENT);
            Map<String, Object> updatedRequest = new HashMap<String, Object>();
            updatedRequest.put(Constants.PUBLISHED_AT, Instant.now());
            updatedRequest.put(Constants.PUBLISHED_BY, userId);
            updatedRequest.put(Constants.UPDATED_AT, Instant.now());
            updatedRequest.put(Constants.COMMENT, comment);
            updatedRequest.put(Constants.UPDATED_BY, userId);
            String existingPlanStatus = (String) existingCbPlan.get(Constants.STATUS);
            if (Constants.LIVE.equalsIgnoreCase(existingPlanStatus)) {
                // Need to update live plan with draft data if any
                // Need to update lookup table entries
                updatedRequest.putAll(prepareCbPlanForRePublish(existingCbPlan, incomingRequest, userId));
            } else if (Constants.DRAFT.equalsIgnoreCase(existingPlanStatus)) {
                // Need to update comment and then publish.
                // Need to update lookup table entries
                updatedRequest.put(Constants.STATUS, Constants.LIVE);
            } else {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(
                        "CbPlan is in invalid state for ID: " + cbPlanId + " current status: " + existingPlanStatus);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            Set<String> rootOrgIdsInCriteria = new HashSet<>();
            List<String> errors = requestValidator.validateContextData(updatedRequest, isCCA, userOrgId,
                    rootOrgIdsInCriteria);
            if (CollectionUtils.isNotEmpty(errors)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(mapper.writeValueAsString(errors));
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            Map<String, Object> resp = cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_CB_PLAN_V2, updatedRequest, Map.of(Constants.PLAN_ID, cbPlanId));
            if (resp.get(Constants.RESPONSE).equals(Constants.SUCCESS)) {
                Map<String, Object> sanitizedMap = sanitizeForElastic(updatedRequest);
                esUtilService.updateDocument(serverProperties.getCpPlanIndex(), Constants.INDEX_TYPE,
                        cbPlanId, sanitizedMap, serverProperties.getElasticCbPlanJsonPath());

                if (Constants.SINGLE.equalsIgnoreCase((String) updatedRequest.get(Constants.ORG_SCOPE)) ||
                        Constants.CUSTOM.equalsIgnoreCase((String) updatedRequest.get(Constants.ORG_SCOPE))) {

                    ApiResponse lookupResp = insertCustomOrgLookup(
                            String.valueOf(cbPlanId),
                            rootOrgIdsInCriteria,
                            parseEndDate(updatedRequest.get(Constants.END_DATE)));
                    if (!Constants.SUCCESS.equals(lookupResp.get(Constants.RESPONSE))) {
                        response.getParams().setStatus(Constants.FAILED);
                        response.getParams().setErr(lookupResp.getParams().getErr());
                        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                        return response;
                    }
                } else if (Constants.ALL.equalsIgnoreCase((String) updatedRequest.get(Constants.ORG_SCOPE))) {
                    ApiResponse singleResp = insertAllOrgLookup(String.valueOf(cbPlanId),
                            parseEndDate(updatedRequest.get(Constants.END_DATE)));
                    if (!Constants.SUCCESS.equals(singleResp.get(Constants.RESPONSE))) {
                        response.getParams().setStatus(Constants.FAILED);
                        response.getParams().setErr(singleResp.getParams().getErr());
                        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                        return response;
                    }
                }
                response.getResult().put(Constants.STATUS, Constants.UPDATED);
                response.getResult().put(Constants.MESSAGE, "Published cbPlan for cbPlanId: " + cbPlanId);
            } else {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams()
                        .setErr((String) resp.get(Constants.ERROR_MESSAGE) + "for cbPlanId: " + cbPlanId);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
            }

        } catch (Exception e) {
            log.error("Failed to Publish CB Plan for OrgId: " + userOrgId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private Instant parseEndDate(Object endDateObj) {
        try {
            if (endDateObj instanceof Date) {
                return ((Date) endDateObj).toInstant();
            }
            if (endDateObj instanceof Instant) {
                return (Instant) endDateObj;
            }
            if (endDateObj instanceof Long) {
                return Instant.ofEpochMilli((Long) endDateObj);
            }
            if (endDateObj instanceof String) {
                String endDateStr = (String) endDateObj;
                try {
                    // Try ISO_INSTANT first (e.g. 2025-12-31T10:15:30Z)
                    return Instant.parse(endDateStr);
                } catch (DateTimeParseException e) {
                    // Fallback: yyyy-MM-dd - parse as end of day in Asia/Kolkata
                    LocalDate localDate = LocalDate.parse(endDateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    ZoneId kolkata = ZoneId.of("Asia/Kolkata");
                    return localDate.atTime(23, 59, 59).atZone(kolkata).toInstant();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Invalid endDate format: " + endDateObj, e);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Set<String> extractUniqueRootOrgIds(Map<String, Object> rawRequest) {
        Set<String> orgIdSet = new HashSet<>();

        Object contextDataObj = rawRequest.get(Constants.CONTEXT_DATA_REQUEST);
        if (contextDataObj == null) {
            return orgIdSet; // nothing to extract
        }

        Map<String, Object> contextData = new HashMap<>();
        try {
            if (contextDataObj instanceof String) {
                ObjectMapper mapper = new ObjectMapper();
                contextData = mapper.readValue((String) contextDataObj, Map.class);
            } else if (contextDataObj instanceof Map) {
                contextData = (Map<String, Object>) contextDataObj;
            } else {
                return orgIdSet; // invalid type
            }
        } catch (Exception e) {
            return orgIdSet; // parsing failed
        }

        Map<String, Object> accessControl = (Map<String, Object>) contextData.getOrDefault(Constants.ACCESS_CONTROL,
                new HashMap<>());
        List<Map<String, Object>> userGroups = (List<Map<String, Object>>) accessControl
                .getOrDefault(Constants.USER_GROUPS, new ArrayList<>());

        for (Map<String, Object> userGroup : userGroups) {
            List<Map<String, Object>> criteriaList = (List<Map<String, Object>>) userGroup
                    .get(Constants.USER_GROUP_CRITERIA_LIST);
            if (criteriaList != null && !criteriaList.isEmpty()) {
                for (Map<String, Object> criteria : criteriaList) {
                    String criteriaKey = (String) criteria.get(Constants.CRITERIA_KEY);
                    if (Constants.ROOT_ORG_ID.equalsIgnoreCase(criteriaKey)) {
                        List<String> values = (List<String>) criteria.get(Constants.CRITERIA_VALUE);
                        if (values != null && !values.isEmpty()) {
                            orgIdSet.addAll(values);
                        }
                    }
                }
            }
        }

        return orgIdSet;
    }

    /**
     * Safely converts an object to java.util.Date.
     * Supports String (ISO 8601), Instant, Timestamp, and Date types.
     *
     * @param endDateObj the object to convert
     * @return Date object or null if conversion fails
     */
    public Date parseToDate(Object endDateObj) {
        if (endDateObj == null)
            return null;

        try {
            if (endDateObj instanceof String) {
                // ISO 8601 string, e.g., "2023-12-14T00:00:00Z"
                String str = (String) endDateObj;
                try {
                    // Try full ISO-8601 datetime first
                    return Date.from(Instant.parse(str));
                } catch (DateTimeParseException e) {
                    // Fallback for date-only strings "yyyy-MM-dd".
                    // Business rule (updated): interpret the date using Asia/Kolkata zone and set
                    // time to 23:59:59 in that zone.
                    // Example: "2026-01-31" -> 2026-01-31T23:59:59+05:30 which is
                    // 2026-01-31T18:29:59Z stored in Cassandra.
                    LocalDate localDate = LocalDate.parse(str, DateTimeFormatter.ISO_LOCAL_DATE);
                    ZoneId kolkata = ZoneId.of("Asia/Kolkata");
                    return Date.from(localDate.atTime(23, 59, 59).atZone(kolkata).toInstant());
                }
            } else if (endDateObj instanceof Instant) {
                return Date.from((Instant) endDateObj);
            } else if (endDateObj instanceof java.sql.Timestamp) {
                return new Date(((java.sql.Timestamp) endDateObj).getTime());
            } else if (endDateObj instanceof java.util.Date) {
                return new Date(((java.util.Date) endDateObj).getTime());
            }
        } catch (Exception e) {
            log.error("Error parsing endDate: {}", endDateObj, e);
        }

        return null;
    }

    public ApiResponse readCbPlan(String cbPlanId, String userOrgId, String authUserToken) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_READ_BY_ID);
        try {
            if (StringUtils.isEmpty(cbPlanId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("CbPlanId is missing.");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            Map<String, Object> cbPlanInfo = new HashMap<>();
            cbPlanInfo.put(Constants.PLAN_ID, cbPlanId);
            List<Map<String, Object>> cbPlanMap = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_CB_PLAN_V2, cbPlanInfo, null, null);

            if (CollectionUtils.isNotEmpty(cbPlanMap)) {
                Map<String, Object> cbPlan = cbPlanMap.get(0);
                Map<String, Object> enrichData = populateReadData(cbPlan);
                enrichData.put(Constants.ID, cbPlanId);
                response.getResult().put(Constants.CONTENT, enrichData);
            } else {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("CbPlan is not exist for ID: " + cbPlanId);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
            }
        } catch (Exception e) {
            log.error("Failed to Read CB Plan for OrgId: " + userOrgId + "for CB PlanId: " + cbPlanId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private Map<String, Object> populateReadData(Map<String, Object> cbPlan) throws Exception {
        Map<String, Object> enrichData = new HashMap<>();
        List<String> contentTypeInfo = new ArrayList<>();
        if (StringUtils.isBlank((String) cbPlan.get(Constants.DRAFT_DATA)) ||
                (StringUtils.isNotBlank((String) cbPlan.get(Constants.DRAFT_DATA))
                        && Constants.LIVE.equalsIgnoreCase((String) cbPlan.get(Constants.STATUS)))) {
            enrichData.put(Constants.NAME, cbPlan.get(Constants.NAME));
            enrichData.put(Constants.CONTENT_TYPE, cbPlan.get(Constants.CONTENT_TYPE));
            contentTypeInfo = (List<String>) cbPlan.get(Constants.CONTENT_LIST);
            enrichData.put(Constants.END_DATE_REQUEST, cbPlan.get(Constants.END_DATE_REQUEST));
            enrichData.put(Constants.CREATED_AT, cbPlan.get(Constants.CREATED_AT_REQ));
            enrichData.put(Constants.IS_APAR, cbPlan.getOrDefault(Constants.IS_APAR, false));
            if (StringUtils.isNotBlank((String) cbPlan.get(Constants.DRAFT_DATA))) {
                Map<String, Object> cbPlanDtoMap = mapper.readValue((String) cbPlan.get(Constants.DRAFT_DATA),
                        new TypeReference<Map<String, Object>>() {
                        });
                cbPlanDtoMap.remove(Constants.ID);
                enrichData.put(Constants.DRAFT_DATA, cbPlanDtoMap);
            }
        } else if (StringUtils.isNotBlank((String) cbPlan.get(Constants.DRAFT_DATA))
                && Constants.DRAFT.equalsIgnoreCase((String) cbPlan.get(Constants.STATUS))) {
            CbPlanDto cbPlanDto = mapper.readValue((String) cbPlan.get(Constants.DRAFT_DATA), CbPlanDto.class);
            enrichData.put(Constants.NAME, cbPlanDto.getName());
            enrichData.put(Constants.CONTENT_TYPE, cbPlanDto.getContentType());
            contentTypeInfo = cbPlanDto.getContentList();
            enrichData.put(Constants.END_DATE_REQUEST, cbPlanDto.getEndDate());
            enrichData.put(Constants.IS_APAR, cbPlanDto.getIsApar() != null ? cbPlanDto.getIsApar() : false);
        }

        enrichData.put(Constants.CREATED_AT, cbPlan.get(Constants.CREATED_AT_REQ));
        enrichData.put(Constants.CB_PUBLISHED_AT, cbPlan.get(Constants.CB_PUBLISHED_AT));
        enrichData.put(Constants.STATUS, cbPlan.get(Constants.STATUS));
        Object contextData = cbPlan.get(Constants.CONTEXT_DATA_REQUEST);
        if (contextData != null) {
            try {
                JsonNode contextDataNode = mapper.readTree(contextData.toString());
                enrichData.put(Constants.CONTEXT_DATA_REQUEST, contextDataNode);
            } catch (Exception ex) {
                log.error("Failed to parse contextDataRequest: {}", contextData, ex);
            }
        } else {
            enrichData.put(Constants.CONTEXT_DATA_REQUEST, null); // or skip putting if you prefer
        }

        String createdBy = (String) cbPlan.get(Constants.CREATED_BY);
        if (StringUtils.isNotBlank(createdBy)) {
            String createdByUserName = "";
            enrichData.put(Constants.CREATED_BY_NAME, createdByUserName);
        }
        enrichData.put(Constants.CREATED_BY, createdBy);
        enrichData.put(Constants.CONTENT_LIST, contentService.enrichContentInfoForCBPlan(contentTypeInfo));
        return enrichData;
    }

    public ApiResponse searchCbPlan(SearchCriteria searchCriteria, String userOrgId, String token) {
        log.info("CbPlanService:searchCbPlan::inside method");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMMUNITY_SEARCH);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(token, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }
            SearchResult searchResult = esUtilService.searchDocuments(serverProperties.getCpPlanIndex(),
                    searchCriteria, serverProperties.getElasticCbPlanJsonPath());
            List<Map<String, Object>> cbPlans = mapper.convertValue(
                    searchResult.getData(),
                    new TypeReference<List<Map<String, Object>>>() {
                    });
            if (!searchResult.getData().isEmpty()) {
                List<Map<String, Object>> dataNode = searchResult.getData();

                if (dataNode != null) {
                    List<Map<String, Object>> enrichedData = new ArrayList<>();

                    for (Map<String, Object> item : dataNode) {
                        // Create a copy of item so we don’t mutate original
                        Map<String, Object> enrichedItem = new HashMap<>(item);
                        String createdBy = (String) enrichedItem.get(Constants.CREATED_BY);
                        if (StringUtils.isNotBlank(createdBy)) {
                            String createdByUserName = "";

                            enrichedItem.put(Constants.CREATED_BY_NAME, createdByUserName);
                        }
                        enrichedItem.put(Constants.CREATED_BY, createdBy);

                        if (item.containsKey(Constants.CONTENT_LIST) && item.get(Constants.CONTENT_LIST) != null) {
                            Object contentListObj = item.get(Constants.CONTENT_LIST);

                            if (contentListObj instanceof List) {
                                enrichedItem.put(Constants.CONTENT_LIST,
                                        contentService.enrichContentInfoForCBPlan((List<String>) contentListObj));
                            }
                        }

                        enrichedData.add(enrichedItem);
                    }

                    // 🔑 Replace original data with enrichedData
                    searchResult.setData(enrichedData);

                    response.getResult().put(Constants.RESULT, searchResult);
                    createSuccessResponse(response);
                    return response;
                }
            }

        } catch (Exception e) {
            log.error("Error occured while searching:", e);
            throw new CustomException(Constants.ERROR, "error while processing",
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private void createSuccessResponse(ApiResponse response) {
        response.setParams(new ApiRespParam());
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
    }

    public ApiResponse retireCbPlan(ApiRequest request, String userOrgId, String token, List<String> userRoles) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_RETIRE);
        Map<String, Object> requestData = (Map<String, Object>) request.getRequest();
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(token, response);
            if (StringUtils.isBlank(userId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            String cbPlanId = requestData.get(Constants.ID).toString();
            String comment = (String) requestData.get(Constants.COMMENT);
            if (StringUtils.isBlank(cbPlanId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("CbPlanId is missing.");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            Map<String, Object> cbPlanInfo = new HashMap<>();
            cbPlanInfo.put(Constants.PLAN_ID, cbPlanId);
            List<Map<String, Object>> cbPlanMap = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_CB_PLAN_V2, cbPlanInfo, null, null);

            if (CollectionUtils.isNotEmpty(cbPlanMap)) {
                Map<String, Object> cbPlan = cbPlanMap.get(0);
                if (!(userId.equals(cbPlan.get(Constants.CREATED_BY)) ||
                        serverProperties.getCbPlanUpdatePublishAuthorizedRoles().stream().anyMatch(
                                roles -> CollectionUtils.isNotEmpty(userRoles) && userRoles.contains(roles)))) {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErr("Not Authorized to delete cbp Plan");
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return response;
                }
                if (Constants.CB_RETIRE.equalsIgnoreCase((String) cbPlan.get(Constants.STATUS))) {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErr("CbPlan is already archived for ID: " + cbPlanId);
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return response;
                }

                cbPlan.put(Constants.UPDATED_AT, Instant.now());
                cbPlan.put(Constants.UPDATED_BY, userId);
                cbPlan.put(Constants.STATUS, Constants.CB_RETIRE);
                if (StringUtils.isNoneBlank(comment)) {
                    cbPlan.put(Constants.COMMENT, comment);
                }
                cbPlan.remove(Constants.PLAN_ID);
                cbPlan.put(Constants.CB_PUBLISHED_AT, Instant.now());
                Map<String, Object> resp = cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD,
                        Constants.TABLE_CB_PLAN_V2, cbPlan, cbPlanInfo);
                if (resp.get(Constants.RESPONSE).equals(Constants.SUCCESS)) {
                    cbPlan.put(Constants.ID, cbPlanId);
                    cbPlan.put(Constants.STATUS, Constants.CB_RETIRE);
                    Map<String, Object> sanitizedMap = sanitizeForElastic(cbPlan);
                    // TO DO : need to use upsert method instead of addDocument
                    esUtilService.addDocument(serverProperties.getCpPlanIndex(), Constants.INDEX_TYPE,
                            cbPlanId, sanitizedMap, serverProperties.getElasticCbPlanJsonPath());
                    CbPlanDto cbPlanDto = mapper.convertValue(sanitizedMap, CbPlanDto.class);
                    List<String> orgIdList = cbPlanDto.getOrgIdList();
                    if (Constants.SINGLE.equalsIgnoreCase(cbPlanDto.getOrgScope())
                            || Constants.CUSTOM.equalsIgnoreCase(cbPlanDto.getOrgScope())) {
                        ApiResponse lookupResp = archiveCustomOrgLookup(cbPlanId, orgIdList);
                        if (!Constants.SUCCESS.equals(lookupResp.get(Constants.RESPONSE))) {
                            response.getParams().setStatus(Constants.FAILED);
                            response.getParams().setErr(lookupResp.getParams().getErr());
                            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                            return response;
                        }
                    }
                    if (Constants.ALL.equalsIgnoreCase(cbPlanDto.getOrgScope())) {
                        Map<String, Object> compositeKeyMap = Map.of(
                                Constants.PLAN_ID_RQST, cbPlanId,
                                Constants.PLAN_YEAR, "ALL");

                        Map<String, Object> singleResp = cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD,
                                Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ALL_ORG,
                                Collections.singletonMap(Constants.IS_ACTIVE, false),
                                compositeKeyMap);
                        if (!Constants.SUCCESS.equals(singleResp.get(Constants.RESPONSE))) {
                            response.getParams().setStatus(Constants.FAILED);
                            response.getParams().setErr((String) singleResp.get(Constants.ERROR_MESSAGE));
                            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                            return response;
                        }
                    }
                    response.getResult().put(Constants.STATUS, Constants.UPDATED);
                    response.getResult().put(Constants.MESSAGE, "Archived cbPlan for cbPlanId: " + cbPlanId);
                } else {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams()
                            .setErr((String) resp.get(Constants.ERROR_MESSAGE) + "for cbPlanId: " + cbPlanId);
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                }
            } else {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("CbPlan is not exist for ID: " + cbPlanId);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
            }
        } catch (Exception e) {
            log.error("Failed to Retire CB Plan for OrgId: " + userOrgId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private ApiResponse archiveCustomOrgLookup(String cbPlanId, List<String> orgIdList) {
        ApiResponse response = new ApiResponse();
        try {
            if (CollectionUtils.isEmpty(orgIdList)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("orgIdList is empty. Cannot archive lookup entries.");
                return response;
            }

            for (String orgId : orgIdList) {
                // attributes to update
                Map<String, Object> updateAttributes = new HashMap<>();
                updateAttributes.put(Constants.IS_ACTIVE, false);
                // primary/composite key for lookup
                Map<String, Object> compositeKey = new HashMap<>();
                compositeKey.put(Constants.PLAN_ID_RQST, cbPlanId);
                compositeKey.put(Constants.ORG_ID_RQST, orgId);

                Map<String, Object> updateResp = cassandraOperation.updateRecord(
                        Constants.KEYSPACE_SUNBIRD,
                        Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ORG,
                        updateAttributes,
                        compositeKey);

                if (!Constants.SUCCESS.equals(updateResp.get(Constants.RESPONSE))) {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErr("Failed to archive record for orgId: " + orgId);
                    return response;
                }
            }
            response.put(Constants.RESPONSE, Constants.SUCCESS);
            response.getParams().setStatus(Constants.SUCCESS);
            response.getResult().put("message", "Lookup entries archived successfully for all orgIds");

        } catch (Exception e) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Exception while archiving org lookup entries: " + e.getMessage());
            log.error("Error archiving org lookup entries for CB Plan: " + cbPlanId, e);
        }

        return response;
    }

    private Map<String, Object> prepareCbPlanForInsert(Map<String, Object> incomingRequest, String userId)
            throws JsonProcessingException {
        Map<String, Object> cbPlan = new HashMap<>();

        cbPlan.put(Constants.PLAN_ID, String.valueOf(Uuids.timeBased()));
        cbPlan.put(Constants.CREATED_BY, userId);
        cbPlan.put(Constants.CREATED_AT, Instant.now());
        cbPlan.put(Constants.STATUS, Constants.DRAFT);
        cbPlan.put(Constants.IS_APAR, incomingRequest.getOrDefault(Constants.IS_APAR, false));
        cbPlan.put(Constants.ORG_ID_LIST, incomingRequest.get(Constants.ORG_ID_LIST));
        cbPlan.put(Constants.ORG_SCOPE, incomingRequest.get(Constants.ORG_SCOPE));
        cbPlan.put(Constants.CONTENT_LIST, incomingRequest.get(Constants.CONTENT_LIST));
        cbPlan.put(Constants.NAME, incomingRequest.get(Constants.NAME));
        cbPlan.put(Constants.COMMENT, incomingRequest.get(Constants.COMMENT));
        cbPlan.put(Constants.CONTENT_TYPE, incomingRequest.get(Constants.CONTENT_TYPE));
        cbPlan.put(Constants.END_DATE_REQUEST,
                parseEndDate(incomingRequest.get(Constants.END_DATE_REQUEST)));
        cbPlan.put(Constants.IS_APAR, incomingRequest.get(Constants.IS_APAR));
        cbPlan.put(Constants.CONTEXT_DATA_REQUEST,
                mapper.writeValueAsString(incomingRequest.get(Constants.CONTEXT_DATA_REQUEST)));
        return cbPlan;
    }

    private Map<String, Object> prepareCbPlanForUpdate(Map<String, Object> incomingRequest,
            Map<String, Object> existingCbPlan, String userId) throws JsonProcessingException {
        Map<String, Object> updatedRequest = new HashMap<>();
        updatedRequest.put(Constants.UPDATED_BY, userId);
        updatedRequest.put(Constants.UPDATED_AT, Instant.now());
        updatedRequest.put(Constants.IS_APAR, incomingRequest.getOrDefault(Constants.IS_APAR, false));
        updatedRequest.put(Constants.ORG_ID_LIST, incomingRequest.get(Constants.ORG_ID_LIST));
        updatedRequest.put(Constants.ORG_SCOPE, incomingRequest.get(Constants.ORG_SCOPE));
        updatedRequest.put(Constants.CONTENT_LIST, incomingRequest.get(Constants.CONTENT_LIST));
        updatedRequest.put(Constants.NAME, incomingRequest.get(Constants.NAME));
        updatedRequest.put(Constants.COMMENT, incomingRequest.get(Constants.COMMENT));
        updatedRequest.put(Constants.CONTENT_TYPE, incomingRequest.get(Constants.CONTENT_TYPE));
        updatedRequest.put(Constants.END_DATE_REQUEST,
                parseEndDate(incomingRequest.get(Constants.END_DATE_REQUEST)));
        updatedRequest.put(Constants.IS_APAR, incomingRequest.get(Constants.IS_APAR));
        updatedRequest.put(Constants.CONTEXT_DATA_REQUEST,
                mapper.writeValueAsString(incomingRequest.get(Constants.CONTEXT_DATA_REQUEST)));
        return updatedRequest;
    }

    private Map<String, Object> prepareCbPlanForRePublish(Map<String, Object> existingCbPlan,
            Map<String, Object> incomingRequest, String userId) throws JsonProcessingException {
        Map<String, Object> dataInDraftObject = existingCbPlan.get(Constants.DRAFT_DATA) != null
                ? mapper.readValue((String) existingCbPlan.get(Constants.DRAFT_DATA),
                        new TypeReference<Map<String, Object>>() {
                        })
                : new HashMap<>();
        if (MapUtils.isEmpty(dataInDraftObject)) {
            return dataInDraftObject;
        }
        Map<String, Object> updatedRequest = new HashMap<>();

        if (dataInDraftObject.containsKey(Constants.IS_APAR)) {
            updatedRequest.put(Constants.IS_APAR, dataInDraftObject.get(Constants.IS_APAR));
        }
        if (dataInDraftObject.containsKey(Constants.ORG_SCOPE)) {
            updatedRequest.put(Constants.ORG_SCOPE, dataInDraftObject.get(Constants.ORG_SCOPE));
        }
        if (dataInDraftObject.containsKey(Constants.NAME)) {
            updatedRequest.put(Constants.NAME, dataInDraftObject.get(Constants.NAME));
        }
        if (dataInDraftObject.containsKey(Constants.CONTEXT_DATA_REQUEST)) {
            updatedRequest.put(Constants.CONTEXT_DATA_REQUEST,
                    mapper.writeValueAsString(dataInDraftObject.get(Constants.CONTEXT_DATA_REQUEST)));
        }
        if (dataInDraftObject.containsKey(Constants.END_DATE_REQUEST)) {
            updatedRequest.put(Constants.END_DATE_REQUEST,
                    parseEndDate(dataInDraftObject.get(Constants.END_DATE_REQUEST)));
        }
        if (dataInDraftObject.containsKey(Constants.ROOT_ORG_IDS_IN_CONTEXT_DATA)) {
            updatedRequest.put(Constants.ROOT_ORG_IDS_IN_CONTEXT_DATA,
                    dataInDraftObject.get(Constants.ROOT_ORG_IDS_IN_CONTEXT_DATA));
        }
        updatedRequest.put(Constants.COMMENT, incomingRequest.get(Constants.COMMENT));

        return updatedRequest;
    }

    public static Map<String, Object> sanitizeForElastic(Map<String, Object> input) {
        Map<String, Object> sanitized = new HashMap<>();
        for (Map.Entry<String, Object> entry : input.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Instant) {
                // Convert Instant → ISO String (e.g., 2025-09-02T09:30:56.446Z)
                sanitized.put(entry.getKey(), DateTimeFormatter.ISO_INSTANT.format((Instant) value));
            } else {
                sanitized.put(entry.getKey(), value);
            }
        }
        return sanitized;
    }

    private ApiResponse insertCustomOrgLookup(String cbPlanId, Set<String> orgIdList, Instant endDate) {
        ApiResponse response = new ApiResponse();
        try {
            if (CollectionUtils.isEmpty(orgIdList)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("orgIdList is empty. Cannot create lookup entries.");
                return response;
            }

            // Prepare all lookup maps
            List<Map<String, Object>> lookupMaps = new ArrayList<>();
            for (String orgId : orgIdList) {
                Map<String, Object> lookupMap = new HashMap<>();
                lookupMap.put("planid", cbPlanId);
                lookupMap.put("orgid", orgId);
                lookupMap.put("enddate", endDate);
                lookupMap.put("isactive", true);
                lookupMaps.add(lookupMap);
            }

            // Call bulk insertion (batching handled inside insertBulkRecord)
            response = cassandraOperation.insertBulkRecord(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ORG,
                    lookupMaps);

            if (!Constants.SUCCESS.equals(response.getParams().getStatus())) {
                return response; // return on first failure
            }

            response.getParams().setStatus(Constants.SUCCESS);
            response.getResult().put("message", "Lookup entries created successfully for all orgIds");

        } catch (Exception e) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Exception while creating org lookup entries: " + e.getMessage());
            log.error("Error inserting org lookup entries for CB Plan: " + cbPlanId, e);
        }

        return response;
    }

    private ApiResponse insertAllOrgLookup(String cbPlanId, Instant endDate) {
        ApiResponse response = new ApiResponse();
        try {
            Map<String, Object> allOrgMap = new HashMap<>();
            allOrgMap.put("planyear", "ALL");
            allOrgMap.put(Constants.PLAN_ID, cbPlanId);
            allOrgMap.put(Constants.END_DATE, endDate); // Instant directly for Cassandra timestamp
            allOrgMap.put("isactive", true);

            response = (ApiResponse) cassandraOperation.insertRecord(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ALL_ORG,
                    allOrgMap);

        } catch (Exception e) {
            response.getParams().setStatus(Constants.FAILED);
            response.put(Constants.RESPONSE, Constants.FAILED);
            response.getParams().setErr("Exception while inserting SINGLE org lookup: " + e.getMessage());
            log.error("Error inserting SINGLE org lookup for CB Plan: " + cbPlanId, e);
        }

        return response;
    }

    private String getRootOrgFromUser(String userId, ApiResponse response) {
        String rootOrgId = null;
        Map<String, Object> userMap = userAndOrgService.readUserProfileFromDB(userId,
                Arrays.asList(Constants.ID, Constants.ROOT_ORG_ID));
        if (MapUtils.isEmpty(userMap)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams()
                    .setErrMsg("Failed to read user details from DB. UserId: " + userId);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return rootOrgId;
        }
        rootOrgId = (String) userMap.get(Constants.ROOT_ORG_ID);

        return rootOrgId;
    }

    private boolean getCCAFromOrg(String orgId, ApiResponse response) {
        boolean isCCA = false;
        Map<String, Object> orgMap = userAndOrgService.readOrgFromDB(orgId, null);
        if (MapUtils.isEmpty(orgMap)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams()
                    .setErrMsg("Failed to read org details from DB. OrgId: " + orgId);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return false;
        }

        if (orgMap.containsKey(Constants.IS_CCA)
                && orgMap.get(Constants.IS_CCA) != null) {
            isCCA = Boolean.parseBoolean(orgMap.get(Constants.IS_CCA).toString());
        }
        return isCCA;
    }

    private void handleUpdateOfLiveCbPlan(ApiResponse response, Map<String, Object> incomingCbPlanRequest,
            Map<String, Object> existingCbPlan, String userId, String rootOrgId, boolean isCCA) {
        try {
            Set<String> rootOrgIdsInContextData = new HashSet<>();
            List<String> errors = requestValidator.validateContextData(incomingCbPlanRequest, isCCA, rootOrgId,
                    rootOrgIdsInContextData);
            if (!errors.isEmpty()) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("Validation errors: " + String.join("; ", errors));
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return;
            }

            List<String> allowedFields = serverProperties.getCbPlanUpdateAllowedFields();

            Map<String, Object> updatedCbPlan = new HashMap<>();
            for (String field : allowedFields) {
                if (incomingCbPlanRequest.containsKey(field)) {
                    if (Constants.IS_APAR.equalsIgnoreCase(field)) {
                        if (existingCbPlan.get(Constants.IS_APAR) == null
                                && (!(Boolean) existingCbPlan.get(Constants.IS_APAR))) {
                            // If existing is not available or false, we can allow update blindly
                            continue;
                        } else {
                            // If existing is true, we cannot allow update to false
                            if (incomingCbPlanRequest.get(field) != null
                                    && !(Boolean) incomingCbPlanRequest.get(field)) {
                                response.getParams().setStatus(Constants.FAILED);
                                response.getParams().setErr("Cannot change isApar from true to false.");
                                response.setResponseCode(HttpStatus.BAD_REQUEST);
                                return;
                            }
                        }
                    }
                    Object value = incomingCbPlanRequest.get(field);
                    if (value != null) {
                        updatedCbPlan.put(field, value);
                    } else {
                        response.getParams().setStatus(Constants.FAILED);
                        response.getParams().setErr("Field '" + field + "' cannot be null.");
                        response.setResponseCode(HttpStatus.BAD_REQUEST);
                        return;
                    }
                }
            }

            updatedCbPlan.put(Constants.UPDATED_AT, Instant.now());
            updatedCbPlan.put(Constants.UPDATED_BY, userId);
            // Add rootOrgIdsInContextData to orgIdList if not already present
            updatedCbPlan.put(Constants.ROOT_ORG_IDS_IN_CONTEXT_DATA, new ArrayList<>(rootOrgIdsInContextData));
            // Save current live state as draft
            String draftData = mapper.writeValueAsString(updatedCbPlan);

            Map<String, Object> resp = cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_CB_PLAN_V2, Map.of(Constants.DRAFT_DATA, draftData),
                    Map.of(Constants.PLAN_ID, existingCbPlan.get(Constants.PLAN_ID)));
            if (resp.get(Constants.RESPONSE).equals(Constants.SUCCESS)) {
                response.getResult().put(Constants.STATUS, Constants.UPDATED);
                response.getResult().put(Constants.MESSAGE,
                        "Updated cbPlan as draft for cbPlanId: " + existingCbPlan.get(Constants.PLAN_ID)
                                + ". Publish to make it live.");
            } else {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams()
                        .setErr((String) resp.get(Constants.ERROR_MESSAGE) + "for cbPlanId: "
                                + existingCbPlan.get(Constants.PLAN_ID));
                response.setResponseCode(HttpStatus.BAD_REQUEST);
            }
        } catch (JsonProcessingException e) {
            log.error("Error serializing existing CB Plan for draft storage", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Error processing existing CB Plan data");
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
