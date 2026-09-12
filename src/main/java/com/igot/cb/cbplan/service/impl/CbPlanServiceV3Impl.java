package com.igot.cb.cbplan.service.impl;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.model.CbPlanDto;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.igot.cb.cache.CbPlanCacheMgrV3;
import com.igot.cb.cbplan.util.AccessRuleEvaluator;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.cbplan.dto.CbPlanContentOccurrence;
import com.igot.cb.cbplan.dto.CbPlanDictionaryCacheEntry;
import com.igot.cb.cbplan.dto.CbPlanReadResponseDto;
import com.igot.cb.cbplan.util.CbPlanYearUtil;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.model.ApiRespParam;
import com.igot.cb.cbplan.service.CbPlanServiceV3;
import com.igot.cb.service.ContentInfoServiceImpl;
import com.igot.cb.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.service.UserAndOrgServiceImpl;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;

import lombok.extern.slf4j.Slf4j;

/**
 * Service implementation for CB Plan V3 operations.
 *
 * @version 3.0
 */
@Service
@Slf4j
public class CbPlanServiceV3Impl implements CbPlanServiceV3 {
    private final AccessTokenValidator accessTokenValidator;
    private final CassandraOperation cassandraOperation;
    private final CbExtServerProperties serverProperties;
    private final EsUtilService esUtilService;
    private final ContentInfoServiceImpl contentService;
    private final ObjectMapper mapper;
    private final CbPlanValidationServiceV3Impl validationService;
    private final CbPlanRequestValidatorImpl cbPlanRequestValidator;
    private final CbPlanDataTransformServiceV3Impl dataTransformService;
    private final CbPlanOrgLookupServiceV3Impl orgLookupService;
    private final CbPlanContentLookupServiceV3Impl contentLookupService;
    private final CbPlanElasticSearchServiceV3Impl elasticSearchService;
    private final CbPlanEnrichmentServiceV3Impl enrichmentService;
    private final CbPlanCacheMgrV3 cbPlanCacheMgrV3;
    private final AccessRuleEvaluator accessRuleEvaluator;
    private final RedisCacheMgr redisCacheMgr;

    public CbPlanServiceV3Impl(AccessTokenValidator accessTokenValidator,
                               CassandraOperation cassandraOperation,
                               CbExtServerProperties serverProperties,
                               UserAndOrgServiceImpl userAndOrgService,
                               EsUtilService esUtilService,
                               ContentInfoServiceImpl contentService,
                               OutboundRequestHandlerServiceImpl outboundRequestHandlerService,
                               CbPlanCacheMgrV3 cbPlanCacheMgrV3,
                               RedisCacheMgr redisCacheMgr) {
        this.cbPlanRequestValidator = new CbPlanRequestValidatorImpl(userAndOrgService, serverProperties);
        this.orgLookupService = new CbPlanOrgLookupServiceV3Impl(cassandraOperation);
        this.contentLookupService = new CbPlanContentLookupServiceV3Impl(cassandraOperation, redisCacheMgr, outboundRequestHandlerService);
        this.elasticSearchService = new CbPlanElasticSearchServiceV3Impl(esUtilService, serverProperties);
        this.enrichmentService = new CbPlanEnrichmentServiceV3Impl(userAndOrgService, contentService);
        this.validationService = new CbPlanValidationServiceV3Impl(accessTokenValidator, userAndOrgService, cbPlanRequestValidator, serverProperties);
        this.dataTransformService = new CbPlanDataTransformServiceV3Impl(serverProperties);
        this.accessTokenValidator = accessTokenValidator;
        this.cassandraOperation = cassandraOperation;
        this.serverProperties = serverProperties;
        this.esUtilService = esUtilService;
        this.contentService = contentService;
        this.cbPlanCacheMgrV3 = cbPlanCacheMgrV3;
        this.accessRuleEvaluator = new AccessRuleEvaluator();
        this.redisCacheMgr = redisCacheMgr;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public ApiResponse createCbPlan(ApiRequest request, String userOrgId, String authToken) {
        log.info("CbPlanServiceV3Impl.createCbPlan: Creating CB Plan for orgId: {}", userOrgId);
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_CREATE);
        String userRootOrgId = null;
        try {
            String userId = validationService.validateAndExtractUserId(authToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }
            Map<String, String> userProfile = buildUserProfile(userId, response);
            userRootOrgId = userProfile.get(Constants.USER_ROOT_ORG_ID);
            if (StringUtils.isEmpty(userRootOrgId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.ERR_USER_ORG_NOT_FOUND);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            log.info("CbPlanServiceV3Impl.createCbPlan: Creating CB Plan for userId: {}, orgId: {}", userId, userRootOrgId);
            boolean isCCA = validationService.validateOrgCCA(userRootOrgId, response);
            if (Constants.FAILED.equalsIgnoreCase(response.getParams().getStatus())) {
                return response;
            }
            if (!validationService.validateRequest(request, isCCA, userRootOrgId, response)) {
                return response;
            }
            executePlanCreation(request, userId, userRootOrgId, response);
        } catch (Exception e) {
            handleException(response, userRootOrgId, e);
        }
        return response;
    }

    private void executePlanCreation(ApiRequest request, String userId, String userOrgId, ApiResponse response) {
        try {
            Map<String, Object> planData = dataTransformService.prepareCbPlanForInsert(request, userId);
            ApiResponse insertResponse = insertPlanToDatabase(planData);
            if (Constants.SUCCESS.equals(insertResponse.get(Constants.RESPONSE))) {
                processSuccessfulCreation(planData, response);
            } else {
                processFailedCreation(insertResponse, userOrgId, response);
            }
        } catch (JsonProcessingException e) {
            handleJsonProcessingException(e, userOrgId, response);
        }
    }

    private ApiResponse insertPlanToDatabase(Map<String, Object> planData) {
        return (ApiResponse) cassandraOperation.insertRecord(
                Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_CB_PLAN_V3,
                planData);
    }

    private void processSuccessfulCreation(Map<String, Object> planData, ApiResponse response) {
        String planId = String.valueOf(planData.get(Constants.PLAN_ID));
        contentLookupService.updateContentLookup(planId, planData);
        elasticSearchService.indexToElasticSearch(planId, planData);
        populateSuccessResponse(response, planId);
    }

    private void populateSuccessResponse(ApiResponse response, String planId) {
        response.getResult().put(Constants.ID, planId);
        response.getResult().put(Constants.STATUS, Constants.CREATED);
        response.setResponseCode(HttpStatus.CREATED);
        log.info("CbPlanServiceV3Impl.createCbPlan: Successfully created CB Plan with ID: {}", planId);
    }

    private void processFailedCreation(ApiResponse insertResponse, String userOrgId, ApiResponse response) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(Constants.ERR_FAILED_TO_CREATE_CB_PLAN + userOrgId
                + Constants.ERR_MESSAGE_SEPARATOR + insertResponse.getParams().getErr());
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        log.error("CbPlanServiceV3Impl.createCbPlan: Insert failed for orgId: {}", userOrgId);
    }

    private void handleJsonProcessingException(JsonProcessingException e, String userOrgId, ApiResponse response) {
        log.error("CbPlanServiceV3Impl.createCbPlan: JSON processing error for orgId: {}", userOrgId, e);
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(e.getMessage());
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    private void handleException(ApiResponse response, String userOrgId, Exception e) {
        log.error("CbPlanServiceV3Impl.createCbPlan: Exception occurred for orgId: {}", userOrgId, e);
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(e.getMessage());
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Override
    public ApiResponse updateCbPlan(ApiRequest request, String userOrgId, String authToken, List<String> userRoles) {
        log.info("CbPlanServiceV3Impl.updateCbPlan: Updating CB Plan for orgId: {}", userOrgId);
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_UPDATE);
        String userRootOrgId = null;
        try {
            String userId = validationService.validateAndExtractUserId(authToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }
            Map<String, String> userProfile = buildUserProfile(userId, response);
            if (userProfile.isEmpty()) {
                return response;
            }
            userRootOrgId = userProfile.get(Constants.USER_ROOT_ORG_ID);
            if (StringUtils.isEmpty(userRootOrgId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.ERR_USER_ORG_NOT_FOUND);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            log.info("CbPlanServiceV3Impl.updateCbPlan: Updating CB Plan for userId: {}, orgId: {}", userId, userRootOrgId);
            if (!validationService.validatePlanIdExists(request, response)) {
                return response;
            }
            executeUpdateFlow(request, userId, userRootOrgId, userRoles, response);
        } catch (Exception e) {
            handleUpdateException(response, userRootOrgId, e);
        }
        return response;
    }

    private void executeUpdateFlow(ApiRequest request, String userId, String userOrgId,
                                   List<String> userRoles, ApiResponse response) throws JsonProcessingException {
        Map<String, Object> updatedCbPlan = (Map<String, Object>) request.getRequest();
        String cbPlanId = (String) updatedCbPlan.get(Constants.ID);
        Map<String, Object> existingCbPlan = fetchExistingPlan(cbPlanId, response);
        if (MapUtils.isEmpty(existingCbPlan)) {
            return;
        }
        if (validationService.isUnauthorizedToUpdate(userId, existingCbPlan, userRoles, response)) {
            return;
        }
        executeAuthorizedUpdate(request, userId, userOrgId, updatedCbPlan, existingCbPlan, response);
    }

    private Map<String, Object> fetchExistingPlan(String cbPlanId, ApiResponse response) {
        List<Map<String, Object>> cbPlanMapInfo = cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD, Constants.TABLE_CB_PLAN_V3,
                Map.of(Constants.PLAN_ID, cbPlanId), null, serverProperties.getCassandraQueryLimitPrimaryKey());
        if (CollectionUtils.isEmpty(cbPlanMapInfo)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.ERR_CB_PLAN_NOT_FOUND + cbPlanId);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return Collections.emptyMap();
        }
        return cbPlanMapInfo.get(0);
    }

    private void executeAuthorizedUpdate(ApiRequest request, String userId, String userOrgId,
                                         Map<String, Object> updatedCbPlan, Map<String, Object> existingCbPlan,
                                         ApiResponse response) throws JsonProcessingException {
        String rootOrgId = validationService.validateUserOrganization(userId, response);
        if (StringUtils.isEmpty(rootOrgId)) {
            return;
        }
        boolean isCCA = validationService.validateOrgCCA(rootOrgId, response);
        if (Constants.FAILED.equalsIgnoreCase(response.getParams().getStatus())) {
            return;
        }
        String existingStatus = (String) existingCbPlan.get(Constants.STATUS);
        if (Constants.LIVE.equalsIgnoreCase(existingStatus)) {
            handleUpdateOfLiveCbPlan(response, updatedCbPlan, existingCbPlan, userId, rootOrgId, isCCA);
        } else if (Constants.DRAFT.equalsIgnoreCase(existingStatus)) {
            handleUpdateOfDraftCbPlan(request, userId, userOrgId, updatedCbPlan, existingCbPlan,
                    isCCA, response);
        }
    }

    private void handleUpdateOfDraftCbPlan(ApiRequest request, String userId, String userOrgId,
                                           Map<String, Object> updatedCbPlan, Map<String, Object> existingCbPlan,
                                           boolean isCCA, ApiResponse response) throws JsonProcessingException {
        List<String> validations = cbPlanRequestValidator.validateCbPlanCreateRequest(request, isCCA, userOrgId, false);
        if (CollectionUtils.isNotEmpty(validations)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(mapper.writeValueAsString(validations));
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return;
        }
        String cbPlanId = (String) updatedCbPlan.get(Constants.ID);
        Map<String, Object> updatedRequest = dataTransformService.prepareCbPlanForUpdate(updatedCbPlan, userId);
        executeDraftPlanUpdate(cbPlanId, updatedRequest, existingCbPlan, response);
    }

    private void executeDraftPlanUpdate(String cbPlanId, Map<String, Object> updatedRequest,
                                        Map<String, Object> existingCbPlan, ApiResponse response) {
        Map<String, Object> resp = cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_CB_PLAN_V3, updatedRequest, Map.of(Constants.PLAN_ID, cbPlanId));
        if (Constants.SUCCESS.equals(resp.get(Constants.RESPONSE))) {
            processDraftUpdateSuccess(cbPlanId, updatedRequest, existingCbPlan, response);
        } else {
            processDraftUpdateFailure(cbPlanId, response);
        }
    }

    private void processDraftUpdateSuccess(String cbPlanId, Map<String, Object> updatedRequest,
                                           Map<String, Object> existingCbPlan, ApiResponse response) {
        contentLookupService.updateContentLookupForModifiedPlan(cbPlanId, updatedRequest, existingCbPlan);
        elasticSearchService.updateElasticSearchForPlan(cbPlanId, updatedRequest);
        response.getResult().put(Constants.STATUS, Constants.UPDATED);
    }

    private void processDraftUpdateFailure(String cbPlanId, ApiResponse response) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(Constants.ERR_CB_PLAN_NOT_FOUND + cbPlanId);
        response.setResponseCode(HttpStatus.BAD_REQUEST);
    }

    private void handleUpdateOfLiveCbPlan(ApiResponse response, Map<String, Object> incomingCbPlanRequest,
                                          Map<String, Object> existingCbPlan, String userId,
                                          String rootOrgId, boolean isCCA) {
        try {
            Set<String> rootOrgIdsInContextData = new HashSet<>();
            if (!validationService.validateContextDataForLivePlan(incomingCbPlanRequest, isCCA, rootOrgId,
                    rootOrgIdsInContextData, response)) {
                return;
            }
            Map<String, Object> updatedCbPlan = dataTransformService.buildUpdatedPlanForLive(incomingCbPlanRequest, existingCbPlan,
                    userId, rootOrgIdsInContextData, response);
            if (MapUtils.isEmpty(updatedCbPlan)) {
                return;
            }
            saveLivePlanAsDraft(updatedCbPlan, existingCbPlan, response);
        } catch (JsonProcessingException e) {
            handleLivePlanUpdateException(e, response);
        }
    }

    private void saveLivePlanAsDraft(Map<String, Object> updatedCbPlan, Map<String, Object> existingCbPlan,
                                     ApiResponse response) throws JsonProcessingException {
        String draftData = mapper.writeValueAsString(updatedCbPlan);
        String planId = (String) existingCbPlan.get(Constants.PLAN_ID);
        Map<String, Object> resp = cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_CB_PLAN_V3, Map.of(Constants.DRAFT_DATA, draftData),
                Map.of(Constants.PLAN_ID, planId));
        if (Constants.SUCCESS.equals(resp.get(Constants.RESPONSE))) {
            response.getResult().put(Constants.STATUS, Constants.UPDATED);
            response.getResult().put(Constants.MESSAGE, String.format(Constants.MSG_UPDATED_AS_DRAFT, planId));
        } else {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr((String) resp.get(Constants.ERROR_MESSAGE) + " for cbPlanId: " + planId);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
        }
    }

    private void handleLivePlanUpdateException(JsonProcessingException e, ApiResponse response) {
        log.error("CbPlanServiceV3Impl.handleUpdateOfLiveCbPlan: {}", Constants.ERR_SERIALIZING_CB_PLAN, e);
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(Constants.ERR_PROCESSING_CB_PLAN_DATA);
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    private void handleUpdateException(ApiResponse response, String userOrgId, Exception e) {
        log.error("CbPlanServiceV3Impl.updateCbPlan: {} {}", Constants.ERR_FAILED_TO_UPDATE_CB_PLAN, userOrgId, e);
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(e.getMessage());
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Override
    public ApiResponse publishCbPlan(ApiRequest request, String userOrgId, String authToken, List<String> userRoles) {
        return publishCbPlan(request, userOrgId, authToken, userRoles, false);
    }

    public ApiResponse publishCbPlan(ApiRequest request, String userOrgId, String authToken, List<String> userRoles, boolean isAdmin) {
        log.info("CbPlanServiceV3Impl.publishCbPlan: Publishing CB Plan for orgId: {}, isAdmin: {}", userOrgId, isAdmin);
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_PUBLISH);
        String userRootOrgId = null;
        try {
            String userId = validationService.validateAndExtractUserId(authToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }
            Map<String, String> userProfile = buildUserProfile(userId, response);
            if (userProfile.isEmpty()) {
                return response;
            }
            userRootOrgId = userProfile.get(Constants.USER_ROOT_ORG_ID);
            if (StringUtils.isEmpty(userRootOrgId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.ERR_USER_ORG_NOT_FOUND);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            log.info("CbPlanServiceV3Impl.publishCbPlan: Publishing CB Plan for userId: {}, orgId: {}, isAdmin: {}",
                    userId, userRootOrgId, isAdmin);
            Map<String, Object> incomingRequest = (Map<String, Object>) request.getRequest();
            String cbPlanId = validationService.validateAndExtractPlanId(incomingRequest, response);
            if (StringUtils.isEmpty(cbPlanId)) {
                return response;
            }
            Map<String, Object> existingCbPlan = fetchExistingPlan(cbPlanId, response);
            if (MapUtils.isEmpty(existingCbPlan)) {
                return response;
            }
            if (!isAdmin && validationService.isUnauthorizedToUpdate(userId, existingCbPlan, userRoles, response)) {
                return response;
            }
            executePublishFlow(request, userId, userRootOrgId, cbPlanId, existingCbPlan, isAdmin, response);
        } catch (Exception e) {
            handlePublishException(response, userRootOrgId, e);
        }
        return response;
    }

    private void executePublishFlow(ApiRequest request, String userId, String userOrgId,
                                    String cbPlanId, Map<String, Object> existingCbPlan,
                                    boolean isAdmin, ApiResponse response) throws JsonProcessingException {
        String rootOrgId = validationService.validateUserOrganization(userId, response);
        if (StringUtils.isEmpty(rootOrgId)) {
            return;
        }
        if (isAdmin) {
            log.info("executePublishFlow: Admin endpoint - using targetedOrganisation={} instead of user's org", userOrgId);
            rootOrgId = userOrgId;
        }
        boolean isCCA = validationService.validateOrgCCA(rootOrgId, response);
        if (Constants.FAILED.equalsIgnoreCase(response.getParams().getStatus())) {
            return;
        }
        Map<String, Object> incomingRequest = (Map<String, Object>) request.getRequest();
        String existingStatus = (String) existingCbPlan.get(Constants.STATUS);
        String planYear = (String) existingCbPlan.get(Constants.PLAN_YEAR);
        Map<String, Object> updatedRequest = preparePublishUpdate(incomingRequest, existingCbPlan,
                userId, isCCA, userOrgId, isAdmin, response);
        if (MapUtils.isEmpty(updatedRequest)) {
            return;
        }
        executePublishTransaction(cbPlanId, planYear, updatedRequest, existingCbPlan, existingStatus, response);
    }

    private Map<String, Object> preparePublishUpdate(Map<String, Object> incomingRequest,
                                                     Map<String, Object> existingCbPlan,
                                                     String userId, boolean isCCA,
                                                     String userOrgId, boolean isAdmin,
                                                     ApiResponse response) throws JsonProcessingException {
        String existingStatus = (String) existingCbPlan.get(Constants.STATUS);
        String comment = (String) incomingRequest.get(Constants.COMMENT);
        Map<String, Object> updatedRequest = new HashMap<>();
        updatedRequest.put(Constants.PUBLISHED_AT, Instant.now());
        updatedRequest.put(Constants.PUBLISHED_BY, userId);
        updatedRequest.put(Constants.UPDATED_AT, Instant.now());
        updatedRequest.put(Constants.COMMENT, comment);
        updatedRequest.put(Constants.UPDATED_BY, userId);
        if (Constants.LIVE.equalsIgnoreCase(existingStatus)) {
            return handleLivePlanPublish(existingCbPlan, incomingRequest, isCCA, userOrgId,
                    updatedRequest, isAdmin, response);
        } else if (Constants.DRAFT.equalsIgnoreCase(existingStatus)) {
            return handleDraftPlanPublish(existingCbPlan, isCCA, userOrgId, updatedRequest, isAdmin, response);
        } else {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("CbPlan is in invalid state for publish. Status: " + existingStatus);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return Collections.emptyMap();
        }
    }

    /**
     * Prepares CB Plan data for re-publishing from draft_data.
     * Selectively extracts only known fields to prevent type mismatches and unexpected data leakage.
     * Aligned with V2 implementation pattern for consistency.
     *
     * @param existingCbPlan  CB Plan record containing draft_data JSON string
     * @param incomingRequest incoming publish request containing comment
     * @return Map containing only validated fields from draft_data, or empty map if no draft_data
     * @throws JsonProcessingException if draft_data JSON parsing fails
     */
    private Map<String, Object> prepareCbPlanForRePublish(Map<String, Object> existingCbPlan,
                                                          Map<String, Object> incomingRequest) throws JsonProcessingException {
        Map<String, Object> dataInDraftObject = Objects.nonNull(existingCbPlan.get(Constants.DRAFT_DATA))
                ? mapper.readValue((String) existingCbPlan.get(Constants.DRAFT_DATA),
                new TypeReference<Map<String, Object>>() {
                })
                : new HashMap<>();
        if (MapUtils.isEmpty(dataInDraftObject)) {
            return dataInDraftObject;
        }
        Map<String, Object> extractedFields = extractDraftFields(dataInDraftObject);
        if (incomingRequest.containsKey(Constants.COMMENT)) {
            extractedFields.put(Constants.COMMENT, incomingRequest.get(Constants.COMMENT));
        }
        return extractedFields;
    }

    private Map<String, Object> handleLivePlanPublish(Map<String, Object> existingCbPlan,
                                                      Map<String, Object> incomingRequest,
                                                      boolean isCCA, String userOrgId,
                                                      Map<String, Object> updatedRequest,
                                                      boolean isAdmin, ApiResponse response) throws JsonProcessingException {
        Set<String> existingRootOrgIdsInCriteria = new HashSet<>();
        Set<String> rootOrgIdsInCriteria = new HashSet<>();
        cbPlanRequestValidator.validateContextData(existingCbPlan, isCCA, userOrgId, existingRootOrgIdsInCriteria, isAdmin);
        updatedRequest.putAll(prepareCbPlanForRePublish(existingCbPlan, incomingRequest));
        if (updatedRequest.containsKey(Constants.CONTEXT_DATA_REQUEST)) {
            List<String> errors = cbPlanRequestValidator.validateContextData(updatedRequest, isCCA, userOrgId,
                    rootOrgIdsInCriteria, isAdmin);
            if (CollectionUtils.isNotEmpty(errors)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(mapper.writeValueAsString(errors));
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return Collections.emptyMap();
            }
        }
        updatedRequest.remove(Constants.ROOT_ORG_IDS_IN_CONTEXT_DATA);
        updatedRequest.put(Constants.DRAFT_DATA, mapper.writeValueAsString(Collections.emptyMap()));
        updatedRequest.put(Constants.EXISTING_ROOT_ORG_IDS, existingRootOrgIdsInCriteria);
        updatedRequest.put(Constants.NEW_ROOT_ORG_IDS, rootOrgIdsInCriteria);
        return updatedRequest;
    }

    private Map<String, Object> handleDraftPlanPublish(Map<String, Object> existingCbPlan, boolean isCCA,
                                                       String userOrgId, Map<String, Object> updatedRequest,
                                                       boolean isAdmin, ApiResponse response) throws JsonProcessingException {
        Set<String> rootOrgIdsInCriteria = new HashSet<>();
        updatedRequest.put(Constants.STATUS, Constants.LIVE);
        updatedRequest.put(Constants.END_DATE_REQUEST, dataTransformService.parseEndDate(existingCbPlan.get(Constants.END_DATE_REQUEST)));
        List<String> errors = cbPlanRequestValidator.validateContextData(existingCbPlan, isCCA, userOrgId,
                rootOrgIdsInCriteria, isAdmin);
        if (CollectionUtils.isNotEmpty(errors)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(mapper.writeValueAsString(errors));
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return Collections.emptyMap();
        }
        updatedRequest.put(Constants.ORG_SCOPE, existingCbPlan.get(Constants.ORG_SCOPE));
        updatedRequest.put(Constants.NEW_ROOT_ORG_IDS, rootOrgIdsInCriteria);
        updatedRequest.put(Constants.EXISTING_ROOT_ORG_IDS, Collections.emptySet());
        return updatedRequest;
    }

    private void executePublishTransaction(String cbPlanId, String planYear, Map<String, Object> updatedRequest,
                                           Map<String, Object> existingCbPlan, String existingStatus,
                                           ApiResponse response) {
        Set<String> existingRootOrgIds = (Set<String>) updatedRequest.get(Constants.EXISTING_ROOT_ORG_IDS);
        Set<String> newRootOrgIds = (Set<String>) updatedRequest.get(Constants.NEW_ROOT_ORG_IDS);
        updatedRequest.remove(Constants.EXISTING_ROOT_ORG_IDS);
        updatedRequest.remove(Constants.NEW_ROOT_ORG_IDS);
        Map<String, Object> sanitizedMap = elasticSearchService.sanitizeForElastic(updatedRequest);
        Map<String, Object> sanitizedExisting = elasticSearchService.sanitizeForElastic(existingCbPlan);
        Map<String, Object> resp = cassandraOperation.updateRecord(
                Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_CB_PLAN_V3,
                updatedRequest,
                Map.of(Constants.PLAN_ID, cbPlanId),
                () -> Objects.nonNull(esUtilService.updateDocument(serverProperties.getCpPlanIndex(), Constants.INDEX_TYPE,
                        cbPlanId, sanitizedMap, serverProperties.getElasticCbPlanJsonPath())),
                () -> {
                    String rollbackResult = esUtilService.updateDocument(serverProperties.getCpPlanIndex(),
                            Constants.INDEX_TYPE, cbPlanId, sanitizedExisting,
                            serverProperties.getElasticCbPlanJsonPath());
                    if (Objects.isNull(rollbackResult)) {
                        log.error("ES_CASSANDRA_DIVERGENCE: failed to roll back ES document for cbPlanId={} "
                                + "after Cassandra commit failure — manual reconciliation required", cbPlanId);
                    }
                });
        if (Constants.SUCCESS.equals(resp.get(Constants.RESPONSE))) {
            updatedRequest.put(Constants.EXISTING_ROOT_ORG_IDS, existingRootOrgIds);
            updatedRequest.put(Constants.NEW_ROOT_ORG_IDS, newRootOrgIds);
            updateOrgLookupTables(cbPlanId, planYear, updatedRequest, existingCbPlan, existingStatus, response);
        } else {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr((String) resp.get(Constants.ERROR_MESSAGE) + " for cbPlanId: " + cbPlanId);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void updateOrgLookupTables(String cbPlanId, String planYear, Map<String, Object> updatedRequest,
                                       Map<String, Object> existingCbPlan, String existingStatus,
                                       ApiResponse response) {
        String orgScope = (String) updatedRequest.getOrDefault(Constants.ORG_SCOPE, existingCbPlan.get(Constants.ORG_SCOPE));
        Instant endDate = (Instant) updatedRequest.getOrDefault(Constants.END_DATE_REQUEST, existingCbPlan.get(Constants.END_DATE_REQUEST));
        Set<String> newRootOrgIds = (Set<String>) updatedRequest.get(Constants.NEW_ROOT_ORG_IDS);
        Set<String> existingRootOrgIds = (Set<String>) updatedRequest.get(Constants.EXISTING_ROOT_ORG_IDS);
        Map<String, Object> contextDataSource = dataTransformService.determineContextDataSource(updatedRequest, existingCbPlan);
        Set<String> newMinistryOrStateIds = orgLookupService.extractMinistryOrStateIds(contextDataSource);
        Set<String> existingMinistryOrStateIds = orgLookupService.extractMinistryOrStateIds(existingCbPlan);
        boolean hasMinistryOrStateId = CollectionUtils.isNotEmpty(newMinistryOrStateIds)
                || CollectionUtils.isNotEmpty(existingMinistryOrStateIds);
        log.debug("updateOrgLookupTables: cbPlanId={}, orgScope={}, hasMinistryOrStateId={}, existingStatus={}",
                cbPlanId, orgScope, hasMinistryOrStateId, existingStatus);
        upsertOrgScopeLookupTables(cbPlanId, planYear, orgScope, newRootOrgIds, endDate, hasMinistryOrStateId, response);
        if (CollectionUtils.isNotEmpty(newMinistryOrStateIds)) {
            insertMinistryOrStateIdLookup(cbPlanId, planYear, newMinistryOrStateIds, endDate, response);
        }
        if (Constants.LIVE.equalsIgnoreCase(existingStatus)) {
            log.info("updateOrgLookupTables: Handling LIVE plan republish - cbPlanId={}, processing lookup changes", cbPlanId);
            orgLookupService.handleOrgLookupChanges(cbPlanId, planYear, existingRootOrgIds, newRootOrgIds,
                    (String) existingCbPlan.get(Constants.ORG_SCOPE), hasMinistryOrStateId, response);
            orgLookupService.handleMinistryOrStateIdLookupChanges(cbPlanId, planYear, existingMinistryOrStateIds,
                    newMinistryOrStateIds, endDate, response);
        }
    }

    private void handlePublishException(ApiResponse response, String userOrgId, Exception e) {
        log.error("CbPlanServiceV3Impl.publishCbPlan: Failed to publish CB Plan for orgId: {}", userOrgId, e);
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(e.getMessage());
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Override
    public ApiResponse retireCbPlan(ApiRequest request, String userOrgId, String authToken, List<String> userRoles) {
        log.info("CbPlanServiceV3Impl.retireCbPlan: Archiving CB Plan for orgId: {}", userOrgId);
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_RETIRE);
        String userRootOrgId = null;
        try {
            String userId = validationService.validateAndExtractUserId(authToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }
            Map<String, String> userProfile = buildUserProfile(userId, response);
            if (userProfile.isEmpty()) {
                return response;
            }
            userRootOrgId = userProfile.get(Constants.USER_ROOT_ORG_ID);
            if (StringUtils.isEmpty(userRootOrgId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.ERR_USER_ORG_NOT_FOUND);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            log.info("CbPlanServiceV3Impl.retireCbPlan: Archiving CB Plan for userId: {}, orgId: {}", userId, userRootOrgId);
            Map<String, Object> requestData = (Map<String, Object>) request.getRequest();
            String cbPlanId = extractPlanIdFromRequest(requestData, response);
            if (StringUtils.isEmpty(cbPlanId)) {
                return response;
            }
            String comment = (String) requestData.get(Constants.COMMENT);
            Map<String, Object> existingCbPlan = fetchExistingPlan(cbPlanId, response);
            if (MapUtils.isEmpty(existingCbPlan)) {
                return response;
            }
            if (validationService.isUnauthorizedToUpdate(userId, existingCbPlan, userRoles, response)) {
                return response;
            }
            if (validationService.isAlreadyArchived(existingCbPlan, cbPlanId, response)) {
                return response;
            }
            executeArchiveFlow(cbPlanId, comment, userId, existingCbPlan, response);
        } catch (Exception e) {
            handleArchiveException(response, userRootOrgId, e);
        }
        return response;
    }

    private String extractPlanIdFromRequest(Map<String, Object> requestData, ApiResponse response) {
        Object planIdObj = requestData.get(Constants.ID);
        if (Objects.isNull(planIdObj)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.ERR_CB_PLAN_ID_MISSING);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return null;
        }
        String cbPlanId = planIdObj.toString();
        if (StringUtils.isBlank(cbPlanId)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.ERR_CB_PLAN_ID_MISSING);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return null;
        }
        return cbPlanId;
    }

    private void executeArchiveFlow(String cbPlanId, String comment, String userId,
                                    Map<String, Object> existingCbPlan, ApiResponse response) {
        String planYear = (String) existingCbPlan.get(Constants.PLAN_YEAR);
        Map<String, Object> updateData = dataTransformService.prepareArchiveUpdate(comment, userId);
        Map<String, Object> resp = cassandraOperation.updateRecord(
                Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_CB_PLAN_V3,
                updateData,
                Map.of(Constants.PLAN_ID, cbPlanId));
        if (Constants.SUCCESS.equals(resp.get(Constants.RESPONSE))) {
            processSuccessfulArchive(cbPlanId, planYear, existingCbPlan, updateData, response);
        } else {
            processFailedArchive(cbPlanId, resp, response);
        }
    }

    private void processSuccessfulArchive(String cbPlanId, String planYear, Map<String, Object> existingCbPlan,
                                          Map<String, Object> updateData, ApiResponse response) {
        contentLookupService.removeFromContentLookup(cbPlanId, existingCbPlan);
        elasticSearchService.updateElasticSearchForArchive(cbPlanId, existingCbPlan, updateData);
        orgLookupService.deactivateOrgLookupEntries(cbPlanId, planYear, existingCbPlan, response);
        if (Constants.SUCCESS.equalsIgnoreCase(response.getParams().getStatus())
                || Objects.isNull(response.getParams().getStatus())) {
            response.getResult().put(Constants.STATUS, Constants.UPDATED);
            response.getResult().put(Constants.MESSAGE, "Archived cbPlan for cbPlanId: " + cbPlanId);
        }
    }

    private void processFailedArchive(String cbPlanId, Map<String, Object> resp, ApiResponse response) {
        response.getParams().setStatus(Constants.FAILED);
        String errorMsg = String.format("%s for cbPlanId: %s", resp.get(Constants.ERROR_MESSAGE), cbPlanId);
        response.getParams().setErr(errorMsg);
        response.setResponseCode(HttpStatus.BAD_REQUEST);
    }

    private void handleArchiveException(ApiResponse response, String userOrgId, Exception e) {
        log.error("CbPlanServiceV3Impl.retireCbPlan: Failed to archive CB Plan for orgId: {}", userOrgId, e);
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(e.getMessage());
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Override
    public ApiResponse searchCbPlan(SearchCriteria searchCriteria, String userOrgId, String authToken) {
        log.info("CbPlanServiceV3Impl.searchCbPlan: Searching CB Plans for orgId: {}", userOrgId);
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMMUNITY_SEARCH);
        try {
            String userId = validationService.validateAndExtractUserId(authToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }
            SearchResult searchResult = esUtilService.searchDocuments(
                    serverProperties.getCpPlanIndex(),
                    searchCriteria,
                    serverProperties.getElasticCbPlanJsonPath());
            if (CollectionUtils.isNotEmpty(searchResult.getData())) {
                List<Map<String, Object>> enrichedData = enrichmentService.enrichSearchResults(searchResult.getData());
                searchResult.setData(enrichedData);
                response.getResult().put(Constants.RESULT, searchResult);
                createSuccessResponse(response);
            }
        } catch (Exception e) {
            handleSearchException(response, userOrgId, e);
        }
        return response;
    }

    private void createSuccessResponse(ApiResponse response) {
        response.setParams(new ApiRespParam());
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
    }

    private void handleSearchException(ApiResponse response, String userOrgId, Exception e) {
        log.error("CbPlanServiceV3Impl.searchCbPlan: Error occurred while searching for orgId: {}", userOrgId, e);
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr("Error while processing search");
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     * Gets CB Plan dictionary for a user - groups content IDs by APAR/non-APAR
     * with plan occurrences and optional enrichment.
     * Caching strategy: Two-tier (Redis user-specific + Caffeine plan-level) to handle
     * 60K TPS. Redis stores plan IDs per user/year; Caffeine caches full plan data.
     *
     * @param request   API request containing planYear (optional) and enrichment (optional)
     * @param authToken authentication token
     * @return ApiResponse with aparContentList, nonAparContentList, and optionally enrichedContentList
     */
    @Override
    public ApiResponse getCBPlanDictionaryForUser(ApiRequest request, String authToken) {
        log.debug("getCBPlanDictionaryForUser: Entry");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CBPLAN_V3_GET_USER_DICTIONARY);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isBlank(userId)) {
                log.warn("getCBPlanDictionaryForUser: Authentication failed - invalid or missing token");
                response.getParams().setErr("Invalid or missing authentication token");
                response.setResponseCode(HttpStatus.UNAUTHORIZED);
                return response;
            }
            Map<String, Object> requestData = (Map<String, Object>) request.getRequest();
            if (Objects.isNull(requestData)) {
                requestData = new HashMap<>();
            }
            String requestedPlanYear = (String) requestData.get(Constants.REQUEST_PARAM_PLAN_YEAR);
            Boolean enrichment = (Boolean) requestData.get(Constants.REQUEST_PARAM_ENRICHMENT);
            boolean isEnrichmentEnabled = Boolean.TRUE.equals(enrichment);
            String planYear = resolvePlanYear(requestedPlanYear, response);
            if (Objects.isNull(planYear)) {
                return response;
            }
            String redisCacheKey = Constants.CB_PLAN_REDIS_KEY_PREFIX + userId + ":" + planYear + ":dict";
            CbPlanDictionaryCacheEntry cacheEntry = getCachedDictionaryEntry(redisCacheKey);
            if (Objects.nonNull(cacheEntry)) {
                log.info("getCBPlanDictionaryForUser: Cache hit - userId={}, planYear={}, aparCount={}, nonAparCount={}",
                        userId, planYear, cacheEntry.getAparCount(), cacheEntry.getNonAparCount());
                enrichAndPopulateResponse(response, cacheEntry, isEnrichmentEnabled);
                log.info("getCBPlanDictionaryForUser: Success - userId={}, aparCount={}, nonAparCount={}",
                        userId, cacheEntry.getAparCount(), cacheEntry.getNonAparCount());
                return response;
            }
            Map<String, String> userProfile = buildUserProfile(userId, response);
            if (userProfile.isEmpty()) {
                return response;
            }
            String userOrgId = userProfile.get(Constants.USER_ROOT_ORG_ID);
            log.info("getCBPlanDictionaryForUser: Cache miss - userId={}, orgId={}, planYear={}",
                    userId, userOrgId, planYear);
            cacheEntry = computeDictionaryFromPlans(userId, userOrgId, planYear, redisCacheKey, response);
            if (Objects.isNull(cacheEntry)) {
                populateEmptyDictionaryResponse(response);
                return response;
            }
            enrichAndPopulateResponse(response, cacheEntry, isEnrichmentEnabled);
            log.info("getCBPlanDictionaryForUser: Success - userId={}, aparCount={}, nonAparCount={}",
                    userId, cacheEntry.getAparCount(), cacheEntry.getNonAparCount());
        } catch (Exception e) {
            log.error("getCBPlanDictionaryForUser: Failed to fetch CB Plan dictionary - userId extracted from token", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Failed to fetch CB Plan dictionary: " + e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    /**
     * Resolves plan year to use: requested year if valid, else current financial year.
     *
     * @param requestedPlanYear plan year from request (nullable)
     * @param response          API response for error reporting
     * @return validated plan year in YYYY-YY format, or null if invalid
     */
    private String resolvePlanYear(String requestedPlanYear, ApiResponse response) {
        if (StringUtils.isBlank(requestedPlanYear)) {
            String currentFY = CbPlanYearUtil.resolveCurrentFinancialYear();
            log.debug("resolvePlanYear: Using current financial year - {}", currentFY);
            return currentFY;
        }
        String normalized = CbPlanYearUtil.validateAndNormalize(requestedPlanYear);
        if (Objects.isNull(normalized)) {
            log.warn("resolvePlanYear: Invalid format - requested={}", requestedPlanYear);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Invalid planYear format. Expected: YYYY-YY (e.g., '2026-27')");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return null;
        }
        log.debug("resolvePlanYear: Using requested year - {}", normalized);
        return normalized;
    }

    /**
     * Builds user profile map with root org and personal details.
     *
     * @param userId   user ID
     * @param response API response for error reporting
     * @return user profile map with flattened keys, or empty map if user not found
     */
    private Map<String, String> buildUserProfile(String userId, ApiResponse response) {
        log.debug("buildUserProfile: Fetching profile for userId={}", userId);
        try {
            String cacheKey = Constants.USER + ":basicProfile:" + userId;
            String cachedData = redisCacheMgr.getFromCache(cacheKey);
            Map<String, Object> userBasicProfile;
            if (StringUtils.isNotBlank(cachedData)) {
                log.debug("buildUserProfile: Redis cache HIT for userId={}", userId);
                userBasicProfile = mapper.readValue(cachedData, new TypeReference<Map<String, Object>>() {
                });
                if (userBasicProfile.containsKey(Constants.PROFILE_DETAILS)) {
                    Object profileDetailsValue = userBasicProfile.remove(Constants.PROFILE_DETAILS);
                    userBasicProfile.put(Constants.PROFILE_DETAILS.toLowerCase(), profileDetailsValue);
                }
            } else {
                log.debug("buildUserProfile: Redis cache MISS, querying Cassandra - userId={}", userId);
                Map<String, Object> propertiesMap = Map.of(Constants.ID, userId);
                List<String> userFields = Arrays.asList(Constants.ID, Constants.ROOT_ORG_ID, Constants.PROFILE_DETAILS);
                List<Map<String, Object>> userList = cassandraOperation.getRecordsByProperties(
                        Constants.KEYSPACE_SUNBIRD, Constants.USER, propertiesMap, userFields,
                        serverProperties.getCassandraQueryLimitPrimaryKey());
                if (CollectionUtils.isEmpty(userList)) {
                    log.warn("buildUserProfile: User not found - userId={}", userId);
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErr("User does not exist");
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return Map.of();
                }
                userBasicProfile = userList.get(0);
            }
            Map<String, String> userProfile = new HashMap<>();
            setUserProfileFromBasicData(userProfile, userBasicProfile);
            log.debug("buildUserProfile: Profile built - userId={}, orgId={}", userId, userProfile.get(Constants.USER_ROOT_ORG_ID));
            return userProfile;
        } catch (Exception e) {
            log.error("Error building user profile for userId: {}", userId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Error fetching user profile");
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return Map.of();
        }
    }

    private void setUserProfileFromBasicData(Map<String, String> userProfile, Map<String, Object> userBasicProfile)
            throws JsonProcessingException {
        if (MapUtils.isEmpty(userBasicProfile)) {
            log.warn("User basic profile is empty");
            return;
        }
        userProfile.put(Constants.USER, (String) userBasicProfile.get(Constants.ID));
        userProfile.put(Constants.USER_ROOT_ORG_ID, (String) userBasicProfile.get(Constants.ROOT_ORG_ID));
        Object rawValue = userBasicProfile.get(Constants.PROFILE_DETAILS.toLowerCase());
        if (Objects.isNull(rawValue)) {
            log.warn("profileDetails is null for userId: {}", userBasicProfile.get(Constants.ID));
            return;
        }
        Map<String, Object> profileDetails = parseProfileDetails(rawValue);
        if (MapUtils.isEmpty(profileDetails)) {
            return;
        }
        extractProfessionalDetails(userProfile, profileDetails);
        userProfile.put(Constants.PROFILE_STATUS_LOWER_KEY,
                (String) profileDetails.get(Constants.PROFILE_STATUS_KEY));
        extractCadreDetails(userProfile, profileDetails);
        enrichmentService.extractMinistryOrStateDetails(userProfile, profileDetails);
        extractExtendedProfile(userProfile, (String) userBasicProfile.get(Constants.ID));
    }

    private Map<String, Object> parseProfileDetails(Object rawValue) throws JsonProcessingException {
        if (rawValue instanceof String str && StringUtils.isNotBlank(str)) {
            return mapper.readValue(str, new TypeReference<Map<String, Object>>() {
            });
        } else if (rawValue instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        } else {
            try {
                return mapper.convertValue(rawValue, new TypeReference<Map<String, Object>>() {
                });
            } catch (Exception e) {
                log.error("Failed to convert profileDetails", e);
                return Map.of();
            }
        }
    }

    private void extractProfessionalDetails(Map<String, String> userProfile, Map<String, Object> profileDetails) {
        List<Map<String, Object>> professionalDetailList =
                (List<Map<String, Object>>) profileDetails.get(Constants.PROFESSIONAL_DETAILS);
        if (CollectionUtils.isNotEmpty(professionalDetailList)) {
            Map<String, Object> professionalDetails = professionalDetailList.get(0);
            userProfile.put(Constants.DESIGNATION, (String) professionalDetails.get(Constants.DESIGNATION));
            userProfile.put(Constants.GROUP, (String) professionalDetails.get(Constants.GROUP));
        }
    }

    private void extractCadreDetails(Map<String, String> userProfile, Map<String, Object> profileDetails) {
        Map<String, Object> cadreDetails = (Map<String, Object>) profileDetails.get(Constants.CADRE_DETAILS);
        boolean centralDeputation = false;
        if (MapUtils.isNotEmpty(cadreDetails)) {
            userProfile.put(Constants.CADRE, (String) cadreDetails.get(Constants.CADRE_NAME));
            userProfile.put(Constants.SERVICE, (String) cadreDetails.get(Constants.CIVIL_SERVICE_NAME));
            if (cadreDetails.containsKey(Constants.CADRE_BATCH)) {
                userProfile.put(Constants.BATCH, String.valueOf(cadreDetails.get(Constants.CADRE_BATCH)));
            }
            if (cadreDetails.containsKey(Constants.CENTRAL_DEPUTATION)) {
                centralDeputation = (Boolean) cadreDetails.get(Constants.CENTRAL_DEPUTATION);
            }
        }
        userProfile.put(Constants.CENTRAL_DEPUTATION_LOWER_KEY, String.valueOf(centralDeputation));
    }

    private void extractExtendedProfile(Map<String, String> userProfile, String userId) {
        try {
            Map<String, Object> propertiesMap = Map.of(Constants.USER_ID, userId);
            List<Map<String, Object>> extendedProfileList = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER_EXTENDED_PROFILE, propertiesMap, List.of(),
                    serverProperties.getCassandraQueryLimitUserExtendedProfile());
            if (CollectionUtils.isNotEmpty(extendedProfileList)) {
                Map<String, Object> extendedProfile = extendedProfileList.get(0);
                for (Map.Entry<String, Object> entry : extendedProfile.entrySet()) {
                    if (entry.getValue() instanceof String value) {
                        userProfile.put(entry.getKey().toLowerCase(), value);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to load extended profile for userId: {}", userId, e);
        }
    }

    /**
     * Processes all plans for dictionary grouping by APAR/non-APAR with access control.
     *
     * @param activeCbPlans     list of active CB plans
     * @param userProfile       user profile with location/cadre details
     * @param userOrgId         user's root org ID
     * @param aparContentMap    accumulator for APAR content
     * @param nonAparContentMap accumulator for non-APAR content
     * @param aparContentIds    tracking set for APAR content IDs
     * @param plansToCache      list of accessible plan IDs for caching
     */
    private void processPlansForDictionary(List<Map<String, Object>> activeCbPlans,
                                           Map<String, String> userProfile,
                                           String userOrgId,
                                           Map<String, List<CbPlanContentOccurrence>> aparContentMap,
                                           Map<String, List<CbPlanContentOccurrence>> nonAparContentMap,
                                           Set<String> aparContentIds,
                                           List<String> plansToCache) {
        log.debug("processPlansForDictionary: Processing {} plans", activeCbPlans.size());
        for (Map<String, Object> cbPlan : activeCbPlans) {
            processSinglePlan(cbPlan, userProfile, userOrgId, aparContentMap, nonAparContentMap, aparContentIds, plansToCache);
        }
        log.debug("processPlansForDictionary: Completed - accessiblePlans={}, aparContent={}, nonAparContent={}",
                plansToCache.size(), aparContentMap.size(), nonAparContentMap.size());
    }

    /**
     * Processes a single plan: evaluates access control and groups content by APAR status.
     *
     * @param cbPlan            CB plan to process
     * @param userProfile       user profile for access control
     * @param userOrgId         user's root org ID
     * @param aparContentMap    accumulator for APAR content
     * @param nonAparContentMap accumulator for non-APAR content
     * @param aparContentIds    tracking set for APAR content IDs
     * @param plansToCache      list of accessible plan IDs for caching
     */
    private void processSinglePlan(Map<String, Object> cbPlan,
                                   Map<String, String> userProfile,
                                   String userOrgId,
                                   Map<String, List<CbPlanContentOccurrence>> aparContentMap,
                                   Map<String, List<CbPlanContentOccurrence>> nonAparContentMap,
                                   Set<String> aparContentIds,
                                   List<String> plansToCache) {
        try {
            if (!evaluateAccessControl(cbPlan, userProfile)) {
                log.debug("processSinglePlan: Access denied - planId={}", cbPlan.get(Constants.PLAN_ID));
                return;
            }
            String planId = (String) cbPlan.get(Constants.PLAN_ID);
            Instant endDate = (Instant) cbPlan.get(Constants.END_DATE_KEY);
            boolean isApar = Boolean.TRUE.equals(cbPlan.get(Constants.IS_APAR));
            String planType = (String) cbPlan.get(Constants.PLAN_TYPE);
            List<String> contentList = (List<String>) cbPlan.get(Constants.CONTENT_LIST);
            if (CollectionUtils.isEmpty(contentList)) {
                log.debug("processSinglePlan: Empty content list - planId={}", planId);
                return;
            }
            plansToCache.add(planId);
            log.debug("processSinglePlan: Processing content - planId={}, isApar={}, planType={}, contentCount={}",
                    planId, isApar, planType, contentList.size());
            ContentProcessingContext context = new ContentProcessingContext(
                    planId, endDate, isApar, planType, userProfile, userOrgId,
                    aparContentMap, nonAparContentMap, aparContentIds);
            processContentList(contentList, context);
        } catch (Exception e) {
            log.error("processSinglePlan: Failed to process - planId={}", cbPlan.get(Constants.PLAN_ID), e);
        }
    }

    /**
     * Processes content list and groups by APAR status with _rc course access control.
     *
     * @param contentList list of content IDs
     * @param context     processing context with plan and user details
     */
    private void processContentList(List<String> contentList, ContentProcessingContext context) {
        for (String contentId : contentList) {
            if (StringUtils.isBlank(contentId) || !isContentAccessible(contentId, context.userProfile, context.userOrgId)) {
                continue;
            }
            addContentOccurrence(contentId, context);
        }
    }

    /**
     * Adds content occurrence to appropriate map (APAR or non-APAR).
     * APAR takes precedence - content in APAR plans moves all occurrences to APAR list.
     *
     * @param contentId content ID to add
     * @param context   processing context with accumulator maps
     */
    private void addContentOccurrence(String contentId, ContentProcessingContext context) {
        CbPlanContentOccurrence occurrence = new CbPlanContentOccurrence(context.planId, context.endDate, context.planType);
        if (context.aparContentIds.contains(contentId)) {
            context.aparContentMap.computeIfAbsent(contentId, k -> new ArrayList<>()).add(occurrence);
        } else if (context.isApar) {
            context.aparContentIds.add(contentId);
            context.aparContentMap.computeIfAbsent(contentId, k -> new ArrayList<>()).add(occurrence);
            List<CbPlanContentOccurrence> nonAparOccurrences = context.nonAparContentMap.remove(contentId);
            if (CollectionUtils.isNotEmpty(nonAparOccurrences)) {
                context.aparContentMap.get(contentId).addAll(nonAparOccurrences);
            }
        } else {
            context.nonAparContentMap.computeIfAbsent(contentId, k -> new ArrayList<>()).add(occurrence);
        }
    }

    /**
     * Context record grouping all parameters needed for content processing.
     */
    private record ContentProcessingContext(
            String planId,
            Instant endDate,
            boolean isApar,
            String planType,
            Map<String, String> userProfile,
            String userOrgId,
            Map<String, List<CbPlanContentOccurrence>> aparContentMap,
            Map<String, List<CbPlanContentOccurrence>> nonAparContentMap,
            Set<String> aparContentIds) {
    }

    /**
     * Evaluates access control rules from plan's context data.
     * Match-all criteria in at least one group logic.
     *
     * @param cbPlan      CB plan with context data
     * @param userProfile user profile for matching
     * @return true if user has access, false otherwise
     */
    private boolean evaluateAccessControl(Map<String, Object> cbPlan, Map<String, String> userProfile) {
        Object contextDataObj = cbPlan.get(Constants.CONTEXT_DATA_REQUEST);
        if (Objects.isNull(contextDataObj)) {
            log.debug("evaluateAccessControl: No context data - access granted by default - planId={}",
                    cbPlan.get(Constants.PLAN_ID));
            return true;
        }
        boolean hasAccess = accessRuleEvaluator.hasAccess(contextDataObj, userProfile,
                cbPlan.get(Constants.PLAN_ID));
        log.debug("evaluateAccessControl: planId={}, hasAccess={}", cbPlan.get(Constants.PLAN_ID), hasAccess);
        return hasAccess;
    }

    private boolean isContentAccessible(String contentId, Map<String, String> userProfile, String userOrgId) {
        if (!contentId.contains(Constants.SECURE_CONTENT_SUFFIX)) {
            return true;
        }
        if (!Constants.VERIFIED.equalsIgnoreCase(userProfile.get(Constants.PROFILE_STATUS_LOWER_KEY))) {
            log.debug("Secure content {} excluded: profile not VERIFIED", contentId);
            return false;
        }
        try {
            Map<String, Object> contentDetails = contentService.readContent(contentId, null);
            if (MapUtils.isEmpty(contentDetails)) {
                return false;
            }
            Object secureSettingsObj = contentDetails.get(Constants.SECURE_SETTINGS);
            if (!(secureSettingsObj instanceof Map)) {
                return false;
            }
            Map<?, ?> secureSettings = (Map<?, ?>) secureSettingsObj;
            Object orgListObj = secureSettings.get(Constants.ORGANISATION);
            if (!(orgListObj instanceof List)) {
                return false;
            }
            List<String> secureOrgList = ((List<?>) orgListObj).stream()
                    .filter(String.class::isInstance)
                    .map(String.class::cast)
                    .toList();
            return secureOrgList.contains(userOrgId);
        } catch (Exception e) {
            log.error("Error checking secure content accessibility: {}", contentId, e);
            return false;
        }
    }

    private Map<String, Object> enrichContentList(Map<String, List<CbPlanContentOccurrence>> aparContentMap,
                                                  Map<String, List<CbPlanContentOccurrence>> nonAparContentMap) {
        Set<String> allContentIds = new HashSet<>();
        allContentIds.addAll(aparContentMap.keySet());
        allContentIds.addAll(nonAparContentMap.keySet());
        List<String> allowedFields = serverProperties.getCbPlanEnrichedContentFieldsList();
        Map<String, Object> enrichedMap = allContentIds.stream()
                .map(contentId -> {
                    try {
                        Map<String, Object> contentDetails = getContentMetadata(contentId);
                        contentDetails.remove(Constants.CACHE_FLAG_FROM_CACHE);
                        Map<String, Object> filteredDetails = filterContentFields(contentDetails, allowedFields);
                        return Map.entry(contentId, filteredDetails);
                    } catch (Exception e) {
                        log.error("Failed to enrich content: {}", contentId, e);
                        return null;
                    }
                })
                .filter(entry -> entry != null && MapUtils.isNotEmpty(entry.getValue()))
                .collect(LinkedHashMap::new,
                        (map, entry) -> map.put(entry.getKey(), entry.getValue()),
                        LinkedHashMap::putAll);
        log.debug("Enriched {} out of {} unique content IDs", enrichedMap.size(), allContentIds.size());
        return enrichedMap;
    }

    private Map<String, Object> filterContentFields(Map<String, Object> contentDetails, List<String> allowedFields) {
        if (MapUtils.isEmpty(contentDetails) || allowedFields.isEmpty()) {
            return contentDetails;
        }
        return contentDetails.entrySet().stream()
                .filter(entry -> allowedFields.contains(entry.getKey()))
                .collect(LinkedHashMap::new,
                        (map, entry) -> map.put(entry.getKey(), entry.getValue()),
                        LinkedHashMap::putAll);
    }

    /**
     * Gets content metadata from Redis cache first, falls back to extended content read API if not cached.
     * Uses the same Redis key format as the extended content read API in knowledge-platform.
     * Redis key: extended_read_content_{contentId}
     * API endpoint: GET /content/v1/extended/read/{contentId}
     *
     * @param contentId content ID
     * @return content metadata map, or empty map if not found
     * @throws JsonProcessingException if deserialization fails
     */
    private Map<String, Object> getContentMetadata(String contentId) throws JsonProcessingException {
        String cacheKey = Constants.EXTENDED_READ_CONTENT_CACHE_KEY_PREFIX + contentId;
        String cachedContent = redisCacheMgr.getFromCache(cacheKey);
        if (StringUtils.isNotBlank(cachedContent)) {
            log.debug("getContentMetadata: Redis cache hit - contentId={}, cacheKey={}", contentId, cacheKey);
            Map<String, Object> cachedData = mapper.readValue(cachedContent, new TypeReference<Map<String, Object>>() {
            });
            Map<String, Object> contentDetails = extractContentFromCachedResponse(cachedData);
            contentDetails.put("_fromCache", true);
            return contentDetails;
        }
        log.debug("getContentMetadata: Redis cache miss, calling extended content read API - contentId={}, cacheKey={}",
                contentId, cacheKey);
        Map<String, Object> contentDetails = contentLookupService.getContentMetadata(contentId);
        return Objects.nonNull(contentDetails) ? contentDetails : new HashMap<>();
    }

    private Map<String, Object> extractContentFromCachedResponse(Map<String, Object> cachedData) {
        if (cachedData.containsKey(Constants.RESULT)) {
            Map<String, Object> result = (Map<String, Object>) cachedData.get(Constants.RESULT);
            if (Objects.nonNull(result) && result.containsKey(Constants.CONTENT)) {
                return (Map<String, Object>) result.get(Constants.CONTENT);
            }
        }
        return cachedData;
    }

    private void populateEmptyDictionaryResponse(ApiResponse response) {
        response.getResult().put(Constants.RESPONSE_KEY_APAR_COUNT, 0);
        response.getResult().put(Constants.RESPONSE_KEY_NON_APAR_COUNT, 0);
        response.getResult().put(Constants.RESPONSE_KEY_APAR_CONTENT_LIST, new LinkedHashMap<>());
        response.getResult().put(Constants.RESPONSE_KEY_NON_APAR_CONTENT_LIST, new LinkedHashMap<>());
        response.setParams(new ApiRespParam());
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
    }

    private void populateDictionaryResponse(ApiResponse response,
                                            Map<String, List<CbPlanContentOccurrence>> aparContentMap,
                                            Map<String, List<CbPlanContentOccurrence>> nonAparContentMap,
                                            Map<String, Object> enrichedContentMap) {
        response.getResult().put(Constants.RESPONSE_KEY_APAR_COUNT, aparContentMap.size());
        response.getResult().put(Constants.RESPONSE_KEY_NON_APAR_COUNT, nonAparContentMap.size());
        response.getResult().put(Constants.RESPONSE_KEY_APAR_CONTENT_LIST, aparContentMap);
        response.getResult().put(Constants.RESPONSE_KEY_NON_APAR_CONTENT_LIST, nonAparContentMap);
        if (Objects.nonNull(enrichedContentMap)) {
            response.getResult().put(Constants.RESPONSE_KEY_ENRICHED_CONTENT_LIST, enrichedContentMap);
        }
        response.setParams(new ApiRespParam());
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
    }

    /**
     * Retrieves cached dictionary entry from Redis.
     *
     * @param redisCacheKey Redis cache key
     * @return CbPlanDictionaryCacheEntry if cache hit, null otherwise
     * @throws JsonProcessingException if deserialization fails
     */
    private CbPlanDictionaryCacheEntry getCachedDictionaryEntry(String redisCacheKey) throws JsonProcessingException {
        String cachedResult = redisCacheMgr.getFromCache(redisCacheKey);
        if (StringUtils.isBlank(cachedResult)) {
            return null;
        }
        return mapper.readValue(cachedResult, CbPlanDictionaryCacheEntry.class);
    }

    /**
     * Computes dictionary grouping from CB Plans (cache miss path).
     *
     * @param userId        user ID
     * @param userOrgId     user's organization ID
     * @param planYear      plan year
     * @param redisCacheKey Redis cache key for caching result
     * @param response      API response for error reporting
     * @return CbPlanDictionaryCacheEntry with computed grouping, or null if no plans found
     * @throws JsonProcessingException if serialization fails
     */
    private CbPlanDictionaryCacheEntry computeDictionaryFromPlans(String userId, String userOrgId, String planYear,
                                                                  String redisCacheKey, ApiResponse response)
            throws JsonProcessingException {
        Map<String, String> userProfile = buildUserProfile(userId, response);
        if (userProfile.isEmpty()) {
            return null;
        }
        AtomicBoolean isCacheEnabled = new AtomicBoolean(false);
        List<Map<String, Object>> activeCbPlans = fetchPlansForUser(userProfile, userOrgId, planYear, isCacheEnabled);
        if (CollectionUtils.isEmpty(activeCbPlans)) {
            return cacheEmptyDictionary(userId, planYear, redisCacheKey);
        }
        return processPlanGroupingAndCache(activeCbPlans, userProfile, userOrgId, userId, planYear,
                redisCacheKey, isCacheEnabled.get());
    }

    /**
     * Caches empty dictionary entry when no plans found.
     *
     * @param userId        user ID
     * @param planYear      plan year
     * @param redisCacheKey Redis cache key
     * @return empty CbPlanDictionaryCacheEntry
     * @throws JsonProcessingException if serialization fails
     */
    private CbPlanDictionaryCacheEntry cacheEmptyDictionary(String userId, String planYear, String redisCacheKey)
            throws JsonProcessingException {
        log.info("cacheEmptyDictionary: No active plans found - userId={}, planYear={}", userId, planYear);
        CbPlanDictionaryCacheEntry emptyEntry = new CbPlanDictionaryCacheEntry(
                new LinkedHashMap<>(), new LinkedHashMap<>(), 0, 0);
        redisCacheMgr.putInCache(redisCacheKey, mapper.writeValueAsString(emptyEntry),
                serverProperties.getCbPlanV3RedisCacheTtlSeconds());
        return emptyEntry;
    }

    /**
     * Processes plan grouping and caches result.
     *
     * @param activeCbPlans  CB Plans to process
     * @param userProfile    user profile
     * @param userOrgId      user's organization ID
     * @param userId         user ID
     * @param planYear       plan year
     * @param redisCacheKey  Redis cache key
     * @param isCacheEnabled whether caching is enabled
     * @return CbPlanDictionaryCacheEntry with computed grouping
     * @throws JsonProcessingException if serialization fails
     */
    private CbPlanDictionaryCacheEntry processPlanGroupingAndCache(List<Map<String, Object>> activeCbPlans,
                                                                   Map<String, String> userProfile, String userOrgId,
                                                                   String userId, String planYear, String redisCacheKey,
                                                                   boolean isCacheEnabled) throws JsonProcessingException {
        Map<String, List<CbPlanContentOccurrence>> aparContentMap = new LinkedHashMap<>();
        Map<String, List<CbPlanContentOccurrence>> nonAparContentMap = new LinkedHashMap<>();
        Set<String> aparContentIds = new HashSet<>();
        List<String> plansToCache = new ArrayList<>();
        processPlansForDictionary(activeCbPlans, userProfile, userOrgId,
                aparContentMap, nonAparContentMap, aparContentIds, plansToCache);
        CbPlanDictionaryCacheEntry cacheEntry = new CbPlanDictionaryCacheEntry(
                aparContentMap, nonAparContentMap, aparContentMap.size(), nonAparContentMap.size());
        if (isCacheEnabled && CollectionUtils.isNotEmpty(plansToCache)) {
            redisCacheMgr.putInCache(redisCacheKey, mapper.writeValueAsString(cacheEntry),
                    serverProperties.getCbPlanV3RedisCacheTtlSeconds());
            log.info("processPlanGroupingAndCache: Cached grouping - userId={}, planYear={}, aparCount={}, nonAparCount={}",
                    userId, planYear, aparContentMap.size(), nonAparContentMap.size());
        }
        return cacheEntry;
    }

    /**
     * Enriches content metadata and populates final response.
     *
     * @param response            API response
     * @param cacheEntry          cache entry with grouping data
     * @param isEnrichmentEnabled whether enrichment is requested
     */
    private void enrichAndPopulateResponse(ApiResponse response, CbPlanDictionaryCacheEntry cacheEntry,
                                           boolean isEnrichmentEnabled) {
        Map<String, Object> enrichedContentMap = null;
        if (isEnrichmentEnabled) {
            log.debug("enrichAndPopulateResponse: Enriching content metadata - contentCount={}",
                    cacheEntry.getAparCount() + cacheEntry.getNonAparCount());
            enrichedContentMap = enrichContentList(cacheEntry.getAparContentList(), cacheEntry.getNonAparContentList());
        }
        populateDictionaryResponse(response, cacheEntry.getAparContentList(),
                cacheEntry.getNonAparContentList(), enrichedContentMap);
    }

    /**
     * Reads a CB Plan by ID with enriched content details.
     * Queries cb_plan_v3 table and enriches the response with content metadata.
     *
     * @param cbPlanId      the CB Plan ID to retrieve
     * @param userOrgId     the organization ID of the user
     * @param authUserToken the authentication token
     * @return ApiResponse containing the CB Plan details or error
     */
    @Override
    public ApiResponse readCbPlan(String cbPlanId, String userOrgId, String authUserToken) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_V3_READ_BY_ID);
        log.info("readCbPlan: Starting - cbPlanId={}, userOrgId={}", cbPlanId, userOrgId);

        try {
            if (StringUtils.isEmpty(cbPlanId)) {
                log.warn("readCbPlan: Missing cbPlanId");
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.ERR_CB_PLAN_ID_MISSING);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            Map<String, Object> queryFilter = new HashMap<>();
            queryFilter.put(Constants.PLAN_ID, cbPlanId);
            List<Map<String, Object>> cbPlanList = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_CB_PLAN_V3, queryFilter, null,
                    serverProperties.getCassandraQueryLimitPrimaryKey());

            if (CollectionUtils.isEmpty(cbPlanList)) {
                log.warn("readCbPlan: CB Plan not found - cbPlanId={}", cbPlanId);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("CbPlan does not exist for ID: " + cbPlanId);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            Map<String, Object> cbPlan = cbPlanList.get(0);
            CbPlanReadResponseDto enrichedData = buildEnrichedPlanData(cbPlan, cbPlanId);
            response.getResult().put(Constants.CONTENT, enrichedData);
            log.info("readCbPlan: Successfully retrieved CB Plan - cbPlanId={}, planYear={}",
                    cbPlanId, enrichedData.getPlanYear());

        } catch (JsonProcessingException e) {
            log.error("readCbPlan: JSON processing failed - cbPlanId={}", cbPlanId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Failed to process plan data");
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception e) {
            log.error("readCbPlan: Failed to read CB Plan - cbPlanId={}, userOrgId={}", cbPlanId, userOrgId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    /**
     * Builds enriched CB Plan data from raw Cassandra record.
     * Extracts plan details from either draftData (for LIVE plans with draft)
     * or direct fields, and enriches content list.
     *
     * @param cbPlan   raw CB Plan record from Cassandra
     * @param cbPlanId CB Plan ID
     * @return enriched plan data DTO
     * @throws JsonProcessingException if JSON parsing fails
     */
    private CbPlanReadResponseDto buildEnrichedPlanData(Map<String, Object> cbPlan, String cbPlanId)
            throws JsonProcessingException {
        String draftData = (String) cbPlan.get(Constants.DRAFT_DATA);
        String status = (String) cbPlan.get(Constants.STATUS);
        boolean hasDraftData = StringUtils.isNotBlank(draftData) && !Constants.EMPTY_JSON.equals(draftData);
        boolean isLiveStatus = Constants.LIVE.equalsIgnoreCase(status);

        String name;
        Instant endDate;
        Boolean isApar;
        List<String> contentIdList;

        if (hasDraftData && isLiveStatus) {
            CbPlanDto cbPlanDto = mapper.readValue(draftData, CbPlanDto.class);
            name = cbPlanDto.getName();
            endDate = cbPlanDto.getEndDate() != null ? cbPlanDto.getEndDate().toInstant() : null;
            isApar = cbPlanDto.getIsApar() != null && cbPlanDto.getIsApar();
            contentIdList = cbPlanDto.getContentList();
        } else {
            name = (String) cbPlan.get(Constants.NAME);
            endDate = (Instant) cbPlan.get(Constants.END_DATE_REQUEST);
            isApar = (Boolean) cbPlan.getOrDefault(Constants.IS_APAR, false);
            contentIdList = extractContentList(cbPlan.get(Constants.CONTENT_LIST));
        }

        return CbPlanReadResponseDto.builder()
                .id(cbPlanId)
                .name(name)
                .planYear((String) cbPlan.get(Constants.PLAN_YEAR))
                .endDate(endDate)
                .isApar(isApar)
                .contentType((String) cbPlan.get(Constants.CONTENT_TYPE))
                .planType((String) cbPlan.get(Constants.PLAN_TYPE))
                .createdAt((Instant) cbPlan.get(Constants.CREATED_AT_REQ))
                .cbPublishedAt((Instant) cbPlan.get(Constants.CB_PUBLISHED_AT))
                .status(status)
                .createdBy((String) cbPlan.get(Constants.CREATED_BY))
                .createdByName(StringUtils.EMPTY)
                .contextData(parseContextDataToJsonNode(cbPlan.get(Constants.CONTEXT_DATA_REQUEST)))
                .contentList(contentIdList)
                .build();
    }

    /**
     * Extracts content ID list from Cassandra object.
     * Handles type conversion safely to avoid unchecked cast warnings.
     *
     * @param contentListObj raw content list object from Cassandra
     * @return list of content IDs, or empty list if null/invalid
     */
    private List<String> extractContentList(Object contentListObj) {
        if (contentListObj == null) {
            return new ArrayList<>();
        }
        try {
            return (List<String>) contentListObj;
        } catch (ClassCastException e) {
            log.warn("extractContentList: Invalid content list type, returning empty list", e);
            return new ArrayList<>();
        }
    }

    /**
     * Parses context data from Cassandra object to JsonNode.
     * Handles JSON parsing and returns null if parsing fails.
     *
     * @param contextData raw context data from Cassandra
     * @return JsonNode representation or null if parsing fails
     */
    private JsonNode parseContextDataToJsonNode(Object contextData) {
        if (contextData == null) {
            return null;
        }

        try {
            return mapper.readTree(contextData.toString());
        } catch (JsonProcessingException e) {
            log.warn("parseContextDataToJsonNode: Failed to parse contextData, returning null", e);
            return null;
        }
    }

    /**
     * Extracts only known fields from draft_data to prevent malformed data propagation.
     * Validates and transforms: IS_APAR, ORG_SCOPE, NAME, CONTEXT_DATA_REQUEST, END_DATE_REQUEST,
     * ROOT_ORG_IDS_IN_CONTEXT_DATA, CONTENT_LIST, PLAN_TYPE (aligned with V2 contract).
     *
     * @param dataInDraftObject deserialized draft_data Map (may contain any fields/types)
     * @return Map with only validated fields, preventing ClassCastException
     * @throws JsonProcessingException if CONTEXT_DATA_REQUEST serialization fails
     */
    private Map<String, Object> extractDraftFields(Map<String, Object> dataInDraftObject)
            throws JsonProcessingException {
        Map<String, Object> extractedFields = new HashMap<>();
        if (dataInDraftObject.containsKey(Constants.IS_APAR)) {
            extractedFields.put(Constants.IS_APAR, dataInDraftObject.get(Constants.IS_APAR));
        }
        if (dataInDraftObject.containsKey(Constants.ORG_SCOPE)) {
            extractedFields.put(Constants.ORG_SCOPE, dataInDraftObject.get(Constants.ORG_SCOPE));
        }
        if (dataInDraftObject.containsKey(Constants.NAME)) {
            extractedFields.put(Constants.NAME, dataInDraftObject.get(Constants.NAME));
        }
        if (dataInDraftObject.containsKey(Constants.CONTEXT_DATA_REQUEST)) {
            extractedFields.put(Constants.CONTEXT_DATA_REQUEST,
                    serializeContextData(dataInDraftObject.get(Constants.CONTEXT_DATA_REQUEST)));
        }
        if (dataInDraftObject.containsKey(Constants.END_DATE_REQUEST)) {
            extractedFields.put(Constants.END_DATE_REQUEST,
                    dataTransformService.parseEndDate(dataInDraftObject.get(Constants.END_DATE_REQUEST)));
        }
        if (dataInDraftObject.containsKey(Constants.ROOT_ORG_IDS_IN_CONTEXT_DATA)) {
            extractedFields.put(Constants.ROOT_ORG_IDS_IN_CONTEXT_DATA,
                    dataInDraftObject.get(Constants.ROOT_ORG_IDS_IN_CONTEXT_DATA));
        }
        if (dataInDraftObject.containsKey(Constants.CONTENT_LIST)) {
            extractedFields.put(Constants.CONTENT_LIST,
                    dataInDraftObject.get(Constants.CONTENT_LIST));
        }
        if (dataInDraftObject.containsKey(Constants.PLAN_TYPE)) {
            extractedFields.put(Constants.PLAN_TYPE, dataInDraftObject.get(Constants.PLAN_TYPE));
        }
        return extractedFields;
    }

    /**
     * Creates a CB Plan through the AI CBP admin flow.
     * Unlike {@link #createCbPlan(ApiRequest, String, String)} the organisation is supplied in the
     * request body as targetedOrganisation rather than in a header. That value becomes both the
     * effective userOrgId and the plan's orgIdList, and the plan is tagged planType = AICBP so
     * downstream consumers can separate AI generated plans from manually authored ones.
     *
     * @param request   the API request containing CB Plan details and targetedOrganisation
     * @param authToken the authentication token
     * @return ApiResponse containing the created plan ID and status
     */
    @Override
    public ApiResponse createCbPlanByAdmin(ApiRequest request, String authToken) {
        String targetedOrganisation = extractTargetedOrganisation(request);
        if (StringUtils.isBlank(targetedOrganisation)) {
            log.warn("createCbPlanByAdmin: {}", Constants.ERR_TARGETED_ORGANISATION_REQUIRED);
            ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_V3_AICBP_CREATE);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.ERR_TARGETED_ORGANISATION_REQUIRED);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }
        Map<String, Object> rawRequest = (Map<String, Object>) request.getRequest();
        rawRequest.put(Constants.ORG_ID_LIST, List.of(targetedOrganisation));
        rawRequest.put(Constants.PLAN_TYPE, Constants.PLAN_TYPE_AI_CBP);
        log.info("createCbPlanByAdmin: Creating AI CBP plan for targetedOrganisation={}", targetedOrganisation);
        return aiCBPCreateCbPlan(request, targetedOrganisation, authToken);
    }

    /**
     * Publishes a CB Plan through the AI CBP admin flow.
     * The organisation is supplied in the request body as targetedOrganisation rather than in a
     * header and becomes the effective userOrgId. ROLE_ADMIN is supplied as the caller's role so
     * the creator/role check is satisfied without an x-authenticated-user-roles header, matching
     * the legacy aicbp publish behaviour.
     *
     * @param request   the API request containing CB Plan ID, comment and targetedOrganisation
     * @param authToken the authentication token
     * @return ApiResponse containing the publish status
     */
    @Override
    public ApiResponse publishCbPlanByAdmin(ApiRequest request, String authToken) {
        String targetedOrganisation = extractTargetedOrganisation(request);
        if (StringUtils.isBlank(targetedOrganisation)) {
            log.warn("publishCbPlanByAdmin: {}", Constants.ERR_TARGETED_ORGANISATION_REQUIRED);
            ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_V3_AICBP_PUBLISH);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.ERR_TARGETED_ORGANISATION_REQUIRED);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }
        log.info("publishCbPlanByAdmin: Publishing AI CBP plan for targetedOrganisation={}", targetedOrganisation);
        return aiCBPPublishCbPlan(request, targetedOrganisation, authToken, Collections.emptyList(), true);
    }


    /**
     * Serializes contextData to a JSON string. A value that is already a
     * serialized JSON string is returned as-is, so it is never double-encoded
     * (a draft stores contextData as a string; publish/update must not wrap it
     * into a quoted JSON string literal again).
     */
    private String serializeContextData(Object contextData) throws JsonProcessingException {
        if (contextData instanceof String str) {
            return str;
        }
        return mapper.writeValueAsString(contextData);
    }

    /**
     * Reads the targeted organisation from the request body.
     *
     * @param request the API request
     * @return targeted organisation ID, or null when absent
     */
    private String extractTargetedOrganisation(ApiRequest request) {
        Map<String, Object> rawRequest = (Map<String, Object>) request.getRequest();
        if (MapUtils.isEmpty(rawRequest)) {
            return null;
        }
        Object value = rawRequest.get(Constants.TARGETED_ORGANISATION);
        return Objects.nonNull(value) ? String.valueOf(value) : null;
    }

    /**
     * Creates a CB Plan through AI CBP admin flow.
     * This method is used by the AI CBP wrapper API where the target organization
     * is provided explicitly in the request body instead of being derived from the
     * authenticated user's organization.
     *
     * @param request   API request containing CB Plan details
     * @param userOrgId target organization ID (from request body, not from user token)
     * @param authToken authentication token for user validation
     * @return ApiResponse with creation status and plan ID
     */
    public ApiResponse aiCBPCreateCbPlan(ApiRequest request, String userOrgId, String authToken) {
        log.info("CbPlanServiceV3Impl.createCbPlan: Creating CB Plan for orgId: {}", userOrgId);
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_CREATE);
        try {
            String userId = validationService.validateAndExtractUserId(authToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }
            String rootOrgId = validationService.validateUserOrganization(userId, response);
            if (StringUtils.isEmpty(rootOrgId)) {
                return response;
            }
            boolean isCCA = validationService.validateOrgCCA(rootOrgId, response);
            if (Constants.FAILED.equalsIgnoreCase(response.getParams().getStatus())) {
                return response;
            }
            if (!validationService.validateRequest(request, isCCA, userOrgId, response)) {
                return response;
            }
            executePlanCreation(request, userId, userOrgId, response);
        } catch (Exception e) {
            handleException(response, userOrgId, e);
        }
        return response;
    }

    /**
     * Publishes a CB Plan through AI CBP admin flow.
     * This method is used by the AI CBP wrapper API where the target organization
     * is provided explicitly in the request body. The isAdmin flag bypasses creator
     * and role-based authorization checks, allowing admin users to publish plans
     * created for any organization.
     *
     * @param request   API request containing plan ID and comment
     * @param userOrgId target organization ID (from request body, not from user token)
     * @param authToken authentication token for user validation
     * @param userRoles user roles (can be empty for admin flow)
     * @param isAdmin   flag to bypass authorization checks (always true for AI CBP flow)
     * @return ApiResponse with publish status and updated plan details
     */
    public ApiResponse aiCBPPublishCbPlan(ApiRequest request, String userOrgId, String authToken, List<String> userRoles, boolean isAdmin) {
        log.info("CbPlanServiceV3Impl.publishCbPlan: Publishing CB Plan for orgId: {}, isAdmin: {}", userOrgId, isAdmin);
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_PUBLISH);
        try {
            String userId = validationService.validateAndExtractUserId(authToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }
            Map<String, Object> incomingRequest = (Map<String, Object>) request.getRequest();
            String cbPlanId = validationService.validateAndExtractPlanId(incomingRequest, response);
            if (StringUtils.isEmpty(cbPlanId)) {
                return response;
            }
            Map<String, Object> existingCbPlan = fetchExistingPlan(cbPlanId, response);
            if (MapUtils.isEmpty(existingCbPlan)) {
                return response;
            }
            if (!isAdmin && validationService.isUnauthorizedToUpdate(userId, existingCbPlan, userRoles, response)) {
                return response;
            }
            executePublishFlow(request, userId, userOrgId, cbPlanId, existingCbPlan, isAdmin, response);
        } catch (Exception e) {
            handlePublishException(response, userOrgId, e);
        }
        return response;
    }

    /**
     * Inserts ministry or state ID lookup entries.
     *
     * @param cbPlanId           CB Plan ID
     * @param planYear           plan year
     * @param ministryOrStateIds set of ministry or state IDs to insert
     * @param endDate            end date
     * @param response           API response object
     */
    private void insertMinistryOrStateIdLookup(String cbPlanId, String planYear,
                                               Set<String> ministryOrStateIds,
                                               Instant endDate, ApiResponse response) {
        log.info("CbPlanServiceV3Impl.insertMinistryOrStateIdLookup: Inserting {} ministryOrStateId(s) for CB Plan: {}",
                ministryOrStateIds.size(), cbPlanId);
        ApiResponse ministryLookupResp = orgLookupService.upsertMinistryOrStateIdLookup(
                cbPlanId, planYear, ministryOrStateIds, endDate, true);
        if (!Constants.SUCCESS.equals(ministryLookupResp.get(Constants.RESPONSE))) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(ministryLookupResp.getParams().getErr());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            log.error("CbPlanServiceV3Impl.insertMinistryOrStateIdLookup: Failed to insert ministryOrStateId lookup for CB Plan: {}",
                    cbPlanId);
        }
    }
    private void upsertOrgScopeLookupTables(String cbPlanId, String planYear, String orgScope,
                                            Set<String> newRootOrgIds, Instant endDate,
                                            boolean hasMinistryOrStateId, ApiResponse response) {
        ApiResponse lookupResp = null;
        if (Constants.SINGLE.equalsIgnoreCase(orgScope) || Constants.CUSTOM.equalsIgnoreCase(orgScope)) {
            log.info("upsertOrgScopeLookupTables: Upserting CUSTOM/SINGLE org lookup - cbPlanId={}, orgCount={}",
                    cbPlanId, CollectionUtils.isNotEmpty(newRootOrgIds) ? newRootOrgIds.size() : 0);
            lookupResp = orgLookupService.upsertCustomOrgLookup(cbPlanId, planYear, newRootOrgIds, endDate, true);
        } else if (Constants.ALL.equalsIgnoreCase(orgScope) && !hasMinistryOrStateId) {
            log.info("upsertOrgScopeLookupTables: Upserting ALL org lookup - cbPlanId={}", cbPlanId);
            lookupResp = orgLookupService.upsertAllOrgLookup(cbPlanId, planYear, endDate, true);
        } else if (hasMinistryOrStateId) {
            log.info("upsertOrgScopeLookupTables: Skipping org/all_org tables (using ministryOrStateId) - cbPlanId={}", cbPlanId);
        }
        if (Objects.nonNull(lookupResp) && !Constants.SUCCESS.equals(lookupResp.get(Constants.RESPONSE))) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(lookupResp.getParams().getErr());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            log.error("upsertOrgScopeLookupTables: Failed for cbPlanId={}, error={}", cbPlanId, lookupResp.getParams().getErr());
        }
    }


    private List<Map<String, Object>> fetchPlansForUser(Map<String, String> userProfile, String userOrgId,
                                                        String planYear, AtomicBoolean isCacheEnabled) {
        List<Map<String, Object>> orgPlans = cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(
                userOrgId, planYear, isCacheEnabled);
        String ministryOrStateId = userProfile.get(Constants.MINISTRY_OR_STATE_ID_RQST);
        if (StringUtils.isBlank(ministryOrStateId)) {
            return orgPlans;
        }
        log.info("fetchPlansForUser: User has ministryOrStateId={}, fetching ministry-specific plans", ministryOrStateId);
        List<Map<String, Object>> ministryPlans = cbPlanCacheMgrV3.getCbPlanForMinistryOrStateId(
                ministryOrStateId, planYear);
        return dataTransformService.mergePlanLists(orgPlans, ministryPlans);
    }

}
