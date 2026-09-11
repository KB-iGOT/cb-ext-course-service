package com.igot.cb.cbplan.service.impl.v4;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.igot.cb.cbplan.service.impl.CbPlanContentLookupServiceV3Impl;
import com.igot.cb.cbplan.service.impl.CbPlanDataTransformServiceV3Impl;
import com.igot.cb.cbplan.service.impl.CbPlanElasticSearchServiceV3Impl;
import com.igot.cb.cbplan.service.impl.CbPlanOrgLookupServiceV3Impl;
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
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.cbplan.dto.CbPlanReadResponseDto;
import com.igot.cb.cbplan.service.CbPlanServiceV4;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import com.igot.cb.util.UserProfileUtil;

import lombok.extern.slf4j.Slf4j;

/**
 * Service implementation for CB Plan V4 operations.
 * V4 uses user group references instead of inline criteria.
 *
 * @version 4.0
 */
@Service
@Slf4j
public class CbPlanServiceV4Impl implements CbPlanServiceV4 {
    private final CassandraOperation cassandraOperation;
    private final CbExtServerProperties serverProperties;
    private final CbPlanValidationServiceV4Impl validationService;
    private final CbPlanDataTransformServiceV3Impl dataTransformService;
    private final CbPlanContentLookupServiceV3Impl contentLookupService;
    private final CbPlanElasticSearchServiceV3Impl elasticSearchService;
    private final CbPlanOrgLookupServiceV3Impl orgLookupService;
    private final CbPlanReadServiceV4Impl readService;
    private final EsUtilService esUtilService;
    private final AccessTokenValidator accessTokenValidator;
    private final UserProfileUtil userProfileUtil;
    private final ObjectMapper mapper;

    public CbPlanServiceV4Impl(CassandraOperation cassandraOperation,
                               CbExtServerProperties serverProperties,
                               CbPlanValidationServiceV4Impl validationService,
                               CbPlanDataTransformServiceV3Impl dataTransformService,
                               CbPlanContentLookupServiceV3Impl contentLookupService,
                               CbPlanElasticSearchServiceV3Impl elasticSearchService,
                               CbPlanOrgLookupServiceV3Impl orgLookupService,
                               CbPlanReadServiceV4Impl readService,
                               EsUtilService esUtilService,
                               AccessTokenValidator accessTokenValidator,
                               UserProfileUtil userProfileUtil) {
        this.cassandraOperation = cassandraOperation;
        this.serverProperties = serverProperties;
        this.validationService = validationService;
        this.dataTransformService = dataTransformService;
        this.contentLookupService = contentLookupService;
        this.elasticSearchService = elasticSearchService;
        this.orgLookupService = orgLookupService;
        this.readService = readService;
        this.esUtilService = esUtilService;
        this.accessTokenValidator = accessTokenValidator;
        this.userProfileUtil = userProfileUtil;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public ApiResponse createCbPlan(ApiRequest request, String authToken) {
        log.info("CbPlanServiceV4Impl.createCbPlan: Creating CB Plan V4");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_V4_CREATE);
        String userRootOrgId = null;

        try {
            if (!validateAuthAndOrganization(authToken, response)) {
                return response;
            }
            String userId = (String) response.getResult().get(Constants.USER_ID);
            userRootOrgId = (String) response.getResult().get(Constants.ROOT_ORG_ID);
            boolean isCCA = (Boolean) response.getResult().get(Constants.IS_CCA);
            response.getResult().clear();

            log.info("CbPlanServiceV4Impl.createCbPlan: userId={}, orgId={}", userId, userRootOrgId);

            if (!validateV4Request(request, isCCA, userRootOrgId, response)) {
                return response;
            }

            executePlanCreation(request, userId, userRootOrgId, response);
        } catch (Exception e) {
            handleException(response, userRootOrgId, e);
        }

        return response;
    }

    /**
     * Validates the caller's token and resolves their root org and CCA status.
     * On success, stashes userId/rootOrgId/isCCA into the response result map so
     * the caller can read them back before clearing it for the real response body.
     *
     * @param authToken authentication token
     * @param response  API response object, populated with an error on failure
     * @return true when the caller is authenticated and their org was resolved
     */
    private boolean validateAuthAndOrganization(String authToken, ApiResponse response) {
        String userId = validationService.validateAndExtractUserId(authToken, response);
        if (StringUtils.isEmpty(userId)) {
            return false;
        }

        String userRootOrgId = validationService.validateUserOrganization(userId, response);
        if (StringUtils.isEmpty(userRootOrgId)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.ERR_USER_ORG_NOT_FOUND);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return false;
        }

        log.info("CbPlanServiceV4Impl.validateAuthAndOrganization: userId={}, orgId={}", userId, userRootOrgId);

        boolean isCCA = validationService.validateOrgCCA(userRootOrgId, response);
        if (Constants.FAILED.equalsIgnoreCase(response.getParams().getStatus())) {
            return false;
        }

        response.getResult().put(Constants.USER_ID, userId);
        response.getResult().put(Constants.ROOT_ORG_ID, userRootOrgId);
        response.getResult().put(Constants.IS_CCA, isCCA);
        return true;
    }

    /**
     * Validates the create request: contentList shape, mandatory fields, and
     * the userGroupId-derived org scope.
     *
     * @param request      the API request containing CB Plan details
     * @param isCCA        whether the logged in org is CCA
     * @param userRootOrgId logged in user's organization ID
     * @param response     API response object, populated with an error on failure
     * @return true when the request is valid
     */
    private boolean validateV4Request(ApiRequest request, boolean isCCA, String userRootOrgId, ApiResponse response) {
        return validationService.validateRequest(request, isCCA, userRootOrgId, response);
    }

    /**
     * Builds the insert record and persists it, then fans out to content-lookup
     * and Elasticsearch on success.
     *
     * @param request   the API request containing CB Plan details
     * @param userId    creator's user ID
     * @param userOrgId creator's organization ID, used only for error messages
     * @param response  API response object, populated with the created plan ID on success
     */
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

    /**
     * Inserts the prepared plan row into {@code cb_plan_v3}.
     *
     * @param planData prepared plan data
     * @return Cassandra insert response
     */
    private ApiResponse insertPlanToDatabase(Map<String, Object> planData) {
        return (ApiResponse) cassandraOperation.insertRecord(
                Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_CB_PLAN_V3,
                planData);
    }

    /**
     * Updates the content-lookup table and Elasticsearch index, then populates
     * the success result.
     *
     * @param planData inserted plan data
     * @param response API response object to populate
     */
    private void processSuccessfulCreation(Map<String, Object> planData, ApiResponse response) {
        String planId = String.valueOf(planData.get(Constants.PLAN_ID));

        contentLookupService.updateContentLookup(planId, planData);
        elasticSearchService.indexToElasticSearch(planId, planData);

        populateSuccessResponse(response, planId);
    }

    /**
     * Populates the 201 Created success response for a new plan.
     *
     * @param response API response object to populate
     * @param planId   newly created plan ID
     */
    private void populateSuccessResponse(ApiResponse response, String planId) {
        response.getResult().put(Constants.ID, planId);
        response.getResult().put(Constants.STATUS, Constants.CREATED);
        response.setResponseCode(HttpStatus.CREATED);
        log.info("CbPlanServiceV4Impl.createCbPlan: Successfully created CB Plan V4 with ID: {}", planId);
    }

    /**
     * Populates a 500 error response when the Cassandra insert itself failed.
     *
     * @param insertResponse failed Cassandra insert response
     * @param userOrgId      creator's organization ID, included in the error message
     * @param response       API response object to populate
     */
    private void processFailedCreation(ApiResponse insertResponse, String userOrgId, ApiResponse response) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(Constants.ERR_FAILED_TO_CREATE_CB_PLAN + userOrgId
                + Constants.ERR_MESSAGE_SEPARATOR + insertResponse.getParams().getErr());
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        log.error("CbPlanServiceV4Impl.createCbPlan: Insert failed for orgId: {}", userOrgId);
    }

    /**
     * Populates a 500 error response for a JSON processing failure during creation.
     *
     * @param e         the JSON processing exception
     * @param userOrgId creator's organization ID
     * @param response  API response object to populate
     */
    private void handleJsonProcessingException(JsonProcessingException e, String userOrgId, ApiResponse response) {
        log.error("CbPlanServiceV4Impl.createCbPlan: JSON processing error for orgId: {}", userOrgId, e);
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(e.getMessage());
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     * Populates a 500 error response for any unhandled exception during creation.
     *
     * @param response  API response object to populate
     * @param userOrgId creator's organization ID
     * @param e         the unhandled exception
     */
    private void handleException(ApiResponse response, String userOrgId, Exception e) {
        log.error("CbPlanServiceV4Impl.createCbPlan: Exception occurred for orgId: {}", userOrgId, e);
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(e.getMessage());
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Override
    public ApiResponse updateCbPlan(ApiRequest request, String authToken) {
        log.info("CbPlanServiceV4Impl.updateCbPlan: Updating CB Plan V4");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_V4_UPDATE);
        String userRootOrgId = null;
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }

            Map<String, String> userProfile = userProfileUtil.buildUserProfile(userId, response);
            userRootOrgId = userProfile.get(Constants.USER_ROOT_ORG_ID);
            String userRolesStr = userProfile.get(Constants.ROLES);

            if (StringUtils.isBlank(userRootOrgId)) {
                log.warn("CbPlanServiceV4Impl.updateCbPlan: Failed to fetch userRootOrgId for userId={}", userId);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.ERR_USER_ORG_NOT_FOUND);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            List<String> userRoles = StringUtils.isNotBlank(userRolesStr)
                    ? List.of(userRolesStr.split(","))
                    : Collections.emptyList();

            log.info("CbPlanServiceV4Impl.updateCbPlan: userId={}, orgId={}, roles={}", userId, userRootOrgId, userRoles);

            if (!validationService.validatePlanIdExists(request, response)) {
                return response;
            }
            executeUpdateFlow(request, userId, userRootOrgId, userRoles, response);
        } catch (Exception e) {
            handleUpdateException(response, userRootOrgId, e);
        }
        return response;
    }

    /**
     * Fetches the existing plan, checks update authorization, then dispatches
     * to the authorized-update flow.
     *
     * @param request   the API request containing updated CB Plan details
     * @param userId    caller's user ID
     * @param userOrgId caller's organization ID
     * @param userRoles caller's roles, used for the authorization check
     * @param response  API response object, populated with an error on failure
     * @throws JsonProcessingException if contextData serialization fails downstream
     */
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

    /**
     * Fetches a CB Plan row by ID from {@code cb_plan_v3}.
     *
     * @param cbPlanId CB Plan ID
     * @param response API response object, populated with a 400 when not found
     * @return the plan row, or an empty map when not found
     */
    private Map<String, Object> fetchExistingPlan(String cbPlanId, ApiResponse response) {
        List<Map<String, Object>> cbPlanMapInfo = cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD, Constants.TABLE_CB_PLAN_V3,
                Map.of(Constants.PLAN_ID, cbPlanId), null, serverProperties.getCassandraQueryLimitPrimaryKey());
        if (CollectionUtils.isEmpty(cbPlanMapInfo)) {
            log.warn("CbPlanServiceV4Impl.fetchExistingPlan: CB Plan not found - cbPlanId={}", cbPlanId);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.ERR_CB_PLAN_NOT_FOUND + cbPlanId);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return Collections.emptyMap();
        }
        return cbPlanMapInfo.get(0);
    }

    /**
     * Re-validates the caller's org/CCA status, then dispatches to the LIVE or
     * DRAFT update handler based on the plan's current status.
     *
     * @param request        the API request containing updated CB Plan details
     * @param userId         caller's user ID
     * @param userOrgId      caller's organization ID
     * @param updatedCbPlan  incoming update request map
     * @param existingCbPlan existing CB Plan record
     * @param response       API response object, populated with an error on failure
     * @throws JsonProcessingException if contextData serialization fails downstream
     */
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
        log.debug("CbPlanServiceV4Impl.executeAuthorizedUpdate: existingStatus={}", existingStatus);
        if (Constants.LIVE.equalsIgnoreCase(existingStatus)) {
            handleUpdateOfLiveCbPlan(response, updatedCbPlan, existingCbPlan, userId, rootOrgId, isCCA);
        } else if (Constants.DRAFT.equalsIgnoreCase(existingStatus)) {
            handleUpdateOfDraftCbPlan(request, userId, userOrgId, updatedCbPlan, existingCbPlan,
                    isCCA, response);
        }
    }

    /**
     * Handles updating a DRAFT plan: full request re-validation, then an
     * immediate overwrite of the plan row (no draft/publish split for DRAFT plans).
     *
     * @param request        the API request containing updated CB Plan details
     * @param userId         caller's user ID
     * @param userOrgId      caller's organization ID
     * @param updatedCbPlan  incoming update request map
     * @param existingCbPlan existing CB Plan record
     * @param isCCA          whether the logged in org is CCA
     * @param response       API response object, populated with an error on failure
     * @throws JsonProcessingException if contextData serialization fails
     */
    private void handleUpdateOfDraftCbPlan(ApiRequest request, String userId, String userOrgId,
                                           Map<String, Object> updatedCbPlan, Map<String, Object> existingCbPlan,
                                           boolean isCCA, ApiResponse response) throws JsonProcessingException {
        if (!validationService.validateRequest(request, isCCA, userOrgId, response)) {
            return;
        }
        String cbPlanId = (String) updatedCbPlan.get(Constants.ID);
        Map<String, Object> updatedRequest = dataTransformService.prepareCbPlanForUpdate(updatedCbPlan, userId);
        executeDraftPlanUpdate(cbPlanId, updatedRequest, existingCbPlan, response);
    }

    /**
     * Persists the DRAFT plan overwrite and fans out to content-lookup/ES on success.
     *
     * @param cbPlanId       CB Plan ID
     * @param updatedRequest prepared update data
     * @param existingCbPlan existing CB Plan record, used to diff content-lookup changes
     * @param response       API response object, populated with the update status
     */
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

    /**
     * Updates content-lookup and Elasticsearch for a successful DRAFT overwrite.
     *
     * @param cbPlanId       CB Plan ID
     * @param updatedRequest applied update data
     * @param existingCbPlan pre-update plan record, used to diff added/removed content
     * @param response       API response object to populate
     */
    private void processDraftUpdateSuccess(String cbPlanId, Map<String, Object> updatedRequest,
                                           Map<String, Object> existingCbPlan, ApiResponse response) {
        contentLookupService.updateContentLookupForModifiedPlan(cbPlanId, updatedRequest, existingCbPlan);
        elasticSearchService.updateElasticSearchForPlan(cbPlanId, updatedRequest);
        response.getResult().put(Constants.STATUS, Constants.UPDATED);
    }

    /**
     * Populates a 400 error response when the DRAFT overwrite's Cassandra update failed.
     *
     * @param cbPlanId CB Plan ID
     * @param response API response object to populate
     */
    private void processDraftUpdateFailure(String cbPlanId, ApiResponse response) {
        log.warn("CbPlanServiceV4Impl.processDraftUpdateFailure: Update failed - cbPlanId={}", cbPlanId);
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(Constants.ERR_CB_PLAN_NOT_FOUND + cbPlanId);
        response.setResponseCode(HttpStatus.BAD_REQUEST);
    }

    /**
     * Handles updating a LIVE plan: normalizes contentList, re-validates the
     * contextData-derived org scope, then stages the change into {@code draftData}
     * rather than applying it directly (LIVE changes only take effect on publish).
     *
     * @param response                API response object, populated with the update status
     * @param incomingCbPlanRequest   incoming update request map
     * @param existingCbPlan          existing CB Plan record
     * @param userId                  caller's user ID
     * @param rootOrgId               caller's organization ID
     * @param isCCA                   whether the logged in org is CCA
     */
    private void handleUpdateOfLiveCbPlan(ApiResponse response, Map<String, Object> incomingCbPlanRequest,
                                          Map<String, Object> existingCbPlan, String userId,
                                          String rootOrgId, boolean isCCA) {
        try {
            if (!normalizeLivePlanContentList(incomingCbPlanRequest, response)) {
                return;
            }
            Set<String> rootOrgIdsInContextData = new HashSet<>();
            if (!validationService.validateContextDataForLivePlanV4(incomingCbPlanRequest, isCCA, rootOrgId,
                    rootOrgIdsInContextData, new HashSet<>(), response)) {
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

    /**
     * Normalizes contentList before it reaches {@link CbPlanDataTransformServiceV3Impl#buildUpdatedPlanForLive}.
     * That method copies allowed fields into draftData verbatim; without this, a contentList
     * submitted as {@code {id, mandatory}} objects would be persisted unnormalized into draftData,
     * breaking both the next read (draftData binds to CbPlanDto.contentList: List&lt;String&gt;)
     * and the next republish (cb_plan_v3.contentlist is a text-list column). A LIVE update may
     * legitimately omit contentList to leave it unchanged, so this only runs when the field is
     * present in the request.
     *
     * @param incomingCbPlanRequest incoming update request, normalized in place
     * @param response              API response for error reporting
     * @return true when valid (or contentList absent), false when validation failed
     */
    private boolean normalizeLivePlanContentList(Map<String, Object> incomingCbPlanRequest, ApiResponse response) {
        if (!incomingCbPlanRequest.containsKey(Constants.CONTENT_LIST)) {
            return true;
        }
        List<String> errors = validationService.validateAndNormalizeContentList(incomingCbPlanRequest);
        if (CollectionUtils.isNotEmpty(errors)) {
            log.warn("CbPlanServiceV4Impl.normalizeLivePlanContentList: Validation failed - errorCount={}", errors.size());
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.ERR_VALIDATION_ERRORS + String.join("; ", errors));
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return false;
        }
        return true;
    }

    /**
     * Serializes the staged LIVE-plan update and writes it to the {@code draftData}
     * column only — the plan's own fields and status are left untouched until publish.
     *
     * @param updatedCbPlan  staged update data to serialize into draftData
     * @param existingCbPlan existing CB Plan record, used for its plan ID
     * @param response       API response object, populated with the update status
     * @throws JsonProcessingException if serialization fails
     */
    private void saveLivePlanAsDraft(Map<String, Object> updatedCbPlan, Map<String, Object> existingCbPlan,
                                     ApiResponse response) throws JsonProcessingException {
        String draftData = mapper.writeValueAsString(updatedCbPlan);
        String planId = (String) existingCbPlan.get(Constants.PLAN_ID);
        Map<String, Object> resp = cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_CB_PLAN_V3, Map.of(Constants.DRAFT_DATA, draftData),
                Map.of(Constants.PLAN_ID, planId));
        if (Constants.SUCCESS.equals(resp.get(Constants.RESPONSE))) {
            log.info("CbPlanServiceV4Impl.saveLivePlanAsDraft: Staged update as draft - cbPlanId={}", planId);
            response.getResult().put(Constants.STATUS, Constants.UPDATED);
            response.getResult().put(Constants.MESSAGE, String.format(Constants.MSG_UPDATED_AS_DRAFT, planId));
        } else {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr((String) resp.get(Constants.ERROR_MESSAGE) + " for cbPlanId: " + planId);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
        }
    }

    /**
     * Populates a 500 error response when serializing the LIVE-plan draft update fails.
     *
     * @param e        the JSON processing exception
     * @param response API response object to populate
     */
    private void handleLivePlanUpdateException(JsonProcessingException e, ApiResponse response) {
        log.error("CbPlanServiceV4Impl.handleUpdateOfLiveCbPlan: {}", Constants.ERR_SERIALIZING_CB_PLAN, e);
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(Constants.ERR_PROCESSING_CB_PLAN_DATA);
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     * Populates a 500 error response for any unhandled exception during update.
     *
     * @param response  API response object to populate
     * @param userOrgId caller's organization ID
     * @param e         the unhandled exception
     */
    private void handleUpdateException(ApiResponse response, String userOrgId, Exception e) {
        log.error("CbPlanServiceV4Impl.updateCbPlan: {} {}", Constants.ERR_FAILED_TO_UPDATE_CB_PLAN, userOrgId, e);
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(e.getMessage());
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Override
    public ApiResponse publishCbPlan(ApiRequest request, String authToken) {
        log.info("CbPlanServiceV4Impl.publishCbPlan: Publishing CB Plan V4");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_V4_PUBLISH);
        String userRootOrgId = null;
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }

            Map<String, String> userProfile = userProfileUtil.buildUserProfile(userId, response);
            userRootOrgId = userProfile.get(Constants.USER_ROOT_ORG_ID);
            String userRolesStr = userProfile.get(Constants.ROLES);

            if (StringUtils.isBlank(userRootOrgId)) {
                log.warn("CbPlanServiceV4Impl.publishCbPlan: Failed to fetch userRootOrgId for userId={}", userId);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.ERR_USER_ORG_NOT_FOUND);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            List<String> userRoles = StringUtils.isNotBlank(userRolesStr)
                    ? List.of(userRolesStr.split(","))
                    : Collections.emptyList();

            log.info("CbPlanServiceV4Impl.publishCbPlan: userId={}, orgId={}, roles={}", userId, userRootOrgId, userRoles);

            Map<String, Object> incomingRequest = (Map<String, Object>) request.getRequest();
            String cbPlanId = validationService.validateAndExtractPlanId(incomingRequest, response);
            if (StringUtils.isEmpty(cbPlanId)) {
                return response;
            }
            Map<String, Object> existingCbPlan = fetchExistingPlan(cbPlanId, response);
            if (MapUtils.isEmpty(existingCbPlan)) {
                return response;
            }
            if (validationService.isUnauthorizedToUpdate(userId, existingCbPlan, userRoles, response)) {
                return response;
            }
            executePublishFlow(request, userId, userRootOrgId, cbPlanId, existingCbPlan, response);
        } catch (Exception e) {
            handlePublishException(response, userRootOrgId, e);
        }
        return response;
    }

    /**
     * Re-validates the caller's org/CCA status, builds the publish update from
     * either the DRAFT plan's own fields or its staged {@code draftData}, and
     * commits the transaction.
     *
     * @param request        the API request containing the plan ID and publish comment
     * @param userId         caller's user ID
     * @param userOrgId      caller's organization ID
     * @param cbPlanId       CB Plan ID
     * @param existingCbPlan existing CB Plan record
     * @param response       API response object, populated with an error on failure
     * @throws JsonProcessingException if contextData/draftData serialization fails
     */
    private void executePublishFlow(ApiRequest request, String userId, String userOrgId, String cbPlanId,
                                    Map<String, Object> existingCbPlan, ApiResponse response) throws JsonProcessingException {
        String rootOrgId = validationService.validateUserOrganization(userId, response);
        if (StringUtils.isEmpty(rootOrgId)) {
            return;
        }
        boolean isCCA = validationService.validateOrgCCA(rootOrgId, response);
        if (Constants.FAILED.equalsIgnoreCase(response.getParams().getStatus())) {
            return;
        }
        Map<String, Object> incomingRequest = (Map<String, Object>) request.getRequest();
        String existingStatus = (String) existingCbPlan.get(Constants.STATUS);
        String planYear = (String) existingCbPlan.get(Constants.PLAN_YEAR);
        Map<String, Object> updatedRequest = preparePublishUpdate(incomingRequest, existingCbPlan, userId, isCCA, userOrgId, response);
        if (MapUtils.isEmpty(updatedRequest)) {
            return;
        }
        executePublishTransaction(cbPlanId, planYear, updatedRequest, existingCbPlan, existingStatus, response);
    }

    /**
     * Builds the base publish fields, then dispatches to the LIVE-republish or
     * DRAFT-first-publish builder based on the plan's current status.
     *
     * @param incomingRequest publish request map (plan ID, comment)
     * @param existingCbPlan  existing CB Plan record
     * @param userId          caller's user ID
     * @param isCCA           whether the logged in org is CCA
     * @param userOrgId       caller's organization ID
     * @param response        API response object, populated with an error on failure
     * @return the full Cassandra update map, or empty map when validation failed
     * @throws JsonProcessingException if draftData parsing/serialization fails
     */
    private Map<String, Object> preparePublishUpdate(Map<String, Object> incomingRequest,
                                                      Map<String, Object> existingCbPlan, String userId,
                                                      boolean isCCA, String userOrgId, ApiResponse response)
            throws JsonProcessingException {
        String existingStatus = (String) existingCbPlan.get(Constants.STATUS);
        log.debug("CbPlanServiceV4Impl.preparePublishUpdate: existingStatus={}", existingStatus);
        String comment = (String) incomingRequest.get(Constants.COMMENT);
        Map<String, Object> updatedRequest = new HashMap<>();
        updatedRequest.put(Constants.PUBLISHED_AT, Instant.now());
        updatedRequest.put(Constants.PUBLISHED_BY, userId);
        updatedRequest.put(Constants.UPDATED_AT, Instant.now());
        updatedRequest.put(Constants.COMMENT, comment);
        updatedRequest.put(Constants.UPDATED_BY, userId);
        if (Constants.LIVE.equalsIgnoreCase(existingStatus)) {
            return handleLivePlanPublish(existingCbPlan, incomingRequest, isCCA, userOrgId, updatedRequest, response);
        } else if (Constants.DRAFT.equalsIgnoreCase(existingStatus)) {
            return handleDraftPlanPublish(existingCbPlan, isCCA, userOrgId, updatedRequest, response);
        } else {
            log.warn("CbPlanServiceV4Impl.preparePublishUpdate: Invalid state for publish - existingStatus={}", existingStatus);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.ERR_CB_PLAN_INVALID_STATE_FOR_PUBLISH + existingStatus);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return Collections.emptyMap();
        }
    }

    /**
     * Builds the publish update for a plan's first publish (DRAFT to LIVE):
     * flips status to LIVE and resolves the org scope from the plan's own
     * contextData (no draftData involved yet).
     *
     * @param existingCbPlan  existing DRAFT CB Plan record
     * @param isCCA           whether the logged in org is CCA
     * @param userOrgId       caller's organization ID
     * @param updatedRequest  base publish fields, populated further in place
     * @param response        API response object, populated with an error on failure
     * @return the full update map, or empty map when validation failed
     */
    private Map<String, Object> handleDraftPlanPublish(Map<String, Object> existingCbPlan, boolean isCCA,
                                                        String userOrgId, Map<String, Object> updatedRequest,
                                                        ApiResponse response) {
        Set<String> rootOrgIdsInCriteria = new HashSet<>();
        Set<String> ministryOrStateIdsInCriteria = new HashSet<>();
        updatedRequest.put(Constants.STATUS, Constants.LIVE);
        updatedRequest.put(Constants.END_DATE_REQUEST,
                dataTransformService.parseEndDate(existingCbPlan.get(Constants.END_DATE_REQUEST)));
        if (!validationService.validateContextDataForLivePlanV4(existingCbPlan, isCCA, userOrgId,
                rootOrgIdsInCriteria, ministryOrStateIdsInCriteria, response)) {
            return Collections.emptyMap();
        }
        updatedRequest.put(Constants.ORG_SCOPE, existingCbPlan.get(Constants.ORG_SCOPE));
        updatedRequest.put(Constants.NEW_ROOT_ORG_IDS, rootOrgIdsInCriteria);
        updatedRequest.put(Constants.EXISTING_ROOT_ORG_IDS, Collections.emptySet());
        updatedRequest.put(Constants.NEW_MINISTRY_OR_STATE_IDS, ministryOrStateIdsInCriteria);
        updatedRequest.put(Constants.EXISTING_MINISTRY_OR_STATE_IDS, Collections.emptySet());
        return updatedRequest;
    }

    /**
     * Builds the publish update for republishing an already-LIVE plan: validates
     * the outgoing (currently-live) org scope for the diff, merges in the fields
     * staged in {@code draftData}, re-validates the resulting org scope, and
     * clears {@code draftData} back to empty.
     *
     * @param existingCbPlan  existing LIVE CB Plan record
     * @param incomingRequest publish request map (plan ID, comment)
     * @param isCCA           whether the logged in org is CCA
     * @param userOrgId       caller's organization ID
     * @param updatedRequest  base publish fields, populated further in place
     * @param response        API response object, populated with an error on failure
     * @return the full update map, or empty map when validation failed
     * @throws JsonProcessingException if draftData parsing/serialization fails
     */
    private Map<String, Object> handleLivePlanPublish(Map<String, Object> existingCbPlan,
                                                       Map<String, Object> incomingRequest, boolean isCCA,
                                                       String userOrgId, Map<String, Object> updatedRequest,
                                                       ApiResponse response) throws JsonProcessingException {
        Set<String> existingRootOrgIdsInCriteria = new HashSet<>();
        Set<String> rootOrgIdsInCriteria = new HashSet<>();
        Set<String> existingMinistryOrStateIdsInCriteria = new HashSet<>();
        Set<String> ministryOrStateIdsInCriteria = new HashSet<>();
        if (!validationService.validateContextDataForLivePlanV4(existingCbPlan, isCCA, userOrgId,
                existingRootOrgIdsInCriteria, existingMinistryOrStateIdsInCriteria, response)) {
            return Collections.emptyMap();
        }
        updatedRequest.putAll(prepareCbPlanForRePublish(existingCbPlan, incomingRequest));
        if (updatedRequest.containsKey(Constants.CONTEXT_DATA_REQUEST)
                && !validationService.validateContextDataForLivePlanV4(updatedRequest, isCCA, userOrgId,
                        rootOrgIdsInCriteria, ministryOrStateIdsInCriteria, response)) {
            return Collections.emptyMap();
        }
        updatedRequest.remove(Constants.ROOT_ORG_IDS_IN_CONTEXT_DATA);
        updatedRequest.put(Constants.DRAFT_DATA, mapper.writeValueAsString(Collections.emptyMap()));
        updatedRequest.put(Constants.EXISTING_ROOT_ORG_IDS, existingRootOrgIdsInCriteria);
        updatedRequest.put(Constants.NEW_ROOT_ORG_IDS, rootOrgIdsInCriteria);
        updatedRequest.put(Constants.EXISTING_MINISTRY_OR_STATE_IDS, existingMinistryOrStateIdsInCriteria);
        updatedRequest.put(Constants.NEW_MINISTRY_OR_STATE_IDS, ministryOrStateIdsInCriteria);
        return updatedRequest;
    }

    /**
     * Reads the plan's staged {@code draftData} (if any) and extracts only the
     * known-safe fields, folding in the incoming request's comment.
     *
     * @param existingCbPlan  existing LIVE CB Plan record
     * @param incomingRequest publish request map, only its comment is used here
     * @return extracted draft fields, empty map when no draftData is staged
     * @throws JsonProcessingException if draftData parsing fails
     */
    private Map<String, Object> prepareCbPlanForRePublish(Map<String, Object> existingCbPlan,
                                                           Map<String, Object> incomingRequest)
            throws JsonProcessingException {
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

    /**
     * Selectively copies only known, safe fields out of a parsed {@code draftData}
     * blob, preventing unexpected/malformed keys from reaching persistence.
     *
     * @param dataInDraftObject parsed draftData map
     * @return map containing only the recognised fields
     * @throws JsonProcessingException if re-serializing contextData fails
     */
    private Map<String, Object> extractDraftFields(Map<String, Object> dataInDraftObject)
            throws JsonProcessingException {
        Map<String, Object> extractedFields = new HashMap<>();
        copyDraftField(dataInDraftObject, extractedFields, Constants.IS_APAR);
        copyDraftField(dataInDraftObject, extractedFields, Constants.ORG_SCOPE);
        copyDraftField(dataInDraftObject, extractedFields, Constants.NAME);
        copyDraftField(dataInDraftObject, extractedFields, Constants.ROOT_ORG_IDS_IN_CONTEXT_DATA);
        copyDraftField(dataInDraftObject, extractedFields, Constants.CONTENT_LIST);
        copyDraftField(dataInDraftObject, extractedFields, Constants.PLAN_TYPE);
        if (dataInDraftObject.containsKey(Constants.CONTEXT_DATA_REQUEST)) {
            extractedFields.put(Constants.CONTEXT_DATA_REQUEST,
                    mapper.writeValueAsString(dataInDraftObject.get(Constants.CONTEXT_DATA_REQUEST)));
        }
        if (dataInDraftObject.containsKey(Constants.END_DATE_REQUEST)) {
            extractedFields.put(Constants.END_DATE_REQUEST,
                    dataTransformService.parseEndDate(dataInDraftObject.get(Constants.END_DATE_REQUEST)));
        }
        return extractedFields;
    }

    /**
     * Copies a single field from source to target only when present in source.
     *
     * @param source field's source map
     * @param target field's destination map
     * @param field  field key to copy
     */
    private void copyDraftField(Map<String, Object> source, Map<String, Object> target, String field) {
        if (source.containsKey(field)) {
            target.put(field, source.get(field));
        }
    }

    /**
     * Commits the publish update to Cassandra with an ES-update/rollback pair,
     * then updates the org/ministry lookup tables on success.
     *
     * @param cbPlanId       CB Plan ID
     * @param planYear       plan year
     * @param updatedRequest full publish update map, including org/ministry diff sets
     * @param existingCbPlan pre-publish plan record, used for ES rollback
     * @param existingStatus pre-publish plan status, decides whether lookup diffing runs
     * @param response       API response object, populated with an error on failure
     */
    private void executePublishTransaction(String cbPlanId, String planYear, Map<String, Object> updatedRequest,
                                           Map<String, Object> existingCbPlan, String existingStatus,
                                           ApiResponse response) {
        Set<String> existingRootOrgIds = (Set<String>) updatedRequest.get(Constants.EXISTING_ROOT_ORG_IDS);
        Set<String> newRootOrgIds = (Set<String>) updatedRequest.get(Constants.NEW_ROOT_ORG_IDS);
        Set<String> existingMinistryOrStateIds = (Set<String>) updatedRequest.get(Constants.EXISTING_MINISTRY_OR_STATE_IDS);
        Set<String> newMinistryOrStateIds = (Set<String>) updatedRequest.get(Constants.NEW_MINISTRY_OR_STATE_IDS);
        updatedRequest.remove(Constants.EXISTING_ROOT_ORG_IDS);
        updatedRequest.remove(Constants.NEW_ROOT_ORG_IDS);
        updatedRequest.remove(Constants.EXISTING_MINISTRY_OR_STATE_IDS);
        updatedRequest.remove(Constants.NEW_MINISTRY_OR_STATE_IDS);
        Map<String, Object> sanitizedMap = elasticSearchService.sanitizeForElastic(updatedRequest);
        Map<String, Object> sanitizedExisting = elasticSearchService.sanitizeForElastic(existingCbPlan);
        Map<String, Object> resp = cassandraOperation.updateRecord(
                Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_CB_PLAN_V3,
                updatedRequest,
                Map.of(Constants.PLAN_ID, cbPlanId),
                () -> Objects.nonNull(esUtilService.updateDocument(serverProperties.getCpPlanIndex(), Constants.INDEX_TYPE,
                        cbPlanId, sanitizedMap, serverProperties.getElasticCbPlanJsonPath())),
                () -> rollbackElasticSearchDocument(cbPlanId, sanitizedExisting));
        if (Constants.SUCCESS.equals(resp.get(Constants.RESPONSE))) {
            log.info("CbPlanServiceV4Impl.executePublishTransaction: Published - cbPlanId={}, planYear={}", cbPlanId, planYear);
            updatedRequest.put(Constants.EXISTING_ROOT_ORG_IDS, existingRootOrgIds);
            updatedRequest.put(Constants.NEW_ROOT_ORG_IDS, newRootOrgIds);
            updatedRequest.put(Constants.EXISTING_MINISTRY_OR_STATE_IDS, existingMinistryOrStateIds);
            updatedRequest.put(Constants.NEW_MINISTRY_OR_STATE_IDS, newMinistryOrStateIds);
            updateOrgLookupTables(cbPlanId, planYear, updatedRequest, existingCbPlan, existingStatus, response);
        } else {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr((String) resp.get(Constants.ERROR_MESSAGE) + " for cbPlanId: " + cbPlanId);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Rolls the Elasticsearch document back to its pre-publish state after a
     * Cassandra commit failure. Logs an ES/Cassandra divergence error if the
     * rollback itself fails, since that leaves the two stores inconsistent.
     *
     * @param cbPlanId          CB Plan ID
     * @param sanitizedExisting pre-publish document to restore
     */
    private void rollbackElasticSearchDocument(String cbPlanId, Map<String, Object> sanitizedExisting) {
        String rollbackResult = esUtilService.updateDocument(serverProperties.getCpPlanIndex(),
                Constants.INDEX_TYPE, cbPlanId, sanitizedExisting, serverProperties.getElasticCbPlanJsonPath());
        if (Objects.isNull(rollbackResult)) {
            log.error("CbPlanServiceV4Impl: ES_CASSANDRA_DIVERGENCE: failed to roll back ES document for cbPlanId={} "
                    + "after Cassandra commit failure - manual reconciliation required", cbPlanId);
        }
    }

    /**
     * Updates the org-scope and ministry-or-state-id lookup tables for the newly
     * published state, then reconciles removed entries when this was a republish
     * of an already-LIVE plan.
     *
     * @param cbPlanId       CB Plan ID
     * @param planYear       plan year
     * @param updatedRequest applied publish update, including org/ministry diff sets
     * @param existingCbPlan pre-publish plan record
     * @param existingStatus pre-publish plan status
     * @param response       API response object, populated with an error on failure
     */
    private void updateOrgLookupTables(String cbPlanId, String planYear, Map<String, Object> updatedRequest,
                                       Map<String, Object> existingCbPlan, String existingStatus,
                                       ApiResponse response) {
        String orgScope = (String) updatedRequest.getOrDefault(Constants.ORG_SCOPE, existingCbPlan.get(Constants.ORG_SCOPE));
        Instant endDate = (Instant) updatedRequest.getOrDefault(Constants.END_DATE_REQUEST, existingCbPlan.get(Constants.END_DATE_REQUEST));
        Set<String> newRootOrgIds = (Set<String>) updatedRequest.get(Constants.NEW_ROOT_ORG_IDS);
        Set<String> existingRootOrgIds = (Set<String>) updatedRequest.get(Constants.EXISTING_ROOT_ORG_IDS);
        Set<String> newMinistryOrStateIds = (Set<String>) updatedRequest.get(Constants.NEW_MINISTRY_OR_STATE_IDS);
        Set<String> existingMinistryOrStateIds = (Set<String>) updatedRequest.get(Constants.EXISTING_MINISTRY_OR_STATE_IDS);
        boolean hasMinistryOrStateId = CollectionUtils.isNotEmpty(newMinistryOrStateIds)
                || CollectionUtils.isNotEmpty(existingMinistryOrStateIds);
        upsertOrgScopeLookupTables(cbPlanId, planYear, orgScope, newRootOrgIds, endDate, hasMinistryOrStateId, response);
        if (CollectionUtils.isNotEmpty(newMinistryOrStateIds)) {
            insertMinistryOrStateIdLookup(cbPlanId, planYear, newMinistryOrStateIds, endDate, response);
        }
        if (Constants.LIVE.equalsIgnoreCase(existingStatus)) {
            log.info("updateOrgLookupTables: Handling LIVE plan republish - cbPlanId={}", cbPlanId);
            orgLookupService.handleOrgLookupChanges(cbPlanId, planYear, existingRootOrgIds, newRootOrgIds,
                    (String) existingCbPlan.get(Constants.ORG_SCOPE), hasMinistryOrStateId, response);
            orgLookupService.handleMinistryOrStateIdLookupChanges(cbPlanId, planYear, existingMinistryOrStateIds,
                    newMinistryOrStateIds, endDate, response);
        }
    }

    /**
     * Upserts the org-scope lookup table (custom/single org list, or the
     * all-org table) matching the plan's orgScope, unless ministryOrStateId
     * criteria is in play, in which case both are skipped.
     *
     * @param cbPlanId             CB Plan ID
     * @param planYear             plan year
     * @param orgScope             plan's org scope (SINGLE/CUSTOM/ALL)
     * @param newRootOrgIds        root org IDs to upsert for SINGLE/CUSTOM scope
     * @param endDate              plan end date
     * @param hasMinistryOrStateId true when ministryOrStateId criteria is used
     * @param response             API response object, populated with an error on failure
     */
    private void upsertOrgScopeLookupTables(String cbPlanId, String planYear, String orgScope,
                                            Set<String> newRootOrgIds, Instant endDate,
                                            boolean hasMinistryOrStateId, ApiResponse response) {
        ApiResponse lookupResp = null;
        if (Constants.SINGLE.equalsIgnoreCase(orgScope) || Constants.CUSTOM.equalsIgnoreCase(orgScope)) {
            lookupResp = orgLookupService.upsertCustomOrgLookup(cbPlanId, planYear, newRootOrgIds, endDate, true);
        } else if (Constants.ALL.equalsIgnoreCase(orgScope) && !hasMinistryOrStateId) {
            lookupResp = orgLookupService.upsertAllOrgLookup(cbPlanId, planYear, endDate, true);
        } else if (hasMinistryOrStateId) {
            log.info("upsertOrgScopeLookupTables: Skipping org/all_org tables (using ministryOrStateId) - cbPlanId={}", cbPlanId);
        }
        if (Objects.nonNull(lookupResp) && !Constants.SUCCESS.equals(lookupResp.get(Constants.RESPONSE))) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(lookupResp.getParams().getErr());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Inserts new ministry-or-state-id lookup entries for the published plan.
     *
     * @param cbPlanId           CB Plan ID
     * @param planYear           plan year
     * @param ministryOrStateIds ministryOrStateId values to insert
     * @param endDate            plan end date
     * @param response           API response object, populated with an error on failure
     */
    private void insertMinistryOrStateIdLookup(String cbPlanId, String planYear, Set<String> ministryOrStateIds,
                                               Instant endDate, ApiResponse response) {
        ApiResponse ministryLookupResp = orgLookupService.upsertMinistryOrStateIdLookup(
                cbPlanId, planYear, ministryOrStateIds, endDate, true);
        if (!Constants.SUCCESS.equals(ministryLookupResp.get(Constants.RESPONSE))) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(ministryLookupResp.getParams().getErr());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            log.error("insertMinistryOrStateIdLookup: Failed to insert ministryOrStateId lookup for CB Plan: {}", cbPlanId);
        }
    }

    /**
     * Populates a 500 error response for any unhandled exception during publish.
     *
     * @param response  API response object to populate
     * @param userOrgId caller's organization ID
     * @param e         the unhandled exception
     */
    private void handlePublishException(ApiResponse response, String userOrgId, Exception e) {
        log.error("CbPlanServiceV4Impl.publishCbPlan: Failed to publish CB Plan V4 for orgId: {}", userOrgId, e);
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(e.getMessage());
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     * Reads a CB Plan V4 by ID. Delegates to {@link CbPlanReadServiceV4Impl},
     * which renders contextData exactly as stored, matching V3's read behaviour.
     *
     * @param cbPlanId      the CB Plan ID to retrieve
     * @param authUserToken the authentication token
     * @return ApiResponse containing the CB Plan details or error
     */
    @Override
    public ApiResponse readCbPlan(String cbPlanId, String authUserToken) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_V4_READ);
        log.info("CbPlanServiceV4Impl.readCbPlan: Starting - cbPlanId={}", cbPlanId);
        try {
            executeReadFlow(cbPlanId, response);
        } catch (JsonProcessingException e) {
            handleReadJsonException(e, cbPlanId, response);
        } catch (Exception e) {
            handleReadException(e, cbPlanId, response);
        }
        return response;
    }

    /**
     * Validates the plan ID, fetches the plan row, and builds the read response.
     *
     * @param cbPlanId CB Plan ID
     * @param response API response object, populated with the plan data or an error
     * @throws JsonProcessingException if draftData parsing fails downstream
     */
    private void executeReadFlow(String cbPlanId, ApiResponse response) throws JsonProcessingException {
        if (StringUtils.isEmpty(cbPlanId)) {
            log.warn("CbPlanServiceV4Impl.readCbPlan: Missing cbPlanId");
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.ERR_CB_PLAN_ID_MISSING);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return;
        }
        Map<String, Object> cbPlan = fetchExistingPlan(cbPlanId, response);
        if (MapUtils.isEmpty(cbPlan)) {
            return;
        }
        CbPlanReadResponseDto enrichedData = readService.buildEnrichedPlanData(cbPlan, cbPlanId);
        response.getResult().put(Constants.CONTENT, enrichedData);
        log.info("CbPlanServiceV4Impl.readCbPlan: Successfully retrieved CB Plan - cbPlanId={}", cbPlanId);
    }

    /**
     * Populates a 500 error response when parsing the plan's draftData fails during read.
     *
     * @param e        the JSON processing exception
     * @param cbPlanId CB Plan ID
     * @param response API response object to populate
     */
    private void handleReadJsonException(JsonProcessingException e, String cbPlanId, ApiResponse response) {
        log.error("CbPlanServiceV4Impl.readCbPlan: JSON processing failed - cbPlanId={}", cbPlanId, e);
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(Constants.ERR_PROCESSING_CB_PLAN_DATA);
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     * Populates a 500 error response for any unhandled exception during read.
     *
     * @param e         the unhandled exception
     * @param cbPlanId  CB Plan ID
     * @param response  API response object to populate
     */
    private void handleReadException(Exception e, String cbPlanId, ApiResponse response) {
        log.error("CbPlanServiceV4Impl.readCbPlan: Failed to read CB Plan - cbPlanId={}", cbPlanId, e);
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(e.getMessage());
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
