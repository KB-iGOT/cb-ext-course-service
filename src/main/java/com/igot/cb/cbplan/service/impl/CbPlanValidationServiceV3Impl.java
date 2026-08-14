package com.igot.cb.cbplan.service.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.UserAndOrgServiceImpl;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import com.igot.cb.util.RequestValidator;

import lombok.extern.slf4j.Slf4j;

/**
 * Service for CB Plan validation operations.
 *
 * @version 3.0
 */
@Service
@Slf4j
public class CbPlanValidationServiceV3Impl {
    private final AccessTokenValidator accessTokenValidator;
    private final UserAndOrgServiceImpl userAndOrgService;
    private final RequestValidator requestValidator;
    private final ObjectMapper mapper;

    public CbPlanValidationServiceV3Impl(AccessTokenValidator accessTokenValidator,
                                         UserAndOrgServiceImpl userAndOrgService,
                                         RequestValidator requestValidator) {
        this.accessTokenValidator = accessTokenValidator;
        this.userAndOrgService = userAndOrgService;
        this.requestValidator = requestValidator;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Validates authentication token and extracts user ID.
     *
     * @param authToken authentication token
     * @param response  API response object
     * @return user ID or null if validation fails
     */
    public String validateAndExtractUserId(String authToken, ApiResponse response) {
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
        if (StringUtils.isEmpty(userId)) {
            log.warn("CbPlanValidationService: {}", Constants.ERR_FAILED_TO_EXTRACT_USER_ID);
            return null;
        }
        return userId;
    }

    /**
     * Validates user organization and returns root org ID.
     *
     * @param userId   user ID
     * @param response API response object
     * @return root org ID or null if validation fails
     */
    public String validateUserOrganization(String userId, ApiResponse response) {
        String rootOrgId = getRootOrgFromUser(userId, response);
        if (Constants.FAILED.equalsIgnoreCase(response.getParams().getStatus())) {
            return null;
        }
        return rootOrgId;
    }

    /**
     * Validates if organization is CCA.
     *
     * @param rootOrgId root organization ID
     * @param response  API response object
     * @return true if CCA, false otherwise
     */
    public boolean validateOrgCCA(String rootOrgId, ApiResponse response) {
        return getCCAFromOrg(rootOrgId, response);
    }

    /**
     * Validates CB Plan create request.
     *
     * @param request   API request
     * @param isCCA     whether org is CCA
     * @param userOrgId user org ID
     * @param response  API response object
     * @return true if valid, false otherwise
     */
    public boolean validateRequest(ApiRequest request, boolean isCCA, String userOrgId, ApiResponse response) {
        try {
            List<String> validations = requestValidator.validateCbPlanCreateRequestV3(request, isCCA, userOrgId, false);
            if (CollectionUtils.isNotEmpty(validations)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(mapper.writeValueAsString(validations));
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                log.warn("CbPlanValidationService: Validation failed for orgId: {}", userOrgId);
                return false;
            }
            return true;
        } catch (JsonProcessingException e) {
            log.error("CbPlanValidationService: {}", Constants.ERR_FAILED_TO_SERIALIZE_VALIDATION, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.ERR_VALIDATION_ERROR);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return false;
        }
    }

    /**
     * Validates if plan ID exists in request.
     *
     * @param request  API request
     * @param response API response object
     * @return true if plan ID exists, false otherwise
     */
    public boolean validatePlanIdExists(ApiRequest request, ApiResponse response) {
        Map<String, Object> updatedCbPlan = (Map<String, Object>) request.getRequest();
        String cbPlanId = (String) updatedCbPlan.get(Constants.ID);
        if (StringUtils.isBlank(cbPlanId)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.ERR_REQUIRED_PARAM_ID_MISSING);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return false;
        }
        return true;
    }

    /**
     * Checks if user is unauthorized to update CB Plan.
     *
     * @param userId         user ID
     * @param existingCbPlan existing CB Plan data
     * @param userRoles      user roles
     * @param response       API response object
     * @return true if unauthorized, false if authorized
     */
    public boolean isUnauthorizedToUpdate(String userId, Map<String, Object> existingCbPlan,
                                          List<String> userRoles, ApiResponse response) {
        String createdBy = (String) existingCbPlan.get(Constants.CREATED_BY);
        boolean isOwner = userId.equals(createdBy);
        boolean isAdmin = CollectionUtils.isNotEmpty(userRoles) && userRoles.contains(Constants.ROLE_ADMIN);
        if (!isOwner && !isAdmin) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.ERR_UNAUTHORIZED_UPDATE);
            response.setResponseCode(HttpStatus.FORBIDDEN);
            log.warn("CbPlanValidationService: User {} unauthorized to update plan created by {}", userId, createdBy);
            return true;
        }
        return false;
    }

    /**
     * Validates context data for LIVE plan updates.
     *
     * @param incomingRequest         incoming request map
     * @param isCCA                   whether org is CCA
     * @param rootOrgId               root org ID
     * @param rootOrgIdsInContextData set to populate with org IDs from context
     * @param response                API response object
     * @return true if valid, false otherwise
     */
    public boolean validateContextDataForLivePlan(Map<String, Object> incomingRequest, boolean isCCA,
                                                  String rootOrgId, java.util.Set<String> rootOrgIdsInContextData,
                                                  ApiResponse response) {
        List<String> errors = requestValidator.validateContextData(incomingRequest, isCCA, rootOrgId,
                rootOrgIdsInContextData);
        if (CollectionUtils.isNotEmpty(errors)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.ERR_VALIDATION_ERRORS + String.join("; ", errors));
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return false;
        }
        return true;
    }

    /**
     * Validates plan ID from request map.
     *
     * @param incomingRequest incoming request map
     * @param response        API response object
     * @return plan ID or null if validation fails
     */
    public String validateAndExtractPlanId(Map<String, Object> incomingRequest, ApiResponse response) {
        String planId = (String) incomingRequest.get(Constants.ID);
        if (StringUtils.isBlank(planId)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.ERR_REQUIRED_PARAM_ID_MISSING);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return null;
        }
        return planId;
    }

    /**
     * Checks if CB Plan is already archived.
     *
     * @param existingCbPlan existing CB Plan data
     * @param cbPlanId       CB Plan ID
     * @param response       API response object
     * @return true if already archived, false otherwise
     */
    public boolean isAlreadyArchived(Map<String, Object> existingCbPlan, String cbPlanId, ApiResponse response) {
        String status = (String) existingCbPlan.get(Constants.STATUS);
        if (Constants.CB_RETIRE.equalsIgnoreCase(status)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(String.format(Constants.ERR_CB_PLAN_ALREADY_RETIRED, cbPlanId));
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            log.warn("CbPlanValidationService: CB Plan {} is already retired", cbPlanId);
            return true;
        }
        return false;
    }

    private String getRootOrgFromUser(String userId, ApiResponse response) {
        Map<String, Object> userMap = userAndOrgService.readUserProfileFromDB(
                userId,
                Arrays.asList(Constants.ID, Constants.ROOT_ORG_ID));
        if (MapUtils.isEmpty(userMap)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrMsg(Constants.ERR_FAILED_TO_READ_USER_DETAILS + userId);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            log.error("CbPlanValidationService.getRootOrgFromUser: User not found for userId: {}", userId);
            return null;
        }
        return (String) userMap.get(Constants.ROOT_ORG_ID);
    }

    private boolean getCCAFromOrg(String orgId, ApiResponse response) {
        Map<String, Object> orgMap = userAndOrgService.readOrgFromDB(orgId, null);
        if (MapUtils.isEmpty(orgMap)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrMsg(Constants.ERR_FAILED_TO_READ_ORG_DETAILS + orgId);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            log.error("CbPlanValidationService.getCCAFromOrg: Org not found for orgId: {}", orgId);
            return false;
        }
        if (orgMap.containsKey(Constants.IS_CCA) && Objects.nonNull(orgMap.get(Constants.IS_CCA))) {
            return Boolean.parseBoolean(orgMap.get(Constants.IS_CCA).toString());
        }
        return false;
    }
}
