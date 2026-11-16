package com.igot.cb.cbplan.service.impl.v4;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.igot.cb.cbplan.service.impl.CbPlanRequestValidatorImpl;
import com.igot.cb.cbplan.service.impl.CbPlanValidationServiceV3Impl;
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
import com.igot.cb.util.Constants;
import com.igot.cb.util.UserProfileUtil;

import lombok.extern.slf4j.Slf4j;

/**
 * Service for CB Plan V4 validation operations.
 * Validates user group references instead of inline criteria.
 *
 * @version 4.0
 */
@Service
@Slf4j
public class CbPlanValidationServiceV4Impl {
    private final CbPlanValidationServiceV3Impl validationServiceV3;
    private final CbPlanRequestValidatorImpl cbPlanRequestValidator;
    private final CbPlanOrgScopeServiceV4Impl orgScopeService;
    private final UserProfileUtil userProfileUtil;
    private final ObjectMapper mapper;

    public CbPlanValidationServiceV4Impl(CbPlanValidationServiceV3Impl validationServiceV3,
                                         CbPlanRequestValidatorImpl cbPlanRequestValidator,
                                         CbPlanOrgScopeServiceV4Impl orgScopeService,
                                         UserProfileUtil userProfileUtil) {
        this.validationServiceV3 = validationServiceV3;
        this.cbPlanRequestValidator = cbPlanRequestValidator;
        this.orgScopeService = orgScopeService;
        this.userProfileUtil = userProfileUtil;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Extracts and validates the caller's user ID from the auth token.
     * Delegates to the shared V3 token-validation logic.
     *
     * @param authToken authentication token
     * @param response  API response object, populated with an error on failure
     * @return user ID, or blank when the token is invalid
     */
    public String validateAndExtractUserId(String authToken, ApiResponse response) {
        return validationServiceV3.validateAndExtractUserId(authToken, response);
    }

    /**
     * Resolves the user's root org ID via the shared, Redis-cached user profile lookup.
     * Matches the pattern used by the UserGroup module (UserProfileUtil.buildUserProfile)
     * instead of V3's uncached, direct-to-Cassandra lookup.
     *
     * @param userId   user ID
     * @param response API response object
     * @return root org ID, or null if the user or their org could not be resolved
     */
    public String validateUserOrganization(String userId, ApiResponse response) {
        Map<String, String> userProfile = userProfileUtil.buildUserProfile(userId, response);
        return userProfile.get(Constants.USER_ROOT_ORG_ID);
    }

    /**
     * Checks whether the given root org is a Central/Common Cadre Authority (CCA) org.
     * Delegates to the shared V3 logic.
     *
     * @param rootOrgId root org ID
     * @param response  API response object, populated with an error on failure
     * @return true when the org is CCA
     */
    public boolean validateOrgCCA(String rootOrgId, ApiResponse response) {
        return validationServiceV3.validateOrgCCA(rootOrgId, response);
    }

    /**
     * Validates that the update request carries a CB Plan ID.
     * Delegates to the shared V3 logic.
     *
     * @param request  the API request
     * @param response API response object, populated with an error on failure
     * @return true when the plan ID is present
     */
    public boolean validatePlanIdExists(ApiRequest request, ApiResponse response) {
        return validationServiceV3.validatePlanIdExists(request, response);
    }

    /**
     * Two-tier authorization check for update/publish: the creator is always
     * authorized (fast path), otherwise the caller must hold one of the
     * configured authorized roles. Delegates to the shared V3 logic.
     *
     * @param userId         caller's user ID
     * @param existingCbPlan existing CB Plan record
     * @param userRoles      caller's roles
     * @param response       API response object, populated with a 403 on failure
     * @return true when the caller is NOT authorized to update the plan
     */
    public boolean isUnauthorizedToUpdate(String userId, Map<String, Object> existingCbPlan,
                                          List<String> userRoles, ApiResponse response) {
        return validationServiceV3.isUnauthorizedToUpdate(userId, existingCbPlan, userRoles, response);
    }

    /**
     * Extracts and validates the CB Plan ID from a publish request.
     * Delegates to the shared V3 logic.
     *
     * @param incomingRequest incoming request map
     * @param response        API response object, populated with an error on failure
     * @return CB Plan ID, or blank when missing
     */
    public String validateAndExtractPlanId(Map<String, Object> incomingRequest, ApiResponse response) {
        return validationServiceV3.validateAndExtractPlanId(incomingRequest, response);
    }

    /**
     * Validates a LIVE CB Plan update's userGroupId references and resolves orgScope/orgIdList
     * from the criteria stored against each referenced group.
     *
     * @param incomingRequest         incoming request map, updated in place with ORG_SCOPE/ORG_ID_LIST
     * @param isCCA                   whether the logged in org is CCA
     * @param rootOrgId               logged in user's organization ID
     * @param rootOrgIdsInContextData set populated with the rootOrgId values resolved from the referenced groups
     * @param ministryOrStateIdsInContextData set populated with the ministryOrStateId values resolved from the referenced groups
     * @param response                API response object
     * @return true if valid, false otherwise
     */
    public boolean validateContextDataForLivePlanV4(Map<String, Object> incomingRequest, boolean isCCA,
                                                     String rootOrgId, Set<String> rootOrgIdsInContextData,
                                                     Set<String> ministryOrStateIdsInContextData,
                                                     ApiResponse response) {
        List<String> errors = orgScopeService.resolveOrgScope(incomingRequest, isCCA, rootOrgId,
                rootOrgIdsInContextData, ministryOrStateIdsInContextData);
        if (CollectionUtils.isNotEmpty(errors)) {
            log.warn("CbPlanValidationServiceV4.validateContextDataForLivePlanV4: Validation failed - errorCount={}",
                    errors.size());
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.ERR_VALIDATION_ERRORS + String.join("; ", errors));
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return false;
        }
        return true;
    }

    /**
     * Validates a full create/draft-update request: contentList shape, mandatory
     * fields, and the userGroupId-derived org scope, in that order.
     *
     * @param request   the API request containing CB Plan details
     * @param isCCA     whether the logged in org is CCA
     * @param userOrgId logged in user's organization ID
     * @param response  API response object, populated with an error on failure
     * @return true when the request is valid
     */
    public boolean validateRequest(ApiRequest request, boolean isCCA, String userOrgId, ApiResponse response) {
        log.debug("CbPlanValidationServiceV4.validateRequest: Entry - userOrgId={}", userOrgId);
        try {
            List<String> validations = new ArrayList<>();
            // Must run before validateBasicFieldsAndOrgScope: it normalizes contentList from
            // {id, mandatory} objects to a flat id list, and cbPlanRequestValidator.validateMandatoryFields
            // binds the request onto CbPlanDto.contentList (List<String>) — Jackson throws
            // IllegalArgumentException if it still sees objects there.
            List<String> contentListValidations = validateContentList(request);
            if (CollectionUtils.isNotEmpty(contentListValidations)) {
                validations.addAll(contentListValidations);
            }
            List<String> basicValidations = validateBasicFieldsAndOrgScope(request, isCCA, userOrgId);
            if (CollectionUtils.isNotEmpty(basicValidations)) {
                validations.addAll(basicValidations);
            }
            if (CollectionUtils.isNotEmpty(validations)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(mapper.writeValueAsString(validations));
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                log.warn("CbPlanValidationServiceV4: Validation failed for orgId: {}", userOrgId);
                return false;
            }
            return true;
        } catch (JsonProcessingException e) {
            log.error("CbPlanValidationServiceV4: {}", Constants.ERR_FAILED_TO_SERIALIZE_VALIDATION, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.ERR_VALIDATION_ERROR);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return false;
        }
    }

    /**
     * Validates mandatory CB Plan fields, then resolves orgScope/orgIdList from the userGroupId
     * references declared in contextData. Unlike V3, this does not require an inline
     * userGroupCriteriaList since V4 groups are referenced by ID only.
     *
     * @param request   the API request containing CB Plan details
     * @param isCCA     whether the logged in org is CCA
     * @param userOrgId logged in user's organization ID
     * @return validation errors, empty when the request is valid
     */
    private List<String> validateBasicFieldsAndOrgScope(ApiRequest request, boolean isCCA, String userOrgId) {
        Map<String, Object> requestMap = (Map<String, Object>) request.getRequest();
        List<String> errors = cbPlanRequestValidator.validateMandatoryFields(requestMap);
        if (CollectionUtils.isNotEmpty(errors)) {
            return errors;
        }
        return orgScopeService.resolveOrgScope(requestMap, isCCA, userOrgId, null, null);
    }

    /**
     * Unwraps the request body and delegates to {@link #validateAndNormalizeContentList}.
     *
     * @param request the API request containing CB Plan details
     * @return validation errors, empty when contentList is valid
     */
    private List<String> validateContentList(ApiRequest request) {
        return validateAndNormalizeContentList((Map<String, Object>) request.getRequest());
    }

    /**
     * Validates and normalizes the contentList field in place, reducing the
     * {@code {id, mandatory}} entries to a flat list of content IDs. Shared by
     * create/draft-update (via {@link #validateRequest}) and the LIVE-plan
     * update path in {@code CbPlanServiceV4Impl.handleUpdateOfLiveCbPlan}, which
     * stores contentList directly into draftData and must not persist the raw
     * {id, mandatory} shape there: draftData is later bound to CbPlanDto
     * (contentList: List&lt;String&gt;) on read, and cb_plan_v3.contentlist is a
     * text-list column on republish, so either path fails on the unnormalized shape.
     *
     * @param requestMap request map holding contentList, updated in place
     * @return validation errors, empty when contentList is valid
     */
    public List<String> validateAndNormalizeContentList(Map<String, Object> requestMap) {
        List<String> validations = new ArrayList<>();
        if (!requestMap.containsKey(Constants.CONTENT_LIST)) {
            validations.add(Constants.ERR_CONTENTS_REQUIRED);
            return validations;
        }
        Object contentListObj = requestMap.get(Constants.CONTENT_LIST);
        if (!(contentListObj instanceof List)) {
            validations.add(Constants.ERR_CONTENTS_REQUIRED);
            return validations;
        }
        List<?> contentList = (List<?>) contentListObj;
        if (CollectionUtils.isEmpty(contentList)) {
            validations.add(Constants.ERR_CONTENTS_EMPTY);
            return validations;
        }
        // Auto-detect format: V3 (plain strings) or V4 (objects)
        Object firstItem = contentList.get(0);
        if (firstItem instanceof String) {
            // V3 format: ["do_123", "do_456"] - validate as plain IDs
            return validateV3Format((List<String>) contentList, validations);
        } else if (firstItem instanceof Map) {
            // V4 format: [{"identifier":"do_123","mandatory":true}] - validate objects
            return validateV4Format((List<Map<String, Object>>) contentList, validations);
        } else {
            validations.add("Content list items must be either strings (V3) or objects (V4)");
            return validations;
        }
    }

    /**
     * Validates a single contentList entry: a present, non-blank {@code identifier} and a
     * present {@code mandatory} flag that is a boolean value.
     *
     * @param content     single contentList entry
     * @param index       entry's position in contentList, used in error messages
     * @param validations collector for validation errors
     */
    private void validateSingleContent(Map<String, Object> content, int index, List<String> validations) {
        if (MapUtils.isEmpty(content)) {
            validations.add(String.format("Content at index %d is empty", index));
            return;
        }
        Object identifier = content.get(Constants.IDENTIFIER);
        if (Objects.isNull(identifier) || StringUtils.isBlank(identifier.toString())) {
            validations.add(String.format(Constants.ERR_FORMAT_AT_INDEX, Constants.ERR_CONTENT_ID_REQUIRED, index));
        }
        if (!content.containsKey(Constants.MANDATORY)) {
            validations.add(String.format(Constants.ERR_FORMAT_AT_INDEX, Constants.ERR_CONTENT_MANDATORY_REQUIRED, index));
        } else {
            Object mandatory = content.get(Constants.MANDATORY);
            if (!(mandatory instanceof Boolean)) {
                validations.add(String.format(Constants.ERR_FORMAT_AT_INDEX, Constants.ERR_CONTENT_MANDATORY_INVALID, index));
            }
        }
    }

    /**
     * Validates V3 format contentList (plain string identifiers).
     */
    private List<String> validateV3Format(List<String> contentList, List<String> validations) {
        for (int i = 0; i < contentList.size(); i++) {
            String contentId = contentList.get(i);
            if (StringUtils.isBlank(contentId)) {
                validations.add(String.format("Content ID at index %d is blank", i));
            }
        }
        return validations;
    }

    /**
     * Validates V4 format contentList (objects with identifier and mandatory).
     */
    private List<String> validateV4Format(List<Map<String, Object>> contentList, List<String> validations) {
        for (int i = 0; i < contentList.size(); i++) {
            Map<String, Object> content = contentList.get(i);
            validateSingleContent(content, i, validations);
        }
        return validations;
    }
}
