package com.igot.cb.usergroups.service.impl;

import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.UserAndOrgServiceImpl;
import com.igot.cb.usergroups.model.CriteriaItem;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Validation service for User Group operations.
 * Follows cbplan/v3 validation patterns - returns boolean and sets errors in ApiResponse.
 */
@Service
public class UserGroupValidationServiceImpl {

    private static final Logger log = LoggerFactory.getLogger(UserGroupValidationServiceImpl.class);

    private final CbExtServerProperties serverProperties;
    private final UserAndOrgServiceImpl userAndOrgService;
    private final EsUtilService esUtilService;

    public UserGroupValidationServiceImpl(CbExtServerProperties serverProperties,
                                          UserAndOrgServiceImpl userAndOrgService,
                                          EsUtilService esUtilService) {
        this.serverProperties = serverProperties;
        this.userAndOrgService = userAndOrgService;
        this.esUtilService = esUtilService;
    }

    public boolean validateCreateRequest(String userGroupName, List<CriteriaItem> criteria,
                                          String userRootOrgId, String userRoles, ApiResponse response) {
        log.debug("validateCreateRequest: userGroupName={}, criteriaCount={}, userRootOrgId={}",
                userGroupName, CollectionUtils.isNotEmpty(criteria) ? criteria.size() : 0, userRootOrgId);

        if (StringUtils.isBlank(userGroupName)) {
            log.warn("Validation failed: userGroupName is required");
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.MSG_USERGROUPNAME_REQUIRED);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return false;
        }

        if (!validateCriteria(criteria, response)) {
            return false;
        }

        return validateRootOrgIdCriteria(criteria, userRootOrgId, userRoles, response);
    }

    public boolean validateUpdateRequest(String userGroupId, String userGroupName, List<CriteriaItem> criteria, ApiResponse response) {
        log.debug("validateUpdateRequest: userGroupId={}", userGroupId);

        if (StringUtils.isBlank(userGroupId)) {
            log.warn("Validation failed: userGroupId is required");
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.MSG_USERGROUPID_REQUIRED);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return false;
        }

        if (StringUtils.isNotEmpty(userGroupName) && StringUtils.isBlank(userGroupName)) {
            log.warn("Validation failed: userGroupName cannot be blank when provided");
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.MSG_USERGROUPNAME_REQUIRED);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return false;
        }

        if (CollectionUtils.isNotEmpty(criteria)) {
            return validateCriteria(criteria, response);
        }

        return true;
    }

    public boolean validateUserGroupId(String userGroupId, ApiResponse response) {
        if (StringUtils.isBlank(userGroupId)) {
            log.warn("Validation failed: usergroupid is required");
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.MSG_USERGROUPID_REQUIRED);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return false;
        }
        return true;
    }

    private boolean validateCriteria(List<CriteriaItem> criteria, ApiResponse response) {
        if (CollectionUtils.isEmpty(criteria)) {
            log.warn("Validation failed: criteria is required and must have at least one entry");
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.MSG_CRITERIA_REQUIRED);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return false;
        }

        for (CriteriaItem item : criteria) {
            if (StringUtils.isBlank(item.criteriaKey())) {
                log.warn("Validation failed: criteriaKey cannot be blank");
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.MSG_CRITERIA_KEY_BLANK);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return false;
            }

            if (CollectionUtils.isEmpty(item.criteriaValue())) {
                log.warn("Validation failed: criteriaValue is empty for criteriaKey={}", item.criteriaKey());
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.MSG_CRITERIA_VALUE_EMPTY + " for criteriaKey: " + item.criteriaKey());
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return false;
            }
        }
        return true;
    }

    public boolean validateUpdateAuthorization(String userId, String userRootOrgId, String userRoles,
                                                String createdBy, String userGroupOrgId, ApiResponse response) {
        log.debug("validateUpdateAuthorization: userId={}, userRootOrgId={}, userRoles={}, createdBy={}, userGroupOrgId={}",
                userId, userRootOrgId, userRoles, createdBy, userGroupOrgId);

        boolean isCreator = StringUtils.equals(userId, createdBy);
        if (isCreator) {
            log.debug("Authorization: User is creator - allowed");
            return true;
        }

        boolean orgMatches = StringUtils.equals(userRootOrgId, userGroupOrgId);
        if (!orgMatches) {
            log.warn("Authorization failed: User rootOrgId {} does not match userGroup orgId {}",
                    userRootOrgId, userGroupOrgId);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(serverProperties.getUserGroupEditUnauthorizedMsg());
            response.setResponseCode(HttpStatus.FORBIDDEN);
            return false;
        }

        String authorizedRole = serverProperties.getUserGroupUpdateAuthorizedRole();
        List<String> rolesList = StringUtils.isNotBlank(userRoles)
                ? Arrays.asList(userRoles.split(Constants.COMMA))
                : List.of();

        boolean hasAuthorizedRole = rolesList.contains(authorizedRole);
        if (!hasAuthorizedRole) {
            log.warn("Authorization failed: User does not have required role {}. User roles: {}",
                    authorizedRole, userRoles);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(serverProperties.getUserGroupEditMissingRoleMsg());
            response.setResponseCode(HttpStatus.FORBIDDEN);
            return false;
        }

        log.debug("Authorization: User has required role {} and matching orgId - allowed", authorizedRole);
        return true;
    }

    /**
     * Validates rootOrgId criteria following CB Plan V3 validation rules.
     * - Non-CCA organizations: rootOrgId is MANDATORY and must match user's org (unless admin)
     * - CCA organizations: rootOrgId is OPTIONAL, multiple values allowed
     *
     * @param criteria      list of criteria items
     * @param userRootOrgId user's organization ID
     * @param userRoles     user's roles (comma-separated)
     * @param response      API response object
     * @return true if validation passes, false otherwise
     */
    private boolean validateRootOrgIdCriteria(List<CriteriaItem> criteria, String userRootOrgId,
                                               String userRoles, ApiResponse response) {
        log.debug("validateRootOrgIdCriteria: userRootOrgId={}", userRootOrgId);

        boolean isCCA = getCCAFromOrg(userRootOrgId, response);
        if (Constants.FAILED.equals(response.getParams().getStatus())) {
            return false;
        }

        boolean isAdmin = checkIfUserIsAdmin(userRoles);
        Set<String> rootOrgIdsInCriteria = extractRootOrgIds(criteria);

        if (isCCA) {
            return validateRootOrgIdForCCA(rootOrgIdsInCriteria);
        } else if (serverProperties.isUserGroupAllowMultipleRootOrgIds()) {
            return !rootOrgIdsInCriteria.isEmpty();
        } else {
            return validateRootOrgIdForNonCCA(rootOrgIdsInCriteria, userRootOrgId, isAdmin, response);
        }
    }

    /**
     * Checks if the given organization is CCA (Central Competency Authority).
     * Same logic as CbPlanValidationServiceV3Impl.getCCAFromOrg
     *
     * @param orgId    organization ID
     * @param response API response object
     * @return true if CCA, false otherwise
     */
    private boolean getCCAFromOrg(String orgId, ApiResponse response) {
        Map<String, Object> orgMap = userAndOrgService.readOrgFromDB(orgId, null);
        if (MapUtils.isEmpty(orgMap)) {
            log.error("validateRootOrgIdCriteria: Failed to read organization for orgId={}", orgId);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.ERR_FAILED_TO_READ_ORG_DETAILS + orgId);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return false;
        }

        Object isCCAObj = orgMap.get(Constants.IS_CCA);
        boolean isCCA = isCCAObj instanceof Boolean booleanValue && booleanValue;
        log.debug("Organization {} isCCA: {}", orgId, isCCA);
        return isCCA;
    }

    /**
     * Checks if user has admin role.
     *
     * @param userRoles comma-separated user roles
     * @return true if user has admin role, false otherwise
     */
    private boolean checkIfUserIsAdmin(String userRoles) {
        String authorizedRole = serverProperties.getUserGroupUpdateAuthorizedRole();
        if (StringUtils.isBlank(userRoles) || StringUtils.isBlank(authorizedRole)) {
            return false;
        }

        List<String> rolesList = Arrays.asList(userRoles.split(Constants.COMMA));
        return rolesList.contains(authorizedRole);
    }

    /**
     * Extracts rootOrgId values from criteria list.
     * Same logic as RequestValidator.validateContextData (lines 168-172)
     *
     * @param criteria list of criteria items
     * @return set of rootOrgId values found in criteria
     */
    private Set<String> extractRootOrgIds(List<CriteriaItem> criteria) {
        Set<String> rootOrgIds = new HashSet<>();
        for (CriteriaItem item : criteria) {
            if (Constants.ROOT_ORG_ID.equalsIgnoreCase(item.criteriaKey())
                    || Constants.TARGETED_ORGANISATION.equalsIgnoreCase(item.criteriaKey())) {
                rootOrgIds.addAll(item.criteriaValue());
            }
        }
        log.debug("Extracted rootOrgIds from criteria: {}", rootOrgIds);
        return rootOrgIds;
    }

    /**
     * Validates rootOrgId criteria for CCA organizations.
     * CCA organizations can have:
     * - No rootOrgId (applies to all orgs)
     * - Single rootOrgId (applies to one org)
     * - Multiple rootOrgIds (applies to multiple orgs)
     *
     * @param rootOrgIdsInCriteria rootOrgIds found in criteria
     * @return true if validation passes (always true for CCA)
     */
    private boolean validateRootOrgIdForCCA(Set<String> rootOrgIdsInCriteria) {
        log.debug("Validating rootOrgId for CCA org: rootOrgIds count={}", rootOrgIdsInCriteria.size());
        return true;
    }

    /**
     * Validates rootOrgId criteria for non-CCA organizations.
     * Same logic as RequestValidator.validateContextData (lines 201-213)
     *
     * Non-CCA organizations must have:
     * - Exactly ONE rootOrgId in criteria (mandatory)
     * - The rootOrgId must match user's organization (unless user is admin)
     *
     * @param rootOrgIdsInCriteria rootOrgIds found in criteria
     * @param userRootOrgId        user's organization ID
     * @param isAdmin              whether user is admin
     * @param response             API response object
     * @return true if validation passes, false otherwise
     */
    private boolean validateRootOrgIdForNonCCA(Set<String> rootOrgIdsInCriteria, String userRootOrgId,
                                                boolean isAdmin, ApiResponse response) {
        log.debug("Validating rootOrgId for non-CCA org: rootOrgIds count={}, isAdmin={}",
                rootOrgIdsInCriteria.size(), isAdmin);

        if (rootOrgIdsInCriteria.isEmpty()) {
            log.warn("Validation failed: rootOrgId is mandatory in criteria for non-CCA organizations");
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.MSG_ROOTORGID_REQUIRED_NON_CCA);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return false;
        }

        if (rootOrgIdsInCriteria.size() > 1) {
            log.warn("Validation failed: Multiple rootOrgIds found but organization is not CCA");
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.MSG_MULTIPLE_ROOTORGID_NON_CCA);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return false;
        }

        String rootOrgIdInCriteria = rootOrgIdsInCriteria.iterator().next();
        if (!isAdmin && !StringUtils.equalsIgnoreCase(rootOrgIdInCriteria, userRootOrgId)) {
            log.warn("Validation failed: rootOrgId in criteria '{}' does not match user's orgId '{}'",
                    rootOrgIdInCriteria, userRootOrgId);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.MSG_ROOTORGID_MISMATCH);
            response.setResponseCode(HttpStatus.FORBIDDEN);
            return false;
        }

        log.debug("rootOrgId validation passed for non-CCA org");
        return true;
    }

    /**
     * Blocks archiving a user group that is still referenced by any CB Plan. Checked against
     * the {@code contextDataV4} field indexed by CB Plan V4 (see
     * {@code CbPlanElasticSearchServiceV4Impl.convertContextDataForEs}) — a plain ES filter on
     * userGroupId, page size 1, since only existence matters.
     * Fails closed: if the ES check itself cannot be completed, the archive is blocked rather
     * than silently allowed, since this is a data-integrity safeguard, not a best-effort cache.
     *
     * @param userGroupId user group ID
     * @param response    API response, populated with an error when in use or the check fails
     * @return true when the group is free to archive
     */
    public boolean validateUserGroupNotInUse(String userGroupId, ApiResponse response) {
        try {
            SearchCriteria searchCriteria = new SearchCriteria();
            HashMap<String, Object> filter = new HashMap<>();
            filter.put(Constants.CONTEXT_DATA_ES_FIELD_V4, userGroupId);
            searchCriteria.setFilter(filter);
            searchCriteria.setRequestedFields(List.of(Constants.ID));
            searchCriteria.setPageSize(1);
            SearchResult searchResult = esUtilService.searchDocumentsV2(
                    serverProperties.getCpPlanIndex(), searchCriteria, serverProperties.getElasticCbPlanJsonPath());
            if (searchResult == null) {
                log.error("validateUserGroupNotInUse: CB Plan reference search returned null for userGroupId={}", userGroupId);
                return failUsageCheck(response);
            }
            if (searchResult.getTotalCount() > 0) {
                log.warn("validateUserGroupNotInUse: userGroupId={} is referenced by {} CB Plan(s), blocking archive",
                        userGroupId, searchResult.getTotalCount());
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.MSG_USERGROUP_IN_USE);
                response.setResponseCode(HttpStatus.CONFLICT);
                return false;
            }
            return true;
        } catch (Exception e) {
            log.error("validateUserGroupNotInUse: Failed to check CB Plan references for userGroupId={}", userGroupId, e);
            return failUsageCheck(response);
        }
    }

    private boolean failUsageCheck(ApiResponse response) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(Constants.ERR_USERGROUP_USAGE_CHECK_FAILED);
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        return false;
    }
}
