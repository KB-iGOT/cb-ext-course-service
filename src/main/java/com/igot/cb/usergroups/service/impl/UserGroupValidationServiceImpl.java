package com.igot.cb.usergroups.service.impl;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.usergroups.model.CriteriaItem;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

/**
 * Validation service for User Group operations.
 * Follows cbplan/v3 validation patterns - returns boolean and sets errors in ApiResponse.
 */
@Service
public class UserGroupValidationServiceImpl {

    private static final Logger log = LoggerFactory.getLogger(UserGroupValidationServiceImpl.class);

    private final CbExtServerProperties serverProperties;

    public UserGroupValidationServiceImpl(CbExtServerProperties serverProperties) {
        this.serverProperties = serverProperties;
    }

    public boolean validateCreateRequest(String userGroupName, List<CriteriaItem> criteria, ApiResponse response) {
        log.debug("validateCreateRequest: userGroupName={}, criteriaCount={}",
                userGroupName, CollectionUtils.isNotEmpty(criteria) ? criteria.size() : 0);

        if (StringUtils.isBlank(userGroupName)) {
            log.warn("Validation failed: userGroupName is required");
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.MSG_USERGROUPNAME_REQUIRED);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return false;
        }

        return validateCriteria(criteria, response);
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
            response.getParams().setErr(Constants.MSG_USER_NOT_AUTHORIZED);
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
            response.getParams().setErr(Constants.MSG_USER_MISSING_ROLE);
            response.setResponseCode(HttpStatus.FORBIDDEN);
            return false;
        }

        log.debug("Authorization: User has required role {} and matching orgId - allowed", authorizedRole);
        return true;
    }
}
