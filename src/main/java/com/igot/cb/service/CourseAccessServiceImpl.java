package com.igot.cb.service;

import java.util.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisCacheMgr;
import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.igot.cb.cache.AccessSettingRuleCacheMgr;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.model.CachedAccessSettingRule;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;

import lombok.extern.slf4j.Slf4j;

/**
 * Service implementation for managing course access based on user profiles and access setting rules.
 */
@Service
@Slf4j
public class CourseAccessServiceImpl {
    private final AccessTokenValidator accessTokenValidator;
    private final UserAndOrgServiceImpl userProfileServiceImpl;
    private final AccessSettingRuleCacheMgr accessSettingRuleCacheMgr;
    private final ContentInfoServiceImpl contentService;

    @Autowired
    private RedisCacheMgr redisCacheMgr;


    @Value("${content.read.fields}")
    private String contentReadFields;

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Constructor for CourseAccessServiceImpl.
     *
     * @param accessTokenValidator Validator for access tokens.
     * @param userProfileServiceImpl Service to fetch user profiles.
     * @param accessSettingRuleCacheMgr Cache manager for access setting rules.
     */
    public CourseAccessServiceImpl(AccessTokenValidator accessTokenValidator,
            UserAndOrgServiceImpl userProfileServiceImpl, AccessSettingRuleCacheMgr accessSettingRuleCacheMgr, ContentInfoServiceImpl contentService) {
        this.accessTokenValidator = accessTokenValidator;
        this.userProfileServiceImpl = userProfileServiceImpl;
        this.accessSettingRuleCacheMgr = accessSettingRuleCacheMgr;
        this.contentService = contentService;
    }

    /**
     * Retrieves courses accessible to a user based on their profile and access setting rules.
     *
     * @param request   The request containing user details.
     * @param authToken The authentication token for the user.
     * @return ApiResponse containing the list of accessible courses or error details.
     */
    public ApiResponse getCoursesForUser(Map<String, Object> request, String authToken) {
        log.info("CourseAccessServiceImpl::getCoursesForUser:inside");
        ApiResponse response = ApiResponse.createDefaultResponse("api/courseAccess/getCoursesForUser");

        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
        if (!StringUtils.hasText(userId)) {
            String errMsg = "Invalid or missing authentication token";
            log.error(errMsg);
            response.updateErrorDetails(errMsg, HttpStatus.UNAUTHORIZED);
            return response;
        }

        // Validate the request payload
        if (MapUtils.isEmpty(request)) {
            String errMsg = "Request body is null or empty";
            log.error(errMsg);
            response.updateErrorDetails(errMsg, HttpStatus.BAD_REQUEST);
            return response;
        }

        String cachedCourseForUser = redisCacheMgr.getFromCache(Constants.ACCESS_KEY + userId);
        if (cachedCourseForUser != null && !cachedCourseForUser.isEmpty()){
            if (cachedCourseForUser.equalsIgnoreCase(Constants.NO_RECORDS_FOUND)){
                response.getResult().put(Constants.CONTENT, new ArrayList<>());
                return response;
            }
            try {
                response.getResult().put(Constants.CONTENT, mapper.readValue(
                        cachedCourseForUser,
                        new TypeReference<List<Map<String, Object>>>() {}
                ));
                log.info("AccessSettingRule evalution: UserId: ", userId);
                return response;
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        // Fetch user profile details
        Map<String, Integer> userProfile = userProfileServiceImpl.getUserProfile(userId);
        // Evaluate the user profile value against all the courses in access settings
        // rule table.
        try {
            List<Map<String, Object>> userCourses = new ArrayList<>();
            if (retrieveUserCourses(userProfile, userCourses)) {
                log.info("AccessSettingRule evalution: UserId: {} courses retrieved: {}", userId, userCourses.size());
                if (!userCourses.isEmpty()) {
                    log.info("No courses found for user profile: {}", userProfile);
                    try {
                        redisCacheMgr.putInCache(Constants.ACCESS_KEY+userId, mapper.writeValueAsString(userCourses));
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }else {
                    redisCacheMgr.putInCache(Constants.ACCESS_KEY+userId, Constants.NO_RECORDS_FOUND);
                }
                response.getResult().put(Constants.CONTENT, userCourses);
            } else {
                response.getResult().put(Constants.CONTENT, new ArrayList<>());
            }
        } catch (Exception e) {
            log.error("Error occurred while migrating access setting rules: {}", e.getMessage(), e);
            response.updateErrorDetails("Rule evalution failed due to an error", HttpStatus.INTERNAL_SERVER_ERROR);
        }
        // If user has access to the course then return that list.
        return response;
    }

    @SuppressWarnings("unchecked")
    private boolean retrieveUserCourses(Map<String, Integer> userProfile, List<Map<String, Object>> userCourses) {
        Collection<CachedAccessSettingRule> cachedAccessSettingRules = accessSettingRuleCacheMgr
                .getAccessSettingRules();
        if (cachedAccessSettingRules.isEmpty()) {
            log.error("No access setting rules found in cache");
            return false;
        }

        for (CachedAccessSettingRule rule : cachedAccessSettingRules) {
            Map<String, Object> accessSettingIdMap = (Map<String, Object>) rule.getContextData()
                    .get(Constants.ACCESS_CONTROL_ID);
            if (evaluateAccessSettingRule(accessSettingIdMap, userProfile)) {
                List<String> fieldsToFetch = Arrays.asList(contentReadFields.split(","));
                Map<String, Object> contentDetails = contentService.readContent(rule.getContextId(), fieldsToFetch);
                userCourses.add(contentDetails);
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private boolean evaluateAccessSettingRule(Map<String, Object> accessSettingIdMap,
            Map<String, Integer> userProfile) {
        if (MapUtils.isEmpty(accessSettingIdMap) || MapUtils.isEmpty(userProfile)) {
            log.error("Access setting ID map or user profile is empty");
            return false;
        }
        List<Map<String, Object>> userGroups = (List<Map<String, Object>>) accessSettingIdMap
                .get(Constants.USER_GROUPS);
        if (CollectionUtils.isEmpty(userGroups)) {
            return false;
        }

        // There could be multiple user groups in the access setting rule.
        // User needs to have access to at least one of the user groups to access the
        // course.
        // Need to iterate through all the user groups to check if the user has access
        // to any of them.
        for (Map<String, Object> userGroup : userGroups) {
            String userGroupId = (String) userGroup.get(Constants.USER_GROUP_ID);
            boolean isUserHasAccess = false;
            List<Map<String, Object>> criteriaList = (List<Map<String, Object>>) userGroup
                    .get(Constants.USER_GROUP_CRITERIA_LIST);
            if (CollectionUtils.isEmpty(criteriaList)) {
                break;
            }
            for (Map<String, Object> criteria : criteriaList) {
                String criteriaKey = (String) criteria.get(Constants.CRITERIA_KEY);
                BitSet criteriaValue = (BitSet) criteria.get(Constants.CRITERIA_VALUE);
                Integer userCriteriaValue = userProfile.get(criteriaKey);
                if (userCriteriaValue == null || !criteriaValue.get(userCriteriaValue)) {
                    log.info("User profile does not contain criteria key: {} in userGroup: {}", criteriaKey,
                            userGroupId);
                    isUserHasAccess = false;
                    break;
                } else {
                    isUserHasAccess = true;
                }
            }

            if (isUserHasAccess) {
                log.info("User profile does matches all criteria in userGroup: {}", userGroupId);
                return true;
            }
        }
        return false;
    }
}
