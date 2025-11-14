package com.igot.cb.service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.exceptions.CustomException;
import org.apache.commons.collections4.MapUtils;
import org.apache.kafka.common.protocol.types.Field;
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
    private final OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @Autowired
    private RedisCacheMgr redisCacheMgr;


    @Value("${content.read.fields}")
    private String contentReadFields;

    @Value("${sb.search.service.host}")
    private String sbSearchServiceHost;

    @Value("${sb.composite.v4.search}")
    private String sbCompositeV4Search;

    @Value("${cb.search.limit:100}")
    private int searchLimit;

    @Value("${cb.search.offset:0}")
    private int searchOffset;

    @Value("${cb.search.accessSettingsEnabled:true}")
    private boolean accessSettingsEnabled;

    private final Map<String, List<String>> courseCategoryCache = new ConcurrentHashMap<>();
    private final Map<String, Long> cacheTimestamps = new ConcurrentHashMap<>();
    private static final long CACHE_TTL_MS = 4 * 60 * 60 * 1000L; // 4 hours

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Constructor for CourseAccessServiceImpl.
     *
     * @param accessTokenValidator Validator for access tokens.
     * @param userProfileServiceImpl Service to fetch user profiles.
     * @param accessSettingRuleCacheMgr Cache manager for access setting rules.
     */
    public CourseAccessServiceImpl(AccessTokenValidator accessTokenValidator,
                                   UserAndOrgServiceImpl userProfileServiceImpl, AccessSettingRuleCacheMgr accessSettingRuleCacheMgr, ContentInfoServiceImpl contentService, OutboundRequestHandlerServiceImpl outboundRequestHandlerService1) {
        this.accessTokenValidator = accessTokenValidator;
        this.userProfileServiceImpl = userProfileServiceImpl;
        this.accessSettingRuleCacheMgr = accessSettingRuleCacheMgr;
        this.contentService = contentService;
        this.outboundRequestHandlerService = outboundRequestHandlerService1;
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

        for (Map<String, Object> userGroup : userGroups) {
            String userGroupId = (String) userGroup.get(Constants.USER_GROUP_ID);
            boolean isUserHasAccess = false;
            List<Map<String, Object>> criteriaList = (List<Map<String, Object>>) userGroup
                    .get(Constants.USER_GROUP_CRITERIA_LIST);
            if (CollectionUtils.isEmpty(criteriaList)) {
                break;
            }
            for (Map<String, Object> criteria : criteriaList) {
                String criteriaKey = criteria.get(Constants.CRITERIA_KEY).toString().toLowerCase();
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

    public ApiResponse getAssignedCoursesForUser(Map<String, Object> request, String authToken) {
        log.info("CourseAccessServiceImpl::getAssignedCoursesForUser:inside");
        ApiResponse response = ApiResponse.createDefaultResponse("api.courseAccess.getCoursesForUser");
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (userId == null) {
                return response;
            }
            // Validate the request payload
            if (MapUtils.isEmpty(request)) {
                String errMsg = "Request body is null or empty";
                log.error(errMsg);
                response.updateErrorDetails(errMsg, HttpStatus.BAD_REQUEST);
                return response;
            }
            String courseCategory = (String) request.get(Constants.COURSE_CATEGORY);
            if (!StringUtils.hasText(courseCategory)) {
                response.updateErrorDetails("Missing course category in request", HttpStatus.BAD_REQUEST);
                return response;
            }
            String redisKey = Constants.ACCESS_KEY + "_" + courseCategory + "_" + userId;
            List<Map<String, Object>> cacheResult = fetchFromRedisCache(redisKey);

            if (cacheResult != null) {
                response.getResult().put(Constants.CONTENT, cacheResult);
                return response;
            }
            List<String> courseIds = getCoursesFromCacheOrService(courseCategory);
            if (CollectionUtils.isEmpty(courseIds)) {
                log.warn("No course identifiers found for category: {}", courseCategory);
                response.getResult().put(Constants.CONTENT, new ArrayList<>());
                return response;
            }
            // Fetch user profile details
            Map<String, Integer> userProfile = userProfileServiceImpl.getUserProfile(userId);
            List<CachedAccessSettingRule> rules = new ArrayList<>();
            for (String courseId : courseIds) {
                CachedAccessSettingRule rule = accessSettingRuleCacheMgr.getOrLoadAccessSettingRule(courseId, courseCategory);
                if (rule != null) {
                    rules.add(rule);
                }
            }
            List<Map<String, Object>> userCourses = new ArrayList<>();
            if (rules.isEmpty()) {
                log.warn("No access setting rules found for course category: {}", courseCategory);
                response.getResult().put(Constants.CONTENT, userCourses);
                return response;
            }
            for (CachedAccessSettingRule rule : rules) {
                Map<String, Object> contextData = rule.getContextData();
                if (contextData == null || !contextData.containsKey(Constants.ACCESS_CONTROL_ID)) {
                    log.warn("No accessControl found in rule: {}", rule.getCacheKey());
                    continue;
                }
                Map<String, Object> accessSettingIdMap =
                        (Map<String, Object>) contextData.get(Constants.ACCESS_CONTROL_ID);

                if (evaluateAccessSettingRule(accessSettingIdMap, userProfile)) {
                    List<String> fieldsToFetch = Arrays.asList(contentReadFields.split(","));
                    Map<String, Object> contentDetails =
                            contentService.readContent(rule.getContextId(), fieldsToFetch);
                    userCourses.add(contentDetails);
                }
            }
            if (!userCourses.isEmpty()) {
                log.info("AccessSettingRule evaluation: UserId: {} | Courses retrieved: {}", userId, userCourses.size());
                redisCacheMgr.putInCache(Constants.ACCESS_KEY+Constants.UNDERSCORE+courseCategory+Constants.UNDERSCORE+userId, mapper.writeValueAsString(userCourses));
                response.getResult().put(Constants.CONTENT, userCourses);
            } else {
                log.info("No matching user courses found for user: {}", userId);
                response.getResult().put(Constants.CONTENT, new ArrayList<>());
            }

        } catch (Exception e) {
            log.error("Error occurred while evaluating access setting rules: {}", e.getMessage(), e);
            response.updateErrorDetails("Rule evaluation failed due to an error", HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    public Map<String, Object> fetchAccessSettingsEnabledCoursesForCategory(String courseCategory) {
        HashMap<String, Object> reqBody = new HashMap<>();
        HashMap<String, Object> req = new HashMap<>();
        Map<String, Object> filters = new HashMap<>();
        filters.put(Constants.COURSE_CATEGORY, courseCategory);
        filters.put(Constants.ACCESS_SETTINGS_ENABLED, accessSettingsEnabled);
        filters.put(Constants.STATUS, Arrays.asList(Constants.LIVE));
        req.put(Constants.FILTERS, filters);
        req.put(Constants.LIMIT, searchLimit);
        req.put(Constants.OFFSET, searchOffset);
        List<String> fields = Collections.singletonList(Constants.IDENTIFIER);
        req.put(Constants.FIELDS, fields);
        reqBody.put(Constants.REQUEST, req);

        Map<String, Object> compositeSearchRes = outboundRequestHandlerService.fetchResultUsingPost(
                sbSearchServiceHost + sbCompositeV4Search, reqBody,
                null);

        return compositeSearchRes;
    }

    private List<String> getCoursesFromCacheOrService(String courseCategory) {
        try {
            List<String> cachedCourses = courseCategoryCache.get(courseCategory);
            Long lastUpdated = cacheTimestamps.get(courseCategory);
            boolean isCacheValid = lastUpdated != null &&
                    (System.currentTimeMillis() - lastUpdated) < CACHE_TTL_MS;

            if (isCacheValid && cachedCourses != null) {
                log.info("Cache hit for category: {}", courseCategory);
                return cachedCourses;
            }

            log.info("Cache miss or expired for category: {}, fetching from service", courseCategory);
            Map<String, Object> fetchedCourses = fetchAccessSettingsEnabledCoursesForCategory(courseCategory);

            if (MapUtils.isNotEmpty(fetchedCourses)) {
                List<String> identifiers = new ArrayList<>();
                try {
                    Map<String, Object> result = (Map<String, Object>) fetchedCourses.get(Constants.RESULT);
                    if (result != null && result.containsKey(Constants.CONTENT)) {
                        List<Map<String, Object>> contentList = (List<Map<String, Object>>) result.get(Constants.CONTENT);
                        if (contentList != null) {
                            identifiers = contentList.stream()
                                    .map(item -> (String) item.get(Constants.IDENTIFIER))
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toList());
                        }
                    }
                } catch (Exception e) {
                    log.error("Error extracting identifiers for category {}: {}", courseCategory, e.getMessage(), e);
                }
                if (!identifiers.isEmpty()) {
                    courseCategoryCache.put("access_settings_enabled_"+courseCategory, identifiers);
                    cacheTimestamps.put(courseCategory, System.currentTimeMillis());
                    log.info("Cached {} course identifiers for category {}", identifiers.size(), courseCategory);
                    return identifiers;
                } else {
                    log.warn("No course identifiers found for category {}", courseCategory);
                }
            }

        } catch (Exception e) {
            log.error("Error while fetching or caching courses for category {}: {}", courseCategory, e.getMessage(), e);
            return Collections.emptyList();
        }
        return Collections.emptyList();
    }

    private List<Map<String, Object>> fetchFromRedisCache(String redisKey) {
        String cachedData = redisCacheMgr.getFromCache(redisKey);
        if (!StringUtils.hasText(cachedData)) {
            return null;
        }
        try {
            return mapper.readValue(
                    cachedData,
                    new TypeReference<List<Map<String, Object>>>() {}
            );
        } catch (JsonProcessingException e) {
            log.error("Failed parsing cached redis data for key {}: {}", redisKey, e.getMessage());
            return null;
        }
    }


}
