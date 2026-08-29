package com.igot.cb.service;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.roaringbitmap.RoaringBitmap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
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

import static com.igot.cb.util.Constants.*;

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
    private final ObjectMapper objectMapper;
    private final CbPlanLearnerServiceImpl cbPlanLearnerService;
    private final CassandraOperation cassandraOperation;

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

    @Value("${cb.search.access.settings.enabled:true}")
    private boolean accessSettingsEnabled;

    @Value("${cb.cache.course.ttl:600000}")
    private long cacheTtlMs;

    @Value("${cios.integration.search.host}")
    private String ciosSearchServiceHost;

    @Value("${cios.integration.search}")
    private String ciosSearch;

    @Value("${cios.search.limit:100}")
    private int ciosSearchLimit;

    @Value("${cios.search.offset:0}")
    private int ciosSearchOffset;

    @Value("${access.course.cache.ttl.seconds:600}")
    private Integer accessCacheTtlSecods;

    @Value("${moderated.course.search.request}")
    private String moderatedCourseSearchRequest;

    @Value("${enrolment.dictionary.url}")
    private String enrolmentDictionaryUrl;

    @Value("${lms.host}")
    private String lmsServiceHost;

    @Value("${standalone.assessment.search.request}")
    private String standaloneAssessmentSearchRequest;

    @Value("${lms.enrollment.details.url}")
    private String enrollmentDetailsUrl;

    private final Map<String, List<String>> courseCategoryCache = new ConcurrentHashMap<>();
    private final Map<String, Long> cacheTimestamps = new ConcurrentHashMap<>();

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Constructor for CourseAccessServiceImpl.
     *
     * @param accessTokenValidator Validator for access tokens.
     * @param userProfileServiceImpl Service to fetch user profiles.
     * @param accessSettingRuleCacheMgr Cache manager for access setting rules.
     */
    public CourseAccessServiceImpl(AccessTokenValidator accessTokenValidator,
                                   UserAndOrgServiceImpl userProfileServiceImpl, AccessSettingRuleCacheMgr accessSettingRuleCacheMgr, ContentInfoServiceImpl contentService, OutboundRequestHandlerServiceImpl outboundRequestHandlerService1, CbPlanLearnerServiceImpl cbPlanLearnerService, CassandraOperation cassandraOperation) {
        this.accessTokenValidator = accessTokenValidator;
        this.userProfileServiceImpl = userProfileServiceImpl;
        this.accessSettingRuleCacheMgr = accessSettingRuleCacheMgr;
        this.contentService = contentService;
        this.outboundRequestHandlerService = outboundRequestHandlerService1;
        this.objectMapper = new ObjectMapper();
        this.cbPlanLearnerService = cbPlanLearnerService;
        this.cassandraOperation = cassandraOperation;
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
                RoaringBitmap criteriaValue = (RoaringBitmap) criteria.get(Constants.CRITERIA_VALUE);
                Integer userCriteriaValue = userProfile.get(criteriaKey);
                if (userCriteriaValue == null || !criteriaValue.contains(userCriteriaValue)) {
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
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
        if (userId == null) {
            return response;
        }
        return getAssignedCoursesForUserByAdmin(userId, request, authToken);
    }

    public ApiResponse getAssignedCoursesForUserByAdmin(String userId, Map<String, Object> request, String authToken) {
        log.info("CourseAccessServiceImpl::getAssignedCoursesForUserByAdmin:inside");
        ApiResponse response = ApiResponse.createDefaultResponse("api.courseAccess.getCoursesForUser");
        try {
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

            if (!CollectionUtils.isEmpty(cacheResult)) {
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
                    // --- Begin custom logic for courseUnits ---
                    if (contentDetails != null &&
                            Constants.COURSE_CATEGORY_COMPREHENSIVE_ASSESSMENT_PROGRAM.equals(contentDetails.get(Constants.COURSE_CATEGORY)) &&
                            contentDetails.get(Constants.CHILD_NODES) instanceof List &&
                            contentDetails.get(Constants.LEAF_NODES) instanceof List) {
                        List<String> childNodes = (List<String>) contentDetails.get(Constants.CHILD_NODES);
                        List<String> leafNodes = (List<String>) contentDetails.get(Constants.LEAF_NODES);
                        Set<String> leafSet = new HashSet<>(leafNodes);
                        List<String> courseUnits = childNodes.stream()
                                .filter(child -> !leafSet.contains(child))
                                .collect(Collectors.toList());
                        contentDetails.put(Constants.COURSE_UNITS, courseUnits);
                        contentDetails.put(Constants.END_DATE_CAMEL, (String) contentDetails.get(Constants.END_DATE_CAMEL));
}
                    // --- End custom logic for courseUnits ---
                    userCourses.add(contentDetails);
                }
            }
            log.info("AccessSettingRule evaluation: UserId: {} | Courses retrieved: {}", userId, userCourses.size());
            redisCacheMgr.putInCache(redisKey, mapper.writeValueAsString(userCourses));
            response.getResult().put(Constants.CONTENT, userCourses);
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
        req.put(Constants.FIELDS, Collections.singletonList(Constants.IDENTIFIER));
        reqBody.put(Constants.REQUEST, req);
        Map<String, Object> compositeSearchRes = outboundRequestHandlerService.fetchResultUsingPost(
                sbSearchServiceHost + sbCompositeV4Search, reqBody, null);
        if (MapUtils.isEmpty(compositeSearchRes)) {
            return compositeSearchRes;
        }
        Map<String, Object> result = (Map<String, Object>) compositeSearchRes.get(Constants.RESULT);
        if (result == null || !result.containsKey(Constants.COUNT)) {
            return compositeSearchRes;
        }
        int totalCount = ((Number) result.get(Constants.COUNT)).intValue();
        if (totalCount <= searchLimit) {
            return compositeSearchRes;
        }
        log.info("Total count {} exceeds search limit {}. Fetching remaining pages...", totalCount, searchLimit);
        List<Map<String, Object>> allContent = new ArrayList<>(totalCount);
        List<Map<String, Object>> initialContent = (List<Map<String, Object>>) result.get(Constants.CONTENT);
        if (initialContent != null) {
            allContent.addAll(initialContent);
        }
        int currentOffset = searchLimit;
        while (currentOffset < totalCount) {
            req.put(Constants.OFFSET, currentOffset);
            Map<String, Object> nextPageRes = outboundRequestHandlerService.fetchResultUsingPost(
                    sbSearchServiceHost + sbCompositeV4Search, reqBody, null);

            if (MapUtils.isNotEmpty(nextPageRes)) {
                Map<String, Object> nextResult = (Map<String, Object>) nextPageRes.get(Constants.RESULT);
                if (nextResult != null) {
                    List<Map<String, Object>> nextContent = (List<Map<String, Object>>) nextResult.get(Constants.CONTENT);
                    if (nextContent != null && !nextContent.isEmpty()) {
                        allContent.addAll(nextContent);
                    } else {
                        break;
                    }
                }
            }
            currentOffset += searchLimit;
        }
        result.put(Constants.CONTENT, allContent);
        log.info("Successfully fetched all {} items across multiple pages", allContent.size());
        return compositeSearchRes;
    }

    private List<String> getCoursesFromCacheOrService(String courseCategory) {
        try {
            String redisKey = "access_settings_enabled_" + courseCategory;
            String cachedData = redisCacheMgr.getFromCache(redisKey);
            if (StringUtils.hasText(cachedData)) {
                try {
                    List<String> cachedCourses = mapper.readValue(cachedData, new TypeReference<List<String>>() {});
                    log.info("Redis cache hit for category: {}", courseCategory);
                    return cachedCourses;
                } catch (Exception e) {
                    log.error("Failed to parse cached course list from Redis for category {}: {}", courseCategory, e.getMessage(), e);
                }
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
                    redisCacheMgr.putInCache(redisKey, mapper.writeValueAsString(identifiers), accessCacheTtlSecods);
                    log.info("Cached {} course identifiers for category {} in Redis", identifiers.size(), courseCategory);
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
        log.info("Fetching data from Redis cache for key: {}", redisKey);
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

    public ApiResponse getAssignedExternalCoursesForUser(Map<String,Object> request,String authToken) {
        log.info("CourseAccessServiceImpl::getAssignedCoursesForUser:inside");
        ApiResponse response = ApiResponse.createDefaultResponse("api.courseAccess.getCoursesForUser");
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (!StringUtils.hasText(userId)) {
                return response;
            }
            if (MapUtils.isEmpty(request)) {
                String errMsg = "Request body is null or empty";
                log.error(errMsg);
                response.updateErrorDetails(errMsg, HttpStatus.BAD_REQUEST);
                return response;
            }
            String partnerId = (String) request.get(Constants.PARTNER_ID);
            if (!StringUtils.hasText(partnerId)) {
                response.updateErrorDetails("Missing partnerId in request", HttpStatus.BAD_REQUEST);
                return response;
            }
            String redisKey = Constants.ACCESS_KEY + partnerId + "_" + userId;
            List<Map<String, Object>> cacheResult = fetchFromRedisCache(redisKey);

            if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(cacheResult)) {
                response.getResult().put(Constants.CONTENT, cacheResult);
                return response;
            }
            List<String> courseIds = getCoursesFromCacheOrServiceForExternalCourse(partnerId);
            if (CollectionUtils.isEmpty(courseIds)) {
                log.warn("No course identifiers found for externalCourses");
                response.getResult().put(Constants.CONTENT, new ArrayList<>());
                return response;
            }

            Map<String, Integer> userProfile = userProfileServiceImpl.getUserProfile(userId);
            List<CachedAccessSettingRule> rules = new ArrayList<>();
            for (String courseId : courseIds) {
                CachedAccessSettingRule rule = accessSettingRuleCacheMgr.getOrLoadAccessSettingRule(courseId, Constants.EXTERNAL_COURSES);
                if (rule != null) {
                    rules.add(rule);
                }
            }
            List<Map<String, Object>> userCourses = new ArrayList<>();
            if (rules.isEmpty()) {
                log.warn("No access setting rules found for External Courses");
                response.getResult().put(Constants.CONTENT, userCourses);
                return response;
            }
            for (CachedAccessSettingRule rule : rules) {
                Map<String, Object> contextData = rule.getContextData();
                if (MapUtils.isNotEmpty(contextData) || contextData.containsKey(Constants.ACCESS_CONTROL_ID)) {
                    Map<String, Object> accessSettingIdMap =
                            (Map<String, Object>) contextData.get(Constants.ACCESS_CONTROL_ID);

                    if (evaluateAccessSettingRule(accessSettingIdMap, userProfile)) {
                        List<String> fieldsToFetch = new ArrayList<>();
                        Map<String, Object> contentDetails =
                                contentService.readContent(rule.getContextId(), fieldsToFetch);
                        Object contentObj = contentDetails.get("content");
                        Map<String, Object> externalCourse = mapper.convertValue(
                                contentObj,
                                new TypeReference<Map<String, Object>>() {
                                }
                        );
                        userCourses.add(externalCourse);
                    }
                }
            }
            log.info("AccessSettingRule evaluation: UserId: {} | Courses retrieved: {}", userId, userCourses.size());
            redisCacheMgr.putInCache(Constants.ACCESS_KEY+partnerId+Constants.UNDERSCORE+userId, mapper.writeValueAsString(userCourses));
            response.getResult().put(Constants.CONTENT, userCourses);
        } catch (Exception e) {
            log.error("Error occurred while evaluating access setting rules: {}", e.getMessage(), e);
            response.updateErrorDetails("Rule evaluation failed due to an error", HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private List<String> getCoursesFromCacheOrServiceForExternalCourse(String partnerId) {
        try {
            String cacheKey = "access_settings_enabled_" + partnerId;
            List<String> cachedCourses = courseCategoryCache.get(cacheKey);
            Long lastUpdated = cacheTimestamps.get(cacheKey);
            boolean isCacheValid = lastUpdated != null &&
                    (System.currentTimeMillis() - lastUpdated) < cacheTtlMs;

            if (isCacheValid && org.apache.commons.collections4.CollectionUtils.isNotEmpty(cachedCourses)) {
                log.info("Cache hit for category: {}", partnerId);
                return cachedCourses;
            }

            log.info("Cache miss or expired for category: {}, fetching from service", partnerId);
            List<String> fetchedCourses = fetchAccessSettingsEnabledCoursesForExternalCourses(partnerId);
            if (!fetchedCourses.isEmpty()) {
                courseCategoryCache.put(cacheKey, fetchedCourses);
                cacheTimestamps.put(cacheKey, System.currentTimeMillis());
                log.info("Cached {} course identifiers for partnerId {}", fetchedCourses.size(), partnerId);
                return fetchedCourses;
            } else {
                log.warn("No course identifiers found for category {}", partnerId);
            }
        } catch (Exception e) {
            log.error("Error while fetching or caching courses for category {}: {}", partnerId, e.getMessage(), e);
            return Collections.emptyList();
        }
        return Collections.emptyList();
    }

    public List<String> fetchAccessSettingsEnabledCoursesForExternalCourses(String partnerId) {
        HashMap<String, Object> req = new HashMap<>();
        Map<String, Object> filters = new HashMap<>();
        filters.put(Constants.ACCESS_SETTINGS_ENABLED, accessSettingsEnabled);
        filters.put(Constants.STATUS, Constants.LIVE_KEY);
        filters.put(Constants.PARTNER_ID, partnerId);
        req.put(Constants.FILTER_CRITERIA_MAP, filters);
        req.put(Constants.PAGE_SIZE, ciosSearchLimit);
        req.put(Constants.PAGE_NUMBER, ciosSearchOffset);
        List<String> fields = Collections.singletonList(Constants.CONTENT_ID);
        req.put(Constants.REQUESTED_FIELDS, fields);

        Map<String, Object> ciosSearchRes = outboundRequestHandlerService.fetchResultUsingPost(
                ciosSearchServiceHost + ciosSearch, req,
                null);
        JsonNode ciosContentNode = mapper.convertValue(ciosSearchRes, JsonNode.class);
        JsonNode dataArray = ciosContentNode.path(Constants.DATA);
        List<String> contentIds = new ArrayList<>();

        if (dataArray.isArray()) {
            for (JsonNode node : dataArray) {
                if (node.has(Constants.CONTENT_ID)) {
                    contentIds.add(node.get(Constants.CONTENT_ID).asText());
                }
            }
        }
        return contentIds;
    }

    public ApiResponse getPersonalContentInfo(String authToken) {
        ApiResponse response = ApiResponse.createDefaultResponse(Constants.API_PERSONAL_CONTENT_INFO);
        try {
            Map<String, Object> tokenData = accessTokenValidator.fetchUserIdAndOrg(authToken);
            String userId = (String) tokenData.get("userId");
            String orgId = (String) tokenData.get("org");
            if (!StringUtils.hasText(userId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.UNAUTHORIZED);
                response.getParams().setErrMsg("Invalid auth token");
                return response;
            }
            Map<String, Object> result = new HashMap<>();
            result.putAll(org.apache.commons.lang3.StringUtils.isNotBlank(userId) ? getPersonalContentInfoFromCacheOrApi(userId, orgId, authToken) : Collections.emptyMap());
            response.setResult(result);
            return response;
        } catch (Exception e) {
            log.error("Error fetching personal content info: {}", e.getMessage(), e);
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setErrMsg("Failed to fetch personal content info: " + e.getMessage());
            return response;
        }
    }

    private Map<String, Object> getPersonalContentInfoFromCacheOrApi(String userId, String orgId, String authToken) throws Exception {
        String redisKey = Constants.PERSONAL_CONTENT_INFO_REDIS_KEY_PREFIX + userId;
        String cached = redisCacheMgr.getFromCache(redisKey);
        if (StringUtils.hasText(cached)) {
            log.info("personalContentInfo cache HIT for userId: {}", userId);
            return objectMapper.readValue(cached, new TypeReference<Map<String, Object>>() {});
        }
        log.info("personalContentInfo cache MISS for userId: {}", userId);
        Map<String, Object> contentInfoMap = buildPersonalContentInfo(userId, orgId, authToken);
        redisCacheMgr.putInCache(redisKey, objectMapper.writeValueAsString(contentInfoMap));
        log.info("personalContentInfo cached for userId: {}", userId);
        return contentInfoMap;
    }

        private Map<String, Object> buildPersonalContentInfo(String userId, String orgId, String authToken) throws Exception {
            List<String> aparIds = new ArrayList<>();;
            List<String> trainingPlanIds = new ArrayList<>();
            List<String> aiCbpIds = new ArrayList<>();
            ApiResponse cbPlanResponse = cbPlanLearnerService.getCBPlanListForUser(orgId, userId, true);
            if (cbPlanResponse != null
                    && cbPlanResponse.getResult() != null
                    && cbPlanResponse.getResult().containsKey(Constants.CONTENT)) {
                List<Map<String, Object>> plans = (List<Map<String, Object>>)
                        cbPlanResponse.getResult().get(Constants.CONTENT);
                if (!CollectionUtils.isEmpty(plans)) {
                     aparIds = plans.stream()
                            .filter(p -> Boolean.TRUE.equals(p.get(Constants.IS_APAR)))
                            .flatMap(p -> ((List<Map<String, Object>>) p.get(CONTENT_LIST)).stream())
                            .map(content -> (String) content.get(Constants.IDENTIFIER))
                            .collect(Collectors.toList());
                     aiCbpIds = plans.stream()
                            .filter(p -> !Boolean.TRUE.equals(p.get(Constants.IS_APAR)))
                            .filter(p -> Constants.PLAN_TYPE_AI_CBP.equalsIgnoreCase(String.valueOf(p.get(Constants.PLAN_TYPE))))
                            .flatMap(p -> ((List<Map<String, Object>>) p.get(CONTENT_LIST)).stream())
                            .map(content -> (String) content.get(Constants.IDENTIFIER))
                            .collect(Collectors.toList());
                     trainingPlanIds = plans.stream()
                            .filter(p -> !Boolean.TRUE.equals(p.get(Constants.IS_APAR)))
                            .filter(p -> !Constants.PLAN_TYPE_AI_CBP.equalsIgnoreCase(String.valueOf(p.get(Constants.PLAN_TYPE))))
                            .flatMap(p -> ((List<Map<String, Object>>) p.get(CONTENT_LIST)).stream())
                            .map(content -> (String) content.get(Constants.IDENTIFIER))
                            .collect(Collectors.toList());
                }
            }
            java.util.List<String> learningPathwayIds = getAssignedCourseCount(userId, Constants.LEARNING_PATHWAY, authToken);
            Map<String, Map<String, Object>> enrolmentDictionary = callEnrolmentDictionaryApi(authToken);
            List<String> caProgramIds = getFilteredCaProgramIdentifiers(userId, authToken, enrolmentDictionary);
            int caProgramCount = caProgramIds.size();
            List<String> standaloneAssessmentIds = getStandaloneAssessmentIdentifiersFromSystem();
            Map<String, Map<String, Object>> enrollmentDetails = callAssessmentEnrollmentDetailsApi(
                            authToken,
                            standaloneAssessmentIds);
            List<String> standaloneIds = filterStandaloneAssessmentIdentifiers(
                            standaloneAssessmentIds,
                            enrollmentDetails);
            Map<String, Object> map = new HashMap<>();
            map.put(Constants.TRAINING_PLAN, trainingPlanIds.size());
            map.put(Constants.APAR, aparIds.size());
            map.put(Constants.AI_CBP, aiCbpIds.size());
            map.put(CA_PROGRAM, caProgramCount);
            map.put(LEARNING_PATHWAY_FIELD, learningPathwayIds.size());
            map.put(STANDALONE_ASSESSMENT, standaloneIds.size());

            Map<String, Object> contentIds = new HashMap<>();
            contentIds.put(Constants.TRAINING_PLAN, trainingPlanIds);
            contentIds.put(Constants.APAR, aparIds);
            contentIds.put(Constants.AI_CBP, aiCbpIds);
            contentIds.put(LEARNING_PATHWAY_FIELD, learningPathwayIds);
            contentIds.put(STANDALONE_ASSESSMENT, standaloneIds);
            contentIds.put(CA_PROGRAM, caProgramIds);
            Map<String, Object> moderatedContent =
                    getModeratedContentIdentifiers(userId, orgId);

            List<String> moderatedContentIds =
                    (List<String>) moderatedContent.get("identifiers");

            Object moderatedContentCount =
                    moderatedContent.get("count");

            map.put(MODERATED_CONTENT, moderatedContentCount);
            contentIds.put(MODERATED_CONTENT, moderatedContentIds);
            map.put(CONTENT_IDS, contentIds);


            return map;
        }

    private Map<String, Object> getModeratedContentIdentifiers(
            String userId, String orgId) throws Exception {

        String redisKey =
                Constants.MODERATED_COURSE_COUNT_REDIS_KEY_PREFIX + userId;

        String cached = redisCacheMgr.getFromCache(redisKey);

        Map<String, Object> moderatedMap = new HashMap<>();

        if (StringUtils.hasText(cached)) {

            moderatedMap = objectMapper.readValue(
                    cached,
                    new TypeReference<Map<String, Object>>() {});

            if (moderatedMap.containsKey(orgId)
                    && MapUtils.isNotEmpty(moderatedMap)) {

                log.info(
                        "moderatedContentIdentifiers cache HIT for userId: {} orgId: {}",
                        userId, orgId);

                return (Map<String, Object>) moderatedMap.get(orgId);
            }

            log.info(
                    "orgId not in map for userId: {}, calling search API",
                    userId);

        } else {
            log.info(
                    "moderatedContentIdentifiers cache MISS for userId: {}",
                    userId);
        }

        Map<String, Object> userProfileDetails =
                userProfileServiceImpl.readUserProfile(userId, null);

        Map<String, Object> moderatedContent =
                getModeratedCourseIdentifiers(
                        orgId,
                        userProfileDetails);
        moderatedMap.put(orgId, moderatedContent);

        redisCacheMgr.putInCache(
                redisKey,
                objectMapper.writeValueAsString(moderatedMap));

        log.info(
                "moderatedContentIdentifiers updated in Redis for userId: {} orgId: {}",
                userId, orgId);

        return moderatedContent;
    }

    private List<String> getAssignedCourseCount(String userId, String courseCategory, String authToken) {
        try {
            Map<String, Object> request = new HashMap<>();
            request.put(Constants.COURSE_CATEGORY, courseCategory);
            ApiResponse response = getAssignedCoursesForUserByAdmin(userId, request, authToken);
            if (response != null && response.getResult() != null) {
                List<Map<String, Object>> courses = (List<Map<String, Object>>)
                        response.getResult().get(Constants.CONTENT);
                return courses.stream()
                        .map(course -> (String) course.get(Constants.IDENTIFIER))
                        .collect(Collectors.toList());
            }
        } catch (Exception e) {
            log.error("Error fetching count for courseCategory: {}, userId: {}, error: {}",
                    courseCategory, userId, e.getMessage());
        }
        return Collections.EMPTY_LIST;
    }

    private Map<String, Object> getModeratedCourseIdentifiers(
            String orgId,
            Map<String, Object> userProfileDetails) {

        try {
            String requestBody =
                    String.format(moderatedCourseSearchRequest, orgId);

            Map<String, Object> requestMap =
                    objectMapper.readValue(
                            requestBody,
                            new TypeReference<Map<String, Object>>() {});

            Map<String, Object> request =
                    (Map<String, Object>) requestMap.get("request");

            Map<String, Object> filters =
                    (Map<String, Object>) request.get("filters");

            Object profileDetailsObject =
                    userProfileDetails.get("profiledetails");

            Map<String, Object> profileDetails = null;

            if (profileDetailsObject instanceof String
                    && StringUtils.hasText((String) profileDetailsObject)) {

                profileDetails = objectMapper.readValue(
                        (String) profileDetailsObject,
                        new TypeReference<Map<String, Object>>() {});
            }

            String profileStatus =
                    profileDetails != null
                            ? (String) profileDetails.get("profileStatus")
                            : null;

            if (profileStatus == null
                    || profileStatus.isEmpty()
                    || !"VERIFIED".equalsIgnoreCase(profileStatus)) {

                filters.put(
                        "secureSettings.isVerifiedKarmayogi",
                        "No");
            }

            String searchUrl =
                    sbSearchServiceHost + sbCompositeV4Search;

            Map<String, Object> searchResponse =
                    outboundRequestHandlerService.fetchResultUsingPost(
                            searchUrl,
                            requestMap,
                            null);

            Map<String, Object> response = new HashMap<>();

            if (MapUtils.isNotEmpty(searchResponse)) {

                Map<String, Object> result =
                        (Map<String, Object>) searchResponse.get(
                                Constants.RESULT);

                if (result != null
                        && result.containsKey(Constants.CONTENT)) {

                    List<Map<String, Object>> contents =
                            (List<Map<String, Object>>)
                                    result.get(Constants.CONTENT);

                    List<String> identifiers = contents.stream()
                            .map(content ->
                                    (String) content.get(Constants.IDENTIFIER))
                            .collect(Collectors.toList());

                    response.put("identifiers", identifiers);
                    response.put("count", result.get("count"));

                    return response;
                }
            }

            response.put("identifiers", Collections.emptyList());
            response.put("count", 0);

            return response;

        } catch (Exception e) {
            log.error(
                    "Error fetching moderated course identifiers for orgId: {}",
                    orgId, e);

            Map<String, Object> response = new HashMap<>();
            response.put("identifiers", Collections.emptyList());
            response.put("count", 0);

            return response;
        }
    }

    private Map<String, Map<String, Object>> callEnrolmentDictionaryApi(
            String userToken) {

        Map<String, String> headers = new HashMap<>();
        headers.put(X_AUTH_TOKEN, userToken);

        Map<String, Object> apiResponse =
                outboundRequestHandlerService.fetchResultUsingGet(
                        lmsServiceHost + enrolmentDictionaryUrl,
                        headers);

        if (MapUtils.isEmpty(apiResponse)) {
            return Collections.emptyMap();
        }

        Map<String, Object> result =
                (Map<String, Object>) apiResponse.get(Constants.RESULT);

        if (result == null || result.get(Constants.RESPONSE) == null) {
            return Collections.emptyMap();
        }

        return (Map<String, Map<String, Object>>) result.get(Constants.RESPONSE);
    }

    private boolean isActiveInProgress(Map<String, Object> enrolment) {
        Number status = (Number) enrolment.get(Constants.STATUS);

        return Boolean.TRUE.equals(enrolment.get(Constants.ACTIVE))
                && status != null
                && (status.intValue() == 0 || status.intValue() == 1);
    }

    private List<String> getStandaloneAssessmentIdentifiers(
            Map<String, Map<String, Object>> enrolmentDictionary) {

        List<String> assessments = getStandaloneAssessmentIdentifiersFromSystem();

        if (CollectionUtils.isEmpty(assessments)) {
            return Collections.emptyList();
        }

        List<String> identifiers = new ArrayList<>();

        for (String identifier : assessments) {

            Map<String, Object> enrolment = enrolmentDictionary.get(identifier);

            if (enrolment == null) {
                identifiers.add(identifier);
                continue;
            }

            if (!isNotCompleted(enrolment)) {
                continue;
            }

            if (!isBatchEndDateValid(enrolment)) {
                continue;
            }

            identifiers.add(identifier);
        }

        return identifiers;
    }

    private boolean isBatchEndDateValid(Map<String, Object> enrolment) {
        Object batchEndDate = enrolment.get(BATCH_END_DATE);

        if (batchEndDate == null) {
            return true;
        }

        LocalDate endDate = LocalDate.parse(batchEndDate.toString());
        return !endDate.isBefore(LocalDate.now());
    }

    private boolean isNotCompleted(Map<String, Object> enrolment) {
        Object status = enrolment.get(Constants.STATUS);
        return status != null && Integer.parseInt(status.toString()) != 2;
    }

    private List<String> getFilteredCaProgramIdentifiers(
            String userId,
            String authToken,
            Map<String, Map<String, Object>> enrolmentDictionary) {

        try {
            Map<String, Object> request = new HashMap<>();
            request.put(Constants.COURSE_CATEGORY,
                    Constants.COURSE_CATEGORY_COMPREHENSIVE_ASSESSMENT_PROGRAM);

            ApiResponse response =
                    getAssignedCoursesForUserByAdmin(userId, request, authToken);

            if (response == null || response.getResult() == null) {
                return Collections.emptyList();
            }

            List<Map<String, Object>> assignedCourses =
                    (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT);

            if (CollectionUtils.isEmpty(assignedCourses)) {
                return Collections.emptyList();
            }

            LocalDate today = LocalDate.now();
            List<String> identifiers = new ArrayList<>();

            for (Map<String, Object> course : assignedCourses) {

                String identifier = (String) course.get(Constants.IDENTIFIER);
                String endDate = (String) course.get(END_DATE_KEY);

                Map<String, Object> enrolment = enrolmentDictionary.get(identifier);

                if (!StringUtils.hasText(endDate)) {
                    identifiers.add(identifier);
                    continue;
                }

                LocalDate contentEndDate = LocalDate.parse(endDate);


                if (contentEndDate.isBefore(today)) {
                    continue;
                }

                if (enrolment != null && isCompleted(enrolment)) {
                    continue;
                }

                identifiers.add(identifier);
            }

            return identifiers;

        } catch (Exception e) {
            log.error("Error filtering CA Program identifiers for userId: {}, error: {}",
                    userId, e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    private List<String> getStandaloneAssessmentIdentifiersFromSystem() {
        try {

            Map<String, Object> requestMap = objectMapper.readValue(
                    standaloneAssessmentSearchRequest,
                    new TypeReference<Map<String, Object>>() {});

            String searchUrl = sbSearchServiceHost + sbCompositeV4Search;

            Map<String, Object> searchResponse =
                    outboundRequestHandlerService.fetchResultUsingPost(searchUrl, requestMap, null);

            if (MapUtils.isNotEmpty(searchResponse)) {

                Map<String, Object> result =
                        (Map<String, Object>) searchResponse.get(Constants.RESULT);

                if (result != null && result.containsKey(Constants.CONTENT)) {

                    List<Map<String, Object>> contents =
                            (List<Map<String, Object>>) result.get(Constants.CONTENT);

                    return contents.stream()
                            .map(content -> (String) content.get(Constants.IDENTIFIER))
                            .collect(Collectors.toList());
                }
            }

        } catch (Exception e) {
            log.error("Error fetching Standalone Assessment identifiers, error: {}",
                    e.getMessage(), e);
        }

        return Collections.emptyList();
    }

    private Map<String, Map<String, Object>> callAssessmentEnrollmentDetailsApi(
            String userToken,
            List<String> assessmentIds) {

        if (CollectionUtils.isEmpty(assessmentIds)) {
            return Collections.emptyMap();
        }

        Map<String, String> headers = new HashMap<>();
        headers.put(X_AUTH_TOKEN, userToken);

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("courseId", assessmentIds);

        Map<String, Object> request = new HashMap<>();
        request.put("request", requestBody);

        Map<String, Object> apiResponse =
                outboundRequestHandlerService.fetchResultUsingPost(
                        lmsServiceHost + enrollmentDetailsUrl,
                        request,
                        headers);

        if (MapUtils.isEmpty(apiResponse)) {
            return Collections.emptyMap();
        }

        Map<String, Object> result =
                (Map<String, Object>) apiResponse.get(Constants.RESULT);

        if (result == null) {
            return Collections.emptyMap();
        }

        List<Map<String, Object>> response =
                (List<Map<String, Object>>) result.get("courses");

        if (CollectionUtils.isEmpty(response)) {
            return Collections.emptyMap();
        }

        Map<String, Map<String, Object>> enrollmentDictionary =
                new HashMap<>();

        for (Map<String, Object> enrollment : response) {

            String courseId =
                    (String) enrollment.get("courseId");

            if (StringUtils.hasText(courseId)) {
                enrollmentDictionary.put(courseId, enrollment);
            }
        }

        return enrollmentDictionary;
    }

    private List<String> filterStandaloneAssessmentIdentifiers(
            List<String> assessmentIds,
            Map<String, Map<String, Object>> enrollmentDictionary) {

        if (CollectionUtils.isEmpty(assessmentIds)) {
            return Collections.emptyList();
        }

        List<String> identifiers = new ArrayList<>();

        for (String identifier : assessmentIds) {

            Map<String, Object> enrollment =
                    enrollmentDictionary.get(identifier);

            if (MapUtils.isEmpty(enrollment)) {
                continue;
            }

            if (isCompleted(enrollment)) {
                continue;
            }

            if (isBatchEndDateValidForStandalone(enrollment)) {
                identifiers.add(identifier);
            }
        }

        return identifiers;
    }

    private boolean isCompleted(Map<String, Object> enrollment) {

        if (MapUtils.isEmpty(enrollment)) {
            return false;
        }

        Object statusObj = enrollment.get("status");
        Object completionPercentageObj =
                enrollment.get("completionPercentage");

        boolean completedByStatus =
                statusObj instanceof Number
                        && ((Number) statusObj).intValue() == 2;

        boolean completedByPercentage =
                completionPercentageObj instanceof Number
                        && ((Number) completionPercentageObj).intValue() == 100;

        return completedByStatus || completedByPercentage;
    }

    @SuppressWarnings("unchecked")
    private boolean isBatchEndDateValidForStandalone(
            Map<String, Object> enrollment) {

        if (MapUtils.isEmpty(enrollment)) {
            return false;
        }

        Object contentObj = enrollment.get("content");

        if (!(contentObj instanceof Map)) {
            return false;
        }

        Map<String, Object> content =
                (Map<String, Object>) contentObj;

        Object batchesObj = content.get("batches");

        if (!(batchesObj instanceof List)) {
            return false;
        }

        List<Map<String, Object>> batches =
                (List<Map<String, Object>>) batchesObj;

        LocalDate today = LocalDate.now();

        for (Map<String, Object> batch : batches) {

            if (MapUtils.isEmpty(batch)) {
                continue;
            }

            Object endDateObj = batch.get("endDate");

            if (!(endDateObj instanceof String)
                    || !StringUtils.hasText((String) endDateObj)) {
                continue;
            }

            LocalDate endDate =
                    LocalDate.parse((String) endDateObj);

            if (!endDate.isBefore(today)) {
                return true;
            }
        }

        return false;
    }

}
