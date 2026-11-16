package com.igot.cb.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cbplan.service.CbPlanServiceV4;
import com.igot.cb.cbplan.util.CbPlanYearUtil;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.CourseAccessServiceImpl;
import com.igot.cb.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.service.UserAndOrgServiceImpl;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared utility component for content info assembly operations.
 * Centralises enrolment, CA program, standalone assessment, and moderated
 * content lookups so they can be reused across content info service versions.
 */
@Component
public final class ContentInfoUtil {

    private static final Logger log = LoggerFactory.getLogger(ContentInfoUtil.class);

    private final OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    private final UserAndOrgServiceImpl userAndOrgService;
    private final RedisCacheMgr redisCacheMgr;
    private final ObjectMapper objectMapper;
    private final CourseAccessServiceImpl courseAccessService;
    private final CbExtServerProperties serverProperties;
    private final CbPlanServiceV4 cbPlanServiceV4;

    /**
     * @param outboundRequestHandlerService HTTP client for downstream service calls
     * @param userAndOrgService             user profile lookup (required for moderated content)
     * @param redisCacheMgr                 Redis cache for moderated content results
     * @param objectMapper                  JSON serialiser / deserialiser
     * @param courseAccessService           provides {@code getAssignedCoursesForUserByAdmin}
     * @param serverProperties              externalised configuration properties
     * @param cbPlanServiceV4               V4 CB plan service for user dictionary lookup
     */
    public ContentInfoUtil(OutboundRequestHandlerServiceImpl outboundRequestHandlerService,
                           UserAndOrgServiceImpl userAndOrgService,
                           RedisCacheMgr redisCacheMgr,
                           ObjectMapper objectMapper,
                           CourseAccessServiceImpl courseAccessService,
                           CbExtServerProperties serverProperties,
                           CbPlanServiceV4 cbPlanServiceV4) {
        this.outboundRequestHandlerService = outboundRequestHandlerService;
        this.userAndOrgService = userAndOrgService;
        this.redisCacheMgr = redisCacheMgr;
        this.objectMapper = objectMapper;
        this.courseAccessService = courseAccessService;
        this.serverProperties = serverProperties;
        this.cbPlanServiceV4 = cbPlanServiceV4;
    }

    /**
     * Fetches content identifiers assigned to the user for the given course category.
     *
     * @param userId         user's ID
     * @param courseCategory category to filter (e.g. {@link Constants#LEARNING_PATHWAY})
     * @param authToken      auth token forwarded to the admin courses endpoint
     * @return list of content identifiers; empty list on error or no results
     */
    public List<String> getAssignedCourseIds(String userId, String courseCategory, String authToken) {
        try {
            return fetchAssignedCoursesByCategory(userId, courseCategory, authToken).stream()
                    .map(course -> (String) course.get(Constants.IDENTIFIER))
                    .toList();
        } catch (Exception e) {
            log.error("getAssignedCourseIds: failed for courseCategory={}, userId={}", courseCategory, userId, e);
        }
        return Collections.emptyList();
    }

    /**
     * Fetches the enrolment dictionary for the authenticated user from the LMS service.
     *
     * @param userToken user's auth token
     * @return map of courseId to enrolment details; empty map if the API returns no data
     */
    public Map<String, Map<String, Object>> callEnrolmentDictionaryApi(String userToken) {
        Map<String, String> headers = new HashMap<>();
        headers.put(Constants.X_AUTH_TOKEN, userToken);
        Map<String, Object> apiResponse = outboundRequestHandlerService.fetchResultUsingGet(
                serverProperties.getLmsServiceHost() + serverProperties.getEnrolmentDictionaryUrl(), headers);
        if (MapUtils.isEmpty(apiResponse)) {
            return Collections.emptyMap();
        }
        Map<String, Object> result = (Map<String, Object>) apiResponse.get(Constants.RESULT);
        if (result == null || result.get(Constants.RESPONSE) == null) {
            return Collections.emptyMap();
        }
        return (Map<String, Map<String, Object>>) result.get(Constants.RESPONSE);
    }

    /**
     * Returns identifiers of CA Programs assigned to the user that are still active:
     * not expired and not completed by the user.
     *
     * @param userId              user's ID
     * @param authToken           auth token for the admin courses endpoint
     * @param enrolmentDictionary user's enrolment dictionary for completion checks
     * @return filtered list of CA Program content identifiers
     */
    public List<String> getFilteredCaProgramIdentifiers(String userId, String authToken,
                                                         Map<String, Map<String, Object>> enrolmentDictionary) {
        try {
            List<Map<String, Object>> assignedCourses = fetchAssignedCoursesByCategory(
                    userId, Constants.COURSE_CATEGORY_COMPREHENSIVE_ASSESSMENT_PROGRAM, authToken);
            if (CollectionUtils.isEmpty(assignedCourses)) {
                return Collections.emptyList();
            }
            LocalDate today = LocalDate.now(ZoneId.of(Constants.TIMEZONE_ASIA_KOLKATA));
            return assignedCourses.stream()
                    .filter(course -> shouldIncludeCaProgram(course, enrolmentDictionary, today))
                    .map(course -> (String) course.get(Constants.IDENTIFIER))
                    .toList();
        } catch (Exception e) {
            log.error("getFilteredCaProgramIdentifiers: failed for userId={}", userId, e);
            return Collections.emptyList();
        }
    }

    /**
     * Fetches all Standalone Assessment identifiers from the search service.
     *
     * @return list of standalone assessment content identifiers; empty list on error
     */
    public List<String> getStandaloneAssessmentIdentifiersFromSystem() {
        try {
            Map<String, Object> requestMap = objectMapper.readValue(
                    serverProperties.getStandaloneAssessmentSearchRequest(),
                    new TypeReference<Map<String, Object>>() {});
            String searchUrl = serverProperties.getSbSearchServiceHost() + serverProperties.getSbCompositeV4Search();
            Map<String, Object> searchResponse =
                    outboundRequestHandlerService.fetchResultUsingPost(searchUrl, requestMap, null);
            return extractIdentifiersFromSearchResult(searchResponse);
        } catch (Exception e) {
            log.error("getStandaloneAssessmentIdentifiersFromSystem: failed", e);
        }
        return Collections.emptyList();
    }

    /**
     * Fetches enrollment details for a list of assessment IDs from the LMS service.
     *
     * @param userToken     user's auth token
     * @param assessmentIds list of assessment content IDs to query
     * @return map of courseId to enrollment details; empty map if no results
     */
    public Map<String, Map<String, Object>> callAssessmentEnrollmentDetailsApi(
            String userToken, List<String> assessmentIds) {
        if (CollectionUtils.isEmpty(assessmentIds)) {
            return Collections.emptyMap();
        }
        Map<String, String> headers = new HashMap<>();
        headers.put(Constants.X_AUTH_TOKEN, userToken);
        Map<String, Object> apiResponse = outboundRequestHandlerService.fetchResultUsingPost(
                serverProperties.getLmsServiceHost() + serverProperties.getEnrollmentDetailsUrl(),
                buildEnrollmentRequest(assessmentIds), headers);
        if (MapUtils.isEmpty(apiResponse)) {
            return Collections.emptyMap();
        }
        Map<String, Object> result = (Map<String, Object>) apiResponse.get(Constants.RESULT);
        if (result == null) {
            return Collections.emptyMap();
        }
        List<Map<String, Object>> courses = (List<Map<String, Object>>) result.get(Constants.COURSES);
        return indexEnrollmentsByCourseId(courses);
    }

    /**
     * Builds the request body for the LMS enrollment details API.
     */
    private Map<String, Object> buildEnrollmentRequest(List<String> assessmentIds) {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.COURSE_ID, assessmentIds);
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.REQUEST, requestBody);
        return request;
    }

    /**
     * Filters standalone assessment IDs to those enrolled, not completed,
     * and with at least one batch with a valid (non-expired) end date.
     *
     * @param assessmentIds        full list of standalone assessment IDs from the system
     * @param enrollmentDictionary user's enrollment details keyed by courseId
     * @return filtered list of assessment IDs the user should see
     */
    public List<String> filterStandaloneAssessmentIdentifiers(
            List<String> assessmentIds,
            Map<String, Map<String, Object>> enrollmentDictionary) {
        if (CollectionUtils.isEmpty(assessmentIds)) {
            return Collections.emptyList();
        }
        List<String> identifiers = new ArrayList<>();
        for (String identifier : assessmentIds) {
            Map<String, Object> enrollment = enrollmentDictionary.get(identifier);
            if (MapUtils.isEmpty(enrollment) || isCompleted(enrollment)) {
                continue;
            }
            if (isBatchEndDateValidForStandalone(enrollment)) {
                identifiers.add(identifier);
            }
        }
        return identifiers;
    }

    /**
     * Returns moderated course identifiers for the user's organisation.
     * Serves from a per-user/org Redis cache; falls back to the search API on cache miss.
     *
     * @param userId user's ID (used as the Redis cache key)
     * @param orgId  organisation ID (inner-map key within the cached entry)
     * @return map with {@code identifiers} (list) and {@code count} keys
     */
    public Map<String, Object> getModeratedContentIdentifiers(String userId, String orgId) throws IOException {
        String redisKey = Constants.MODERATED_COURSE_COUNT_REDIS_KEY_PREFIX + userId;
        String cached = redisCacheMgr.getFromCache(redisKey);
        Map<String, Object> moderatedMap = new HashMap<>();
        if (StringUtils.hasText(cached)) {
            moderatedMap = objectMapper.readValue(cached, new TypeReference<Map<String, Object>>() {});
            if (MapUtils.isNotEmpty(moderatedMap) && moderatedMap.containsKey(orgId)) {
                log.info("getModeratedContentIdentifiers: cache HIT for userId={}, orgId={}", userId, orgId);
                return (Map<String, Object>) moderatedMap.get(orgId);
            }
        }
        log.info("getModeratedContentIdentifiers: cache MISS for userId={}, orgId={}", userId, orgId);
        Map<String, Object> userProfileDetails = userAndOrgService.readUserProfile(userId, null);
        Map<String, Object> moderatedContent = getModeratedCourseIdentifiers(orgId, userProfileDetails);
        moderatedMap.put(orgId, moderatedContent);
        redisCacheMgr.putInCache(redisKey, objectMapper.writeValueAsString(moderatedMap));
        log.info("getModeratedContentIdentifiers: cache updated for userId={}, orgId={}", userId, orgId);
        return moderatedContent;
    }

    /**
     * Fetches and unwraps courses from {@code getAssignedCoursesForUserByAdmin} for the given category.
     */
    private List<Map<String, Object>> fetchAssignedCoursesByCategory(
            String userId, String courseCategory, String authToken) {
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.COURSE_CATEGORY, courseCategory);
        ApiResponse response = courseAccessService.getAssignedCoursesForUserByAdmin(userId, request, authToken);
        if (response == null || response.getResult() == null) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> courses =
                (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT);
        return CollectionUtils.isEmpty(courses) ? Collections.emptyList() : courses;
    }

    /**
     * Calls the search service to retrieve moderated course identifiers for the given org.
     */
    private Map<String, Object> getModeratedCourseIdentifiers(
            String orgId, Map<String, Object> userProfileDetails) {
        try {
            String requestBody = String.format(serverProperties.getModeratedCourseSearchRequest(), orgId);
            Map<String, Object> requestMap = objectMapper.readValue(
                    requestBody, new TypeReference<Map<String, Object>>() {});
            Map<String, Object> filters =
                    (Map<String, Object>) ((Map<String, Object>) requestMap.get(Constants.REQUEST))
                            .get(Constants.FILTERS);
            applyVerifiedStatusFilter(filters, userProfileDetails);
            String searchUrl = serverProperties.getSbSearchServiceHost() + serverProperties.getSbCompositeV4Search();
            Map<String, Object> searchResponse =
                    outboundRequestHandlerService.fetchResultUsingPost(searchUrl, requestMap, null);
            return buildModeratedContentResult(searchResponse);
        } catch (Exception e) {
            log.error("getModeratedCourseIdentifiers: failed for orgId={}", orgId, e);
            return emptyModeratedResult();
        }
    }

    /**
     * Applies the verified-karmayogi filter when the user's profile is not VERIFIED.
     * If profile details cannot be parsed, the user is treated as unverified.
     */
    private void applyVerifiedStatusFilter(Map<String, Object> filters,
                                           Map<String, Object> userProfileDetails) {
        String profileStatus = null;
        Object profileDetailsObj = userProfileDetails.get("profiledetails");
        if (profileDetailsObj instanceof String profileDetailsStr && StringUtils.hasText(profileDetailsStr)) {
            try {
                Map<String, Object> profileDetails = objectMapper.readValue(
                        profileDetailsStr, new TypeReference<Map<String, Object>>() {});
                profileStatus = (String) profileDetails.get(Constants.PROFILE_STATUS_KEY);
            } catch (IOException e) {
                log.warn("applyVerifiedStatusFilter: failed to parse profiledetails, treating as unverified", e);
            }
        }
        if (!Constants.VERIFIED.equalsIgnoreCase(profileStatus)) {
            filters.put(Constants.SECURE_SETTINGS_IS_VERIFIED_KARMAYOGI, "No");
        }
    }

    /**
     * Extracts the moderated content result ({@code identifiers} + {@code count}) from the search response.
     */
    private Map<String, Object> buildModeratedContentResult(Map<String, Object> searchResponse) {
        if (MapUtils.isNotEmpty(searchResponse)) {
            Map<String, Object> result = (Map<String, Object>) searchResponse.get(Constants.RESULT);
            if (result != null && result.containsKey(Constants.CONTENT)) {
                List<Map<String, Object>> contents =
                        (List<Map<String, Object>>) result.get(Constants.CONTENT);
                List<String> identifiers = contents.stream()
                        .map(content -> (String) content.get(Constants.IDENTIFIER))
                        .toList();
                Map<String, Object> response = new HashMap<>();
                response.put(Constants.IDENTIFIERS, identifiers);
                response.put(Constants.COUNT, result.get(Constants.COUNT));
                return response;
            }
        }
        return emptyModeratedResult();
    }

    /**
     * Builds an empty moderated content result map.
     */
    private Map<String, Object> emptyModeratedResult() {
        Map<String, Object> response = new HashMap<>();
        response.put(Constants.IDENTIFIERS, Collections.emptyList());
        response.put(Constants.COUNT, 0);
        return response;
    }

    /**
     * Extracts content identifiers from a composite search API response.
     */
    private List<String> extractIdentifiersFromSearchResult(Map<String, Object> searchResponse) {
        if (MapUtils.isEmpty(searchResponse)) {
            return Collections.emptyList();
        }
        Map<String, Object> result = (Map<String, Object>) searchResponse.get(Constants.RESULT);
        if (result == null || !result.containsKey(Constants.CONTENT)) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> contents = (List<Map<String, Object>>) result.get(Constants.CONTENT);
        return CollectionUtils.isEmpty(contents) ? Collections.emptyList() :
                contents.stream()
                        .map(content -> (String) content.get(Constants.IDENTIFIER))
                        .toList();
    }

    /**
     * Determines whether a CA Program course should be included in the result.
     */
    private boolean shouldIncludeCaProgram(Map<String, Object> course,
                                            Map<String, Map<String, Object>> enrolmentDictionary,
                                            LocalDate today) {
        String identifier = (String) course.get(Constants.IDENTIFIER);
        String endDate = (String) course.get(Constants.END_DATE_KEY);
        Map<String, Object> enrolment = enrolmentDictionary.get(identifier);
        if (!StringUtils.hasText(endDate)) {
            return true;
        }
        if (LocalDate.parse(endDate).isBefore(today)) {
            return false;
        }
        return enrolment == null || !isCompleted(enrolment);
    }

    /**
     * Indexes a list of enrollment records into a map keyed by courseId.
     */
    private Map<String, Map<String, Object>> indexEnrollmentsByCourseId(
            List<Map<String, Object>> courses) {
        if (CollectionUtils.isEmpty(courses)) {
            return Collections.emptyMap();
        }
        Map<String, Map<String, Object>> enrollmentDictionary = new HashMap<>();
        for (Map<String, Object> enrollment : courses) {
            String courseId = (String) enrollment.get(Constants.COURSE_ID);
            if (StringUtils.hasText(courseId)) {
                enrollmentDictionary.put(courseId, enrollment);
            }
        }
        return enrollmentDictionary;
    }

    /**
     * Returns true if the enrollment indicates completion (status == 2 or completionPercentage == 100).
     */
    private boolean isCompleted(Map<String, Object> enrollment) {
        if (MapUtils.isEmpty(enrollment)) {
            return false;
        }
        Object statusObj = enrollment.get("status");
        Object completionPctObj = enrollment.get("completionPercentage");
        boolean completedByStatus = statusObj instanceof Number statusNum && statusNum.intValue() == 2;
        boolean completedByPct = completionPctObj instanceof Number pctNum && pctNum.intValue() == 100;
        return completedByStatus || completedByPct;
    }

    /**
     * Returns true if the enrollment's content has at least one batch with a non-expired end date.
     */
    private boolean isBatchEndDateValidForStandalone(Map<String, Object> enrollment) {
        Object contentObj = enrollment.get("content");
        if (!(contentObj instanceof Map)) {
            return false;
        }
        Object batchesObj = ((Map<String, Object>) contentObj).get("batches");
        if (!(batchesObj instanceof List)) {
            return false;
        }
        LocalDate today = LocalDate.now(ZoneId.of(Constants.TIMEZONE_ASIA_KOLKATA));
        for (Map<String, Object> batch : (List<Map<String, Object>>) batchesObj) {
            if (isActiveBatch(batch, today)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if the batch has a non-blank end date that is today or in the future.
     */
    private boolean isActiveBatch(Map<String, Object> batch, LocalDate today) {
        if (MapUtils.isEmpty(batch)) {
            return false;
        }
        Object endDateObj = batch.get("endDate");
        if (!(endDateObj instanceof String endDate) || !StringUtils.hasText(endDate)) {
            return false;
        }
        return !LocalDate.parse(endDate).isBefore(today);
    }

    /**
     * Fetches CB Plan V4 user dictionary and partitions content identifiers into
     * {@code apar}, {@code trainingPlan}, and {@code aiCbp} categories.
     * The current financial year is requested explicitly so that, when it has
     * no accessible plans, the dictionary service's own previous-year fallback
     * is triggered and its content is merged in below.
     *
     * @param authToken user's auth token
     * @return map keyed by {@link Constants#APAR}, {@link Constants#TRAINING_PLAN},
     *         and {@link Constants#AI_CBP}; empty lists on error or no active plans
     */
    public Map<String, List<String>> getCbPlanV4ContentIds(String authToken) {
        Map<String, List<String>> result = initCbPlanIdsMap();
        try {
            ApiRequest request = new ApiRequest();
            request.setRequest(Map.of(Constants.REQUEST_PARAM_PLAN_YEAR, CbPlanYearUtil.resolveCurrentFinancialYear()));
            ApiResponse cbPlanResponse = cbPlanServiceV4.getCBPlanDictionaryForUser(request, authToken);
            if (cbPlanResponse == null || MapUtils.isEmpty(cbPlanResponse.getResult())) {
                return result;
            }
            for (Object yearValue : cbPlanResponse.getResult().values()) {
                if (yearValue instanceof Map<?, ?> yearData) {
                    processYearEntry(yearData, result);
                }
            }
        } catch (Exception e) {
            log.error("getCbPlanV4ContentIds: failed", e);
        }
        return result;
    }

    private Map<String, List<String>> initCbPlanIdsMap() {
        Map<String, List<String>> map = new HashMap<>();
        map.put(Constants.APAR, new ArrayList<>());
        map.put(Constants.TRAINING_PLAN, new ArrayList<>());
        map.put(Constants.AI_CBP, new ArrayList<>());
        return map;
    }

    private void processYearEntry(Map<?, ?> yearData, Map<String, List<String>> output) {
        Map<String, Map<String, Object>> aparPlanList =
                (Map<String, Map<String, Object>>) yearData.get(Constants.RESPONSE_KEY_APAR_PLAN_LIST);
        Map<String, Map<String, Object>> nonAparPlanList =
                (Map<String, Map<String, Object>>) yearData.get(Constants.RESPONSE_KEY_NON_APAR_PLAN_LIST);
        if (MapUtils.isEmpty(aparPlanList) && MapUtils.isEmpty(nonAparPlanList)) {
            return;
        }
        Set<String> aiCbpIds = collectAiCbpIdentifiers(aparPlanList, nonAparPlanList);
        collectNonAiCbpFromPlanList(aparPlanList, aiCbpIds, output.get(Constants.APAR));
        collectNonAiCbpFromPlanList(nonAparPlanList, aiCbpIds, output.get(Constants.TRAINING_PLAN));
        output.get(Constants.AI_CBP).addAll(aiCbpIds);
    }

    private Set<String> collectAiCbpIdentifiers(Map<String, Map<String, Object>> aparPlanList,
                                                 Map<String, Map<String, Object>> nonAparPlanList) {
        Set<String> aiCbpIds = new LinkedHashSet<>();
        addPlanContentIdsIfAiCbp(aparPlanList, aiCbpIds);
        addPlanContentIdsIfAiCbp(nonAparPlanList, aiCbpIds);
        return aiCbpIds;
    }

    private void addPlanContentIdsIfAiCbp(Map<String, Map<String, Object>> planList, Set<String> target) {
        if (MapUtils.isEmpty(planList)) {
            return;
        }
        for (Map<String, Object> plan : planList.values()) {
            if (Constants.PLAN_TYPE_AI_CBP.equalsIgnoreCase((String) plan.get(Constants.PLAN_TYPE))) {
                addContentIdsFromPlan(plan, target);
            }
        }
    }

    private void collectNonAiCbpFromPlanList(Map<String, Map<String, Object>> planList,
                                              Set<String> aiCbpIds, List<String> target) {
        if (MapUtils.isEmpty(planList)) {
            return;
        }
        for (Map<String, Object> plan : planList.values()) {
            if (!Constants.PLAN_TYPE_AI_CBP.equalsIgnoreCase((String) plan.get(Constants.PLAN_TYPE))) {
                addNonAiCbpContentIds(plan, aiCbpIds, target);
            }
        }
    }

    private void addNonAiCbpContentIds(Map<String, Object> plan, Set<String> aiCbpIds, List<String> target) {
        List<Map<String, Object>> contentList =
                (List<Map<String, Object>>) plan.get(Constants.CONTENT_LIST);
        if (CollectionUtils.isEmpty(contentList)) {
            return;
        }
        for (Map<String, Object> item : contentList) {
            String id = (String) item.get(Constants.IDENTIFIER);
            if (StringUtils.hasText(id) && !aiCbpIds.contains(id)) {
                target.add(id);
            }
        }
    }

    private void addContentIdsFromPlan(Map<String, Object> plan, Set<String> target) {
        List<Map<String, Object>> contentList =
                (List<Map<String, Object>>) plan.get(Constants.CONTENT_LIST);
        if (CollectionUtils.isEmpty(contentList)) {
            return;
        }
        for (Map<String, Object> item : contentList) {
            String id = (String) item.get(Constants.IDENTIFIER);
            if (StringUtils.hasText(id)) {
                target.add(id);
            }
        }
    }
}
