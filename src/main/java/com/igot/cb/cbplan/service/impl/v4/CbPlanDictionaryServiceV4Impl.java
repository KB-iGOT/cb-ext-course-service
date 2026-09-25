package com.igot.cb.cbplan.service.impl.v4;

import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cbplan.service.impl.CbPlanContentLookupServiceV3Impl;
import com.igot.cb.cbplan.service.impl.CbPlanDataTransformServiceV3Impl;
import com.igot.cb.cbplan.service.impl.CbPlanEnrichmentServiceV3Impl;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.model.ApiRespParam;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.cbplan.util.CbPlanYearUtil;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;

import lombok.extern.slf4j.Slf4j;

/**
 * Service for CB Plan V4 user dictionary: returns active plans grouped by APAR/non-APAR.
 * Access control is evaluated by resolving userGroupId references (V4) or inline
 * userGroupCriteriaList (V3 plans stored in the same table) against the user's profile.
 *
 * @version 4.0
 */
@Service
@Slf4j
public class CbPlanDictionaryServiceV4Impl {

    private final CassandraOperation cassandraOperation;
    private final CbPlanCacheMgrV3 cbPlanCacheMgrV3;
    private final CbPlanUserGroupLookupServiceV4Impl userGroupLookupService;
    private final AccessTokenValidator accessTokenValidator;
    private final RedisCacheMgr redisCacheMgr;
    private final CbExtServerProperties serverProperties;
    private final CbPlanEnrichmentServiceV3Impl enrichmentService;
    private final CbPlanDataTransformServiceV3Impl dataTransformService;
    private final CbPlanContentLookupServiceV3Impl contentLookupService;
    private final ObjectMapper mapper;
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF = new TypeReference<>() {};

    public CbPlanDictionaryServiceV4Impl(CassandraOperation cassandraOperation,
                                         CbPlanCacheMgrV3 cbPlanCacheMgrV3,
                                         CbPlanUserGroupLookupServiceV4Impl userGroupLookupService,
                                         AccessTokenValidator accessTokenValidator,
                                         RedisCacheMgr redisCacheMgr,
                                         CbExtServerProperties serverProperties,
                                         CbPlanEnrichmentServiceV3Impl enrichmentService,
                                         CbPlanDataTransformServiceV3Impl dataTransformService,
                                         CbPlanContentLookupServiceV3Impl contentLookupService) {
        this.cassandraOperation = cassandraOperation;
        this.cbPlanCacheMgrV3 = cbPlanCacheMgrV3;
        this.userGroupLookupService = userGroupLookupService;
        this.accessTokenValidator = accessTokenValidator;
        this.redisCacheMgr = redisCacheMgr;
        this.serverProperties = serverProperties;
        this.enrichmentService = enrichmentService;
        this.dataTransformService = dataTransformService;
        this.contentLookupService = contentLookupService;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Returns the user's accessible active CB Plans grouped by APAR/non-APAR.
     *
     * @param request   API request containing planYear
     * @param authToken authentication token
     * @return ApiResponse with aparPlanList and nonAparPlanList keyed by planYear
     */
    public ApiResponse getCBPlanDictionaryForUser(ApiRequest request, String authToken) {
        log.debug("getCBPlanDictionaryForUser: Entry");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CBPLAN_V4_GET_USER_DICTIONARY);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isBlank(userId)) {
                log.warn("getCBPlanDictionaryForUser: Invalid or missing token");
                response.getParams().setErr("Invalid or missing authentication token");
                response.setResponseCode(HttpStatus.UNAUTHORIZED);
                return response;
            }
            Map<String, Object> requestData = extractRequestData(request);
            String requestedPlanYear = (String) requestData.get(Constants.REQUEST_PARAM_PLAN_YEAR);
            String planYear = resolvePlanYear(requestedPlanYear, response);
            if (Objects.isNull(planYear)) {
                return response;
            }
            String cacheKey = Constants.CB_PLAN_V4_REDIS_KEY_PREFIX + userId + ":" + planYear + ":dict";
            String cachedJson = redisCacheMgr.getFromCache(cacheKey);
            if (StringUtils.isNotBlank(cachedJson)) {
                log.info("getCBPlanDictionaryForUser: Cache hit - userId={}, planYear={}", userId, planYear);
                populateResponseFromCache(response, cachedJson, planYear);
                return response;
            }
            Map<String, String> userProfile = buildUserProfile(userId, response);
            if (userProfile.isEmpty()) {
                return response;
            }
            String userOrgId = userProfile.get(Constants.USER_ROOT_ORG_ID);
            log.info("getCBPlanDictionaryForUser: Cache miss - userId={}, orgId={}, planYear={}", userId, userOrgId, planYear);
            AtomicBoolean isCacheEnabled = new AtomicBoolean(false);
            List<Map<String, Object>> activePlans = fetchPlansForUser(userProfile, userOrgId, planYear, isCacheEnabled);
            if (CollectionUtils.isEmpty(activePlans)) {
                log.info("getCBPlanDictionaryForUser: No active plans - userId={}, planYear={}", userId, planYear);
                if (StringUtils.isNotBlank(requestedPlanYear)) {
                    response.getResult().put(planYear, buildYearResult(new LinkedHashMap<>(), new LinkedHashMap<>()));
                    fetchAndAppendPreviousYear(response, userProfile, userOrgId,
                            CbPlanYearUtil.resolvePreviousYear(planYear));
                    response.setParams(new ApiRespParam());
                    response.getParams().setStatus(Constants.SUCCESS);
                    response.setResponseCode(HttpStatus.OK);
                } else {
                    populateEmptyResponse(response, planYear);
                }
                cacheResult(cacheKey, response.getResult(), isCacheEnabled.get());
                return response;
            }
            Map<String, Map<String, Object>> aparPlanMap = new LinkedHashMap<>();
            Map<String, Map<String, Object>> nonAparPlanMap = new LinkedHashMap<>();
            buildPlanPartitions(activePlans, userProfile, aparPlanMap, nonAparPlanMap);
            Set<String> orgIds = collectCreatedByOrgIds(aparPlanMap, nonAparPlanMap);
            Map<String, Map<String, String>> orgDetailsMap = fetchOrgDetails(orgIds);
            enrichOrgDetails(aparPlanMap, orgDetailsMap);
            enrichOrgDetails(nonAparPlanMap, orgDetailsMap);
            filterLiveContentInPlans(aparPlanMap, nonAparPlanMap);
            Map<String, Object> yearResult = buildYearResult(aparPlanMap, nonAparPlanMap);
            response.getResult().put(planYear, yearResult);
            if (StringUtils.isNotBlank(requestedPlanYear) && aparPlanMap.isEmpty() && nonAparPlanMap.isEmpty()) {
                log.info("getCBPlanDictionaryForUser: Plans found but none accessible - userId={}, planYear={}, fetching previous year", userId, planYear);
                fetchAndAppendPreviousYear(response, userProfile, userOrgId,
                        CbPlanYearUtil.resolvePreviousYear(planYear));
            }
            response.setParams(new ApiRespParam());
            response.getParams().setStatus(Constants.SUCCESS);
            response.setResponseCode(HttpStatus.OK);
            cacheResult(cacheKey, response.getResult(), isCacheEnabled.get());
            log.info("getCBPlanDictionaryForUser: Success - userId={}, planYear={}, aparCount={}, nonAparCount={}",
                    userId, planYear, aparPlanMap.size(), nonAparPlanMap.size());
        } catch (IllegalArgumentException e) {
            log.error("getCBPlanDictionaryForUser: Invalid argument", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Invalid request parameters");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            log.error("getCBPlanDictionaryForUser: Unexpected error", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Failed to fetch CB Plan dictionary");
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    /**
     * Checks whether the given Comprehensive Assessment do_id is linked (via caLinkedId) to any
     * plan the user is eligible for, searching the current financial year then the previous year -
     * unlike getCBPlanDictionaryForUser, this always searches both, since callers here never pass
     * an explicit planYear to opt into the fallback. Reuses the same per-year plan-resolution
     * pipeline (org/ministry scoping, access-control rule evaluation, live-content filtering) and
     * the same per-year Redis cache as the dictionary endpoint, so a warm dictionary cache is
     * reused here too, and a miss here populates the same cache the dictionary endpoint would use.
     *
     * @param doId CA content identifier to check eligibility for
     * @param authToken authentication token
     * @return ApiResponse with result = {eligible: boolean, mandatoryCourses: List<String>}
     */
    public ApiResponse getComprehensiveAssessmentEligibility(String doId, String authToken) {
        log.debug("getComprehensiveAssessmentEligibility: Entry - doId={}", doId);
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CBPLAN_V4_ASSESSMENT_ELIGIBILITY);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isBlank(userId)) {
                log.warn("getComprehensiveAssessmentEligibility: Invalid or missing token");
                response.getParams().setErr("Invalid or missing authentication token");
                response.setResponseCode(HttpStatus.UNAUTHORIZED);
                return response;
            }
            Map<String, String> userProfile = buildUserProfile(userId, response);
            if (userProfile.isEmpty()) {
                return response;
            }
            String userOrgId = userProfile.get(Constants.USER_ROOT_ORG_ID);
            AtomicBoolean isCacheEnabled = new AtomicBoolean(false);
            String currentYear = CbPlanYearUtil.resolveCurrentFinancialYear();
            for (String planYear : List.of(currentYear, CbPlanYearUtil.resolvePreviousYear(currentYear))) {
                Map<String, Object> yearResult = resolveYearResult(userId, userProfile, userOrgId, planYear, isCacheEnabled);
                log.info("yearResult={}, ", yearResult);
                Map<String, Object> match = findPlanByCaLinkedId(yearResult, doId);
                if (Objects.nonNull(match)) {
                    List<String> mandatoryCourseIds = extractMandatoryCourseIds(match);
                    log.info("getComprehensiveAssessmentEligibility: Match found - userId={}, doId={}, planYear={}", userId, doId, planYear);
                    log.info("getComprehensiveAssessmentEligibility: Result - userId={}, doId={}, eligible=true, mandatoryCourses={}", userId, doId, mandatoryCourseIds);
                    response.getResult().put(Constants.ELIGIBLE, true);
                    response.getResult().put(Constants.MANDATORY_COURSES, mandatoryCourseIds);
                    response.setParams(new ApiRespParam());
                    response.getParams().setStatus(Constants.SUCCESS);
                    response.setResponseCode(HttpStatus.OK);
                    return response;
                }
            }
            log.info("getComprehensiveAssessmentEligibility: No match - userId={}, doId={}", userId, doId);
            log.info("getComprehensiveAssessmentEligibility: Result - userId={}, doId={}, eligible=false, mandatoryCourses=[]", userId, doId);
            response.getResult().put(Constants.ELIGIBLE, false);
            response.getResult().put(Constants.MANDATORY_COURSES, List.of());
            response.setParams(new ApiRespParam());
            response.getParams().setStatus(Constants.SUCCESS);
            response.setResponseCode(HttpStatus.OK);
        } catch (Exception e) {
            log.error("getComprehensiveAssessmentEligibility: Unexpected error - doId={}", doId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Failed to check assessment eligibility");
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    /**
     * Resolves one year's eligible-plan result via the same pipeline and the same per-year Redis
     * cache key getCBPlanDictionaryForUser's cache-miss path uses - this does not modify or call
     * into that method, it independently drives the same already-existing, already-shared steps
     * (fetchPlansForUser, buildPlanPartitions, org enrichment, live-content filtering) so this is
     * purely additive: getCBPlanDictionaryForUser's own code path is untouched.
     */
    private Map<String, Object> resolveYearResult(String userId, Map<String, String> userProfile,
            String userOrgId, String planYear, AtomicBoolean isCacheEnabled) {
        String cacheKey = Constants.CB_PLAN_V4_REDIS_KEY_PREFIX + userId + ":" + planYear + ":dict";
        String cachedJson = redisCacheMgr.getFromCache(cacheKey);
        if (StringUtils.isNotBlank(cachedJson)) {
            try {
                Map<String, Object> cachedResult = mapper.readValue(cachedJson, MAP_TYPE_REF);
                // The dictionary endpoint caches its full result keyed by plan year, while this
                // method normally works with the unwrapped year result.
                Object yearResult = cachedResult.get(planYear);
                if (yearResult instanceof Map<?, ?>) {
                    return (Map<String, Object>) yearResult;
                }
                return cachedResult;
            } catch (JsonProcessingException e) {
                log.warn("resolveYearResult: Failed to deserialize cache - key={}", cacheKey, e);
            }
        }
        List<Map<String, Object>> activePlans = fetchPlansForUser(userProfile, userOrgId, planYear, isCacheEnabled);
        if (CollectionUtils.isEmpty(activePlans)) {
            Map<String, Object> empty = buildYearResult(new LinkedHashMap<>(), new LinkedHashMap<>());
            cacheResult(cacheKey, empty, isCacheEnabled.get());
            return empty;
        }
        Map<String, Map<String, Object>> aparPlanMap = new LinkedHashMap<>();
        Map<String, Map<String, Object>> nonAparPlanMap = new LinkedHashMap<>();
        buildPlanPartitions(activePlans, userProfile, aparPlanMap, nonAparPlanMap);
        Set<String> orgIds = collectCreatedByOrgIds(aparPlanMap, nonAparPlanMap);
        Map<String, Map<String, String>> orgDetailsMap = fetchOrgDetails(orgIds);
        enrichOrgDetails(aparPlanMap, orgDetailsMap);
        enrichOrgDetails(nonAparPlanMap, orgDetailsMap);
        filterLiveContentInPlans(aparPlanMap, nonAparPlanMap);
        Map<String, Object> yearResult = buildYearResult(aparPlanMap, nonAparPlanMap);
        cacheResult(cacheKey, yearResult, isCacheEnabled.get());
        return yearResult;
    }

    /**
     * Scans a year's aparPlanList/nonAparPlanList for the plan whose caLinkedId equals doId.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> findPlanByCaLinkedId(Map<String, Object> yearResult, String doId) {
        for (String listKey : List.of(Constants.RESPONSE_KEY_APAR_PLAN_LIST, Constants.RESPONSE_KEY_NON_APAR_PLAN_LIST)) {
            Object listObj = yearResult.get(listKey);
            if (!(listObj instanceof Map)) {
                continue;
            }
            for (Object planObj : ((Map<String, Object>) listObj).values()) {
                if (planObj instanceof Map) {
                    Map<String, Object> plan = (Map<String, Object>) planObj;
                    if (doId.equals(plan.get(Constants.CA_LINKED_ID))) {
                        return plan;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Extracts identifiers of contentList entries marked mandatory:true.
     */
    @SuppressWarnings("unchecked")
    private List<String> extractMandatoryCourseIds(Map<String, Object> plan) {
        Object contentListObj = plan.get(Constants.CONTENT_LIST);
        List<String> mandatoryIds = new ArrayList<>();
        if (contentListObj instanceof List) {
            for (Object entryObj : (List<Object>) contentListObj) {
                if (entryObj instanceof Map) {
                    Map<String, Object> entry = (Map<String, Object>) entryObj;
                    if (Boolean.TRUE.equals(entry.get(Constants.MANDATORY)) && entry.get(Constants.IDENTIFIER) != null) {
                        mandatoryIds.add((String) entry.get(Constants.IDENTIFIER));
                    }
                }
            }
        }
        return mandatoryIds;
    }

    /**
     * Resolves the plan year: uses the requested year if valid, otherwise defaults to the current financial year.
     */
    private String resolvePlanYear(String requestedPlanYear, ApiResponse response) {
        if (StringUtils.isBlank(requestedPlanYear)) {
            return CbPlanYearUtil.resolveCurrentFinancialYear();
        }
        String normalized = CbPlanYearUtil.validateAndNormalize(requestedPlanYear);
        if (Objects.isNull(normalized)) {
            log.warn("resolvePlanYear: Invalid format - requested={}", requestedPlanYear);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Invalid planYear format. Expected: YYYY-YY (e.g., '2026-27')");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return null;
        }
        return normalized;
    }

    /**
     * Builds the user profile map from Redis cache or Cassandra.
     * Returns an empty map and sets an error on the response when the user is not found.
     */
    private Map<String, String> buildUserProfile(String userId, ApiResponse response) {
        try {
            String cacheKey = Constants.USER + ":basicProfile:" + userId;
            String cachedData = redisCacheMgr.getFromCache(cacheKey);
            Map<String, Object> userBasicProfile;
            if (StringUtils.isNotBlank(cachedData)) {
                userBasicProfile = mapper.readValue(cachedData, MAP_TYPE_REF);
                Object profileDetailsValue = userBasicProfile.remove(Constants.PROFILE_DETAILS);
                if (Objects.nonNull(profileDetailsValue)) {
                    userBasicProfile.put(Constants.PROFILE_DETAILS.toLowerCase(), profileDetailsValue);
                }
            } else {
                Map<String, Object> propertiesMap = Map.of(Constants.ID, userId);
                List<String> fields = Arrays.asList(Constants.ID, Constants.ROOT_ORG_ID, Constants.PROFILE_DETAILS);
                List<Map<String, Object>> userList = cassandraOperation.getRecordsByProperties(
                        Constants.KEYSPACE_SUNBIRD, Constants.USER, propertiesMap, fields,
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
            userProfile.put(Constants.USER, userId);
            userProfile.put(Constants.USER_ROOT_ORG_ID, (String) userBasicProfile.get(Constants.ROOT_ORG_ID));
            Object rawProfileDetails = userBasicProfile.get(Constants.PROFILE_DETAILS.toLowerCase());
            if (Objects.nonNull(rawProfileDetails)) {
                Map<String, Object> profileDetails = parseProfileDetails(rawProfileDetails);
                if (MapUtils.isNotEmpty(profileDetails)) {
                    extractProfessionalDetails(userProfile, profileDetails);
                    extractCadreDetails(userProfile, profileDetails);
                    userProfile.put(Constants.PROFILE_STATUS_LOWER_KEY,
                            (String) profileDetails.get(Constants.PROFILE_STATUS_KEY));
                    enrichmentService.extractMinistryOrStateDetails(userProfile, profileDetails);
                }
            }
            log.debug("buildUserProfile: Profile built - userId={}, orgId={}", userId,
                    userProfile.get(Constants.USER_ROOT_ORG_ID));
            return userProfile;
        } catch (JsonProcessingException e) {
            log.error("buildUserProfile: JSON parsing failed - userId={}", userId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Error parsing user profile data");
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return Map.of();
        } catch (Exception e) {
            log.error("buildUserProfile: Error building user profile - userId={}", userId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Error fetching user profile");
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return Map.of();
        }
    }

    private Map<String, Object> parseProfileDetails(Object rawValue) throws JsonProcessingException {
        if (rawValue instanceof String str && StringUtils.isNotBlank(str)) {
            return mapper.readValue(str, MAP_TYPE_REF);
        } else if (rawValue instanceof Map<?, ?>) {
            return (Map<String, Object>) rawValue;
        }
        return Map.of();
    }

    private void extractProfessionalDetails(Map<String, String> userProfile, Map<String, Object> profileDetails) {
        Object professionalDetailsObj = profileDetails.get(Constants.PROFESSIONAL_DETAILS);
        if (!(professionalDetailsObj instanceof List<?>) || ((List<?>) professionalDetailsObj).isEmpty()) {
            return;
        }
        List<?> rawList = (List<?>) professionalDetailsObj;
        Object firstItem = rawList.get(0);
        if (firstItem instanceof Map<?, ?> detailsMap) {
            userProfile.put(Constants.DESIGNATION, (String) detailsMap.get(Constants.DESIGNATION));
            userProfile.put(Constants.GROUP, (String) detailsMap.get(Constants.GROUP));
        }
    }

    private void extractCadreDetails(Map<String, String> userProfile, Map<String, Object> profileDetails) {
        Object cadreDetailsObj = profileDetails.get(Constants.CADRE_DETAILS);
        boolean centralDeputation = false;
        if (cadreDetailsObj instanceof Map<?, ?> cadreMap) {
            userProfile.put(Constants.CADRE, (String) cadreMap.get(Constants.CADRE_NAME));
            userProfile.put(Constants.SERVICE, (String) cadreMap.get(Constants.CIVIL_SERVICE_NAME));
            if (cadreMap.containsKey(Constants.CADRE_BATCH)) {
                userProfile.put(Constants.BATCH, String.valueOf(cadreMap.get(Constants.CADRE_BATCH)));
            }
            if (cadreMap.containsKey(Constants.CENTRAL_DEPUTATION)) {
                centralDeputation = Boolean.TRUE.equals(cadreMap.get(Constants.CENTRAL_DEPUTATION));
            }
        }
        userProfile.put(Constants.CENTRAL_DEPUTATION_LOWER_KEY, String.valueOf(centralDeputation));
    }

    /**
     * Fetches active LIVE CB Plans for the user's org (and ministry if applicable).
     * Uses the same V3 lookup tables since V4 plans are stored there too.
     */
    private List<Map<String, Object>> fetchPlansForUser(Map<String, String> userProfile, String userOrgId,
                                                        String planYear, AtomicBoolean isCacheEnabled) {
        List<Map<String, Object>> orgPlans = cbPlanCacheMgrV3.getCbPlanForAllAndOrgId(userOrgId, planYear, isCacheEnabled);
        String ministryOrStateId = userProfile.get(Constants.MINISTRY_OR_STATE_ID_RQST);
        if (StringUtils.isBlank(ministryOrStateId)) {
            return orgPlans;
        }
        log.info("fetchPlansForUser: Fetching ministry plans - ministryOrStateId={}", ministryOrStateId);
        List<Map<String, Object>> ministryPlans = cbPlanCacheMgrV3.getCbPlanForMinistryOrStateId(ministryOrStateId, planYear);
        return dataTransformService.mergePlanLists(orgPlans, ministryPlans);
    }

    /**
     * Pre-fetches all V4 userGroupIds referenced across the plan list, grouped by each plan's
     * creator orgId (orgidlist[0]). User groups are partitioned by their creator org in Cassandra,
     * so the lookup must use the plan's orgId, not the requesting user's orgId.
     */
    private Map<String, Map<String, Object>> batchFetchUserGroupsForPlans(List<Map<String, Object>> plans,
                                                                          Map<String, Map<String, Object>> parsedContextData) {
        Map<String, Set<String>> orgToGroupIds = new HashMap<>();
        for (Map<String, Object> plan : plans) {
            String planOrgId = extractCreatedByOrgId(plan);
            if (StringUtils.isBlank(planOrgId)) {
                continue;
            }
            Set<String> groupIds = new HashSet<>();
            collectV4UserGroupIds(plan, groupIds, parsedContextData);
            if (CollectionUtils.isNotEmpty(groupIds)) {
                orgToGroupIds.computeIfAbsent(planOrgId, k -> new HashSet<>()).addAll(groupIds);
            }
        }
        if (orgToGroupIds.isEmpty()) {
            return Collections.emptyMap();
        }
        int totalGroups = orgToGroupIds.values().stream().mapToInt(Set::size).sum();
        log.debug("batchFetchUserGroupsForPlans: Pre-fetching {} user groups across {} plan orgIds",
                totalGroups, orgToGroupIds.size());
        Map<String, Map<String, Object>> groups = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : orgToGroupIds.entrySet()) {
            groups.putAll(userGroupLookupService.fetchUserGroupsByIds(
                    new ArrayList<>(entry.getValue()), entry.getKey()));
        }
        normalizeCriteriaKeysInGroups(groups);
        return groups;
    }

    private void collectV4UserGroupIds(Map<String, Object> plan, Set<String> userGroupIds,
                                        Map<String, Map<String, Object>> parsedContextData) {
        String planId = (String) plan.get(Constants.PLAN_ID);
        Map<String, Object> contextData = parsedContextData.get(planId);
        if (MapUtils.isEmpty(contextData)) {
            return;
        }
        Object accessControlObj = contextData.get(Constants.ACCESS_CONTROL);
        if (!(accessControlObj instanceof Map<?, ?>)) {
            return;
        }
        Object userGroupsObj = ((Map<?, ?>) accessControlObj).get(Constants.USER_GROUPS);
        if (!(userGroupsObj instanceof List<?>)) {
            return;
        }
        for (Object userGroupObj : (List<?>) userGroupsObj) {
            if (userGroupObj instanceof Map<?, ?> userGroupMap) {
                String id = (String) userGroupMap.get(Constants.USER_GROUP_ID);
                if (StringUtils.isNotBlank(id)) {
                    userGroupIds.add(id);
                }
            }
        }
    }

    /**
     * Iterates all plans, evaluates access control, and partitions them into APAR and non-APAR maps.
     */
    private void processPlans(List<Map<String, Object>> plans,
                              Map<String, String> userProfile,
                              Map<String, Map<String, Object>> prefetchedUserGroups,
                              Map<String, Map<String, Object>> parsedContextData,
                              Map<String, Map<String, Object>> aparPlanMap,
                              Map<String, Map<String, Object>> nonAparPlanMap) {
        for (Map<String, Object> plan : plans) {
            try {
                if (!evaluateAccessControl(plan, userProfile, prefetchedUserGroups, parsedContextData)) {
                    continue;
                }
                String planId = (String) plan.get(Constants.PLAN_ID);
                boolean isApar = Boolean.TRUE.equals(plan.get(Constants.IS_APAR));
                Map<String, Object> planEntry = buildPlanEntry(plan);
                if (isApar) {
                    aparPlanMap.put(planId, planEntry);
                } else {
                    nonAparPlanMap.put(planId, planEntry);
                }
            } catch (Exception e) {
                log.error("processPlans: Failed to process plan={}", plan.get(Constants.PLAN_ID), e);
            }
        }
    }

    /**
     * Evaluates whether the user has access to a plan.
     * Detects V3 inline format vs V4 userGroupId references and dispatches accordingly.
     */
    private boolean evaluateAccessControl(Map<String, Object> plan,
                                          Map<String, String> userProfile,
                                              Map<String, Map<String, Object>> prefetchedUserGroups,
                                          Map<String, Map<String, Object>> parsedContextData) {
        String planId = (String) plan.get(Constants.PLAN_ID);
        if (!parsedContextData.containsKey(planId)) {
            return Objects.isNull(plan.get(Constants.CONTEXT_DATA_REQUEST));
        }
        Map<String, Object> contextData = parsedContextData.get(planId);
        if (MapUtils.isEmpty(contextData)) {
            return true;
        }
        Object accessControlObj = contextData.get(Constants.ACCESS_CONTROL);
        if (!(accessControlObj instanceof Map<?, ?>)) {
            return false;
        }
        Object userGroupsObj = ((Map<?, ?>) accessControlObj).get(Constants.USER_GROUPS);
        if (!(userGroupsObj instanceof List<?> rawList) || rawList.isEmpty()) {
            return false;
        }
        List<Map<String, Object>> userGroups = castToMapList(rawList);
        boolean hasV4Format = userGroups.stream().anyMatch(g -> g.containsKey(Constants.USER_GROUP_ID));
        if (hasV4Format) {
            return evaluateV4UserGroups(userGroups, userProfile, prefetchedUserGroups);
        }
        return evaluateV3InlineCriteria(userGroups, userProfile);
    }

    /**
     * Evaluates V4 access control by matching each referenced user group's criteria against the user's profile.
     * A user has access if they match ALL criteria in at least ONE user group (OR-across-groups, AND-within-group).
     */
    private boolean evaluateV4UserGroups(List<Map<String, Object>> userGroups,
                                         Map<String, String> userProfile,
                                         Map<String, Map<String, Object>> prefetchedUserGroups) {
        for (Map<String, Object> userGroup : userGroups) {
            String userGroupId = (String) userGroup.get(Constants.USER_GROUP_ID);
            if (StringUtils.isNotBlank(userGroupId)) {
                Map<String, Object> groupEntity = prefetchedUserGroups.get(userGroupId);
                if (MapUtils.isNotEmpty(groupEntity)) {
                    if (matchesUserGroupCriteria(groupEntity, userProfile)) {
                        return true;
                    }
                } else {
                    log.debug("evaluateV4UserGroups: User group not found - userGroupId={}", userGroupId);
                }
            }
        }
        return false;
    }

    /**
     * Checks whether the user's profile satisfies all criteria declared in the user group entity.
     */
    private boolean matchesUserGroupCriteria(Map<String, Object> groupEntity, Map<String, String> userProfile) {
        List<Map<String, Set<String>>> criteriaList =
                (List<Map<String, Set<String>>>) groupEntity.get(Constants.COL_CRITERIA);
        if (CollectionUtils.isEmpty(criteriaList)) {
            return false;
        }
        for (Map<String, Set<String>> criteriaEntry : criteriaList) {
            if (!matchesSingleGroupCriteria(criteriaEntry, userProfile)) {
                return false;
            }
        }
        return true;
    }

    private boolean matchesSingleGroupCriteria(Map<String, Set<String>> criteriaEntry,
                                               Map<String, String> userProfile) {
        for (Map.Entry<String, Set<String>> entry : criteriaEntry.entrySet()) {
            String criteriaKey = entry.getKey();
            Set<String> allowedValues = entry.getValue();
            if (CollectionUtils.isEmpty(allowedValues)) {
                return false;
            }
            if (Constants.CENTRAL_DEPUTATION_LOWER_KEY.equals(criteriaKey)) {
                boolean expected = Boolean.parseBoolean(allowedValues.iterator().next());
                boolean actual = Boolean.parseBoolean(
                        userProfile.getOrDefault(Constants.CENTRAL_DEPUTATION_LOWER_KEY, "false"));
                if (expected != actual) {
                    return false;
                }
                continue;
            }
            String actualValue = userProfile.get(criteriaKey);
            if (Objects.isNull(actualValue)) {
                return false;
            }
            if (!allowedValues.contains(actualValue.toLowerCase())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Evaluates V3 inline access control criteria against the user's profile.
     */
    private boolean evaluateV3InlineCriteria(List<Map<String, Object>> userGroups, Map<String, String> userProfile) {
        for (Map<String, Object> userGroup : userGroups) {
            if (matchesV3UserGroup(userGroup, userProfile)) {
                return true;
            }
        }
        return false;
    }

    private boolean matchesV3UserGroup(Map<String, Object> userGroup, Map<String, String> userProfile) {
        List<Map<String, Object>> criteriaList =
                (List<Map<String, Object>>) userGroup.get(Constants.USER_GROUP_CRITERIA_LIST);
        if (CollectionUtils.isEmpty(criteriaList)) {
            return false;
        }
        for (Map<String, Object> criteria : criteriaList) {
            if (!matchesV3Criteria(criteria, userProfile)) {
                return false;
            }
        }
        return true;
    }

    private boolean matchesV3Criteria(Map<String, Object> criteria, Map<String, String> userProfile) {
        String criteriaKey = (String) criteria.get(Constants.CRITERIA_KEY);
        Object rawCriteriaValue = criteria.get(Constants.CRITERIA_VALUE);
        if (Constants.CENTRAL_DEPUTATION_LOWER_KEY.equals(criteriaKey)) {
            String firstValue = rawCriteriaValue instanceof Set<?> s && !s.isEmpty()
                    ? (String) s.iterator().next()
                    : String.valueOf(rawCriteriaValue);
            boolean expected = Boolean.parseBoolean(firstValue);
            boolean actual = Boolean.parseBoolean(
                    userProfile.getOrDefault(Constants.CENTRAL_DEPUTATION_LOWER_KEY, "false"));
            return expected == actual;
        }
        if (!(rawCriteriaValue instanceof Set<?> expectedSet) || expectedSet.isEmpty()) {
            return false;
        }
        String actualValue = userProfile.get(criteriaKey);
        if (Objects.isNull(actualValue)) {
            return false;
        }
        return ((Set<String>) expectedSet).contains(actualValue.toLowerCase());
    }

    /**
     * Builds the plan entry map for the dictionary response.
     * ContentList items stored as JSON strings are parsed back to {identifier, mandatory} objects.
     */
    private Map<String, Object> buildPlanEntry(Map<String, Object> plan) {
        Map<String, Object> entry = new LinkedHashMap<>();
        String planId = (String) plan.get(Constants.PLAN_ID);
        entry.put(Constants.PLAN_ID, planId);
        entry.put(Constants.NAME, plan.get(Constants.NAME));
        entry.put(Constants.END_DATE_REQUEST, plan.get(Constants.END_DATE_REQUEST));
        entry.put(Constants.PLAN_TYPE, plan.get(Constants.PLAN_TYPE));
        entry.put(Constants.CONTENT_LIST, parseContentList(plan.get(Constants.CONTENT_LIST)));
        entry.put(Constants.CA_LINKED_ID, plan.get(Constants.CA_LINKED_ID_DB));
        String createdByOrgId = extractCreatedByOrgId(plan);
        entry.put(Constants.CREATED_BY_ORG_ID, createdByOrgId);
        entry.put(Constants.CREATED_BY_ORG_NAME, null);
        entry.put(Constants.CREATED_BY_ORG_LOGO, null);
        return entry;
    }

    private String extractCreatedByOrgId(Map<String, Object> plan) {
        Object orgIdListObj = plan.get(Constants.ORG_ID_LIST);
        if (orgIdListObj instanceof List<?> rawList && !rawList.isEmpty()) {
            return (String) rawList.get(0);
        }
        return null;
    }

    /**
     * Parses the contentList stored in Cassandra.
     * BACKWARD COMPATIBLE: Handles both old (JSON strings/plain IDs) and new (nested objects) formats.
     * - If already objects → return as-is
     * - If JSON strings → parse to objects
     * - If plain strings → convert to objects with mandatory=false
     *
     * @param contentListObj contentList from Cassandra
     * @return list of V4 content objects with identifier and mandatory fields
     */
    private List<Map<String, Object>> parseContentList(Object contentListObj) {
        if (!(contentListObj instanceof List<?> rawList) || rawList.isEmpty()) {
            return Collections.emptyList();
        }
        if (isAlreadyObjectFormat(rawList)) {
            log.debug("ContentList already in object format, skipping transformation");
            return (List<Map<String, Object>>) rawList;
        }
        log.debug("ContentList in old format, applying transformation");
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object item : rawList) {
            if (Objects.isNull(item)) {
                continue;
            }
            result.add(toContentItem(String.valueOf(item)));
        }
        return result;
    }

    /**
     * Collects all unique createdByOrgId values across both plan maps for batch org-name lookup.
     */
    private Set<String> collectCreatedByOrgIds(Map<String, Map<String, Object>> aparPlanMap,
                                               Map<String, Map<String, Object>> nonAparPlanMap) {
        Set<String> orgIds = new HashSet<>();
        collectOrgIdsFromMap(aparPlanMap, orgIds);
        collectOrgIdsFromMap(nonAparPlanMap, orgIds);
        return orgIds;
    }

    private void collectOrgIdsFromMap(Map<String, Map<String, Object>> planMap, Set<String> orgIds) {
        for (Map<String, Object> entry : planMap.values()) {
            String orgId = (String) entry.get(Constants.CREATED_BY_ORG_ID);
            if (StringUtils.isNotBlank(orgId)) {
                orgIds.add(orgId);
            }
        }
    }

    /**
     * Batch fetches org details (name and logo) for the given org IDs using a single Cassandra IN query.
     *
     * @param orgIds org IDs to resolve
     * @return map of orgId to org details (name, logo); missing entries for orgs not found
     */
    private Map<String, Map<String, String>> fetchOrgDetails(Set<String> orgIds) {
        if (CollectionUtils.isEmpty(orgIds)) {
            return Collections.emptyMap();
        }
        Map<String, Map<String, String>> orgDetailsMap = new HashMap<>();
        try {
            Map<String, Object> queryMap = Map.of(Constants.ID, new ArrayList<>(orgIds));
            List<Map<String, Object>> orgList = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.ORG_TABLE, queryMap,
                    List.of(Constants.ID, Constants.ORG_NAME, Constants.LOGO), null);
            if (CollectionUtils.isNotEmpty(orgList)) {
                for (Map<String, Object> org : orgList) {
                    String id = (String) org.get(Constants.ID);
                    String name = (String) org.get(Constants.ORG_NAME);
                    String logo = (String) org.get(Constants.LOGO);
                    if (StringUtils.isNotBlank(id)) {
                        Map<String, String> details = new HashMap<>();
                        details.put(Constants.ORG_NAME, name);
                        details.put(Constants.LOGO, logo);
                        orgDetailsMap.put(id, details);
                    }
                }
            }
            log.debug("fetchOrgDetails: Resolved {} org details for {} org IDs", orgDetailsMap.size(), orgIds.size());
        } catch (Exception e) {
            log.error("fetchOrgDetails: Failed to batch fetch org details for {} orgs", orgIds.size(), e);
        }
        return orgDetailsMap;
    }

    /**
     * Enriches plan entries in the given map with resolved org names and logos.
     */
    private void enrichOrgDetails(Map<String, Map<String, Object>> planMap, Map<String, Map<String, String>> orgDetailsMap) {
        for (Map<String, Object> entry : planMap.values()) {
            String orgId = (String) entry.get(Constants.CREATED_BY_ORG_ID);
            if (StringUtils.isNotBlank(orgId) && orgDetailsMap.containsKey(orgId)) {
                Map<String, String> orgDetails = orgDetailsMap.get(orgId);
                entry.put(Constants.CREATED_BY_ORG_NAME, orgDetails.get(Constants.ORG_NAME));
                entry.put(Constants.CREATED_BY_ORG_LOGO, orgDetails.get(Constants.LOGO));
            }
        }
    }

    /**
     * Builds the plan-year result map with counts and plan lists.
     */
    private Map<String, Object> buildYearResult(Map<String, Map<String, Object>> aparPlanMap,
                                                Map<String, Map<String, Object>> nonAparPlanMap) {
        Map<String, Object> yearResult = new LinkedHashMap<>();
        yearResult.put(Constants.RESPONSE_KEY_APAR_PLAN_COUNT, aparPlanMap.size());
        yearResult.put(Constants.RESPONSE_KEY_APAR_PLAN_LIST, aparPlanMap);
        yearResult.put(Constants.RESPONSE_KEY_NON_APAR_PLAN_COUNT, nonAparPlanMap.size());
        yearResult.put(Constants.RESPONSE_KEY_NON_APAR_PLAN_LIST, nonAparPlanMap);
        return yearResult;
    }

    private void populateEmptyResponse(ApiResponse response, String planYear) {
        response.getResult().put(planYear, buildYearResult(new LinkedHashMap<>(), new LinkedHashMap<>()));
        response.setParams(new ApiRespParam());
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
    }

    private void populateResponseFromCache(ApiResponse response, String cachedJson, String planYear) {
        try {
            Map<String, Object> cachedResult = mapper.readValue(cachedJson, MAP_TYPE_REF);
            response.getResult().putAll(cachedResult);
            response.setParams(new ApiRespParam());
            response.getParams().setStatus(Constants.SUCCESS);
            response.setResponseCode(HttpStatus.OK);
        } catch (JsonProcessingException e) {
            log.warn("populateResponseFromCache: Failed to deserialize cache for planYear={}", planYear, e);
            response.getResult().put(planYear, buildYearResult(new LinkedHashMap<>(), new LinkedHashMap<>()));
            response.setParams(new ApiRespParam());
            response.getParams().setStatus(Constants.SUCCESS);
            response.setResponseCode(HttpStatus.OK);
        }
    }

    private void cacheResult(String cacheKey, Map<String, Object> result, boolean isCacheEnabled) {
        if (!isCacheEnabled) {
            return;
        }
        try {
            String json = mapper.writeValueAsString(result);
            redisCacheMgr.putInCache(cacheKey, json, serverProperties.getCbPlanV3RedisCacheTtlSeconds());
            log.debug("cacheResult: Cached dictionary result - key={}", cacheKey);
        } catch (JsonProcessingException e) {
            log.warn("cacheResult: Failed to serialize result for caching - key={}", cacheKey, e);
        }
    }

    private Map<String, Object> parseContextDataObj(Object contextDataObj) throws JsonProcessingException {
        if (contextDataObj instanceof String str && StringUtils.isNotBlank(str)) {
            return mapper.readValue(str, MAP_TYPE_REF);
        } else if (contextDataObj instanceof Map<?, ?>) {
            return (Map<String, Object>) contextDataObj;
        }
        return Map.of();
    }

    /**
     * Extracts request data map from ApiRequest with type safety.
     * Suppresses unchecked warning as this is a controlled cast of the request object.
     */
    private Map<String, Object> extractRequestData(ApiRequest request) {
        if (request.getRequest() instanceof Map<?, ?> requestMap) {
            return (Map<String, Object>) requestMap;
        }
        return new HashMap<>();
    }

    /**
     * Safely casts a raw list to List<Map<String, Object>>.
     * Suppresses unchecked warning at a single controlled location.
     */
    private List<Map<String, Object>> castToMapList(List<?> rawList) {
        return (List<Map<String, Object>>) rawList;
    }


    /**
     * Checks if contentList is already in the new object format (post-migration).
     * Returns true if first item is a Map with "identifier" key.
     *
     * @param rawList list to check
     * @return true if already object format, false if old string format
     */
    private boolean isAlreadyObjectFormat(List<?> rawList) {
        if (CollectionUtils.isEmpty(rawList)) {
            return false;
        }
        Object firstItem = rawList.get(0);
        if (firstItem instanceof Map<?, ?> firstMap) {
            return firstMap.containsKey(Constants.IDENTIFIER);
        }
        return false;
    }

    /**
     * Fetches plans for the previous year and appends the result to the response.
     * If no plans are found for the previous year, an empty entry is still added.
     */
    private void fetchAndAppendPreviousYear(ApiResponse response, Map<String, String> userProfile,
                                            String userOrgId, String previousYear) {
        AtomicBoolean prevCacheEnabled = new AtomicBoolean(false);
        List<Map<String, Object>> prevPlans = fetchPlansForUser(userProfile, userOrgId, previousYear, prevCacheEnabled);
        if (CollectionUtils.isEmpty(prevPlans)) {
            log.info("fetchAndAppendPreviousYear: No plans found - previousYear={}", previousYear);
            response.getResult().put(previousYear, buildYearResult(new LinkedHashMap<>(), new LinkedHashMap<>()));
            return;
        }
        Map<String, Map<String, Object>> aparPlanMap = new LinkedHashMap<>();
        Map<String, Map<String, Object>> nonAparPlanMap = new LinkedHashMap<>();
        buildPlanPartitions(prevPlans, userProfile, aparPlanMap, nonAparPlanMap);
        Set<String> orgIds = collectCreatedByOrgIds(aparPlanMap, nonAparPlanMap);
        Map<String, Map<String, String>> orgDetailsMap = fetchOrgDetails(orgIds);
        enrichOrgDetails(aparPlanMap, orgDetailsMap);
        enrichOrgDetails(nonAparPlanMap, orgDetailsMap);
        filterLiveContentInPlans(aparPlanMap, nonAparPlanMap);
        response.getResult().put(previousYear, buildYearResult(aparPlanMap, nonAparPlanMap));
        log.info("fetchAndAppendPreviousYear: previousYear={}, aparCount={}, nonAparCount={}",
                previousYear, aparPlanMap.size(), nonAparPlanMap.size());
    }

    /**
     * Parses contextData for every plan once and returns the results keyed by planId.
     * Plans whose contextData fails to parse are excluded — evaluateAccessControl treats
     * absence as a parse error and denies access for that plan.
     */
    private Map<String, Map<String, Object>> parseContextDataForPlans(List<Map<String, Object>> plans) {
        Map<String, Map<String, Object>> result = new HashMap<>();
        for (Map<String, Object> plan : plans) {
            String planId = (String) plan.get(Constants.PLAN_ID);
            Object contextDataObj = plan.get(Constants.CONTEXT_DATA_REQUEST);
            if (StringUtils.isBlank(planId) || Objects.isNull(contextDataObj)) {
                continue;
            }
            try {
                Map<String, Object> parsed = parseContextDataObj(contextDataObj);
                normalizeV3CriteriaKeyValues(parsed);
                result.put(planId, parsed);
            } catch (JsonProcessingException e) {
                log.debug("parseContextDataForPlans: Failed to parse contextData for planId={}", planId, e);
            }
        }
        return result;
    }

    /**
     * Converts a raw content list entry to a V4 content item map without exception-as-control-flow.
     * V3 plain IDs (e.g. {@code do_123}) are detected by the absence of a leading '{' and returned
     * as {@code {identifier, mandatory=false}} with no Jackson call.
     * V4 JSON objects are parsed; if they carry an {@code identifier} key they are returned as-is,
     * otherwise fall back to plain-ID form.
     *
     * @param itemStr string representation of one content list entry
     * @return content item with at minimum identifier and mandatory fields
     */
    private Map<String, Object> toContentItem(String itemStr) {
        try {
            Object parsed = mapper.readValue(itemStr, Object.class);
            if (parsed instanceof Map<?, ?> parsedMap && parsedMap.containsKey(Constants.IDENTIFIER)) {
                return (Map<String, Object>) parsedMap;
            }
        } catch (JsonProcessingException e) {
            log.debug("toContentItem: not valid JSON, treating as plain ID - item={}", itemStr);
        }
        return buildPlainContentItem(itemStr);
    }

    /**
     * Builds a minimal content item map for a plain identifier with {@code mandatory=false}.
     *
     * @param identifier content identifier
     * @return map with identifier and mandatory fields
     */
    private Map<String, Object> buildPlainContentItem(String identifier) {
        Map<String, Object> item = new HashMap<>();
        item.put(Constants.IDENTIFIER, identifier);
        item.put(Constants.MANDATORY, false);
        return item;
    }

    private void buildPlanPartitions(List<Map<String, Object>> plans,
                                     Map<String, String> userProfile,
                                     Map<String, Map<String, Object>> aparPlanMap,
                                     Map<String, Map<String, Object>> nonAparPlanMap) {
        Map<String, Map<String, Object>> parsedContextData = parseContextDataForPlans(plans);
        Map<String, Map<String, Object>> prefetchedUserGroups = batchFetchUserGroupsForPlans(plans, parsedContextData);
        processPlans(plans, userProfile, prefetchedUserGroups, parsedContextData, aparPlanMap, nonAparPlanMap);
    }

    /**
     * Normalizes criteria map keys to lowercase-trimmed form in all fetched V4 user groups once,
     * so {@code matchesSingleGroupCriteria} can look up keys directly without per-call allocation.
     *
     * @param groups mutable map of userGroupId → group entity returned by the lookup service
     */
    private void normalizeCriteriaKeysInGroups(Map<String, Map<String, Object>> groups) {
        for (Map<String, Object> group : groups.values()) {
            Object criteriaObj = group.get(Constants.COL_CRITERIA);
            if (!(criteriaObj instanceof List<?> rawList) || rawList.isEmpty()) {
                continue;
            }
            List<Map<String, Set<String>>> normalized = new ArrayList<>(rawList.size());
            for (Object item : rawList) {
                if (!(item instanceof Map<?, ?> criteriaMap)) {
                    continue;
                }
                Map<String, Set<String>> normalizedEntry = new LinkedHashMap<>(criteriaMap.size());
                for (Map.Entry<?, ?> entry : criteriaMap.entrySet()) {
                    String normalizedKey = entry.getKey().toString().toLowerCase().trim();
                    normalizedEntry.put(normalizedKey, buildNormalizedValueSet(entry.getValue()));
                }
                normalized.add(normalizedEntry);
            }
            group.put(Constants.COL_CRITERIA, normalized);
        }
    }

    /**
     * Normalizes V3 criteriaKey values to lowercase-trimmed form in parsed contextData once,
     * so {@code matchesV3Criteria} can use the key directly without per-call allocation.
     *
     * @param contextData mutable contextData map parsed from the plan's contextData JSON
     */
    private void normalizeV3CriteriaKeyValues(Map<String, Object> contextData) {
        Object accessControlObj = contextData.get(Constants.ACCESS_CONTROL);
        if (!(accessControlObj instanceof Map<?, ?> accessControl)) {
            return;
        }
        Object userGroupsObj = accessControl.get(Constants.USER_GROUPS);
        if (!(userGroupsObj instanceof List<?> userGroupsList)) {
            return;
        }
        for (Object userGroupObj : userGroupsList) {
            if (userGroupObj instanceof Map<?, ?> userGroup) {
                normalizeUserGroupCriteria(userGroup);
            }
        }
    }

    private void normalizeUserGroupCriteria(Map<?, ?> userGroup) {
        Object criteriaListObj = userGroup.get(Constants.USER_GROUP_CRITERIA_LIST);
        if (!(criteriaListObj instanceof List<?> criteriaList)) {
            return;
        }
        for (Object criteriaObj : criteriaList) {
            if (!(criteriaObj instanceof Map<?, ?> criteria)
                    || !(criteria.get(Constants.CRITERIA_KEY) instanceof String criteriaKey)) {
                continue;
            }
            Map<String, Object> criteriaMap = (Map<String, Object>) criteria;
            criteriaMap.put(Constants.CRITERIA_KEY, criteriaKey.toLowerCase().trim());
            criteriaMap.put(Constants.CRITERIA_VALUE, buildNormalizedValueSet(criteriaMap.get(Constants.CRITERIA_VALUE)));
        }
    }

    /**
     * Converts a raw criteria value (List or scalar) to a lowercase-trimmed Set<String>
     * for O(1) contains lookups during criteria evaluation, replacing the per-call stream pipeline.
     *
     * @param rawValue raw value from Cassandra or Jackson (may be List, String, or Boolean)
     * @return set of lowercase-trimmed string values; empty set if null
     */
    private Set<String> buildNormalizedValueSet(Object rawValue) {
        if (rawValue instanceof List<?> list) {
            Set<String> result = new HashSet<>(list.size());
            for (Object v : list) {
                if (v != null) {
                    result.add(v.toString().toLowerCase());
                }
            }
            return result;
        }
        if (rawValue != null) {
            return Collections.singleton(rawValue.toString().toLowerCase());
        }
        return Collections.emptySet();
    }

    /**
     * Removes plans whose caLinkedId is present but resolves to a non-Live status.
     * Plans with a null/blank caLinkedId are kept — they are valid training plans with no CA link.
     * Each unique caLinkedId is resolved exactly once via extended content read.
     */
    private void filterLiveContentInPlans(Map<String, Map<String, Object>> aparPlanMap,
                                           Map<String, Map<String, Object>> nonAparPlanMap) {
        Set<String> caIds = Stream.concat(aparPlanMap.values().stream(), nonAparPlanMap.values().stream())
                .map(e -> e.get(Constants.CA_LINKED_ID))
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toSet());
        Set<String> liveIds = caIds.isEmpty() ? Collections.emptySet() : resolveLiveCaLinkedIds(caIds);
        log.debug("filterLiveContentInPlans: liveCount={}, totalCount={}", liveIds.size(), caIds.size());
        aparPlanMap.entrySet().removeIf(e -> isLiveCaLinkedId(e.getValue(), liveIds));
        nonAparPlanMap.entrySet().removeIf(e -> isLiveCaLinkedId(e.getValue(), liveIds));
    }

    private Set<String> resolveLiveCaLinkedIds(Set<String> caIds) {
        Set<String> liveIds = new HashSet<>();
        for (String id : caIds) {
            try {
                Map<String, Object> meta = contentLookupService.getContentMetadata(id);
                if (MapUtils.isNotEmpty(meta) && Constants.LIVE.equalsIgnoreCase((String) meta.get(Constants.STATUS))) {
                    liveIds.add(id);
                } else {
                    log.debug("resolveLiveCaLinkedIds: Non-Live caLinkedId={}, status={}", id,
                            MapUtils.isNotEmpty(meta) ? meta.get(Constants.STATUS) : "not found");
                }
            } catch (Exception e) {
                log.warn("resolveLiveCaLinkedIds: Metadata read failed - caLinkedId={}", id, e);
            }
        }
        return liveIds;
    }

    private boolean isLiveCaLinkedId(Map<String, Object> planEntry, Set<String> liveIds) {
        Object caId = planEntry.get(Constants.CA_LINKED_ID);
        if (!(caId instanceof String id) || StringUtils.isBlank(id)) {
            return false;
        }
        return !liveIds.contains(id);
    }

}
