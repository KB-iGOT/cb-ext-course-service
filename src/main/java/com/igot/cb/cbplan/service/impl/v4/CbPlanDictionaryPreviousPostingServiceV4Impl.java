package com.igot.cb.cbplan.service.impl.v4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.igot.cb.cache.CbPlanCacheMgrV4;
import com.igot.cb.cache.CbPlanCacheVersions;
import com.igot.cb.cache.CbPlanVersionMgr;
import com.igot.cb.cache.CbExtRedisCacheMgr;
import com.igot.cb.cache.UserGroupCacheMgrV4;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.cbplan.service.impl.CbPlanContentLookupServiceV3Impl;
import com.igot.cb.cbplan.service.impl.CbPlanDataTransformServiceV3Impl;
import com.igot.cb.cbplan.service.impl.CbPlanEnrichmentServiceV3Impl;
import com.igot.cb.cbplan.util.CbPlanYearUtil;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiRespParam;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * CB Plan V4 user dictionary for the "previous posting" case: when a caller supplies
 * {@code previousRootOrgId} and/or {@code previousDesignation}, plans are listed for THAT posting.
 * The supplied values replace the user's own rootOrgId and designation, and access control is
 * evaluated against the resulting profile only - the user's current posting is not separately
 * matched, so plans they would normally see are absent unless the supplied posting also satisfies
 * them.
 * <p>
 * This is a self-contained copy of the standard dictionary pipeline (fetch, context-data parse,
 * user-group prefetch, rule evaluation, APAR/non-APAR partition, org enrichment, live-content
 * filtering) deliberately kept separate from {@code CbPlanDictionaryServiceV4Impl} so the current
 * production dictionary path is left untouched. Two intentional differences from that service:
 * <ul>
 *   <li>access control is evaluated against the supplied posting rather than the user's own, and</li>
 *   <li>results are cached under the standard dictionary key with {@code prevdict} in place of
 *       {@code dict}, so this endpoint's entries can never overwrite or be served in place of the
 *       standard dictionary's.</li>
 * </ul>
 *
 * @version 4.0
 */
@Service
@Slf4j
public class CbPlanDictionaryPreviousPostingServiceV4Impl {

    private final CassandraOperation cassandraOperation;
    private final CbPlanCacheMgrV4 cbPlanCacheMgrV4;
    private final CbPlanVersionMgr versionMgr;
    private final UserGroupCacheMgrV4 userGroupCacheMgrV4;
    private final AccessTokenValidator accessTokenValidator;
    private final CbExtRedisCacheMgr userProfileRedisCacheMgr;
    private final CbExtRedisCacheMgr redisCacheMgr;
    private final CbExtServerProperties serverProperties;
    private final CbPlanEnrichmentServiceV3Impl enrichmentService;
    private final CbPlanDataTransformServiceV3Impl dataTransformService;
    private final CbPlanContentLookupServiceV3Impl contentLookupService;
    private final ObjectMapper mapper;
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF = new TypeReference<>() {
    };

    public CbPlanDictionaryPreviousPostingServiceV4Impl(CassandraOperation cassandraOperation,
                                                        CbPlanCacheMgrV4 cbPlanCacheMgrV4,
                                                        CbPlanVersionMgr versionMgr,
                                                        UserGroupCacheMgrV4 userGroupCacheMgrV4,
                                                        AccessTokenValidator accessTokenValidator,
                                                        @Qualifier("userProfileRedisCacheMgr") CbExtRedisCacheMgr userProfileRedisCacheMgr,
                                                        @Qualifier("cbPlanRedisCacheMgr") CbExtRedisCacheMgr redisCacheMgr,
                                                        CbExtServerProperties serverProperties,
                                                        CbPlanEnrichmentServiceV3Impl enrichmentService,
                                                        CbPlanDataTransformServiceV3Impl dataTransformService,
                                                        CbPlanContentLookupServiceV3Impl contentLookupService) {
        this.cassandraOperation = cassandraOperation;
        this.cbPlanCacheMgrV4 = cbPlanCacheMgrV4;
        this.versionMgr = versionMgr;
        this.userGroupCacheMgrV4 = userGroupCacheMgrV4;
        this.accessTokenValidator = accessTokenValidator;
        this.userProfileRedisCacheMgr = userProfileRedisCacheMgr;
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
     * Returns the active CB Plans the supplied posting grants access to, grouped by APAR/non-APAR.
     * Results are read from and written to this endpoint's own cache key, which is scoped to the
     * supplied posting and therefore never shares an entry with the standard dictionary.
     *
     * @param request   API request containing planYear and previousRootOrgId/previousDesignation
     * @param authToken authentication token
     * @return ApiResponse with aparPlanList and nonAparPlanList keyed by planYear
     */
    public ApiResponse getDictionaryForUserWithPreviousPosting(ApiRequest request, String authToken) {
        log.debug("getDictionaryForUserWithPreviousPosting: Entry");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CBPLAN_V4_GET_USER_DICTIONARY);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isBlank(userId)) {
                log.warn("getDictionaryForUserWithPreviousPosting: Invalid or missing token");
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
            Map<String, String> userProfile = buildUserProfile(userId, response);
            if (userProfile.isEmpty()) {
                return response;
            }
            Map<String, String> evaluationProfile = resolveEvaluationProfile(requestData, userProfile);
            String userOrgId = userProfile.get(Constants.USER_ROOT_ORG_ID);
            EvaluationContext context = new EvaluationContext(userProfile, evaluationProfile, planYear);
            CbPlanCacheVersions versions = versionMgr.getVersions(userOrgId,
                    userProfile.get(Constants.MINISTRY_OR_STATE_ID_RQST), planYear);
            String cacheKey = buildPreviousPostingCacheKey(userId, userOrgId, planYear);
            Optional<Map<String, Object>> cached = versionMgr.readValidatedDict(cacheKey, versions);
            if (cached.isPresent()) {
                log.info("getDictionaryForUserWithPreviousPosting: Cache hit - userId={}, planYear={}, key={}",
                        userId, planYear, cacheKey);
                populateResponseFromCache(response, cached.get());
                return response;
            }
            log.info("getDictionaryForUserWithPreviousPosting: Cache miss - userId={}, orgId={}, planYear={}, key={}",
                    userId, userOrgId, planYear, cacheKey);
            AtomicBoolean isCacheEnabled = new AtomicBoolean(false);
            CbPlanCacheVersions evaluatedOrgVersions = resolveEvaluatedOrgVersions(context, userOrgId, versions);
            List<Map<String, Object>> activePlans = fetchCandidatePlans(context, userOrgId, isCacheEnabled, versions,
                    evaluatedOrgVersions);
            if (CollectionUtils.isEmpty(activePlans)) {
                log.info("getDictionaryForUserWithPreviousPosting: No active plans - userId={}, planYear={}",
                        userId, planYear);
                populateNoActivePlansResponse(response, context, userOrgId, requestedPlanYear);
                cacheResult(cacheKey, response.getResult(), isCacheEnabled.get(), versions);
                return response;
            }
            boolean noneAccessible = appendYearResult(response, activePlans, context);
            if (noneAccessible && StringUtils.isNotBlank(requestedPlanYear)) {
                log.info("getDictionaryForUserWithPreviousPosting: None accessible - userId={}, planYear={}, "
                        + "fetching previous year", userId, planYear);
                fetchAndAppendPreviousYear(response, context, userOrgId,
                        CbPlanYearUtil.resolvePreviousYear(planYear));
            }
            markSuccess(response);
            cacheResult(cacheKey, response.getResult(), isCacheEnabled.get(), versions);
            log.info("getDictionaryForUserWithPreviousPosting: Success - userId={}, planYear={}, evaluatedOrgId={}",
                    userId, planYear, evaluationProfile.get(Constants.USER_ROOT_ORG_ID));
        } catch (IllegalArgumentException e) {
            log.error("getDictionaryForUserWithPreviousPosting: Invalid argument", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Invalid request parameters");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            log.error("getDictionaryForUserWithPreviousPosting: Unexpected error", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Failed to fetch CB Plan dictionary");
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    /**
     * Builds the profile that access control is evaluated against: a copy of the user's profile
     * with rootOrgId and/or designation replaced by the values the caller supplied, so plans are
     * listed for the posting that was passed in rather than the user's current one. Only the keys
     * the caller supplies are overlaid; everything else (service, batch, central deputation,
     * ministry) stays as the user's current profile, since no previous value exists for those.
     * <p>
     * The user's current profile is NOT separately evaluated - a plan is listed only when it
     * matches this profile. When neither value is supplied the user's own profile is returned
     * unchanged, so a direct call without a previous posting behaves like the standard dictionary.
     *
     * @param requestData request body map
     * @param userProfile the user's current profile
     * @return the profile to evaluate access control against, never null
     */
    private Map<String, String> resolveEvaluationProfile(Map<String, Object> requestData,
                                                         Map<String, String> userProfile) {
        String previousRootOrgId = asTrimmedString(requestData.get(Constants.REQUEST_PARAM_PREVIOUS_ROOT_ORG_ID));
        String previousDesignation = asTrimmedString(requestData.get(Constants.REQUEST_PARAM_PREVIOUS_DESIGNATION));
        if (StringUtils.isBlank(previousRootOrgId) && StringUtils.isBlank(previousDesignation)) {
            log.info("resolveEvaluationProfile: No previous posting supplied - evaluating current profile, orgId={}",
                    userProfile.get(Constants.USER_ROOT_ORG_ID));
            return userProfile;
        }
        Map<String, String> evaluationProfile = new HashMap<>(userProfile);
        if (StringUtils.isNotBlank(previousRootOrgId)) {
            evaluationProfile.put(Constants.USER_ROOT_ORG_ID, previousRootOrgId);
        }
        if (StringUtils.isNotBlank(previousDesignation)) {
            evaluationProfile.put(Constants.DESIGNATION, previousDesignation);
        }
        log.info("resolveEvaluationProfile: Evaluating supplied posting - currentOrgId={}, evaluatedOrgId={}, "
                        + "designationOverlaid={}",
                userProfile.get(Constants.USER_ROOT_ORG_ID), evaluationProfile.get(Constants.USER_ROOT_ORG_ID),
                StringUtils.isNotBlank(previousDesignation));
        return evaluationProfile;
    }

    private String asTrimmedString(Object value) {
        if (value instanceof String str) {
            return str.trim();
        }
        return null;
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
            String cachedData = userProfileRedisCacheMgr.getFromCache(cacheKey);
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
                                                        String planYear, AtomicBoolean isCacheEnabled,
                                                        CbPlanCacheVersions versions) {
        List<Map<String, Object>> orgPlans = cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(userOrgId, planYear,
                isCacheEnabled, versions);
        String ministryOrStateId = userProfile.get(Constants.MINISTRY_OR_STATE_ID_RQST);
        if (StringUtils.isBlank(ministryOrStateId)) {
            return orgPlans;
        }
        log.info("fetchPlansForUser: Fetching ministry plans - ministryOrStateId={}", ministryOrStateId);
        String ministryStamp = Objects.isNull(versions) ? null : String.valueOf(versions.ministryVersion());
        List<Map<String, Object>> ministryPlans = cbPlanCacheMgrV4.getCbPlanForMinistryOrStateId(ministryOrStateId,
                planYear, ministryStamp);
        return dataTransformService.mergePlanLists(orgPlans, ministryPlans);
    }

    /**
     * Builds the candidate plan set to evaluate: the plans scoped to the user's current org/ministry,
     * plus - when the supplied posting names a different root org - the plans scoped to that org.
     * Without the second fetch, a plan targeted strictly at the supplied org (via its own org-scoped
     * lookup) would never be fetched, so it could not be listed. The two lists are merged and
     * deduplicated by planId; access control then decides which survive, so including the user's own
     * org here only widens the candidate set, it does not relax the filter. The supplied posting's
     * ministry/state is not resolved, since only its root org is an input.
     *
     * @param context              per-request evaluation context
     * @param userOrgId            the user's current root org ID
     * @param isCacheEnabled       lookup-table cache flag, passed through to the cache manager
     * @param versions             version snapshot for the current org/ministry fetch
     * @param evaluatedOrgVersions version snapshot for the supplied posting's org, already resolved
     *                             by the caller so it is read once per request rather than twice
     * @return merged, deduplicated candidate plan list
     */
    private List<Map<String, Object>> fetchCandidatePlans(EvaluationContext context, String userOrgId,
                                                          AtomicBoolean isCacheEnabled, CbPlanCacheVersions versions,
                                                          CbPlanCacheVersions evaluatedOrgVersions) {
        String planYear = context.planYear();
        List<Map<String, Object>> currentPlans = fetchPlansForUser(context.userProfile(), userOrgId, planYear,
                isCacheEnabled, versions);
        String evaluatedOrgId = context.evaluationProfile().get(Constants.USER_ROOT_ORG_ID);
        if (StringUtils.isBlank(evaluatedOrgId) || evaluatedOrgId.equals(userOrgId)) {
            return currentPlans;
        }
        List<Map<String, Object>> evaluatedOrgPlans = cbPlanCacheMgrV4.getCbPlanForAllAndOrgId(evaluatedOrgId, planYear,
                isCacheEnabled, evaluatedOrgVersions);
        List<Map<String, Object>> mergedPlans = dataTransformService.mergePlanLists(currentPlans, evaluatedOrgPlans);
        log.info("fetchCandidatePlans: Merged supplied-org candidates - planYear={}, evaluatedOrgId={}, "
                        + "currentOrgPlans={}, evaluatedOrgPlans={}, mergedCandidates={}",
                planYear, evaluatedOrgId, currentPlans.size(), evaluatedOrgPlans.size(), mergedPlans.size());
        return mergedPlans;
    }

    /**
     * Fetches plans for the previous year and appends the result to the response, evaluating access
     * control against the same supplied-posting profile as the current year.
     */
    private void fetchAndAppendPreviousYear(ApiResponse response, EvaluationContext context,
                                            String userOrgId, String previousYear) {
        EvaluationContext previousYearContext = new EvaluationContext(context.userProfile(),
                context.evaluationProfile(), previousYear);
        AtomicBoolean prevCacheEnabled = new AtomicBoolean(false);
        CbPlanCacheVersions prevVersions = versionMgr.getVersions(userOrgId,
                context.userProfile().get(Constants.MINISTRY_OR_STATE_ID_RQST), previousYear);
        CbPlanCacheVersions prevEvaluatedOrgVersions = resolveEvaluatedOrgVersions(previousYearContext, userOrgId,
                prevVersions);
        List<Map<String, Object>> prevPlans = fetchCandidatePlans(previousYearContext, userOrgId, prevCacheEnabled,
                prevVersions, prevEvaluatedOrgVersions);
        if (CollectionUtils.isEmpty(prevPlans)) {
            log.info("fetchAndAppendPreviousYear: No plans found - previousYear={}", previousYear);
            response.getResult().put(previousYear, buildYearResult(new LinkedHashMap<>(), new LinkedHashMap<>()));
            return;
        }
        appendYearResult(response, prevPlans, previousYearContext);
    }

    private void buildPlanPartitions(List<Map<String, Object>> plans,
                                     EvaluationContext context,
                                     Map<String, Map<String, Object>> aparPlanMap,
                                     Map<String, Map<String, Object>> nonAparPlanMap) {
        Map<String, Map<String, Object>> parsedContextData = parseContextDataForPlans(plans);
        Map<String, Map<String, Object>> prefetchedUserGroups = batchFetchUserGroupsForPlans(plans, parsedContextData);
        processPlans(plans, context, prefetchedUserGroups, parsedContextData, aparPlanMap, nonAparPlanMap);
    }

    /**
     * Iterates all plans, evaluates access control against the supplied-posting profile, and
     * partitions the accessible ones into APAR and non-APAR maps. Counts are reported in a single
     * aggregated log line afterwards, never per plan.
     */
    private void processPlans(List<Map<String, Object>> plans,
                              EvaluationContext context,
                              Map<String, Map<String, Object>> prefetchedUserGroups,
                              Map<String, Map<String, Object>> parsedContextData,
                              Map<String, Map<String, Object>> aparPlanMap,
                              Map<String, Map<String, Object>> nonAparPlanMap) {
        for (Map<String, Object> plan : plans) {
            try {
                if (!isPlanAccessible(plan, context, prefetchedUserGroups, parsedContextData)) {
                    continue;
                }
                partitionPlanEntry(plan, aparPlanMap, nonAparPlanMap);
            } catch (Exception e) {
                log.error("processPlans: Failed to process plan={}", plan.get(Constants.PLAN_ID), e);
            }
        }
        log.info("processPlans: planYear={}, evaluatedOrgId={}, candidates={}, accessible={}, apar={}, nonApar={}",
                context.planYear(), context.evaluationProfile().get(Constants.USER_ROOT_ORG_ID), plans.size(),
                aparPlanMap.size() + nonAparPlanMap.size(), aparPlanMap.size(), nonAparPlanMap.size());
    }

    /**
     * A plan is accessible when the supplied-posting profile satisfies its access-control criteria.
     * The user's own current profile is deliberately not evaluated here: this endpoint lists the
     * plans for the orgId and designation the caller passed in, not the ones the user can already
     * see. Plans carrying no access control at all stay accessible, as they are open to everyone.
     *
     * @param plan                 plan record to evaluate
     * @param context              per-request evaluation context
     * @param prefetchedUserGroups user groups pre-fetched for this request, keyed by userGroupId
     * @param parsedContextData    contextData parsed once per plan, keyed by planId
     * @return true when the supplied posting satisfies the plan's criteria
     */
    private boolean isPlanAccessible(Map<String, Object> plan,
                                     EvaluationContext context,
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
        List<Map<String, Object>> userGroups = extractUserGroups(contextData);
        if (CollectionUtils.isEmpty(userGroups)) {
            return false;
        }
        return matchesProfileAgainstUserGroups(userGroups, hasV4UserGroupFormat(userGroups),
                context.evaluationProfile(), prefetchedUserGroups);
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
            userGroupCacheMgrV4.fetchUserGroupsByIds(
                            new ArrayList<>(entry.getValue()), entry.getKey())
                    .forEach((id, group) -> groups.put(id, new HashMap<>(group)));
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
                return serverProperties.isUserGroupAllowEmptyRootOrgIds()
                        && Constants.ROOT_ORG_ID.equalsIgnoreCase(criteriaKey);
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

    private Map<String, Object> buildPlainContentItem(String identifier) {
        Map<String, Object> item = new HashMap<>();
        item.put(Constants.IDENTIFIER, identifier);
        item.put(Constants.MANDATORY, false);
        return item;
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
        markSuccess(response);
    }

    private Map<String, Object> parseContextDataObj(Object contextDataObj) throws JsonProcessingException {
        if (contextDataObj instanceof String str && StringUtils.isNotBlank(str)) {
            return mapper.readValue(str, MAP_TYPE_REF);
        } else if (contextDataObj instanceof Map<?, ?>) {
            return (Map<String, Object>) contextDataObj;
        }
        return Map.of();
    }

    private Map<String, Object> extractRequestData(ApiRequest request) {
        if (request.getRequest() instanceof Map<?, ?> requestMap) {
            return (Map<String, Object>) requestMap;
        }
        return new HashMap<>();
    }

    private List<Map<String, Object>> castToMapList(List<?> rawList) {
        return (List<Map<String, Object>>) rawList;
    }

    /**
     * Parses contextData for every plan once and returns the results keyed by planId.
     * Plans whose contextData fails to parse are excluded - isPlanAccessible treats
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
     * Normalizes criteria map keys to lowercase-trimmed form in all fetched V4 user groups once,
     * so {@code matchesSingleGroupCriteria} can look up keys directly without per-call allocation.
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
     * for O(1) contains lookups during criteria evaluation.
     */
    private Set<String> buildNormalizedValueSet(Object rawValue) {
        if (rawValue instanceof Collection<?> col) {
            Set<String> result = new HashSet<>(col.size());
            for (Object v : col) {
                if (Objects.nonNull(v)) {
                    result.add(v.toString().toLowerCase());
                }
            }
            return result;
        }
        if (Objects.nonNull(rawValue)) {
            return Collections.singleton(rawValue.toString().toLowerCase());
        }
        return Collections.emptySet();
    }

    /**
     * Strips the caLinkedId from plans whose caLinkedId resolves to a non-Live status.
     * The plan itself is kept and shown to the user - only the stale CA link is removed.
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
        aparPlanMap.values().forEach(planEntry -> stripNonLiveCaLinkedId(planEntry, liveIds));
        nonAparPlanMap.values().forEach(planEntry -> stripNonLiveCaLinkedId(planEntry, liveIds));
    }

    private Set<String> resolveLiveCaLinkedIds(Set<String> caIds) {
        Set<String> liveIds = new HashSet<>();
        List<String> nonLiveIds = new ArrayList<>();
        for (String id : caIds) {
            try {
                Map<String, Object> meta = contentLookupService.getContentMetadata(id);
                if (MapUtils.isNotEmpty(meta) && Constants.LIVE.equalsIgnoreCase((String) meta.get(Constants.STATUS))) {
                    liveIds.add(id);
                } else {
                    nonLiveIds.add(id);
                }
            } catch (Exception e) {
                log.warn("resolveLiveCaLinkedIds: Metadata read failed - caLinkedId={}", id, e);
            }
        }
        if (CollectionUtils.isNotEmpty(nonLiveIds)) {
            log.info("resolveLiveCaLinkedIds: Stripping {} non-Live caLinkedId(s) of {} checked - ids={}",
                    nonLiveIds.size(), caIds.size(), nonLiveIds);
        }
        return liveIds;
    }

    private void stripNonLiveCaLinkedId(Map<String, Object> planEntry, Set<String> liveIds) {
        Object caId = planEntry.get(Constants.CA_LINKED_ID);
        if (!(caId instanceof String id) || StringUtils.isBlank(id)) {
            return;
        }
        if (!liveIds.contains(id)) {
            planEntry.remove(Constants.CA_LINKED_ID);
        }
    }

    /**
     * Extracts the access-control user-group list from a plan's parsed contextData, returning an
     * empty list when the structure is absent or malformed. The result is profile-independent, so
     * it is resolved once per plan and reused for both access-control evaluations.
     *
     * @param contextData parsed contextData for one plan
     * @return user-group list, or an empty list when access control is absent or malformed
     */
    private List<Map<String, Object>> extractUserGroups(Map<String, Object> contextData) {
        Object accessControlObj = contextData.get(Constants.ACCESS_CONTROL);
        if (!(accessControlObj instanceof Map<?, ?> accessControl)) {
            return List.of();
        }
        Object userGroupsObj = accessControl.get(Constants.USER_GROUPS);
        if (!(userGroupsObj instanceof List<?> rawList) || rawList.isEmpty()) {
            return List.of();
        }
        return castToMapList(rawList);
    }

    /**
     * Returns true when any entry carries a userGroupId, which marks the list as V4 reference
     * format rather than V3 inline criteria. Uses a plain loop rather than a stream so no pipeline
     * is allocated per plan.
     *
     * @param userGroups resolved user-group list
     * @return true when the list is in V4 userGroupId reference format
     */
    private boolean hasV4UserGroupFormat(List<Map<String, Object>> userGroups) {
        for (Map<String, Object> userGroup : userGroups) {
            if (userGroup.containsKey(Constants.USER_GROUP_ID)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Matches a single profile against an already-resolved user-group list, dispatching on the
     * format detected once by the caller.
     *
     * @param userGroups           resolved user-group list
     * @param hasV4Format          true when the list is in V4 userGroupId reference format
     * @param userProfile          the profile to match - current or previous posting
     * @param prefetchedUserGroups user groups pre-fetched for this request, keyed by userGroupId
     * @return true when the profile satisfies at least one user group
     */
    private boolean matchesProfileAgainstUserGroups(List<Map<String, Object>> userGroups, boolean hasV4Format,
                                                     Map<String, String> userProfile,
                                                     Map<String, Map<String, Object>> prefetchedUserGroups) {
        return hasV4Format
                ? evaluateV4UserGroups(userGroups, userProfile, prefetchedUserGroups)
                : evaluateV3InlineCriteria(userGroups, userProfile);
    }

    /**
     * Adds one accessible plan to the APAR or non-APAR partition.
     *
     * @param plan           source plan record
     * @param aparPlanMap    APAR partition, keyed by planId
     * @param nonAparPlanMap non-APAR partition, keyed by planId
     */
    private void partitionPlanEntry(Map<String, Object> plan,
                                    Map<String, Map<String, Object>> aparPlanMap,
                                    Map<String, Map<String, Object>> nonAparPlanMap) {
        String planId = (String) plan.get(Constants.PLAN_ID);
        Map<String, Object> planEntry = buildPlanEntry(plan);
        if (Boolean.TRUE.equals(plan.get(Constants.IS_APAR))) {
            aparPlanMap.put(planId, planEntry);
        } else {
            nonAparPlanMap.put(planId, planEntry);
        }
    }

    /**
     * Per-request inputs for the partition pipeline. Bundled so these related values are not
     * threaded individually through every level.
     * <p>
     * {@code userProfile} is used only to scope the candidate fetch (the user's own org and
     * ministry lookups). {@code evaluationProfile} is the one access control is matched against -
     * the user's profile overlaid with the orgId and designation the caller supplied, or the
     * user's own profile when none was supplied.
     *
     * @param userProfile       the user's current profile, used for candidate fetch scoping
     * @param evaluationProfile the profile access control is evaluated against
     * @param planYear          plan year being resolved
     */
    private record EvaluationContext(Map<String, String> userProfile,
                                     Map<String, String> evaluationProfile,
                                     String planYear) {
    }

    /**
     * Resolves the publish-version snapshot for the supplied posting's org. Returns the user's own
     * snapshot when the supplied posting names the same org (or none), so no extra Redis read is
     * made in that case.
     *
     * @param context         per-request evaluation context
     * @param userOrgId       the user's current root org ID
     * @param userOrgVersions snapshot already read for the user's own org
     * @return snapshot covering the org whose plans this request additionally depends on
     */
    private CbPlanCacheVersions resolveEvaluatedOrgVersions(EvaluationContext context, String userOrgId,
                                                            CbPlanCacheVersions userOrgVersions) {
        String evaluatedOrgId = context.evaluationProfile().get(Constants.USER_ROOT_ORG_ID);
        if (StringUtils.isBlank(evaluatedOrgId) || evaluatedOrgId.equals(userOrgId)) {
            return userOrgVersions;
        }
        return versionMgr.getVersions(evaluatedOrgId, null, context.planYear());
    }

    /**
     * Builds this endpoint's Redis key: the standard dictionary key with {@code prevdict} in place
     * of {@code dict}, and nothing else changed.
     * <p>
     * standard: {@code <prefix><userOrgId>:<userId>:<planYear>:dict}<br>
     * this one: {@code <prefix><userOrgId>:<userId>:<planYear>:prevdict}
     * <p>
     * One entry per user and plan year, which assumes a user has a single previous posting that is
     * the same on every call. The supplied orgId and designation are deliberately NOT part of the
     * key, so if a caller sends different values for the same user and year within the TTL, the
     * first result is served for the second.
     *
     * @param userId    requesting user
     * @param userOrgId the user's current root org ID
     * @param planYear  plan year being resolved
     * @return fully qualified Redis key for this request's result
     */
    private String buildPreviousPostingCacheKey(String userId, String userOrgId, String planYear) {
        return Constants.CB_PLAN_V4_REDIS_KEY_PREFIX + userOrgId + ":" + userId + ":" + planYear
                + Constants.CB_PLAN_V4_PREV_POSTING_DICT_KEY_SUFFIX;
    }

    /**
     * Replays a cached payload onto the response.
     *
     * @param response     API response to populate
     * @param cachedResult payload returned by the version-validated cache read
     */
    private void populateResponseFromCache(ApiResponse response, Map<String, Object> cachedResult) {
        response.getResult().putAll(cachedResult);
        markSuccess(response);
    }

    /**
     * Caches the result stamped with the publish versions it was built from. The stamp is added to
     * a copy, so the map the API response still holds is never mutated. Mirrors the standard
     * dictionary in only caching when the underlying plan lists came from cache, so a result built
     * during a cold read is not frozen for the full TTL.
     *
     * @param cacheKey       this endpoint's own key
     * @param result         the response result map to cache
     * @param isCacheEnabled whether the candidate plan lists were served from cache
     * @param versions       snapshot the result was built from
     */
    private void cacheResult(String cacheKey, Map<String, Object> result, boolean isCacheEnabled,
                             CbPlanCacheVersions versions) {
        if (!isCacheEnabled) {
            return;
        }
        try {
            String json = mapper.writeValueAsString(versionMgr.stamp(result, versions));
            redisCacheMgr.putInCache(cacheKey, json, serverProperties.getCbPlanV4DictionaryRedisCacheTtlSeconds());
            log.debug("cacheResult: Cached previous-posting dictionary result - key={}", cacheKey);
        } catch (JsonProcessingException e) {
            log.warn("cacheResult: Failed to serialize result for caching - key={}", cacheKey, e);
        }
    }

    /**
     * Marks the response as a successful 200, replacing the default params block.
     *
     * @param response API response to mark
     */
    private void markSuccess(ApiResponse response) {
        response.setParams(new ApiRespParam());
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
    }

    /**
     * Handles the no-candidate-plans case: emits an empty result for the resolved year and, when
     * the caller asked for a specific plan year, also appends the previous-year fallback.
     *
     * @param response          API response to populate
     * @param context           per-request evaluation context
     * @param userOrgId         the user's current root org ID
     * @param requestedPlanYear plan year as supplied by the caller, blank when defaulted
     */
    private void populateNoActivePlansResponse(ApiResponse response, EvaluationContext context,
                                               String userOrgId, String requestedPlanYear) {
        if (StringUtils.isBlank(requestedPlanYear)) {
            populateEmptyResponse(response, context.planYear());
            return;
        }
        response.getResult().put(context.planYear(),
                buildYearResult(new LinkedHashMap<>(), new LinkedHashMap<>()));
        fetchAndAppendPreviousYear(response, context, userOrgId,
                CbPlanYearUtil.resolvePreviousYear(context.planYear()));
        markSuccess(response);
    }

    /**
     * Partitions the candidate plans, enriches org details, strips non-Live CA links and writes the
     * year result onto the response.
     *
     * @param response    API response to populate
     * @param activePlans candidate plans for the year
     * @param context     per-request evaluation context
     * @return true when no plan was accessible, so the caller can decide on the previous-year fallback
     */
    private boolean appendYearResult(ApiResponse response, List<Map<String, Object>> activePlans,
                                     EvaluationContext context) {
        Map<String, Map<String, Object>> aparPlanMap = new LinkedHashMap<>();
        Map<String, Map<String, Object>> nonAparPlanMap = new LinkedHashMap<>();
        buildPlanPartitions(activePlans, context, aparPlanMap, nonAparPlanMap);
        Set<String> orgIds = collectCreatedByOrgIds(aparPlanMap, nonAparPlanMap);
        Map<String, Map<String, String>> orgDetailsMap = fetchOrgDetails(orgIds);
        enrichOrgDetails(aparPlanMap, orgDetailsMap);
        enrichOrgDetails(nonAparPlanMap, orgDetailsMap);
        filterLiveContentInPlans(aparPlanMap, nonAparPlanMap);
        response.getResult().put(context.planYear(), buildYearResult(aparPlanMap, nonAparPlanMap));
        return aparPlanMap.isEmpty() && nonAparPlanMap.isEmpty();
    }

}
