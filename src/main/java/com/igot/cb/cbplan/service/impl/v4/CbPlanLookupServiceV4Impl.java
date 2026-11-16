package com.igot.cb.cbplan.service.impl.v4;

import com.igot.cb.cache.CbPlanVersionMgr;
import com.igot.cb.cbplan.util.CbPlanYearUtil;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * V4 service for the private CB Plan user lookup API.
 * Returns a flat courseId -&gt; planEndDate map for the user's APAR plans only,
 * computed via the V4 access-control pipeline (userGroupId resolution against user profile).
 * Non-APAR plans are excluded.
 *
 * @version 4.0
 */
@Service
@Slf4j
public class CbPlanLookupServiceV4Impl {

    private static final String GENERIC_ERROR = "Failed to lookup user CB plan courses";

    private final CbPlanDictionaryServiceV4Impl dictionaryService;
    private final CbPlanVersionMgr versionMgr;

    public CbPlanLookupServiceV4Impl(CbPlanDictionaryServiceV4Impl dictionaryService,
                                     CbPlanVersionMgr versionMgr) {
        this.dictionaryService = dictionaryService;
        this.versionMgr = versionMgr;
    }

    /**
     * Returns the flat courseId -&gt; planEndDate map for the given user across their active APAR
     * plans for the current financial year. Caller supplies the already-resolved userId and
     * userOrgId; no token validation is performed.
     *
     * @param userId    the resolved user ID
     * @param userOrgId the user's root organisation ID
     * @return ApiResponse with result.contents = Map&lt;courseId, planEndDate&gt;
     */
    public ApiResponse getCBPlanCourseListForUser(String userId, String userOrgId) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.CB_PLAN_USER_LOOKUP_V4_API);
        try {
            if (!validateInputs(userId, userOrgId, response)) {
                return response;
            }
            String planYear = CbPlanYearUtil.resolveCurrentFinancialYear();
            log.info("getCBPlanCourseListForUser: userId={}, userOrgId={}, planYear={}", userId, userOrgId, planYear);

            Map<String, String> courseMap = readFromDictionaryCache(userId, userOrgId, planYear)
                    .orElseGet(() -> resolveFromPipeline(userId, userOrgId, planYear, response));
            if (Constants.FAILED.equalsIgnoreCase(response.getParams().getStatus())) {
                return response;
            }
            populateSuccess(response, courseMap);
            log.info("getCBPlanCourseListForUser: Success - userId={}, courseCount={}", userId, courseMap.size());
        } catch (Exception e) {
            log.error("getCBPlanCourseListForUser: Failed - userId={}", userId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(GENERIC_ERROR);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    /**
     * Validates the required inputs for the lookup. Both userId and userOrgId must be non-blank,
     * since no token-based fallback is available on this private endpoint.
     * On failure, populates the response with FAILED status and HTTP 400 BAD_REQUEST.
     *
     * @param userId    the resolved user ID from the request header
     * @param userOrgId the user's root organisation ID from the request header
     * @param response  API response, populated with an error message on validation failure
     * @return true when both inputs are present; false after setting the error on the response
     */
    private boolean validateInputs(String userId, String userOrgId, ApiResponse response) {
        if (StringUtils.isBlank(userId)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("UserId is blank");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return false;
        }
        if (StringUtils.isBlank(userOrgId)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("UserOrgId is blank");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return false;
        }
        return true;
    }

    /**
     * Fallback path when the dictionary Redis cache is cold or malformed. Runs the full V4
     * access-control pipeline via {@link CbPlanDictionaryServiceV4Impl#resolveAccessibleAparPlansByUserId}
     * and flattens the resulting APAR plans into a courseId &rarr; endDate map.
     * If the pipeline sets FAILED on the response (e.g. user not found), returns an empty
     * map and lets the outer method short-circuit to return the error response.
     *
     * @param userId    resolved user ID
     * @param userOrgId user's root organisation ID
     * @param planYear  financial year in YYYY-YY form
     * @param response  API response, populated with an error on profile load failure
     * @return flat courseId &rarr; endDate map; empty on pipeline failure (status is FAILED) or no plans
     */
    private Map<String, String> resolveFromPipeline(String userId, String userOrgId, String planYear,
                                                    ApiResponse response) {
        List<Map<String, Object>> aparPlans = dictionaryService.resolveAccessibleAparPlansByUserId(
                userId, userOrgId, planYear, response);
        if (Constants.FAILED.equalsIgnoreCase(response.getParams().getStatus())) {
            return new LinkedHashMap<>();
        }
        return flattenToCourseEndDateMap(aparPlans);
    }

    /**
     * Populates the final successful response: puts the course map under
     * {@code result.contents}, sets params.status = SUCCESS and HTTP 200 OK.
     *
     * @param response  API response to populate
     * @param courseMap flat courseId &rarr; endDate map (may be empty)
     */
    private void populateSuccess(ApiResponse response, Map<String, String> courseMap) {
        response.getResult().put(Constants.CONTENTS, courseMap);
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
    }

    /**
     * Flattens APAR plan entries into a single courseId &rarr; endDate map.
     * Each plan contributes its {@code contentList} identifiers paired with the plan's own
     * endDate (as an ISO-8601 string). If the same courseId appears across multiple plans,
     * the first-seen endDate wins (via {@code putIfAbsent}) to preserve the earliest-added
     * plan's deadline. Non-Map/non-String contentList entries and blank identifiers are skipped.
     *
     * @param plans APAR plan entries (either from the dictionary pipeline or the Redis cache)
     * @return flat courseId &rarr; endDate ISO string map; empty when plans is empty/null
     */
    private Map<String, String> flattenToCourseEndDateMap(List<Map<String, Object>> plans) {
        Map<String, String> courseMap = new LinkedHashMap<>();
        if (CollectionUtils.isEmpty(plans)) {
            return courseMap;
        }
        for (Map<String, Object> plan : plans) {
            String endDate = extractEndDateIso(plan.get(Constants.END_DATE_REQUEST));
            Object contentListObj = plan.get(Constants.CONTENT_LIST);
            if (!(contentListObj instanceof List<?> contentList)) {
                continue;
            }
            for (Object item : contentList) {
                String identifier = extractIdentifier(item);
                if (StringUtils.isNotBlank(identifier)) {
                    courseMap.putIfAbsent(identifier, endDate);
                }
            }
        }
        return courseMap;
    }

    /**
     * Extracts the course identifier from a single contentList entry. Supports both V4 object
     * form ({@code {identifier, mandatory}}) and legacy plain-string form (bare course ID).
     *
     * @param item single contentList entry; may be Map, String, or any other type
     * @return the identifier string; null when the entry is not recognisable
     */
    private String extractIdentifier(Object item) {
        if (item instanceof Map<?, ?> entry) {
            Object id = entry.get(Constants.IDENTIFIER);
            return id instanceof String s ? s : null;
        }
        if (item instanceof String s) {
            return s;
        }
        return null;
    }

    /**
     * Normalises a plan's endDate to an ISO-8601 string regardless of its source type.
     * Cassandra reads surface {@link Instant}, while cached JSON surfaces the ISO string directly.
     *
     * @param endDateObj raw endDate value from the plan record
     * @return ISO-8601 string representation; null when the input is null
     */
    private String extractEndDateIso(Object endDateObj) {
        if (Objects.isNull(endDateObj)) {
            return null;
        }
        if (endDateObj instanceof Instant instant) {
            return instant.toString();
        }
        return endDateObj.toString();
    }

    /**
     * Reads the dictionary API's per-user Redis cache (populated by
     * {@code CbPlanDictionaryServiceV4Impl#getCBPlanDictionaryForUser}) through
     * {@link CbPlanVersionMgr}, which serves it only while its publish-version stamp still matches
     * the live counters for the user's org, ministry and the ALL scope, and flattens its
     * {@code aparPlanList} into a courseId &rarr; endDate map. Returns an empty Optional on miss,
     * stale or unstamped entry, malformed JSON, or missing aparPlanList so the caller falls back
     * to the full pipeline. No user profile is loaded on this path.
     *
     * @param userId    resolved user ID
     * @param userOrgId user's root organisation ID
     * @param planYear  financial year in YYYY-YY form
     * @return courseId &rarr; endDate map on cache hit; empty Optional otherwise
     */
    private Optional<Map<String, String>> readFromDictionaryCache(String userId, String userOrgId, String planYear) {
        String cacheKey = Constants.CB_PLAN_V4_REDIS_KEY_PREFIX + userOrgId + ":" + userId + ":" + planYear + ":dict";
        Optional<Map<String, Object>> cached = versionMgr.readValidatedDict(cacheKey, userOrgId, planYear);
        if (cached.isEmpty()) {
            return Optional.empty();
        }
        Optional<List<Map<String, Object>>> aparPlans = extractAparPlans(cached.get(), planYear);
        if (aparPlans.isEmpty()) {
            return Optional.empty();
        }
        Map<String, String> courseMap = flattenToCourseEndDateMap(aparPlans.get());
        log.info("readFromDictionaryCache: Cache hit - userId={}, courseCount={}", userId, courseMap.size());
        return Optional.of(courseMap);
    }

    /**
     * Navigates the cached dictionary structure down to the APAR plan list for the given planYear.
     * Expected shape: {@code { <planYear>: { aparPlanList: { planId: planEntry, ... }, ... } }}
     * (the {@code _cv} version stamp has already been stripped by {@link CbPlanVersionMgr}).
     * Returns an empty Optional when the shape doesn't match so the caller falls back to the
     * pipeline. A matching shape with an empty aparPlanList is a valid cache hit and returns
     * {@code Optional.of(emptyList)}.
     *
     * @param cached   deserialized Redis cache payload
     * @param planYear financial year used as the top-level key in the cache
     * @return list of APAR plan entries wrapped in Optional on structural match; empty Optional otherwise
     */
    private Optional<List<Map<String, Object>>> extractAparPlans(Map<String, Object> cached, String planYear) {
        Object yearResultObj = cached.get(planYear);
        if (!(yearResultObj instanceof Map<?, ?> yearResult)) {
            return Optional.empty();
        }
        Object aparListObj = yearResult.get(Constants.RESPONSE_KEY_APAR_PLAN_LIST);
        if (!(aparListObj instanceof Map<?, ?> aparPlanMap)) {
            return Optional.empty();
        }
        List<Map<String, Object>> plans = new ArrayList<>(aparPlanMap.size());
        for (Object value : aparPlanMap.values()) {
            if (value instanceof Map<?, ?> entry) {
                plans.add(toStringObjectMap(entry));
            }
        }
        return Optional.of(plans);
    }

    /**
     * Narrows a raw wildcard-typed Map to {@code Map<String, Object>}. Called only on nested
     * maps that originated from a JSON-to-Map deserialization, so String keys are
     * guaranteed and the cast cannot throw at runtime.
     *
     * @param raw wildcard-typed map freshly deserialized from the cache payload
     * @return the same map re-typed to {@code Map<String, Object>}
     */
    private Map<String, Object> toStringObjectMap(Map<?, ?> raw) {
        return (Map<String, Object>) raw;
    }
}
