package com.igot.cb.cbplan.service;

import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;

/**
 * Service interface for CB Plan V4 operations.
 * V4 uses user group references instead of inline criteria.
 *
 * @version 4.0
 */
public interface CbPlanServiceV4 {

    /**
     * Creates a new CB Plan with user group references.
     * User org ID is extracted from the authentication token.
     *
     * @param request   the API request containing CB Plan details
     * @param authToken the authentication token
     * @return ApiResponse containing the created plan ID and status
     */
    ApiResponse createCbPlan(ApiRequest request, String authToken);

    /**
     * Updates an existing CB Plan with user group references.
     * User org ID and roles are extracted from the authentication token.
     *
     * @param request   the API request containing updated CB Plan details
     * @param authToken the authentication token
     * @return ApiResponse containing the update status
     */
    ApiResponse updateCbPlan(ApiRequest request, String authToken);

    /**
     * Publishes an existing CB Plan with user group references.
     * User org ID and roles are extracted from the authentication token.
     *
     * @param request   the API request containing the plan ID and publish comment
     * @param authToken the authentication token
     * @return ApiResponse containing the publish status
     */
    ApiResponse publishCbPlan(ApiRequest request, String authToken);

    /**
     * Reads a CB Plan by ID. Only allows reading LIVE plans.
     * Draft plans return 200 OK with error status and message in response body.
     * Returns contextData exactly as stored, so it works for plans created by
     * either V3 (inline userGroupName) or V4 (userGroupId reference).
     *
     * @param cbPlanId      the CB Plan ID to retrieve
     * @param authUserToken the authentication token
     * @return ApiResponse containing the CB Plan details or error message for DRAFT plans
     */
    ApiResponse readCbPlan(String cbPlanId, String authUserToken);

    /**
     * Admin read: Reads a CB Plan by ID regardless of status (DRAFT or LIVE).
     * Returns contextData exactly as stored, so it works for plans created by
     * either V3 (inline userGroupName) or V4 (userGroupId reference).
     *
     * @param cbPlanId      the CB Plan ID to retrieve
     * @param authUserToken the authentication token
     * @return ApiResponse containing the CB Plan details or error
     */
    ApiResponse readCbPlanAdmin(String cbPlanId, String authUserToken);

    /**
     * Searches CB Plans. Client controls all filtering via the request body.
     * Backend constructs SearchCriteria from the generic request.
     * User org ID is extracted from the authentication token.
     *
     * @param request   the API request containing search parameters (query, filters, pagination, etc.)
     * @param authToken the authentication token
     * @return ApiResponse containing search results
     */
    ApiResponse searchCbPlan(ApiRequest request, String authToken);

    /**
     * Archives (retires) a CB Plan V4.
     * Delegates to V3 implementation as the archive logic is version-agnostic.
     * User org ID and roles are extracted from the authentication token.
     *
     * @param request   the API request containing CB Plan ID and optional comment
     * @param authToken the authentication token
     * @return ApiResponse containing the archive status
     */
    ApiResponse retireCbPlan(ApiRequest request, String authToken);

    /**
     * Returns the user's active CB Plans grouped by APAR/non-APAR for the given plan year.
     * Each plan entry carries name, contentList, comprehensiveAssessment, org details, and endDate.
     * V4 plans are access-controlled by resolving userGroupId references against the user's profile.
     *
     * @param request   the API request containing planYear
     * @param authToken the authentication token
     * @return ApiResponse with aparPlanList and nonAparPlanList grouped by planYear
     */
    ApiResponse getCBPlanDictionaryForUser(ApiRequest request, String authToken);

    /**
     * Checks whether the given Comprehensive Assessment do_id is linked (via caLinkedId) to any
     * plan the user is eligible for, searching the current and previous financial year.
     *
     * @param doId      CA content identifier to check eligibility for
     * @param authToken the authentication token
     * @return ApiResponse with result = {eligible: boolean, mandatoryCourses: List<String>}
     */
    ApiResponse getComprehensiveAssessmentEligibility(String doId, String authToken);

    /**
     * Sets or clears the Comprehensive Assessment link (calinkedid) on a CB Plan, syncs the
     * ElasticSearch document and invalidates the plan/dictionary caches.
     * Used by the authenticated update API and by the training-plan CA-link Kafka consumer.
     *
     * @param cbPlanId   CB Plan ID
     * @param caLinkedId CA content identifier to link, or null to clear the link
     * @param updatedBy  user or system identifier recorded in updatedBy
     * @return true when Cassandra and ElasticSearch were updated, false when the Cassandra update failed
     */
    boolean updateCaLinkedId(String cbPlanId, String caLinkedId, String updatedBy);
}
