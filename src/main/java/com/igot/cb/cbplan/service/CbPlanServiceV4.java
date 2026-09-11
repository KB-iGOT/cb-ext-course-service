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
}
