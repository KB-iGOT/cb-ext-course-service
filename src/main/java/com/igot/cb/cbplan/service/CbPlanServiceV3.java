package com.igot.cb.cbplan.service;

import java.util.List;

import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;

/**
 * Service interface for CB Plan V3 operations.
 *
 * @version 3.0
 */
public interface CbPlanServiceV3 {
    /**
     * Creates a new CB Plan.
     *
     * @param request   the API request containing CB Plan details
     * @param userOrgId the organization ID of the user
     * @param authToken the authentication token
     * @return ApiResponse containing the created plan ID and status
     */
    ApiResponse createCbPlan(ApiRequest request, String userOrgId, String authToken);

    /**
     * Updates an existing CB Plan.
     *
     * @param request   the API request containing updated CB Plan details
     * @param userOrgId the organization ID of the user
     * @param authToken the authentication token
     * @param userRoles the roles of the user
     * @return ApiResponse containing the update status
     */
    ApiResponse updateCbPlan(ApiRequest request, String userOrgId, String authToken, List<String> userRoles);

    /**
     * Publishes a CB Plan.
     *
     * @param request   the API request containing CB Plan ID and comment
     * @param userOrgId the organization ID of the user
     * @param authToken the authentication token
     * @param userRoles the roles of the user
     * @return ApiResponse containing the publish status
     */
    ApiResponse publishCbPlan(ApiRequest request, String userOrgId, String authToken, List<String> userRoles);

    /**
     * Archives (retires) a CB Plan.
     *
     * @param request   the API request containing CB Plan ID and comment
     * @param userOrgId the organization ID of the user
     * @param authToken the authentication token
     * @param userRoles the roles of the user
     * @return ApiResponse containing the archive status
     */
    ApiResponse retireCbPlan(ApiRequest request, String userOrgId, String authToken, List<String> userRoles);

    /**
     * Searches CB Plans based on criteria.
     *
     * @param searchCriteria the search criteria
     * @param userOrgId      the organization ID of the user
     * @param authToken      the authentication token
     * @return ApiResponse containing search results
     */
    ApiResponse searchCbPlan(SearchCriteria searchCriteria, String userOrgId, String authToken);

    /**
     * Gets CB Plan dictionary for a user - content IDs grouped by APAR/non-APAR
     * with plan occurrences and optional enrichment.
     *
     * @param request   the API request containing planYear and enrichment
     * @param authToken the authentication token
     * @return ApiResponse with aparContentList, nonAparContentList, and optionally enrichedContentList
     */
    ApiResponse getCBPlanDictionaryForUser(ApiRequest request, String authToken);

    /**
     * Reads a CB Plan by ID with enriched content details.
     *
     * @param cbPlanId     the CB Plan ID to retrieve
     * @param userOrgId    the organization ID of the user
     * @param authUserToken the authentication token
     * @return ApiResponse containing the CB Plan details or error
     */
    ApiResponse readCbPlan(String cbPlanId, String userOrgId, String authUserToken);
}
