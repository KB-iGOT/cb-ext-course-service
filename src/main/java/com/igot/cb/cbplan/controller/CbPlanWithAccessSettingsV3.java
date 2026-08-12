package com.igot.cb.cbplan.controller;

import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.igot.cb.cbplan.service.CbPlanServiceV3;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;

/**
 * REST Controller for CB Plan V3 operations with Access Settings.
 *
 * @version 3.0
 */
@RestController
@RequestMapping("/cbplan/v3")
public class CbPlanWithAccessSettingsV3 {
    private final CbPlanServiceV3 cbPlanServiceV3;

    public CbPlanWithAccessSettingsV3(CbPlanServiceV3 cbPlanServiceV3) {
        this.cbPlanServiceV3 = cbPlanServiceV3;
    }

    /**
     * Creates a new CB Plan.
     *
     * @param request   the API request containing CB Plan details
     * @param token     the authentication token
     * @param userOrgId the organization ID of the user
     * @return ResponseEntity containing ApiResponse with created plan details
     */
    @PostMapping("/create")
    public ResponseEntity<ApiResponse> createCbPlan(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String userOrgId) {
        ApiResponse response = cbPlanServiceV3.createCbPlan(request, userOrgId, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Updates an existing CB Plan.
     *
     * @param request   the API request containing updated CB Plan details
     * @param token     the authentication token
     * @param userOrgId the organization ID of the user
     * @param userRoles the roles of the user
     * @return ResponseEntity containing ApiResponse with update status
     */
    @PostMapping("/update")
    public ResponseEntity<ApiResponse> updateCbPlan(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String userOrgId,
            @RequestHeader(Constants.X_AUTH_USER_ROLES) List<String> userRoles) {
        ApiResponse response = cbPlanServiceV3.updateCbPlan(request, userOrgId, token, userRoles);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Publishes a CB Plan.
     *
     * @param request   the API request containing CB Plan ID and comment
     * @param token     the authentication token
     * @param userOrgId the organization ID of the user
     * @param userRoles the roles of the user
     * @return ResponseEntity containing ApiResponse with publish status
     */
    @PostMapping("/publish")
    public ResponseEntity<ApiResponse> publishCbPlan(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String userOrgId,
            @RequestHeader(Constants.X_AUTH_USER_ROLES) List<String> userRoles) {
        ApiResponse response = cbPlanServiceV3.publishCbPlan(request, userOrgId, token, userRoles);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Archives (retires) a CB Plan.
     *
     * @param request   the API request containing CB Plan ID and comment
     * @param token     the authentication token
     * @param userOrgId the organization ID of the user
     * @param userRoles the roles of the user
     * @return ResponseEntity containing ApiResponse with archive status
     */
    @DeleteMapping("/archive")
    public ResponseEntity<ApiResponse> retireCbPlan(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String userOrgId,
            @RequestHeader(Constants.X_AUTH_USER_ROLES) List<String> userRoles) {
        ApiResponse response = cbPlanServiceV3.retireCbPlan(request, userOrgId, token, userRoles);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Reads a CB Plan by ID with enriched content details.
     *
     * @param cbPlanId  the CB Plan ID to retrieve
     * @param token     the authentication token
     * @param userOrgId the organization ID of the user
     * @return ResponseEntity containing ApiResponse with CB Plan details
     */
    @GetMapping("/read/{cbPlanId}")
    public ResponseEntity<ApiResponse> readCbPlan(
            @PathVariable("cbPlanId") String cbPlanId,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String userOrgId) {
        ApiResponse response = cbPlanServiceV3.readCbPlan(cbPlanId, userOrgId, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Searches CB Plans based on criteria.
     *
     * @param request   the search criteria
     * @param token     the authentication token
     * @param userOrgId the organization ID of the user
     * @return ResponseEntity containing ApiResponse with search results
     */
    @PostMapping("/search")
    public ResponseEntity<ApiResponse> searchCbPlan(
            @RequestBody SearchCriteria request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String userOrgId) {
        ApiResponse response = cbPlanServiceV3.searchCbPlan(request, userOrgId, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Gets CB Plan dictionary for a user - content IDs grouped by APAR/non-APAR
     * with plan occurrences and optional enrichment.
     *
     * @param request the API request containing planYear and enrichment
     * @param token   the authentication token
     * @return ResponseEntity containing ApiResponse with content dictionary
     */
    @PostMapping("/user/dictionary")
    public ResponseEntity<ApiResponse> getCBPlanDictionary(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        ApiResponse response = cbPlanServiceV3.getCBPlanDictionaryForUser(request, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
