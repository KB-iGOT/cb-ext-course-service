package com.igot.cb.cbplan.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.igot.cb.cbplan.service.CbPlanServiceV4;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;

/**
 * REST Controller for CB Plan V4 operations with User Group References.
 * V4 accepts userGroupId instead of inline userGroupCriteriaList.
 * User org ID and roles are extracted from the authentication token.
 *
 * @version 4.0
 */
@RestController
@RequestMapping("/cbplan/v4")
public class CbPlanWithAccessSettingsV4 {
    private final CbPlanServiceV4 cbPlanServiceV4;

    public CbPlanWithAccessSettingsV4(CbPlanServiceV4 cbPlanServiceV4) {
        this.cbPlanServiceV4 = cbPlanServiceV4;
    }

    /**
     * Creates a new CB Plan V4 with user group references.
     * Accepts userGroupId in contextData.accessControl.userGroups.
     * Does NOT accept userGroupCriteriaList or userGroupName.
     *
     * @param request the API request containing CB Plan details
     * @param token   the authentication token
     * @return ResponseEntity containing ApiResponse with created plan details
     */
    @PostMapping("/create")
    public ResponseEntity<ApiResponse> createCbPlan(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        ApiResponse response = cbPlanServiceV4.createCbPlan(request, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Updates an existing CB Plan V4 with user group references.
     *
     * @param request the API request containing updated CB Plan details
     * @param token   the authentication token
     * @return ResponseEntity containing ApiResponse with update status
     */
    @PostMapping("/update")
    public ResponseEntity<ApiResponse> updateCbPlan(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        ApiResponse response = cbPlanServiceV4.updateCbPlan(request, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Publishes an existing CB Plan V4 with user group references.
     *
     * @param request the API request containing the plan ID and publish comment
     * @param token   the authentication token
     * @return ResponseEntity containing ApiResponse with publish status
     */
    @PostMapping("/publish")
    public ResponseEntity<ApiResponse> publishCbPlan(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        ApiResponse response = cbPlanServiceV4.publishCbPlan(request, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Reads a CB Plan by ID. Only allows reading LIVE plans.
     * Draft plans return 200 OK with error status and message in response body.
     * Returns contextData exactly as stored, so it works for plans created by
     * either V3 (inline userGroupName) or V4 (userGroupId reference).
     *
     * @param cbPlanId the CB Plan ID to retrieve
     * @param token    the authentication token
     * @return ResponseEntity containing ApiResponse with CB Plan details or error message
     */
    @GetMapping("/read/{cbPlanId}")
    public ResponseEntity<ApiResponse> readCbPlan(
            @PathVariable("cbPlanId") String cbPlanId,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        ApiResponse response = cbPlanServiceV4.readCbPlan(cbPlanId, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Admin read: Reads a CB Plan by ID regardless of status (DRAFT or LIVE).
     * Returns contextData exactly as stored, so it works for plans created by
     * either V3 (inline userGroupName) or V4 (userGroupId reference).
     *
     * @param cbPlanId the CB Plan ID to retrieve
     * @param token    the authentication token
     * @return ResponseEntity containing ApiResponse with CB Plan details
     */
    @GetMapping("/admin/read/{cbPlanId}")
    public ResponseEntity<ApiResponse> readCbPlanAdmin(
            @PathVariable("cbPlanId") String cbPlanId,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        ApiResponse response = cbPlanServiceV4.readCbPlanAdmin(cbPlanId, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Searches CB Plans. Client controls all filtering via the request body.
     * Backend constructs SearchCriteria from the generic request.
     * User org ID is extracted from the authentication token.
     *
     * @param request the API request containing search parameters
     * @param token   the authentication token
     * @return ResponseEntity containing ApiResponse with search results
     */
    @PostMapping("/search")
    public ResponseEntity<ApiResponse> searchCbPlan(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        ApiResponse response = cbPlanServiceV4.searchCbPlan(request, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Archives (retires) a CB Plan V4.
     * Delegates to V3 implementation as the archive logic is version-agnostic.
     * User org ID and roles are extracted from the authentication token.
     *
     * @param request the API request containing CB Plan ID and optional comment
     * @param token   the authentication token
     * @return ResponseEntity containing ApiResponse with archive status
     */
    @DeleteMapping("/archive")
    public ResponseEntity<ApiResponse> retireCbPlan(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        ApiResponse response = cbPlanServiceV4.retireCbPlan(request, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
