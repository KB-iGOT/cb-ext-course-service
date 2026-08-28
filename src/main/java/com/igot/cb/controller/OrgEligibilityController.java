package com.igot.cb.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.OrgEligibilityServiceImpl;
import com.igot.cb.util.Constants;

@RestController
@RequestMapping("/orgeligibility/v1")
public class OrgEligibilityController {

    private final OrgEligibilityServiceImpl orgEligibilityService;

    public OrgEligibilityController(OrgEligibilityServiceImpl orgEligibilityService) {
        this.orgEligibilityService = orgEligibilityService;
    }

    @PostMapping("/upsert")
    public ResponseEntity<ApiResponse> upsertOrgEligibility(
            @RequestBody Map<String, Object> request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        ApiResponse response = orgEligibilityService.upsertOrgEligibility(request, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/read/{orgId}")
    public ResponseEntity<ApiResponse> readOrgEligibility(
            @PathVariable("orgId") String orgId,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        ApiResponse response = orgEligibilityService.readOrgEligibility(orgId, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
