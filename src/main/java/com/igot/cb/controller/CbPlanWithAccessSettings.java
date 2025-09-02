package com.igot.cb.controller;

import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.CbPlanServiceImpl;
import com.igot.cb.util.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/cbplan/v2")
public class CbPlanWithAccessSettings {

    @Autowired
    private CbPlanServiceImpl cbPlanService;

    @PostMapping("/create")
    public ResponseEntity<ApiResponse> createCbPlan(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String userOrgId) throws Exception {

        ApiResponse response = cbPlanService.createCbPlan(request, userOrgId, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/update")
    public ResponseEntity<ApiResponse> updateCbPlan(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String userOrgId,
            @RequestHeader(Constants.X_AUTH_USER_ROLES) List<String> userRoles) throws Exception {

        ApiResponse response = cbPlanService.updateCbPlan(request, userOrgId, token, userRoles);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/publish")
    public ResponseEntity<ApiResponse> publishCbPlan(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String userOrgId,
            @RequestHeader(Constants.X_AUTH_USER_ROLES) List<String> userRoles) throws Exception {

        ApiResponse response = cbPlanService.publishCbPlan(request, userOrgId, token, userRoles);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/read/{cbPlanId}")
    public ResponseEntity<ApiResponse> readCbPlan(
            @PathVariable("cbPlanId") String cbPlanId,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String userOrgId) throws Exception {

        ApiResponse response = cbPlanService.readCbPlan(cbPlanId, userOrgId, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/search")
    public ResponseEntity<ApiResponse> searchCbPlan(
            @RequestBody SearchCriteria request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String userOrgId) throws Exception {

        ApiResponse response = cbPlanService.searchCbPlan(request, userOrgId, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
