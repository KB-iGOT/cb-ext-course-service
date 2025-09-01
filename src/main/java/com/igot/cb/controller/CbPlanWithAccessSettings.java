package com.igot.cb.controller;

import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.AccessSettingMigrationServiceImpl;
import com.igot.cb.service.CbPlanServiceImpl;
import com.igot.cb.util.Constants;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/cbplan/v2")
public class CbPlanWithAccessSettings {

    private final CbPlanServiceImpl cbPlanService;
    private final AccessSettingMigrationServiceImpl accessSettingMigrationService;

    public CbPlanWithAccessSettings(CbPlanServiceImpl cbPlanService, AccessSettingMigrationServiceImpl accessSettingMigrationService) {
        this.cbPlanService = cbPlanService;
        this.accessSettingMigrationService = accessSettingMigrationService;
    }

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

    @GetMapping("/v1/cbplan/migrate")
    public ResponseEntity<ApiResponse> migrateCBPlanAccessSettingRules() {
        ApiResponse response = accessSettingMigrationService.migrateAccessSettingRules();
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
