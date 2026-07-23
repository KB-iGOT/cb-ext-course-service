package com.igot.cb.controller;

import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.AccessSettingMigrationServiceImpl;
import com.igot.cb.service.CbPlanLearnerServiceImpl;
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
    private final CbPlanLearnerServiceImpl cbPlanLearnerService;

    public CbPlanWithAccessSettings(CbPlanServiceImpl cbPlanService, AccessSettingMigrationServiceImpl accessSettingMigrationService, CbPlanLearnerServiceImpl cbPlanLearnerService) {
        this.cbPlanService = cbPlanService;
        this.accessSettingMigrationService = accessSettingMigrationService;
        this.cbPlanLearnerService = cbPlanLearnerService;
    }

    @PostMapping("/create")
    public ResponseEntity<ApiResponse> createCbPlan(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String userOrgId) throws Exception {

        ApiResponse response = cbPlanService.createCbPlan(request, userOrgId, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/aicbp/create")
    public ResponseEntity<ApiResponse> createCbPlanByAdmin(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) throws Exception {

        ApiResponse response = cbPlanService.createCbPlanByAdmin(request, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/aicbp/publish")
    public ResponseEntity<ApiResponse> publishCbPlanByAdmin(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) throws Exception {

        ApiResponse response = cbPlanService.publishCbPlanByAdmin(request, token);
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

    @GetMapping("/migrate")
    public ResponseEntity<ApiResponse> migrateCBPlanAccessSettingRules() {
        ApiResponse response = accessSettingMigrationService.migrateCBPlanAccessSettingRules();
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

    @DeleteMapping("/archive")
    public ResponseEntity<ApiResponse> retireCbPlan(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String userOrgId,
            @RequestHeader(Constants.X_AUTH_USER_ROLES) List<String> userRoles) throws Exception {

        ApiResponse response = cbPlanService.retireCbPlan(request, userOrgId, token, userRoles);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/user/list")
    public ResponseEntity<ApiResponse> getCBPlanListForUser(
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestHeader(Constants.X_AUTH_USER_ORG_ID)String userOrgId) throws Exception {

        ApiResponse response = cbPlanLearnerService.getCBPlanListForUser(userOrgId, token, false);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/user/lookup")
    public ResponseEntity<ApiResponse> getPrivateCBPlanListForUser(
            @RequestHeader(Constants.X_AUTH_USER_ORG_ID)String userOrgId,
            @RequestHeader(Constants.X_AUTH_USER_ID) String userId)  {

        ApiResponse response = cbPlanLearnerService.getCBPlanCourseListForUser(userId,userOrgId);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/admin/user/list/{userId}")
    public ResponseEntity<ApiResponse> getCBPlanListForUserByAdmin(
            @PathVariable("userId") String userId,
            @RequestHeader(Constants.X_AUTH_USER_ORG_ID)String userOrgId) throws Exception {

        ApiResponse response = cbPlanLearnerService.getCBPlanListForUser(userOrgId, userId, true);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
