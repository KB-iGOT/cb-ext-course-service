package com.igot.cb.controller;

import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.CbPlanServiceImpl;
import com.igot.cb.util.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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
}
