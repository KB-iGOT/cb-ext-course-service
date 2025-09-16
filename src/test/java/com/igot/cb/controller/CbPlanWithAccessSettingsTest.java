package com.igot.cb.controller;

import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.AccessSettingMigrationServiceImpl;
import com.igot.cb.service.CbPlanLearnerServiceImpl;
import com.igot.cb.service.CbPlanServiceImpl;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CbPlanWithAccessSettingsTest {

    @Mock
    private CbPlanServiceImpl cbPlanService;

    @Mock
    private AccessSettingMigrationServiceImpl accessSettingMigrationService;

    @Mock
    private CbPlanLearnerServiceImpl cbPlanLearnerService;

    @InjectMocks
    private CbPlanWithAccessSettings controller;

    @Test
    void testCreateCbPlan() throws Exception {
        ApiRequest request = new ApiRequest();
        request.setRequest(new HashMap<>());
        
        ApiResponse mockResponse = new ApiResponse();
        mockResponse.getParams().setStatus(Constants.SUCCESS);
        mockResponse.setResponseCode(HttpStatus.OK);
        
        when(cbPlanService.createCbPlan(any(), anyString(), anyString())).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = controller.createCbPlan(request, "token", "orgId");
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
    }

    @Test
    void testUpdateCbPlan() throws Exception {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, "plan123");
        request.setRequest(requestMap);
        
        ApiResponse mockResponse = new ApiResponse();
        mockResponse.getParams().setStatus(Constants.SUCCESS);
        mockResponse.setResponseCode(HttpStatus.OK);
        
        when(cbPlanService.updateCbPlan(any(), anyString(), anyString(), any())).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = controller.updateCbPlan(request, "token", "orgId", Arrays.asList("ADMIN"));
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
    }

    @Test
    void testPublishCbPlan() throws Exception {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, "plan123");
        request.setRequest(requestMap);
        
        ApiResponse mockResponse = new ApiResponse();
        mockResponse.getParams().setStatus(Constants.SUCCESS);
        mockResponse.setResponseCode(HttpStatus.OK);
        
        when(cbPlanService.publishCbPlan(any(), anyString(), anyString(), any())).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = controller.publishCbPlan(request, "token", "orgId", Arrays.asList("ADMIN"));
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
    }

    @Test
    void testReadCbPlan() throws Exception {
        ApiResponse mockResponse = new ApiResponse();
        mockResponse.getParams().setStatus(Constants.SUCCESS);
        mockResponse.setResponseCode(HttpStatus.OK);
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.ID, "plan123");
        mockResponse.getResult().put(Constants.CONTENT, content);
        
        when(cbPlanService.readCbPlan(anyString(), anyString(), anyString())).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = controller.readCbPlan("plan123", "token", "orgId");
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
    }

    @Test
    void testMigrateCBPlanAccessSettingRules() {
        ApiResponse mockResponse = new ApiResponse();
        mockResponse.getParams().setStatus(Constants.SUCCESS);
        mockResponse.setResponseCode(HttpStatus.OK);
        
        when(accessSettingMigrationService.migrateCBPlanAccessSettingRules()).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = controller.migrateCBPlanAccessSettingRules();
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
    }

    @Test
    void testSearchCbPlan() throws Exception {
        SearchCriteria criteria = new SearchCriteria();
        
        ApiResponse mockResponse = new ApiResponse();
        mockResponse.getParams().setStatus(Constants.SUCCESS);
        mockResponse.setResponseCode(HttpStatus.OK);
        
        when(cbPlanService.searchCbPlan(any(), anyString(), anyString())).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = controller.searchCbPlan(criteria, "token", "orgId");
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
    }

    @Test
    void testRetireCbPlan() throws Exception {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, "plan123");
        request.setRequest(requestMap);
        
        ApiResponse mockResponse = new ApiResponse();
        mockResponse.getParams().setStatus(Constants.SUCCESS);
        mockResponse.setResponseCode(HttpStatus.OK);
        
        when(cbPlanService.retireCbPlan(any(), anyString(), anyString(), any())).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = controller.retireCbPlan(request, "token", "orgId", Arrays.asList("ADMIN"));
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
    }

    @Test
    void testGetCBPlanListForUser() throws Exception {
        ApiResponse mockResponse = new ApiResponse();
        mockResponse.getParams().setStatus(Constants.SUCCESS);
        mockResponse.setResponseCode(HttpStatus.OK);
        mockResponse.getResult().put(Constants.COUNT, 2);
        
        when(cbPlanLearnerService.getCBPlanListForUser(anyString(), anyString(), eq(false))).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = controller.getCBPlanListForUser("token", "orgId");
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
    }

    @Test
    void testConstructor() {
        CbPlanWithAccessSettings newController = new CbPlanWithAccessSettings(
            cbPlanService, accessSettingMigrationService, cbPlanLearnerService);
        
        assertNotNull(newController);
    }
}