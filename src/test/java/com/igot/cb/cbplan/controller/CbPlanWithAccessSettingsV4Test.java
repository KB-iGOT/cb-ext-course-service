package com.igot.cb.cbplan.controller;

import com.igot.cb.cbplan.service.CbPlanServiceV4;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanWithAccessSettingsV4Test {

    private static final String TOKEN = "token";
    private static final String PLAN_ID = "plan123";

    @Mock
    private CbPlanServiceV4 cbPlanServiceV4;

    @InjectMocks
    private CbPlanWithAccessSettingsV4 controller;

    private static ApiResponse successResponse() {
        ApiResponse response = new ApiResponse();
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
        return response;
    }

    private static ApiRequest apiRequest() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, PLAN_ID);
        request.setRequest(requestMap);
        return request;
    }

    @Test
    void createCbPlan_delegatesToServiceAndReturnsCreated() {
        ApiResponse mockResponse = successResponse();
        mockResponse.setResponseCode(HttpStatus.CREATED);
        when(cbPlanServiceV4.createCbPlan(any(), anyString())).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = controller.createCbPlan(apiRequest(), TOKEN);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
        verify(cbPlanServiceV4).createCbPlan(any(), eq(TOKEN));
    }

    @Test
    void createCbPlan_propagatesFailureStatusCode() {
        ApiResponse mockResponse = new ApiResponse();
        mockResponse.getParams().setStatus(Constants.FAILED);
        mockResponse.setResponseCode(HttpStatus.BAD_REQUEST);
        when(cbPlanServiceV4.createCbPlan(any(), anyString())).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = controller.createCbPlan(apiRequest(), TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals(Constants.FAILED, response.getBody().getParams().getStatus());
    }

    @Test
    void updateCbPlan_delegatesToServiceWithAllArgs() {
        when(cbPlanServiceV4.updateCbPlan(any(), anyString())).thenReturn(successResponse());

        ResponseEntity<ApiResponse> response = controller.updateCbPlan(apiRequest(), TOKEN);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
        verify(cbPlanServiceV4).updateCbPlan(any(), eq(TOKEN));
    }

    @Test
    void publishCbPlan_delegatesToServiceWithAllArgs() {
        when(cbPlanServiceV4.publishCbPlan(any(), anyString())).thenReturn(successResponse());

        ResponseEntity<ApiResponse> response = controller.publishCbPlan(apiRequest(), TOKEN);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
        verify(cbPlanServiceV4).publishCbPlan(any(), eq(TOKEN));
    }

    @Test
    void readCbPlan_delegatesToServiceAndReturnsContent() {
        ApiResponse mockResponse = successResponse();
        mockResponse.getResult().put(Constants.CONTENT, Map.of(Constants.ID, PLAN_ID));
        when(cbPlanServiceV4.readCbPlan(anyString(), anyString())).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = controller.readCbPlan(PLAN_ID, TOKEN);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody().getResult().get(Constants.CONTENT));
        verify(cbPlanServiceV4).readCbPlan(PLAN_ID, TOKEN);
    }

    @Test
    void readCbPlan_propagatesErrorStatus() {
        ApiResponse mockResponse = new ApiResponse();
        mockResponse.getParams().setStatus(Constants.FAILED);
        mockResponse.setResponseCode(HttpStatus.BAD_REQUEST);
        when(cbPlanServiceV4.readCbPlan(anyString(), anyString())).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = controller.readCbPlan(PLAN_ID, TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals(Constants.FAILED, response.getBody().getParams().getStatus());
    }

    @Test
    void constructor_wiresServiceDependency() {
        assertNotNull(new CbPlanWithAccessSettingsV4(cbPlanServiceV4));
    }
}
