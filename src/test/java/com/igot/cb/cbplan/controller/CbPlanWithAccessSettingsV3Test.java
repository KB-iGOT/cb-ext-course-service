package com.igot.cb.cbplan.controller;

import com.igot.cb.cbplan.service.CbPlanServiceV3;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
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

import java.util.Arrays;
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
class CbPlanWithAccessSettingsV3Test {

    private static final String TOKEN = "token";
    private static final String ORG_ID = "orgId";
    private static final String PLAN_ID = "plan123";

    @Mock
    private CbPlanServiceV3 cbPlanServiceV3;

    @InjectMocks
    private CbPlanWithAccessSettingsV3 controller;

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
    void testCreateCbPlan() {
        ApiResponse mockResponse = successResponse();
        mockResponse.setResponseCode(HttpStatus.CREATED);
        when(cbPlanServiceV3.createCbPlan(any(), anyString(), anyString())).thenReturn(mockResponse);
        ResponseEntity<ApiResponse> response = controller.createCbPlan(apiRequest(), TOKEN, ORG_ID);
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
        verify(cbPlanServiceV3).createCbPlan(any(), eq(ORG_ID), eq(TOKEN));
    }

    @Test
    void testUpdateCbPlan() {
        when(cbPlanServiceV3.updateCbPlan(any(), anyString(), anyString(), any())).thenReturn(successResponse());
        ResponseEntity<ApiResponse> response = controller.updateCbPlan(
                apiRequest(), TOKEN, ORG_ID, Arrays.asList("ADMIN"));
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
    }

    @Test
    void testPublishCbPlan() {
        when(cbPlanServiceV3.publishCbPlan(any(), anyString(), anyString(), any())).thenReturn(successResponse());
        ResponseEntity<ApiResponse> response = controller.publishCbPlan(
                apiRequest(), TOKEN, ORG_ID, Arrays.asList("ADMIN"));
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
    }

    @Test
    void testRetireCbPlan() {
        when(cbPlanServiceV3.retireCbPlan(any(), anyString(), anyString(), any())).thenReturn(successResponse());
        ResponseEntity<ApiResponse> response = controller.retireCbPlan(
                apiRequest(), TOKEN, ORG_ID, Arrays.asList("ADMIN"));
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
    }

    @Test
    void testReadCbPlan() {
        ApiResponse mockResponse = successResponse();
        mockResponse.getResult().put(Constants.CONTENT, Map.of(Constants.ID, PLAN_ID));
        when(cbPlanServiceV3.readCbPlan(anyString(), anyString(), anyString())).thenReturn(mockResponse);
        ResponseEntity<ApiResponse> response = controller.readCbPlan(PLAN_ID, TOKEN, ORG_ID);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody().getResult().get(Constants.CONTENT));
        verify(cbPlanServiceV3).readCbPlan(PLAN_ID, ORG_ID, TOKEN);
    }

    @Test
    void testReadCbPlanPropagatesErrorStatus() {
        ApiResponse mockResponse = new ApiResponse();
        mockResponse.getParams().setStatus(Constants.FAILED);
        mockResponse.setResponseCode(HttpStatus.BAD_REQUEST);
        when(cbPlanServiceV3.readCbPlan(anyString(), anyString(), anyString())).thenReturn(mockResponse);
        ResponseEntity<ApiResponse> response = controller.readCbPlan(PLAN_ID, TOKEN, ORG_ID);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals(Constants.FAILED, response.getBody().getParams().getStatus());
    }

    @Test
    void testSearchCbPlan() {
        when(cbPlanServiceV3.searchCbPlan(any(), anyString(), anyString())).thenReturn(successResponse());
        ResponseEntity<ApiResponse> response = controller.searchCbPlan(new SearchCriteria(), TOKEN, ORG_ID);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
    }

    @Test
    void testGetCBPlanDictionary() {
        ApiResponse mockResponse = successResponse();
        mockResponse.getResult().put("aparCount", 2);
        when(cbPlanServiceV3.getCBPlanDictionaryForUser(any(), anyString())).thenReturn(mockResponse);
        ResponseEntity<ApiResponse> response = controller.getCBPlanDictionary(apiRequest(), TOKEN);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(2, response.getBody().getResult().get("aparCount"));
    }

    @Test
    void testConstructor() {
        assertNotNull(new CbPlanWithAccessSettingsV3(cbPlanServiceV3));
    }
}
