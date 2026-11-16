package com.igot.cb.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.OrgEligibilityServiceImpl;
import com.igot.cb.util.Constants;

@ExtendWith(MockitoExtension.class)
class OrgEligibilityControllerTest {

    @Mock
    private OrgEligibilityServiceImpl orgEligibilityService;

    @InjectMocks
    private OrgEligibilityController controller;

    private static final String AUTH_TOKEN = "Bearer test-token";
    private static final String ORG_ID = "0146196505889341440";

    private Map<String, Object> requestBody;

    @BeforeEach
    void setUp() {
        requestBody = new HashMap<>();
        requestBody.put(Constants.ORG_ID, ORG_ID);
        requestBody.put(Constants.COURSE_COUNT, 1);
    }

    @Test
    void testUpsertOrgEligibility_Success() {
        ApiResponse mockResponse = ApiResponse.createDefaultResponse(Constants.API_ORG_ELIGIBILITY_UPSERT);
        mockResponse.getResult().put(Constants.ORG_ID, ORG_ID);
        when(orgEligibilityService.upsertOrgEligibility(anyMap(), anyString())).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = controller.upsertOrgEligibility(requestBody, AUTH_TOKEN);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(ORG_ID, response.getBody().getResult().get(Constants.ORG_ID));
        verify(orgEligibilityService, times(1)).upsertOrgEligibility(requestBody, AUTH_TOKEN);
    }

    @Test
    void testUpsertOrgEligibility_Failure() {
        ApiResponse mockResponse = ApiResponse.createDefaultResponse(Constants.API_ORG_ELIGIBILITY_UPSERT);
        mockResponse.getParams().setStatus(Constants.FAILED);
        mockResponse.getParams().setErr("orgId is missing in request");
        mockResponse.setResponseCode(HttpStatus.BAD_REQUEST);
        when(orgEligibilityService.upsertOrgEligibility(anyMap(), anyString())).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = controller.upsertOrgEligibility(new HashMap<>(), AUTH_TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals(Constants.FAILED, response.getBody().getParams().getStatus());
    }

    @Test
    void testReadOrgEligibility_Success() {
        ApiResponse mockResponse = ApiResponse.createDefaultResponse(Constants.API_ORG_ELIGIBILITY_READ);
        mockResponse.getResult().put(Constants.ORG_ID, ORG_ID);
        mockResponse.getResult().put(Constants.COURSE_COUNT, 10);
        when(orgEligibilityService.readOrgEligibility(ORG_ID, AUTH_TOKEN)).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = controller.readOrgEligibility(ORG_ID, AUTH_TOKEN);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(ORG_ID, response.getBody().getResult().get(Constants.ORG_ID));
        verify(orgEligibilityService, times(1)).readOrgEligibility(ORG_ID, AUTH_TOKEN);
    }

    @Test
    void testReadOrgEligibility_NotFound() {
        ApiResponse mockResponse = ApiResponse.createDefaultResponse(Constants.API_ORG_ELIGIBILITY_READ);
        mockResponse.getParams().setStatus(Constants.FAILED);
        mockResponse.getParams().setErr("Org eligibility not found for orgId: " + ORG_ID);
        mockResponse.setResponseCode(HttpStatus.BAD_REQUEST);
        when(orgEligibilityService.readOrgEligibility(ORG_ID, AUTH_TOKEN)).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = controller.readOrgEligibility(ORG_ID, AUTH_TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals(Constants.FAILED, response.getBody().getParams().getStatus());
    }
}
