package com.igot.cb.consentacknowledge;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConsentAcknowledgeControllerTest {
    private IConsentAcknowledgeService acknowledgeService;
    private ConsentAcknowledgeController controller;

    @BeforeEach
    void setUp() {
        acknowledgeService = Mockito.mock(IConsentAcknowledgeService.class);
        controller = new ConsentAcknowledgeController(acknowledgeService);
    }

    @Test
    void testAcknowledgeDeclaration_success() {
        String authToken = "token";
        Map<String, Object> request = new HashMap<>();
        ApiResponse apiResponse = new ApiResponse();
        apiResponse.setResponseCode(HttpStatus.OK);
        Mockito.when(acknowledgeService.acknowledgeDeclaration(request, authToken)).thenReturn(apiResponse);

        ResponseEntity<ApiResponse> response = controller.acknowledgeDeclaration(authToken, request);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void testAcknowledgeDeclaration_error() {
        String authToken = "token";
        Map<String, Object> request = new HashMap<>();
        ApiResponse apiResponse = new ApiResponse();
        apiResponse.setResponseCode(HttpStatus.BAD_REQUEST);
        Mockito.when(acknowledgeService.acknowledgeDeclaration(request, authToken)).thenReturn(apiResponse);

        ResponseEntity<ApiResponse> response = controller.acknowledgeDeclaration(authToken, request);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void testGetConsentAcknowledgementDetails_success() {
        String authToken = "token";
        String contentId = "content123";
        String consentId = "consent456";
        ApiResponse apiResponse = new ApiResponse();
        apiResponse.setResponseCode(HttpStatus.OK);
        Mockito.when(acknowledgeService.getConsentAcknowledgementDetails(contentId, consentId, authToken)).thenReturn(apiResponse);

        ResponseEntity<ApiResponse> response = controller.getConsentAcknowledgementDetails(consentId, contentId, authToken);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void testGetConsentAcknowledgementDetails_error() {
        String authToken = "token";
        String contentId = "content123";
        String consentId = "consent456";
        ApiResponse apiResponse = new ApiResponse();
        apiResponse.setResponseCode(HttpStatus.NOT_FOUND);
        Mockito.when(acknowledgeService.getConsentAcknowledgementDetails(contentId, consentId, authToken)).thenReturn(apiResponse);

        ResponseEntity<ApiResponse> response = controller.getConsentAcknowledgementDetails(consentId, contentId, authToken);
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }
}
