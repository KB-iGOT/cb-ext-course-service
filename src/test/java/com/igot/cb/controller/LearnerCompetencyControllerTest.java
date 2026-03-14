package com.igot.cb.controller;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.CompetencyService;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Test class for LearnerCompetencyController
 * Tests all scenarios including success, empty data, and error cases
 */
@ExtendWith(MockitoExtension.class)
class LearnerCompetencyControllerTest {

    @Mock
    private CompetencyService competencyService;

    @InjectMocks
    private LearnerCompetencyController controller;

    private String authToken;
    private ApiResponse mockResponse;

    @BeforeEach
    void setUp() {
        authToken = "Bearer test-token-12345";
        mockResponse = new ApiResponse();
        mockResponse.setId(Constants.API_FETCH_USER_COMPETENCY);
    }

    @Test
    void testGetLearnerCompetency_Success_WithData() {
        // Arrange
        Map<String, Object> competencyData = createMockCompetencyData();
        mockResponse.setResponseCode(HttpStatus.OK);
        mockResponse.setResult(competencyData);

        when(competencyService.fetchUserCompetency(authToken)).thenReturn(mockResponse);

        // Act
        ResponseEntity<Object> response = controller.getLearnerCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody() instanceof ApiResponse);

        ApiResponse apiResponse = (ApiResponse) response.getBody();
        assertEquals(HttpStatus.OK, apiResponse.getResponseCode());
        assertNotNull(apiResponse.getResult());
        assertEquals(competencyData, apiResponse.getResult());

        verify(competencyService, times(1)).fetchUserCompetency(authToken);
    }

    @Test
    void testGetLearnerCompetency_Success_EmptyData() {
        // Arrange
        mockResponse.setResponseCode(HttpStatus.OK);
        mockResponse.setResult(new HashMap<>());

        when(competencyService.fetchUserCompetency(authToken)).thenReturn(mockResponse);

        // Act
        ResponseEntity<Object> response = controller.getLearnerCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());

        ApiResponse apiResponse = (ApiResponse) response.getBody();
        assertNotNull(apiResponse.getResult());
        assertTrue(apiResponse.getResult().isEmpty());

        verify(competencyService, times(1)).fetchUserCompetency(authToken);
    }

    @Test
    void testGetLearnerCompetency_InternalServerError() {
        // Arrange
        mockResponse.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        mockResponse.getParams().setStatus(Constants.FAILED);
        mockResponse.getParams().setErrMsg("Database connection failed");

        Map<String, Object> errorResult = new HashMap<>();
        errorResult.put(Constants.RESPONSE, "Error fetching competency data");
        mockResponse.setResult(errorResult);

        when(competencyService.fetchUserCompetency(authToken)).thenReturn(mockResponse);

        // Act
        ResponseEntity<Object> response = controller.getLearnerCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());

        ApiResponse apiResponse = (ApiResponse) response.getBody();
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, apiResponse.getResponseCode());
        assertEquals(Constants.FAILED, apiResponse.getParams().getStatus());
        assertNotNull(apiResponse.getParams().getErrMsg());

        verify(competencyService, times(1)).fetchUserCompetency(authToken);
    }

    @Test
    void testGetLearnerCompetency_WithNullAuthToken() {
        // Arrange
        mockResponse.setResponseCode(HttpStatus.UNAUTHORIZED);
        mockResponse.getParams().setStatus(Constants.FAILED);
        mockResponse.getParams().setErrMsg("Invalid authentication token");

        when(competencyService.fetchUserCompetency(null)).thenReturn(mockResponse);

        // Act
        ResponseEntity<Object> response = controller.getLearnerCompetency(null);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.UNAUTHORIZED, response.getStatusCode());

        verify(competencyService, times(1)).fetchUserCompetency(null);
    }

    @Test
    void testGetLearnerCompetency_WithEmptyAuthToken() {
        // Arrange
        String emptyToken = "";
        mockResponse.setResponseCode(HttpStatus.UNAUTHORIZED);
        mockResponse.getParams().setStatus(Constants.FAILED);

        when(competencyService.fetchUserCompetency(emptyToken)).thenReturn(mockResponse);

        // Act
        ResponseEntity<Object> response = controller.getLearnerCompetency(emptyToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.UNAUTHORIZED, response.getStatusCode());

        verify(competencyService, times(1)).fetchUserCompetency(emptyToken);
    }

    @Test
    void testGetLearnerCompetency_ServiceReturnsNull() {
        // Arrange
        when(competencyService.fetchUserCompetency(authToken)).thenReturn(null);

        // Act & Assert
        assertThrows(NullPointerException.class, () -> {
            controller.getLearnerCompetency(authToken);
        });

        verify(competencyService, times(1)).fetchUserCompetency(authToken);
    }

    @Test
    void testGetLearnerCompetency_MultipleInvocations() {
        // Arrange
        Map<String, Object> competencyData = createMockCompetencyData();
        mockResponse.setResponseCode(HttpStatus.OK);
        mockResponse.setResult(competencyData);

        when(competencyService.fetchUserCompetency(anyString())).thenReturn(mockResponse);

        // Act
        controller.getLearnerCompetency("token1");
        controller.getLearnerCompetency("token2");
        controller.getLearnerCompetency("token3");

        // Assert
        verify(competencyService, times(3)).fetchUserCompetency(anyString());
    }

    @Test
    void testGetLearnerCompetency_VerifyResponseMapping() {
        // Arrange
        mockResponse.setResponseCode(HttpStatus.OK);
        mockResponse.setResult(createMockCompetencyData());

        when(competencyService.fetchUserCompetency(authToken)).thenReturn(mockResponse);

        // Act
        ResponseEntity<Object> response = controller.getLearnerCompetency(authToken);

        // Assert
        assertEquals(mockResponse, response.getBody());
        assertEquals(mockResponse.getResponseCode(), response.getStatusCode());
    }

    // Helper method to create mock competency data
    private Map<String, Object> createMockCompetencyData() {
        Map<String, Object> data = new HashMap<>();
        data.put("competency_subtheme_id", "kcmfinal_fw_subtheme_test");
        data.put("user_id", "test-user-123");
        data.put("competency_theme_id", "kcmfinal_fw_theme_test");
        data.put("competency_area_id", "kcmfinal_fw_competencyarea_test");

        Map<String, Object> competencyDetails = new HashMap<>();
        data.put("competency_details", competencyDetails);

        return data;
    }
}
