package com.igot.cb.controller;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.CourseAccessServiceImpl;

@ExtendWith(MockitoExtension.class)
class CourseAccessControllerTest {

    @Mock
    private CourseAccessServiceImpl courseAccessService;

    @InjectMocks
    private CourseAccessController courseAccessController;

    @Test
    void testGetCoursesForUser_Success() {
        // Arrange
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("userId", "user-123");

        String authToken = "Bearer some-auth-token";

        ApiResponse mockResponse = new ApiResponse();
        mockResponse.setResponseCode(HttpStatus.OK);
        mockResponse.setResult(Map.of("course1", "Course A", "course2", "Course B"));

        when(courseAccessService.getCoursesForUser(requestBody, authToken)).thenReturn(mockResponse);

        // Act
        ResponseEntity<ApiResponse> response = courseAccessController.getCoursesForUser(requestBody, authToken);

        // Assert
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockResponse, response.getBody());
        verify(courseAccessService, times(1)).getCoursesForUser(requestBody, authToken);
    }

    @Test
    void testGetCoursesForUser_EmptyResult() {
        // Arrange
        Map<String, Object> requestBody = Map.of("userId", "user-456");
        String authToken = "Bearer token";

        ApiResponse emptyResponse = new ApiResponse();
        emptyResponse.setResponseCode(HttpStatus.NO_CONTENT);
        emptyResponse.setResult(Map.of());

        when(courseAccessService.getCoursesForUser(requestBody, authToken)).thenReturn(emptyResponse);

        // Act
        ResponseEntity<ApiResponse> response = courseAccessController.getCoursesForUser(requestBody, authToken);

        // Assert
        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        assertTrue(response.getBody().getResult().isEmpty());
        verify(courseAccessService, times(1)).getCoursesForUser(requestBody, authToken);
    }

    @Test
    void testGetAssignedCoursesForUser() {
        // Arrange
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("userId", 123);

        String authToken = "dummy-token";

        ApiResponse mockResponse = new ApiResponse();
        mockResponse.setResponseCode(HttpStatus.OK);
        mockResponse.setResult(Map.of("course1", "Course A", "course2", "Course B"));

        when(courseAccessService.getAssignedCoursesForUser(requestBody, authToken))
                .thenReturn(mockResponse);

        // Act
        ResponseEntity<ApiResponse> response = courseAccessController.getAssignedCoursesForUser(requestBody, authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockResponse, response.getBody());
        assertEquals(mockResponse, response.getBody());

        verify(courseAccessService, times(1))
                .getAssignedCoursesForUser(requestBody, authToken);
    }

    @Test
    void testGetAssignedExternalCoursesForUser_Success() {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("partnerId", "partner-1");

        String authToken = "Bearer some-auth-token";

        ApiResponse mockResponse = new ApiResponse();
        mockResponse.setResponseCode(HttpStatus.OK);
        mockResponse.setResult(Map.of("content", Map.of("id", "C1", "name", "External Course")));

        when(courseAccessService.getAssignedExternalCoursesForUser(requestBody, authToken)).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = courseAccessController.getAssignedExternalCoursesForUser(requestBody, authToken);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockResponse, response.getBody());
        verify(courseAccessService, times(1)).getAssignedExternalCoursesForUser(requestBody, authToken);
    }

    @Test
    void testGetAssignedExternalCoursesForUser_NoContent() {
        Map<String, Object> requestBody = Map.of("partnerId", "partner-2");
        String authToken = "Bearer token";

        ApiResponse noContent = new ApiResponse();
        noContent.setResponseCode(HttpStatus.NO_CONTENT);
        noContent.setResult(Map.of());

        when(courseAccessService.getAssignedExternalCoursesForUser(requestBody, authToken)).thenReturn(noContent);

        ResponseEntity<ApiResponse> response = courseAccessController.getAssignedExternalCoursesForUser(requestBody, authToken);

        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().getResult().isEmpty());
        verify(courseAccessService, times(1)).getAssignedExternalCoursesForUser(requestBody, authToken);
    }

    @Test
    void testGetAssignedExternalCoursesForUser_ServiceCalledWithRequestAndToken() {
        Map<String, Object> requestBody = Map.of("partnerId", "partner-verify");
        String authToken = "Bearer verify-token";

        ApiResponse ok = new ApiResponse();
        ok.setResponseCode(HttpStatus.OK);
        ok.setResult(Map.of("status", "ok"));

        when(courseAccessService.getAssignedExternalCoursesForUser(anyMap(), anyString())).thenReturn(ok);

        ResponseEntity<ApiResponse> response = courseAccessController.getAssignedExternalCoursesForUser(requestBody, authToken);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        ArgumentCaptor<Map> reqCaptor = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<String> tokenCaptor = ArgumentCaptor.forClass(String.class);
        verify(courseAccessService, times(1)).getAssignedExternalCoursesForUser(reqCaptor.capture(), tokenCaptor.capture());

        assertEquals(requestBody, reqCaptor.getValue());
        assertEquals(authToken, tokenCaptor.getValue());
    }
}
