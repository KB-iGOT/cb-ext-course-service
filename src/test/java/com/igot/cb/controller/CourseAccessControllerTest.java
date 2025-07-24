package com.igot.cb.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.CourseAccessServiceImpl;

@ExtendWith(MockitoExtension.class)
public class CourseAccessControllerTest {

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
}
