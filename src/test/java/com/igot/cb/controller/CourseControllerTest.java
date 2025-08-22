package com.igot.cb.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.ContentStateServiceImpl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@ExtendWith(MockitoExtension.class)
class CourseControllerTest {

    @Mock
    private ContentStateServiceImpl courseService;

    @InjectMocks
    private CourseController courseController;

    @Test
    void testReadContentState() {
        Map<String, Object> requestBody = new HashMap<>();
        String authToken = "Bearer test-token";
        ApiResponse mockResponse = new ApiResponse();
        mockResponse.setResponseCode(HttpStatus.OK);

        when(courseService.readContentState(requestBody, authToken)).thenReturn(mockResponse);

        ResponseEntity<Object> response = courseController.readContentState(requestBody, authToken);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockResponse, response.getBody());
        verify(courseService).readContentState(requestBody, authToken);
    }

    @Test
    void testUpdateContentState() {
        Map<String, Object> requestBody = new HashMap<>();
        String authToken = "Bearer test-token";
        ApiResponse mockResponse = new ApiResponse();
        mockResponse.setResponseCode(HttpStatus.OK);

        when(courseService.updateContentState(requestBody, authToken)).thenReturn(mockResponse);

        ResponseEntity<Object> response = courseController.updateContentState(requestBody, authToken);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockResponse, response.getBody());
        verify(courseService).updateContentState(requestBody, authToken);
    }
}
