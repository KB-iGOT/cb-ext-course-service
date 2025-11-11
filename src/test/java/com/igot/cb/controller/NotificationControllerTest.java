package com.igot.cb.controller;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.NotificationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class NotificationControllerTest {

    @Mock
    private NotificationService notificationService;

    @InjectMocks
    private NotificationController notificationController;

    private Map<String, Object> mockRequest;
    private static final String TOKEN = "test-token";
    private ApiResponse successResponse;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        mockRequest = Map.of("courseId", "C101", "batchId", "B202", "assignmentTitle", "Test Assignment");
        successResponse = new ApiResponse();
        successResponse.setResponseCode(HttpStatus.OK);
    }

    @Test
    void testNotifyAssignmentUploaded_Success() {
        when(notificationService.notifyAssignmentUploaded(mockRequest, TOKEN)).thenReturn(successResponse);
        ResponseEntity<Object> response = notificationController.notifyAssignmentUploaded(mockRequest, TOKEN);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(successResponse, response.getBody());
    }

    @Test
    void testNotifyAssignmentEvaluation_Success() {
        when(notificationService.notifyAssignmentEvaluate(mockRequest, TOKEN)).thenReturn(successResponse);
        ResponseEntity<Object> response = notificationController.notifyAssignmentEvaluation(mockRequest, TOKEN);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(successResponse, response.getBody());
    }

    @Test
    void testNotifyAssignmentSubmit_Success() {
        when(notificationService.notifyAssignmentSubmit(mockRequest, TOKEN)).thenReturn(successResponse);
        ResponseEntity<Object> response = notificationController.notifyAssignmentSubmit(mockRequest, TOKEN);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(successResponse, response.getBody());
    }

}
