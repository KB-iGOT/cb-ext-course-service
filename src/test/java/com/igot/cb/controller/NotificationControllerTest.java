package com.igot.cb.controller;

import com.igot.cb.model.ApiRespParam;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.NotificationService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationControllerTest {
    @Mock
    private NotificationService notificationService;

    @InjectMocks
    private NotificationController notificationController;

    private ApiResponse createApiResponse(String id, HttpStatus code, Map<String, Object> result) {
        ApiResponse response = new ApiResponse(id);
        ApiRespParam params = new ApiRespParam("mock-res-id");
        params.setStatus("successful");
        response.setParams(params);
        response.setResponseCode(code);
        response.setResult(result);
        return response;
    }

    @Test
    void testNotifyAssignmentUploaded() {
        Map<String, Object> request = Map.of("courseId", "c1", "batchId", "b1", "assignmentTitle", "A1");
        ApiResponse mockResponse = createApiResponse("notify.upload", HttpStatus.OK, Map.of("notified", true));

        when(notificationService.notifyAssignmentUploaded(request, "mock-token")).thenReturn(mockResponse);

        ResponseEntity<Object> response = notificationController.notifyAssignmentUploaded(request, "mock-token");
        ApiResponse body = (ApiResponse) response.getBody();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("notify.upload", body.getId());
        assertEquals(true, body.getResult().get("notified"));
    }

    @Test
    void testNotifyAssignmentEvaluation() {
        Map<String, Object> request = Map.of("courseId", "c1", "batchId", "b1", "assignmentTitle", "A1", "learnerId", "l1");
        ApiResponse mockResponse = createApiResponse("notify.evaluate", HttpStatus.OK, Map.of("evaluated", true));

        when(notificationService.notifyAssignmentEvaluate(request, "mock-token")).thenReturn(mockResponse);

        ResponseEntity<Object> response = notificationController.notifyAssignmentEvaluation(request, "mock-token");
        ApiResponse body = (ApiResponse) response.getBody();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("notify.evaluate", body.getId());
        assertEquals(true, body.getResult().get("evaluated"));
    }

    @Test
    void testNotifyAssignmentSubmit() {
        Map<String, Object> request = Map.of("courseId", "c1", "batchId", "b1", "assignmentTitle", "A1", "instructorId", "i1");
        ApiResponse mockResponse = createApiResponse("notify.submit", HttpStatus.CREATED, Map.of("submitted", true));

        when(notificationService.notifyAssignmentSubmit(request, "mock-token")).thenReturn(mockResponse);

        ResponseEntity<Object> response = notificationController.notifyAssignmentSubmit(request, "mock-token");
        ApiResponse body = (ApiResponse) response.getBody();

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals("notify.submit", body.getId());
        assertEquals(true, body.getResult().get("submitted"));
    }
}
