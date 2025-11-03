package com.igot.cb.controller;

import com.igot.cb.util.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.NotificationService;

import java.util.Map;

@RestController
@RequestMapping("/v1/notifyAssignment")
public class NotificationController {

    @Autowired
    private NotificationService notificationService;

    /**
     * Notify learners in a batch that instructor uploaded an assignment.
     * Example: POST /notifyAssignment/upload with JSON body {"courseId":"...","batchId":"...","assignmentTitle":"..."}
     */
    @PostMapping("/upload")
    public ResponseEntity<Object> notifyAssignmentUploaded(@RequestBody Map<String, Object> requestData, @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = notificationService.notifyAssignmentUploaded(requestData, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Notify learners in a batch that instructor evaluated an assignment.
     * Example: POST /notifyAssignment/evaluate with JSON body {"courseId":"...","batchId":"...","assignmentTitle":"...","learnerId":"..."}
     */
    @PostMapping("/evaluate")
    public ResponseEntity<Object> notifyAssignmentEvaluation(@RequestBody Map<String, Object> requestData, @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = notificationService.notifyAssignmentEvaluate(requestData, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Notify learners in a batch that instructor evaluated an assignment.
     * Example: POST /notifyAssignment/submit with JSON body {"courseId":"...","batchId":"...","assignmentTitle":"...","instructorId":"..."}
     */
    @PostMapping("/submit")
    public ResponseEntity<Object> notifyAssignmentSubmit(@RequestBody Map<String, Object> requestData, @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = notificationService.notifyAssignmentSubmit(requestData, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
