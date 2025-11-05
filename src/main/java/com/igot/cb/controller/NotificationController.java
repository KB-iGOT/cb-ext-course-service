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
     * Notifies learners in a batch that an instructor uploaded an assignment.
     *
     * @param requestData the request body containing courseId, batchId and assignmentTitle
     * @param authToken   the authentication token for the user
     * @return a ResponseEntity containing the ApiResponse with notification result
     */
    @PostMapping("/upload")
    public ResponseEntity<Object> notifyAssignmentUploaded(@RequestBody Map<String, Object> requestData, @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = notificationService.notifyAssignmentUploaded(requestData, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Notifies learners in a batch that an instructor evaluated an assignment.
     *
     * @param requestData the request body containing courseId, batchId, assignmentTitle and learnerId
     * @param authToken   the authentication token for the user
     * @return a ResponseEntity containing the ApiResponse with notification result
     */
    @PostMapping("/evaluate")
    public ResponseEntity<Object> notifyAssignmentEvaluation(@RequestBody Map<String, Object> requestData, @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = notificationService.notifyAssignmentEvaluate(requestData, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Notifies learners in a batch that a learner submitted an assignment.
     *
     * @param requestData the request body containing courseId, batchId and assignmentTitle and instructorId
     * @param authToken   the authentication token for the user
     * @return a ResponseEntity containing the ApiResponse with notification result
     */
    @PostMapping("/submit")
    public ResponseEntity<Object> notifyAssignmentSubmit(@RequestBody Map<String, Object> requestData, @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = notificationService.notifyAssignmentSubmit(requestData, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
