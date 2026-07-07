package com.igot.cb.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.CourseAccessServiceImpl;
import com.igot.cb.util.Constants;

/**
 * Controller for handling course access related requests.
 * This controller provides an endpoint to retrieve courses assigned to a user.
 * 
 * @author Karthikeyan.R (karthik-tarento)
 */
@RestController
public class CourseAccessController {

    private final CourseAccessServiceImpl courseAccessService;

    public CourseAccessController(CourseAccessServiceImpl courseAccessService) {
        this.courseAccessService = courseAccessService;
    }

    /**
     * Retrieves the courses assigned to a user.
     *
     * @param requestBody the request body containing user details
     * @param authToken   the authentication token for the user
     * @return a ResponseEntity containing the ApiResponse with course details
     */
    @PostMapping("/user/v1/assignedcourses")
    public ResponseEntity<ApiResponse> getCoursesForUser(@RequestBody Map<String, Object> requestBody,
            @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = courseAccessService.getCoursesForUser(requestBody, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/user/v2/assignedcourses")
    public ResponseEntity<ApiResponse> getAssignedCoursesForUser(@RequestBody Map<String, Object> requestBody,
                                                         @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = courseAccessService.getAssignedCoursesForUser(requestBody, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/user/v1/assigned/externalcourses")
    public ResponseEntity<ApiResponse> getAssignedExternalCoursesForUser(@RequestBody Map<String, Object> requestBody, @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = courseAccessService.getAssignedExternalCoursesForUser(requestBody,authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/admin/user/v2/assignedcourses/{userId}")
    public ResponseEntity<ApiResponse> getAssignedCoursesForUserByAdmin(@PathVariable("userId") String userId,
            @RequestBody Map<String, Object> requestBody, @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = courseAccessService.getAssignedCoursesForUserByAdmin(userId, requestBody, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/content/user/info")
    public ResponseEntity<ApiResponse> getPersonalContentInfo(@RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = courseAccessService.getPersonalContentInfo(authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
