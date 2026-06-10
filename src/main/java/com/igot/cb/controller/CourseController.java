package com.igot.cb.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.ContentStateServiceImpl;
import com.igot.cb.util.Constants;

@RestController
@RequestMapping("/content/v2")
public class CourseController {
    private final ContentStateServiceImpl courseService;

    public CourseController(ContentStateServiceImpl courseService) {
        this.courseService = courseService;
    }

    @PostMapping("/state/read")
    public ResponseEntity<Object> readContentState(@RequestBody Map<String, Object> requestBody, @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = courseService.readContentState(requestBody, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PatchMapping("/state/update")
    public ResponseEntity<Object> updateContentState(@RequestBody Map<String, Object> requestBody, @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = courseService.updateContentState(requestBody, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("chatbot/consumption/v2/read")
    public ResponseEntity<Object> readUserContentConsumptionV2(@RequestBody Map<String, Object> requestBody) {
        ApiResponse response = courseService.readUserContentConsumptionV2(requestBody);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
