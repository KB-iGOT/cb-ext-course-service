package com.igot.cb.controller;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.ContentHealthService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST Controller for Content Health operations.
 * Provides endpoints to fetch content health data from Redis.
 */
@RestController
@RequestMapping("/contenthealth/v1")
public class ContentHealthController {

    private final ContentHealthService contentHealthService;

    public ContentHealthController(ContentHealthService contentHealthService) {
        this.contentHealthService = contentHealthService;
    }

    /**
     * Get content health report for a single content ID.
     *
     * @param contentId The content ID to fetch health report for
     * @return ResponseEntity containing the content health data
     */
    @GetMapping("/report/{contentId}")
    public ResponseEntity<ApiResponse> getContentHealthReport(@PathVariable String contentId) {
        ApiResponse response = contentHealthService.getContentHealthReport(contentId);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Get content health summary for multiple content IDs.
     *
     * @param requestBody Request body containing list of content IDs
     * @return ResponseEntity containing the content health summary data
     */
    @PostMapping("/summary")
    public ResponseEntity<ApiResponse> getContentHealthSummary(@RequestBody Map<String, Object> requestBody) {
        ApiResponse response = contentHealthService.getContentHealthSummary(requestBody);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

}