package com.igot.cb.service;

import com.igot.cb.model.ApiResponse;

import java.util.Map;

/**
 * Service interface for Content Health operations.
 * Provides methods to fetch content health data from Redis.
 */
public interface ContentHealthService {

    /**
     * Get content health report for a single content ID.
     *
     * @param contentId The content ID to fetch health report for
     * @return ApiResponse containing the content health data
     */
    ApiResponse getContentHealthReport(String contentId);

    /**
     * Get content health summary for multiple content IDs.
     *
     * @param requestBody Request body containing list of content IDs
     * @return ApiResponse containing the content health summary data
     */
    ApiResponse getContentHealthSummary(Map<String, Object> requestBody);
}


