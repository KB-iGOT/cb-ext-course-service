package com.igot.cb.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisDataCacheMgr;
import com.igot.cb.common.ServerProperties;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.ContentHealthService;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service implementation for Content Health operations.
 * Fetches content health data from Redis using RedisDataCacheMgr.
 */
@Service
@Slf4j
public class ContentHealthServiceImpl implements ContentHealthService {

    private final RedisDataCacheMgr redisDataCacheMgr;
    private final ServerProperties serverProperties;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ContentHealthServiceImpl(RedisDataCacheMgr redisDataCacheMgr, 
                                   ServerProperties serverProperties) {
        this.redisDataCacheMgr = redisDataCacheMgr;
        this.serverProperties = serverProperties;
    }

    /**
     * Get content health report for a single content ID.
     * Fetches data from Redis using the pattern: content:health:{contentId}
     *
     * @param contentId The content ID to fetch health report for
     * @return ApiResponse containing the content health data
     */
    @Override
    public ApiResponse getContentHealthReport(String contentId) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CONTENT_HEALTH_REPORT);

        try {
            if (StringUtils.isBlank(contentId)) {
                setFailedResponse(response, "Content ID is required and cannot be empty");
                return response;
            }

            String redisKey = Constants.COURSE_METRICS_KEY_PREFIX + contentId.trim();
            Map<String, String> cachedData = redisDataCacheMgr.getAllHashFields(redisKey, serverProperties.getContentHealthDbIndex());

            List<Map<String, Object>> healthDataList = new ArrayList<>();
            if (MapUtils.isNotEmpty(cachedData)) {
                for (Map.Entry<String, String> entry : cachedData.entrySet()) {
                    try {
                        Map<String, Object> metricData = objectMapper.readValue(entry.getValue(), Map.class);
                        healthDataList.add(metricData);
                    } catch (Exception e) {
                        log.error("ContentHealthServiceImpl:getContentHealthReport - Error parsing field {}: {}", entry.getKey(), e.getMessage());
                    }
                }
                log.info("ContentHealthServiceImpl:getContentHealthReport - Successfully retrieved {} metrics for contentId: {}", healthDataList.size(), contentId);
            } else {
                log.warn("ContentHealthServiceImpl:getContentHealthReport - No health data found for contentId: {}, returning empty list", contentId);
            }

            Map<String, Object> contentData = new HashMap<>();
            contentData.put(contentId.trim(), healthDataList);

            List<Map<String, Object>> resultList = new ArrayList<>();
            resultList.add(contentData);

            response.put(Constants.CONTENT_LIST, resultList);
            response.setResponseCode(HttpStatus.OK);
        } catch (Exception e) {
            log.error("ContentHealthServiceImpl:getContentHealthReport - Error fetching health report for contentId: {}", contentId, e);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.put(Constants.ERROR, Constants.FAILED);
            response.put(Constants.ERROR_MESSAGE, "Failed to fetch content health report: " + e.getMessage());
        }
        return response;
    }

    /**
     * Get content health summary for multiple content IDs.
     * Fetches data from Redis for each content ID.
     *
     * @param requestBody Request body containing list of content IDs
     * @return ApiResponse containing the content health summary data
     */
    @Override
    public ApiResponse getContentHealthSummary(Map<String, Object> requestBody) {
        log.info("ContentHealthServiceImpl:getContentHealthSummary - Request received");

        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CONTENT_HEALTH_SUMMARY);

        try {
            if (MapUtils.isEmpty(requestBody)) {
                setFailedResponse(response, "Request body is empty");
                return response;
            }

            Object courseIdObj = requestBody.get(Constants.COURSE_ID);
            if (!(courseIdObj instanceof List) || ((List<?>) courseIdObj).isEmpty()) {
                setFailedResponse(response, "'courseId' is mandatory and should be a non-empty list");
                return response;
            }

            List<String> courseIds = (List<String>) courseIdObj;
            log.info("ContentHealthServiceImpl:getContentHealthSummary - Processing {} course IDs", courseIds.size());

            List<Map<String, Object>> resultList = new ArrayList<>();

            for (Object courseIdObjItem : courseIds) {
                if (courseIdObjItem == null || StringUtils.isBlank(courseIdObjItem.toString())) {
                    log.warn("ContentHealthServiceImpl:getContentHealthSummary - Skipping empty/null course ID");
                    continue;
                }

                String courseId = courseIdObjItem.toString().trim();
                List<Map<String, Object>> courseMetrics = new ArrayList<>();

                try {
                    String redisKey = Constants.COURSE_METRICS_KEY_PREFIX + courseId;

                    String dropOffRateJson = redisDataCacheMgr.getHashField(redisKey, Constants.DROP_OFF_RATE, serverProperties.getContentHealthDbIndex());

                    if (StringUtils.isNotBlank(dropOffRateJson)) {
                        Map<String, Object> dropOffData = objectMapper.readValue(dropOffRateJson, Map.class);
                        courseMetrics.add(dropOffData);
                    } else {
                        log.debug("ContentHealthServiceImpl:getContentHealthSummary - No dropoff_rate data for courseId: {}", courseId);
                    }
                } catch (Exception e) {
                    log.error("ContentHealthServiceImpl:getContentHealthSummary - Error processing courseId: {}", courseId, e);
                }

                Map<String, Object> courseData = new HashMap<>();
                courseData.put(courseId, courseMetrics);
                resultList.add(courseData);
            }
            response.getResult().put(Constants.CONTENT_LIST, resultList);
            response.setResponseCode(HttpStatus.OK);
            log.info("ContentHealthServiceImpl:getContentHealthSummary - Retrieved summary for {} course IDs", courseIds.size());
        } catch (Exception e) {
            log.error("ContentHealthServiceImpl:getContentHealthSummary - Error fetching health summary", e);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.put(Constants.ERROR, Constants.FAILED);
            response.put(Constants.ERROR_MESSAGE, "Failed to fetch content health summary: " + e.getMessage());
        }
        return response;
    }

    /**
     * Helper method to set failed response
     */
    private void setFailedResponse(ApiResponse response, String message) {
        response.setResponseCode(HttpStatus.BAD_REQUEST);
        response.put(Constants.ERROR, Constants.FAILED);
        response.put(Constants.ERROR_MESSAGE, message);
    }
}

