package com.igot.cb.service.impl;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.ContentInfoServiceImpl;
import com.igot.cb.service.LearningPathwayRetireService;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

@Service
@Slf4j
public class LearningPathwayRetireServiceImpl implements LearningPathwayRetireService {

    private final AccessTokenValidator accessTokenValidator;
    private final CassandraOperation cassandraOperation;
    private final ContentInfoServiceImpl contentService;

    public LearningPathwayRetireServiceImpl(AccessTokenValidator accessTokenValidator,
                                           CassandraOperation cassandraOperation,
                                           ContentInfoServiceImpl contentService) {
        this.accessTokenValidator = accessTokenValidator;
        this.cassandraOperation = cassandraOperation;
        this.contentService = contentService;
    }

    @Override
    public ApiResponse retireLearningPathway(String userToken, String contentId) {
        ApiResponse response = new ApiResponse();

        try {
            log.info("LearningPathwayRetireServiceImpl::retireLearningPathway: starting validation for contentId: {}", contentId);
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(userToken, response);

            if (StringUtils.isBlank(userId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            // Validate user has SPV_PUBLISHER role
            if (!hasSPVPublisherRole(userId)) {
                log.info("User {} does not have SPV_PUBLISHER role", userId);
                return commonApiResponse(response, "User does not have SPV_PUBLISHER role", HttpStatus.FORBIDDEN);
            }

            Map<String, Object> content = contentService.readContent(contentId, Arrays.asList(Constants.BATCHES, Constants.CREATED_BY, Constants.STATUS, Constants.COURSE_CATEGORY));

            if (MapUtils.isEmpty(content)) {
                return commonApiResponse(response, "Content not found", HttpStatus.NOT_FOUND);
            }

            String createdBy = (String) content.get(Constants.CREATED_BY);
            if(StringUtils.isNotBlank(createdBy) && !userId.equals(createdBy)){
                return commonApiResponse(response, "Content is not created by user", HttpStatus.BAD_REQUEST);
            }

            if(!Constants.LEARNING_PATHWAY.equals(content.get(Constants.COURSE_CATEGORY))){
                return commonApiResponse(response, "Content is not learning pathway", HttpStatus.BAD_REQUEST);
            }

            List<Map<String, Object>> batches = (List<Map<String, Object>>) content.get(Constants.BATCHES);

            if (CollectionUtils.isEmpty(batches)) {
                log.info("No batches found for content {}, proceeding with retirement", contentId);
                return proceedWithRetirement(contentId, response);
            }

            // Check for active enrollments
            if (hasActiveEnrollments(batches)) {
                log.info("Active enrollments found for content {}, cannot retire", contentId);
                return commonApiResponse(response, "Active enrollments available. Cannot retire the content.", 
                        HttpStatus.BAD_REQUEST);
            }

            log.info("No active enrollments found for content {}, proceeding with retirement", contentId);
            return proceedWithRetirement(contentId, response);

        } catch (Exception e) {
            log.error("Error while retiring content with validation for contentId: {}", contentId, e);
            return commonApiResponse(response, "Failed to retire content: " + e.getMessage(), 
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Check if there are active enrollments for the content
     * @param batches - List of batches associated with content
     * @return true if active enrollments exist, false otherwise
     */
    private boolean hasActiveEnrollments(List<Map<String, Object>> batches) {

        for (Map<String, Object> batch : batches) {
            String batchId = (String) batch.get(Constants.BATCH_ID);

            if (StringUtils.isBlank(batchId)) {
                continue;
            }
            log.debug("Checking enrollments for batchId: {}", batchId);
            Map<String, Object> properties = new HashMap<>();
            properties.put(Constants.BATCH_ID, batchId);
            List<Map<String, Object>> enrollments = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD_COURSE,
                    Constants.ENROLLMENT_BATCH_LOOKUP,
                    properties,
                    Arrays.asList(Constants.USER_ID, Constants.ACTIVE),
                    null
            );

            if (!CollectionUtils.isEmpty(enrollments)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Proceed with content retirement
     * @param contentId - Content ID to retire
     * @param response - ApiResponse object to populate
     * @return ApiResponse with retirement status
     */
    private ApiResponse proceedWithRetirement(String contentId, ApiResponse response) {
        try {
            Map<String, Object> contentRetireStatusMap = contentService.retireContent(contentId);
            if (MapUtils.isNotEmpty(contentRetireStatusMap)) {
                log.info("Content retired successfully: {}", contentId);
                return commonApiResponse(response, "Content retired successfully", HttpStatus.OK);
            } else {
                log.info("Retirement API returned empty response for {}", contentId);
                return commonApiResponse(response, "Retirement API returned empty response", 
                        HttpStatus.INTERNAL_SERVER_ERROR);
            }
        } catch (Exception ex) {
            log.error("Failed to retire content {}", contentId, ex);
            return commonApiResponse(response, "Failed to retire content: " + ex.getMessage(), 
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Check if user has SPV_PUBLISHER role
     * @param userId - User ID to validate
     * @return true if user has SPV_PUBLISHER role, false otherwise
     */
    private boolean hasSPVPublisherRole(String userId) {
        try {
            log.debug("Checking if user {} has SPV_PUBLISHER role", userId);

            Map<String, Object> properties = new HashMap<>();
            properties.put(Constants.USER_ID, userId);
            properties.put(Constants.ROLE, Constants.SPV_PUBLISHER);

            List<Map<String, Object>> userRoles = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_USER_ROLES,
                    properties,
                    null,
                    null
            );

            boolean hasRole = !CollectionUtils.isEmpty(userRoles);
            log.info("User {} has SPV_PUBLISHER role: {}", userId, hasRole);
            return hasRole;

        } catch (Exception e) {
            log.error("Error checking role for user {}", userId, e);
            return false;
        }
    }

    /**
     * Build response with custom data
     * @param response - ApiResponse object to populate
     * @param message - Message to set
     * @param statusCode - HTTP status code
     * @return ApiResponse with provided details
     */
    private ApiResponse commonApiResponse(ApiResponse response, String message, HttpStatus statusCode) {
        response.put(Constants.MESSAGE, message);
        response.setResponseCode(statusCode);
        return response;
    }
}
