package com.igot.cb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.common.ServerProperties;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
@Slf4j
@RequiredArgsConstructor
public class CompetencyServiceImpl implements CompetencyService {

    private final CassandraOperation cassandraOperation;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final AccessTokenValidator accessTokenValidator;
    private final ServerProperties serverProperties;


    /**
     * Fetches user competency data and processes courses.
     * <p>
     * Processing Flow:
     * 1. Fetch user competency data from user_competency_mapping table
     * 2. Check if igotCourses details are present
     * 3. If not present, fetch completed courses from user_enrolment_v2 and publish Kafka events
     * 4. Fetch external course enrollments and publish Kafka events for completed courses
     *
     * @param authToken The authentication token
     * @return ApiResponse containing competency data or error message
     */
    @Override
    public ApiResponse fetchUserCompetency(String authToken) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_FETCH_USER_COMPETENCY);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isBlank(userId)) {
                response.getParams().setErrMsg("User ID not found in token");
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            log.debug("Starting competency fetch for userId: {}", userId);
            Map<String, Object> userCompetencyData = fetchUserCompetencyMapping(userId);

            if (MapUtils.isNotEmpty(userCompetencyData)) {
                log.info("Competency data already present for userId: {}", userId);
                response.setResponseCode(HttpStatus.OK);
                response.getParams().setStatus(Constants.SUCCESS);
                response.setResult(userCompetencyData);
                return response;
            }

            publishFirstTimeCompetencyEvent(userId);

            // Prepare response with empty object when no data found
            response.setResponseCode(HttpStatus.OK);
            response.getParams().setStatus(Constants.SUCCESS);
            response.setResult(new HashMap<>());
        } catch (Exception e) {
            log.error("Error fetching competency for ", e);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrMsg("Error fetching competency data: " + e.getMessage());

            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put(Constants.RESPONSE, "Error fetching competency data");
            response.setResult(errorResult);
        }

        return response;
    }


    /**
     * Fetches user competency mapping from Cassandra.
     *
     * @param userId The user ID
     * @return Map containing user competency data, or empty map if not found
     */
    private Map<String, Object> fetchUserCompetencyMapping(String userId) {
        log.debug("Fetching user competency mapping for userId: {}", userId);

        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.USER_ID_KEY, userId.trim());

        List<Map<String, Object>> records = cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD,
                Constants.USER_COMPETENCY_MAPPING_TABLE,
                propertyMap,
                null,
                1
        );

        if (CollectionUtils.isNotEmpty(records)) {
            return records.get(0);
        }

        log.warn("User competency mapping not found for userId: {}", userId);
        return new HashMap<>();
    }

    private void publishFirstTimeCompetencyEvent(String userId) {

        try {
            Map<String, Object> eventData = new HashMap<>();
            eventData.put(Constants.EVENT_TYPE, Constants.COMPETENCY_ACQUIRED_EVENT);
            eventData.put(Constants.USER_ID, userId);
            eventData.put(Constants.IS_FIRST_TIME_USER, "true");

            Map<String, Object> eventWrapper = new HashMap<>();
            eventWrapper.put(Constants.E_DATA, eventData);

            String eventJson = objectMapper.writeValueAsString(eventWrapper);

            log.debug("Publishing first-time competency event for userId: {}", userId);

            kafkaTemplate.send(serverProperties.getCompetencyAcquiredTopicName(), eventJson);

            log.info("First-time competency event published successfully for userId: {}", userId);

        } catch (Exception e) {
            log.error("Error publishing first-time competency event for userId: {}", userId, e);
        }
    }
}