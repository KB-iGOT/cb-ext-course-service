package com.igot.cb.consentacknowledge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ConsentAcknowledgeServiceImpl implements IConsentAcknowledgeService {

    private final Logger logger = LoggerFactory.getLogger(ConsentAcknowledgeServiceImpl.class);


    private final AccessTokenValidator accessTokenValidator;
    private final CassandraOperation cassandraOperation;
    private final ObjectMapper objectMapper;

    public ConsentAcknowledgeServiceImpl(AccessTokenValidator accessTokenValidator, CassandraOperation cassandraOperation, ObjectMapper objectMapper) {
        this.cassandraOperation = cassandraOperation;
        this.accessTokenValidator = accessTokenValidator;
        this.objectMapper = objectMapper;
    }

    /**
     * Method to acknowledge declaration
     * Required fields in request body : contentId, consentId
     * Optional fields in request body : additionalData
     */
    @Override
    public ApiResponse acknowledgeDeclaration(Map<String, Object> requestDataBody, String authToken) {
        ApiResponse response = ProjectUtil.createDefaultResponse("api.consent.acknowledge.create");
        Map<String, Object> requestDataMap = (Map<String, Object>) requestDataBody.get(Constants.REQUEST);
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
        if (StringUtils.isEmpty(userId)) {
            return response;
        }
        String errMsg = validateAcknowledgeDeclarationRequest(requestDataMap);
        if (!StringUtils.isEmpty(errMsg)) {
            ProjectUtil.errorResponse(response, errMsg, HttpStatus.BAD_REQUEST);
            return response;
        }
        String contentId = (String) requestDataMap.get(Constants.CONTENT_ID);
        String consentId = (String) requestDataMap.get(Constants.CONSENT_ID);
        Map<String, Object> additionalData = (Map<String, Object>) requestDataMap.get(Constants.ADDITIONAL_ATTRIBUTES);
        String additionalDataStr = null;
        try {
            additionalDataStr = objectMapper.writeValueAsString(additionalData);
        } catch (JsonProcessingException e) {
            logger.error("Error while converting additionalData to string", e);
            ProjectUtil.errorResponse(response,"Failed to Parse the additional attributes", HttpStatus.BAD_REQUEST);
            return response;
        }
        Map<String, Object> compositeKeyMap = new HashMap<>();
        compositeKeyMap.put(Constants.USER_ID, userId);
        compositeKeyMap.put(Constants.CONSENT_ID, consentId);
        Map<String, Object> declarationDataMap = new HashMap<>();
        ZoneId zoneId = ZoneId.of("Asia/Kolkata");
        ZonedDateTime submittedAt = ZonedDateTime.ofInstant(Instant.now(), zoneId);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        declarationDataMap.put(Constants.SUBMITTED_BY, userId);
        declarationDataMap.put(Constants.ADDITIONAL_ATTRIBUTES, additionalDataStr);
        declarationDataMap.put(Constants.SUBMITTED_AT, submittedAt.format(formatter));
        try {
            response = (ApiResponse) cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_DECLARATION_ACKNOWLEDGMENT, Constants.CONTENT_ID, contentId, compositeKeyMap, declarationDataMap);
            if (Constants.FAILED.equals(response.get(Constants.RESPONSE))) {
                logger.error("Error while inserting declaration acknowledgment record in DB: {}",
                        response.get(Constants.ERROR_MESSAGE));
                ProjectUtil.errorResponse(
                        response,
                        "Failed to acknowledge declaration. Please try again later.",
                        HttpStatus.INTERNAL_SERVER_ERROR
                );
                return response;
            }
        } catch (Exception e) {
            logger.error("Error while inserting declaration acknowledgment record in DB", e);
            ProjectUtil.errorResponse(response, "Failed to acknowledge declaration. Please try again later.", HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
        response.getParams().setStatus(Constants.OK);
        response.setResponseCode(HttpStatus.OK);
        Map<String, Object> result = new HashMap<>();
        Map<String, Object> consentAckDetailsMap = new HashMap<>();
        consentAckDetailsMap.put(Constants.CONTENT_ID, contentId);
        consentAckDetailsMap.put(Constants.CONSENT_ID, consentId);
        consentAckDetailsMap.put(Constants.USER_ID, userId);
        consentAckDetailsMap.put(Constants.MESSAGE,  "Declaration acknowledged successfully");
        result.put(Constants.RESPONSE,consentAckDetailsMap);
        response.getResult().putAll(result);
        return response;
    }

    /**
     * Method to validate acknowledge declaration request
     */
    private String validateAcknowledgeDeclarationRequest(Map<String, Object> requestData) {
        if (requestData == null) {
            return "Request data is missing.";
        }
        if (!requestData.containsKey(Constants.CONTENT_ID)
                || StringUtils.isEmpty(requestData.get(Constants.CONTENT_ID).toString())) {
            return "Missing or invalid contentId.";
        }
        if (!requestData.containsKey(Constants.CONSENT_ID)
                || StringUtils.isEmpty(requestData.get(Constants.CONSENT_ID).toString())) {
            return "Missing or invalid consentId.";
        }
        if (requestData.containsKey(Constants.ADDITIONAL_ATTRIBUTES)) {
            Map<String, Object> additionalData = (Map<String, Object>) requestData.get(Constants.ADDITIONAL_ATTRIBUTES);
            if (MapUtils.isEmpty(additionalData)) {
                return "Missing or invalid additionalData.";
            }
        }
        return "";
    }


    /**
     * Method to get consent details by consentId
     */
    @Override
    public ApiResponse getConsentDetails(String consentId, String authToken) {
        ApiResponse response = ProjectUtil.createDefaultResponse("api.consent.read");
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
        if (StringUtils.isEmpty(userId)) {
            return response;
        }
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.CONSENT_ID, consentId);
        List<Map<String, Object>> consentDetails;
        try {
            consentDetails = cassandraOperation
                    .getRecordsByProperties(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_CONSENT_DETAILS, propertyMap, Arrays.asList(Constants.CONSENT_ID, Constants.ADDITIONAL_DATA, Constants.DESCRIPTION), null);
        } catch (Exception e) {
            logger.error("Error while fetching consent details from DB", e);
            ProjectUtil.errorResponse(response, "Failed to fetch consent details. Please try again later.", HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
        Map<String, Object> consentDetailsMap = consentDetails.get(0);
        response.getParams().setStatus(Constants.OK);
        response.setResponseCode(HttpStatus.OK);
        Map<String, Object> result = new HashMap<>();
        result.put(Constants.RESPONSE, consentDetailsMap);
        response.getResult().putAll(result);
        return response;
    }
}
