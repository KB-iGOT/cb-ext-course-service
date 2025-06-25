package com.igot.cb.access_settings.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.access_settings.service.CourseService;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import com.igot.cb.transactional.util.ApiResponse;
import com.igot.cb.transactional.util.Constants;
import com.igot.cb.transactional.util.ProjectUtil;
import com.igot.cb.transactional.util.exceptions.CustomException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class CourseServiceImpl implements CourseService {

    private final Logger logger = LoggerFactory.getLogger(CourseServiceImpl.class);

    @Autowired
    private AccessTokenValidator accessTokenValidator;

    @Autowired
    CassandraOperation cassandraOperation;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${user.entity.consumption.allowed.fields}")
    private String allowedFieldsConfig;

    @Value("${content.state.update.required.fields}")
    private String requiredFieldsConfig;

    // Example date format: adjust to match your actual format
    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSSZ");

    @Override
    public ApiResponse readContentState(Map<String, Object> requestBody, String authToken) {
        logger.info("CourseService::readContentState:inside");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CONTENT_V2_STATE_READ);
        try {
            String userId = "";
            userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }
            if (StringUtils.isBlank(userId)) {
                response.getParams().setErrMsg(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            // Payload validation
            if (requestBody == null || requestBody.isEmpty()) {
                setFailedResponse(response, "Request body is empty");
                return response;
            }
            Object requestObj = requestBody.get(Constants.REQUEST);
            if (!(requestObj instanceof Map)) {
                setFailedResponse(response, "Missing or invalid 'request' object in payload");
                return response;
            }
            Map<String, Object> requestMap = (Map<String, Object>) requestObj;
            Object contentIdsObj = requestMap.get(Constants.CONTENT_IDS);
            if (!(contentIdsObj instanceof List) || ((List<?>) contentIdsObj).isEmpty()) {
                setFailedResponse(response, "'contentIds' is mandatory and should be a non-empty list");
                return response;
            }
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.USER_ID_LOWER_CASE, userId);
            propertyMap.put(Constants.RESOURCE_ID, requestMap.get(Constants.CONTENT_IDS));

            Object fieldsObj = requestMap.get(Constants.FIELDS);
            log.info("fieldsObj class: {}, value: {}", fieldsObj != null ? fieldsObj.getClass() : "null", fieldsObj);
            List<String> fields = null;
            if (fieldsObj instanceof List<?>) {
                List<String> allowedFields = Arrays.asList(allowedFieldsConfig.split(","));
                List<String> requestedFields = ((List<?>) fieldsObj).stream()
                        .filter(Objects::nonNull)
                        .map(Object::toString)
                        .collect(Collectors.toList());
                // Validate requested fields
                List<String> invalidFields = requestedFields.stream()
                        .filter(f -> !allowedFields.contains(f))
                        .collect(Collectors.toList());
                if (!invalidFields.isEmpty()) {
                    setFailedResponse(response, "Invalid fields in request: " + invalidFields);
                    return response;
                }
                fields = requestedFields;
            }
            List<Map<String, Object>> userContentDetails = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    Constants.KEYSPACE_SUNBIRD_RESOURCE, Constants.USER_ENTITY_CONSUMPTION, propertyMap, fields, null);
            response.getResult().put(Constants.CONTENT_LIST,
                    objectMapper.convertValue(userContentDetails, new TypeReference<Object>() {
                    }));
            response.setResponseCode(HttpStatus.OK);
            return response;

        } catch (Exception e) {
            logger.error("Error while upserting access settings", e);
            setFailedResponse(response, "Failed to create access settings: " + e.getMessage());
            return response;

        }
    }

    @Override
    public ApiResponse updateContentState(Map<String, Object> requestBody, String authToken) {
        logger.info("CourseService::readContentState:inside");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CONTENT_V2_STATE_READ);
        try {
            String userId = "";
            userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }
            if (StringUtils.isBlank(userId)) {
                response.getParams().setErrMsg(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            // Payload validation
            if (requestBody == null || requestBody.isEmpty()) {
                setFailedResponse(response, "Request body is empty");
                return response;
            }
            String errMsg = validateContentStateUpdatePayload(requestBody);
            if (org.apache.commons.lang.StringUtils.isNotBlank(errMsg)) {
                setFailedResponse(response, errMsg);
                return response;
            }
            Object requestObj = requestBody.get(Constants.REQUEST);
            if (requestObj instanceof Map) {
                Map<String, Object> requestMap = (Map<String, Object>) requestObj;
                Object contentsObj = requestMap.get(Constants.CONTENTS);
                if (contentsObj instanceof List && !((List<?>) contentsObj).isEmpty()) {
                    Object firstContent = ((List<?>) contentsObj).get(0);
                    if (firstContent instanceof Map) {
                        Object contentId = ((Map<?, ?>) firstContent).get(Constants.CONTENT_ID);
                        Map<String, Object> propertyMap = new HashMap<>();
                        propertyMap.put(Constants.USER_ID_LOWER_CASE, userId);
                        propertyMap.put(Constants.RESOURCE_ID, contentId);
                        List<Map<String, Object>> userContentDetails = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                                Constants.KEYSPACE_SUNBIRD_RESOURCE, Constants.USER_ENTITY_CONSUMPTION, propertyMap, null, null);
                        Map<String, Object>  processContentConsumption =processContentConsumption((Map<String, Object>) firstContent, userContentDetails.isEmpty() ? null : userContentDetails.get(0), userId);

                        Map<String, String> payloadToCassandraMap = new HashMap<String, String>() {{
                            put("userId", "userid");
                            put("contentId", "resourceid");
                            put("lastAccessTime", "last_access_time");
                            put("lastCompletedTime", "last_completed_time");
                            put("lastUpdatedTime", "last_updated_time");
                            put("progress", "progress");
                            put("progressDetails", "progressdetails");
                            put("status", "status");
                            put("completionPercentage", "completion_percentage");
                        }};
                        Map<String, Object> cassandraMap = processContentConsumption.entrySet().stream()
                                .collect(Collectors.toMap(
                                        entry -> payloadToCassandraMap.getOrDefault(entry.getKey(), entry.getKey()),
                                        Map.Entry::getValue
                                ));
                        cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD_RESOURCE, Constants.USER_ENTITY_CONSUMPTION,cassandraMap);
                        response.getResult().put((String) contentId, Constants.SUCCESS);
                        // contentId now holds the value "do_11433459960646860815"
                    }
                }
            }

            return response;

        } catch (Exception e) {
            logger.error("Error while upserting access settings", e);
            setFailedResponse(response, "Failed to create access settings: " + e.getMessage());
            return response;

        }
    }

    private String validateContentStateUpdatePayload(Map<String, Object> requestBody) {
        List<String> errList = new ArrayList<>();
        Object requestObj = requestBody.get(Constants.REQUEST);
        if (!(requestObj instanceof Map)) {
            errList.add(Constants.REQUEST);
        } else {
            Map<String, Object> requestMap = (Map<String, Object>) requestObj;
            Object contentsObj = requestMap.get("contents");
            if (!(contentsObj instanceof List)) {
                errList.add("contents");
            } else {
                List<?> contents = (List<?>) contentsObj;
                List<String> requiredAttributes = Arrays.asList(requiredFieldsConfig.split(","));
                for (int i = 0; i < contents.size(); i++) {
                    Object contentObj = contents.get(i);
                    if (contentObj instanceof Map) {
                        Map<String, Object> content = (Map<String, Object>) contentObj;
                        for (String attr : requiredAttributes) {
                            Object value = content.get(attr);
                            if (value == null || (value instanceof String && StringUtils.isBlank((String) value))) {
                                errList.add("contents[" + i + "]." + attr);
                            }
                        }
                    } else {
                        errList.add("contents[" + i + "]");
                    }
                }
            }
        }
        if (!errList.isEmpty()) {
            return "Missing or invalid fields: " + errList;
        }
        return "";
    }

    public Map<String, Object> processContentConsumption(
            Map<String, Object> inputContent,
            Map<String, Object> existingContent,
            String userId) throws JsonProcessingException {

        int inputStatus = ((Number) inputContent.getOrDefault(Constants.STATUS, 0)).intValue();
        Map<String, Object> updatedContent = new HashMap<>(inputContent);

        Map<String, Object> parsedMap = new HashMap<>();
        Set<String> jsonFields = new HashSet<>();
        jsonFields.add("progressdetails");
        for (String field : jsonFields) {
            if (inputContent.containsKey(field)) {
                parsedMap.put(field, objectMapper.writeValueAsString(inputContent.get(field)));
            }
        }
        updatedContent.putAll(parsedMap);

        Date inputCompletedTime = parseDate((String) inputContent.getOrDefault(Constants.LAST_COMPLETED_TIME, ""));
        Date inputAccessTime = parseDate((String) inputContent.getOrDefault(Constants.LAST_ACCESS_TIME, ""));

        if (existingContent != null && !existingContent.isEmpty()) {
            Date existingAccessTime;
            Object existingAccessTimeObj = existingContent.get(Constants.LAST_ACCESS_TIME);
            if (existingAccessTimeObj instanceof String) {
                existingAccessTime = parseDate((String) existingAccessTimeObj);
            } else if (existingAccessTimeObj instanceof Date) {
                existingAccessTime = (Date) existingAccessTimeObj;
            } else {
                existingAccessTime = null;
            }
            if (existingAccessTime == null) {
                existingAccessTime = parseDate((String) existingContent.getOrDefault(Constants.OLD_LAST_ACCESS_TIME, ""));
            }
            updatedContent.put(Constants.LAST_ACCESS_TIME, compareTime(existingAccessTime, inputAccessTime));

            int inputProgress = ((Number) inputContent.getOrDefault(Constants.PROGRESS, 0)).intValue();
            int existingProgress = ((Number) existingContent.getOrDefault(Constants.PROGRESS, 0)).intValue();
            updatedContent.put(Constants.PROGRESS, Math.max(inputProgress, existingProgress));

            int existingStatus = ((Number) existingContent.getOrDefault(Constants.STATUS, 0)).intValue();
            Object existingCompletedTimeObj = existingContent.get(Constants.LAST_COMPLETED_TIME);
            Date existingCompletedTime;
            if (existingCompletedTimeObj instanceof String) {
                existingCompletedTime = parseDate((String) existingCompletedTimeObj);
            } else if (existingCompletedTimeObj instanceof Date) {
                existingCompletedTime = (Date) existingCompletedTimeObj;
            } else {
                existingCompletedTime = null;
            }
            if (existingCompletedTime == null) {
                existingCompletedTime = parseDate((String) existingContent.getOrDefault(Constants.OLD_LAST_COMPLETED_TIME, ""));
            }

            if (inputStatus >= existingStatus) {
                if (inputStatus >= 2) {
                    updatedContent.put(Constants.STATUS, 2);
                    updatedContent.put(Constants.PROGRESS, 100);
                    updatedContent.put(Constants.LAST_COMPLETED_TIME, compareTime(existingCompletedTime, inputCompletedTime));
                }
            } else {
                updatedContent.put(Constants.STATUS, existingStatus);
            }
        } else {
            if (inputStatus >= 2) {
                updatedContent.put(Constants.PROGRESS, 100);
                updatedContent.put(Constants.LAST_COMPLETED_TIME, compareTime(null, inputCompletedTime));
                updatedContent.put(Constants.STATUS, 2);
            } else {
                updatedContent.put(Constants.PROGRESS, 0);
            }
            updatedContent.put(Constants.LAST_ACCESS_TIME, compareTime(null, inputAccessTime));
        }

        updatedContent.put(Constants.LAST_UPDATED_TIME, Instant.now());
        updatedContent.put(Constants.USER_ID, userId);
        updatedContent.replaceAll((k, v) -> v instanceof Date ? ((Date) v).toInstant() : v);

        return updatedContent;
    }
    public Date parseDate(String dateString) {
        if (StringUtils.isNotBlank(dateString) && !StringUtils.equalsIgnoreCase(Constants.NULL, dateString)) {
            try {
                return dateFormatter.parse(dateString);
            } catch (ParseException e) {
                logger.error("Error parsing date: {}", dateString, e);
                // Log and return null if date format is invalid
                 // Replace with proper logging in production
            }
        }
        return null;
    }

    // Method to map payload keys to Cassandra columns
    public static Map<String, Object> mapPayloadToCassandraColumns(Map<String, Object> payload, Map<String, String> mapping) {
        Map<String, Object> cassandraMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            String cassandraKey = mapping.getOrDefault(entry.getKey(), entry.getKey());
            cassandraMap.put(cassandraKey, entry.getValue());
        }
        return cassandraMap;
    }

    private void setFailedResponse(ApiResponse response, String errorMessage) {
        response.getParams().setStatus(com.igot.cb.access_settings.util.Constants.FAILED);
        response.setResponseCode(HttpStatus.BAD_REQUEST);
        response.getParams().setErrMsg(errorMessage);
    }

    private Date compareTime(Date existingTime, Date inputTime) {
        if (existingTime == null && inputTime == null) {
            return ProjectUtil.getTimeStamp();
        } else if (existingTime == null) {
            return inputTime;
        } else if (inputTime == null) {
            return existingTime;
        } else {
            return inputTime.after(existingTime) ? inputTime : existingTime;
        }
    }
}
