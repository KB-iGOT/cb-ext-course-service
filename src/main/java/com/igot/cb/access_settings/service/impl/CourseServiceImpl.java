package com.igot.cb.access_settings.service.impl;

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
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

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
            if (!requestMap.containsKey(Constants.FIELDS)) {
                setFailedResponse(response, "'fields' key is mandatory in the request payload");
                return response;
            }
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.USER_ID_LOWER_CASE, userId);
            propertyMap.put(Constants.RESOURCE_ID, requestMap.get(Constants.CONTENT_IDS));
            Object fieldsObj = requestMap.get(Constants.FIELDS);
            log.info("fieldsObj class: {}, value: {}", fieldsObj != null ? fieldsObj.getClass() : "null", fieldsObj);
            List<String> fields = null;
            if (fieldsObj instanceof List<?>) {
                fields = ((List<?>) fieldsObj).stream()
                        .filter(Objects::nonNull)
                        .map(Object::toString)
                        .collect(Collectors.toList());
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
    private void setFailedResponse(ApiResponse response, String errorMessage) {
        response.getParams().setStatus(com.igot.cb.access_settings.util.Constants.FAILED);
        response.setResponseCode(HttpStatus.BAD_REQUEST);
        response.getParams().setErrMsg(errorMessage);
    }
}
