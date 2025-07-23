package com.igot.cb.service;

import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.AccessTokenValidator;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class CourseAccessServiceImpl {

    private final AccessTokenValidator accessTokenValidator;
    private final RedisCacheMgr redisCacheMgr;

    public CourseAccessServiceImpl(AccessTokenValidator accessTokenValidator, RedisCacheMgr redisCacheMgr) {
        this.accessTokenValidator = accessTokenValidator;
        this.redisCacheMgr = redisCacheMgr;
    }
    
    public ApiResponse getCoursesForUser(Map<String, Object> request, String authToken) {
        log.info("CourseAccessServiceImpl::getCoursesForUser:inside");
        ApiResponse response = ApiResponse.createDefaultResponse("api/courseAccess/getCoursesForUser");

        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
        if (!StringUtils.hasText(userId)) {
            String errMsg = "Invalid or missing authentication token";
            log.error(errMsg);
            response.updateErrorDetails(errMsg, HttpStatus.UNAUTHORIZED);
            return response;
        }

        // Check redis for the existing data - if exist return the same
        // If not exist, then fetch the user profile attributes from the user service

        //Evaluate the user profile value against all the courses in access settings rule table.

        // If user has access to the course then return that list.

        // Validate the request payload
        if (MapUtils.isEmpty(request)) {
            String errMsg = "Request body is null or empty";
            log.error(errMsg);
            response.updateErrorDetails(errMsg, HttpStatus.BAD_REQUEST);
            return response;
        }
        
        return response;
    }
}
