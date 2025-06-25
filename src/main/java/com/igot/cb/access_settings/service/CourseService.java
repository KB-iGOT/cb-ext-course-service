package com.igot.cb.access_settings.service;

import com.igot.cb.transactional.util.ApiResponse;

import java.util.Map;

public interface CourseService {
    ApiResponse readContentState(Map<String, Object> requestBody, String authToken);

    ApiResponse updateContentState(Map<String, Object> requestBody, String authToken);
}
