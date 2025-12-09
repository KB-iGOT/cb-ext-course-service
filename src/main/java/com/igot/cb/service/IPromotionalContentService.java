package com.igot.cb.service;

import com.igot.cb.model.ApiResponse;

import java.util.Map;

public interface IPromotionalContentService {
    ApiResponse upsertPromotionalContentMetadata(Map<String, Object> userGroupDetails, String authToken);

    ApiResponse getPromotionalContentForUsers(String authToken);

    ApiResponse delete(String contentId, String authToken);

    ApiResponse read(String contentId, String authToken);
}
