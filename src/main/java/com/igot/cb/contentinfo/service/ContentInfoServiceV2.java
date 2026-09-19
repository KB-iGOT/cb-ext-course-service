package com.igot.cb.contentinfo.service;

import com.igot.cb.model.ApiResponse;

/**
 * Service contract for Content Info V2 API.
 * Provides an enhanced view of a user's personalised content landscape,
 * including training plans, APAR, AI-CBP, CA programs, learning pathways,
 * standalone assessments, and moderated content.
 */
public interface ContentInfoServiceV2 {

    /**
     * Returns a structured summary of content assigned to the authenticated user,
     * including counts and content identifier lists per category.
     *
     * @param authToken the user's authentication token
     * @return {@link ApiResponse} containing the content info map or error details
     */
    ApiResponse getContentInfo(String authToken);
}
