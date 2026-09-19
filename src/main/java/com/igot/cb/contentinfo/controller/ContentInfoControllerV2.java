package com.igot.cb.contentinfo.controller;

import com.igot.cb.contentinfo.service.ContentInfoServiceV2;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * V2 REST controller for Content Info.
 *
 * @version 2.0
 */
@RestController
@RequestMapping("/content/v2")
public class ContentInfoControllerV2 {

    private final ContentInfoServiceV2 contentInfoService;

    /**
     * @param contentInfoService V2 service handling the content info assembly
     */
    public ContentInfoControllerV2(ContentInfoServiceV2 contentInfoService) {
        this.contentInfoService = contentInfoService;
    }

    /**
     * Returns a structured summary of content assigned to the authenticated user,
     * including counts and content identifier lists per category (training plans,
     * APAR, AI-CBP, CA programs, learning pathways, standalone assessments,
     * and moderated content).
     *
     * @param authToken the user's authentication token
     * @return {@link ResponseEntity} wrapping the {@link ApiResponse}
     */
    @GetMapping("/user/info")
    public ResponseEntity<ApiResponse> getContentInfo(
            @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = contentInfoService.getContentInfo(authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
