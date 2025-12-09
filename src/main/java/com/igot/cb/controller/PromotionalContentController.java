package com.igot.cb.controller;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.IPromotionalContentService;
import com.igot.cb.util.Constants;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/promotionalcontent")
public class PromotionalContentController {

    public final IPromotionalContentService promotionalContentService;

    public PromotionalContentController(IPromotionalContentService promotionalContentService) {
        this.promotionalContentService = promotionalContentService;
    }

    @PutMapping("/metadata/upsert")
    public ResponseEntity<ApiResponse> upsertPromotionalContentMetadata(@RequestBody Map<String, Object> userGroupDetails,
                                                                        @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = promotionalContentService.upsertPromotionalContentMetadata(userGroupDetails, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/assignedto/users")
    public ResponseEntity<ApiResponse> getCoursesForUser(@RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = promotionalContentService.getPromotionalContentForUsers(authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @DeleteMapping("/v1/delete/{contentId}")
    public ResponseEntity<ApiResponse> delete(@PathVariable("contentId") String contentId,
                                              @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = promotionalContentService.delete(contentId, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/v1/read/{contentId}")
    public ResponseEntity<ApiResponse> read(@PathVariable("contentId") String contentId,
                                            @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = promotionalContentService.read(contentId, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

}
