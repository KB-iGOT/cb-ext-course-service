package com.igot.cb.usergroups.controller;

import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.usergroups.service.UserGroupService;
import com.igot.cb.util.Constants;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST controller for User Group operations.
 * Provides CRUD and search APIs.
 */
@RestController
@RequestMapping("/usergroup/v1")
public class UserGroupController {

    private final UserGroupService userGroupService;

    public UserGroupController(UserGroupService userGroupService) {
        this.userGroupService = userGroupService;
    }

    /**
     * Creates a new user group.
     *
     * @param request API request with user group details
     * @param token   authentication token
     * @return API response with created user group ID
     */
    @PostMapping("/create")
    public ResponseEntity<ApiResponse> createUserGroup(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        ApiResponse response = userGroupService.createUserGroup(request, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Reads a user group by ID.
     *
     * @param userGroupId user group ID
     * @param token       authentication token
     * @return API response with user group details
     */
    @GetMapping("/read/{userGroupId}")
    public ResponseEntity<ApiResponse> readUserGroup(
            @PathVariable String userGroupId,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        ApiResponse response = userGroupService.readUserGroup(userGroupId, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Updates an existing user group.
     *
     * @param request API request with update details (including userGroupId in payload)
     * @param token   authentication token
     * @return API response with update status
     */
    @PatchMapping("/update")
    public ResponseEntity<ApiResponse> updateUserGroup(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        ApiResponse response = userGroupService.updateUserGroup(request, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Deletes a user group (soft delete - archives).
     *
     * @param userGroupId user group ID
     * @param token       authentication token
     * @return API response with deletion status
     */
    @DeleteMapping("/delete/{userGroupId}")
    public ResponseEntity<ApiResponse> deleteUserGroup(
            @PathVariable String userGroupId,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        ApiResponse response = userGroupService.deleteUserGroup(userGroupId, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Searches user groups with filters and pagination.
     *
     * @param request API request with search filters
     * @param token   authentication token
     * @return API response with search results
     */
    @PostMapping("/search")
    public ResponseEntity<ApiResponse> searchUserGroups(
            @RequestBody ApiRequest request,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        ApiResponse response = userGroupService.searchUserGroups(request, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
