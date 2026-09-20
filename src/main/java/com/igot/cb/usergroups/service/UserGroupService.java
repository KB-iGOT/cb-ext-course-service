package com.igot.cb.usergroups.service;

import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;

/**
 * User Group Service interface for CRUD and search operations.
 */
public interface UserGroupService {

    /**
     * Creates a new user group.
     *
     * @param request   API request containing user group details
     * @param authToken authentication token
     * @return API response with created user group ID
     */
    ApiResponse createUserGroup(ApiRequest request, String authToken);

    /**
     * Reads a user group by ID.
     *
     * @param userGroupId user group ID
     * @param authToken   authentication token
     * @return API response with user group details
     */
    ApiResponse readUserGroup(String userGroupId, String authToken);

    /**
     * Updates an existing user group.
     * userGroupId must be provided in the request payload.
     *
     * @param request   API request containing update details (including userGroupId)
     * @param authToken authentication token
     * @return API response with update status
     */
    ApiResponse updateUserGroup(ApiRequest request, String authToken);

    /**
     * Deletes a user group (soft delete - sets status to ARCHIVED).
     *
     * @param userGroupId user group ID
     * @param authToken   authentication token
     * @return API response with deletion status
     */
    ApiResponse deleteUserGroup(String userGroupId, String authToken);

    /**
     * Searches user groups with filters and pagination.
     *
     * @param request   API request containing search filters
     * @param authToken authentication token
     * @return API response with search results
     */
    ApiResponse searchUserGroups(ApiRequest request, String authToken);
}
