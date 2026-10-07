package com.igot.cb.usergroups.controller;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.usergroups.service.UserGroupService;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.*;

@ExtendWith(MockitoExtension.class)
class UserGroupControllerTest {

    private static final String TEST_AUTH_TOKEN = "test_token";
    private static final String TEST_USER_GROUP_ID = "ug_789";
    private static final String BASE_URL = "/usergroup/v1";

    @Mock
    private UserGroupService userGroupService;

    private MockMvc mockMvc;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        UserGroupController controller = new UserGroupController(userGroupService);
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
        objectMapper = new ObjectMapper();
    }

    @Test
    void createUserGroup_withValidRequest_shouldReturn201() throws Exception {
        ApiRequest request = createApiRequest();
        ApiResponse response = createSuccessResponse(HttpStatus.CREATED);
        response.put(Constants.ID, TEST_USER_GROUP_ID);

        when(userGroupService.createUserGroup(any(ApiRequest.class), anyString()))
                .thenReturn(response);

        mockMvc.perform(post(BASE_URL + "/create")
                        .header(Constants.X_AUTH_TOKEN, TEST_AUTH_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.result.id").value(TEST_USER_GROUP_ID));

        verify(userGroupService, times(1)).createUserGroup(any(ApiRequest.class), eq(TEST_AUTH_TOKEN));
    }

    @Test
    void createUserGroup_withMissingAuthToken_shouldReturn400() throws Exception {
        ApiRequest request = createApiRequest();

        mockMvc.perform(post(BASE_URL + "/create")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());

        verify(userGroupService, never()).createUserGroup(any(), anyString());
    }

    @Test
    void readUserGroup_withValidId_shouldReturn200() throws Exception {
        ApiResponse response = createSuccessResponse(HttpStatus.OK);
        response.put(Constants.COL_USERGROUPID, TEST_USER_GROUP_ID);
        response.put(Constants.COL_USERGROUPNAME, "Test Group");

        when(userGroupService.readUserGroup(anyString(), anyString()))
                .thenReturn(response);

        mockMvc.perform(get(BASE_URL + "/read/" + TEST_USER_GROUP_ID)
                        .header(Constants.X_AUTH_TOKEN, TEST_AUTH_TOKEN))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result.usergroupid").value(TEST_USER_GROUP_ID));

        verify(userGroupService, times(1)).readUserGroup(TEST_USER_GROUP_ID, TEST_AUTH_TOKEN);
    }

    @Test
    void updateUserGroup_withValidRequest_shouldReturn200() throws Exception {
        ApiRequest request = createApiRequest();
        ApiResponse response = createSuccessResponse(HttpStatus.OK);
        response.put(Constants.ID, TEST_USER_GROUP_ID);

        when(userGroupService.updateUserGroup(any(ApiRequest.class), anyString()))
                .thenReturn(response);

        mockMvc.perform(patch(BASE_URL + "/update")
                        .header(Constants.X_AUTH_TOKEN, TEST_AUTH_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result.id").value(TEST_USER_GROUP_ID));

        verify(userGroupService, times(1))
                .updateUserGroup(any(ApiRequest.class), eq(TEST_AUTH_TOKEN));
    }

    @Test
    void deleteUserGroup_withValidId_shouldReturn200() throws Exception {
        ApiResponse response = createSuccessResponse(HttpStatus.OK);
        response.put(Constants.RESPONSE, Constants.MSG_USER_GROUP_ARCHIVED);

        when(userGroupService.deleteUserGroup(anyString(), anyString()))
                .thenReturn(response);

        mockMvc.perform(delete(BASE_URL + "/delete/" + TEST_USER_GROUP_ID)
                        .header(Constants.X_AUTH_TOKEN, TEST_AUTH_TOKEN))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result.response").value(Constants.MSG_USER_GROUP_ARCHIVED));

        verify(userGroupService, times(1)).deleteUserGroup(TEST_USER_GROUP_ID, TEST_AUTH_TOKEN);
    }

    @Test
    void searchUserGroups_withFilters_shouldReturn200() throws Exception {
        ApiRequest request = createSearchRequest();
        ApiResponse response = createSuccessResponse(HttpStatus.OK);
        response.put("content", List.of());
        response.put("totalHits", 0);

        when(userGroupService.searchUserGroups(any(ApiRequest.class), anyString()))
                .thenReturn(response);

        mockMvc.perform(post(BASE_URL + "/search")
                        .header(Constants.X_AUTH_TOKEN, TEST_AUTH_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result.content").isArray());

        verify(userGroupService, times(1)).searchUserGroups(any(ApiRequest.class), eq(TEST_AUTH_TOKEN));
    }

    @Test
    void searchUserGroupsV2_withValidRequest_shouldReturn200() throws Exception {
        ApiRequest request = createSearchV2Request();
        ApiResponse response = createSuccessResponse(HttpStatus.OK);
        response.put(Constants.COUNT, 1L);
        response.put(Constants.CONTENT, List.of(Map.of(Constants.COL_USERGROUPID, TEST_USER_GROUP_ID)));

        when(userGroupService.searchUserGroupsV2(any(ApiRequest.class), anyString()))
                .thenReturn(response);

        mockMvc.perform(post(BASE_URL + "/searchV2")
                        .header(Constants.X_AUTH_TOKEN, TEST_AUTH_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result.count").value(1));

        verify(userGroupService, times(1)).searchUserGroupsV2(any(ApiRequest.class), eq(TEST_AUTH_TOKEN));
    }

    @Test
    void searchUserGroupsV2_withMissingAuthToken_shouldReturn400() throws Exception {
        ApiRequest request = createSearchV2Request();

        mockMvc.perform(post(BASE_URL + "/searchV2")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());

        verify(userGroupService, never()).searchUserGroupsV2(any(), anyString());
    }

    @Test
    void searchUserGroupsV2_whenNotFound_shouldReturn404() throws Exception {
        ApiRequest request = createSearchV2Request();
        ApiResponse response = new ApiResponse();
        response.setResponseCode(HttpStatus.NOT_FOUND);

        when(userGroupService.searchUserGroupsV2(any(ApiRequest.class), anyString()))
                .thenReturn(response);

        mockMvc.perform(post(BASE_URL + "/searchV2")
                        .header(Constants.X_AUTH_TOKEN, TEST_AUTH_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isNotFound());

        verify(userGroupService, times(1)).searchUserGroupsV2(any(ApiRequest.class), eq(TEST_AUTH_TOKEN));
    }


    private ApiRequest createApiRequest() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("usergroupname", "Test Group");
        requestMap.put("criteria", List.of(
                Map.of("criteriaKey", "department", "criteriaValue", List.of("HR"))
        ));
        request.setRequest(requestMap);
        return request;
    }

    private ApiRequest createSearchRequest() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> searchMap = new HashMap<>();
        Map<String, Object> filters = new HashMap<>();
        filters.put("status", List.of("ACTIVE"));
        searchMap.put("filters", filters);
        request.setRequest(searchMap);
        return request;
    }

    private ApiRequest createSearchV2Request() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> searchMap = new HashMap<>();
        Map<String, Object> filters = new HashMap<>();
        filters.put(Constants.COL_USERGROUPNAME, "Test Group");
        filters.put(Constants.COL_ORGID, "org_456");
        searchMap.put(Constants.FILTERS, filters);
        request.setRequest(searchMap);
        return request;
    }

    private ApiResponse createSuccessResponse(HttpStatus status) {
        ApiResponse response = new ApiResponse();
        response.setResponseCode(status);
        return response;
    }

    @Test
    void createUserGroupAdmin_withValidRequest_shouldReturn201() throws Exception {
        ApiRequest request = createAdminApiRequest();
        ApiResponse response = createSuccessResponse(HttpStatus.CREATED);
        response.put(Constants.COL_USERGROUPID, TEST_USER_GROUP_ID);

        when(userGroupService.createUserGroupAdmin(any(ApiRequest.class), anyString()))
                .thenReturn(response);

        mockMvc.perform(post(BASE_URL + "/admin/create")
                        .header(Constants.X_AUTH_TOKEN, TEST_AUTH_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.result.usergroupid").value(TEST_USER_GROUP_ID));

        verify(userGroupService, times(1)).createUserGroupAdmin(any(ApiRequest.class), eq(TEST_AUTH_TOKEN));
    }

    @Test
    void createUserGroupAdmin_withMissingAuthToken_shouldReturn400() throws Exception {
        ApiRequest request = createAdminApiRequest();

        mockMvc.perform(post(BASE_URL + "/admin/create")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());

        verify(userGroupService, never()).createUserGroupAdmin(any(), anyString());
    }

    @Test
    void createUserGroupAdmin_whenServiceReturnsBadRequest_shouldPropagateStatus() throws Exception {
        ApiRequest request = createAdminApiRequest();
        ApiResponse response = new ApiResponse();
        response.setResponseCode(HttpStatus.BAD_REQUEST);
        response.getParams().setErr(Constants.MSG_ROOTORGID_REQUIRED_IN_BODY);

        when(userGroupService.createUserGroupAdmin(any(ApiRequest.class), anyString()))
                .thenReturn(response);

        mockMvc.perform(post(BASE_URL + "/admin/create")
                        .header(Constants.X_AUTH_TOKEN, TEST_AUTH_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());

        verify(userGroupService, times(1)).createUserGroupAdmin(any(ApiRequest.class), eq(TEST_AUTH_TOKEN));
    }

    private ApiRequest createAdminApiRequest() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ROOT_ORG_ID, "target_org_999");
        requestMap.put(Constants.COL_USERGROUPNAME, "Test Group");
        requestMap.put(Constants.COL_CRITERIA, List.of(
                Map.of(Constants.CRITERIA_KEY, "department", Constants.CRITERIA_VALUE, List.of("HR"))
        ));
        request.setRequest(requestMap);
        return request;
    }
}
