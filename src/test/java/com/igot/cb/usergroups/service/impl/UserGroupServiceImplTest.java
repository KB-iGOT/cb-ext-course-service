package com.igot.cb.usergroups.service.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.usergroups.model.CriteriaItem;
import com.igot.cb.usergroups.model.UserGroupEntity;
import com.igot.cb.usergroups.model.UserGroupRequest;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import com.igot.cb.util.UserProfileUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

@ExtendWith(MockitoExtension.class)
class UserGroupServiceImplTest {

    private static final String TEST_USER_ID = "user_123";
    private static final String TEST_ORG_ID = "org_456";
    private static final String TEST_USER_GROUP_ID = "ug_789";
    private static final String TEST_USER_GROUP_NAME = "Test Group";
    private static final String TEST_AUTH_TOKEN = "test_token";
    private static final String TEST_ROLES = "MDO_LEADER,ADMIN";

    @Mock
    private CassandraOperation cassandraOperation;
    @Mock
    private UserGroupValidationServiceImpl validationService;
    @Mock
    private UserGroupDataTransformServiceImpl dataTransformService;
    @Mock
    private UserGroupElasticSearchServiceImpl esService;
    @Mock
    private AccessTokenValidator accessTokenValidator;
    @Mock
    private UserProfileUtil userProfileUtil;
    @Mock
    private ObjectMapper objectMapper;

    private UserGroupServiceImpl userGroupService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        userGroupService = new UserGroupServiceImpl(
                cassandraOperation,
                validationService,
                dataTransformService,
                esService,
                accessTokenValidator,
                userProfileUtil,
                objectMapper
        );
    }

    @Test
    void createUserGroup_withValidRequest_shouldReturnCreatedResponse() {
        // Arrange
        ApiRequest request = createApiRequest();
        UserGroupRequest userGroupRequest = new UserGroupRequest(null, TEST_USER_GROUP_NAME, createCriteriaList());
        UserGroupEntity entity = createUserGroupEntity();

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any())).thenReturn(TEST_USER_ID);
        when(userProfileUtil.buildUserProfile(eq(TEST_USER_ID), any())).thenReturn(createUserProfile());
        when(objectMapper.convertValue(any(), eq(UserGroupRequest.class))).thenReturn(userGroupRequest);
        when(dataTransformService.buildEntityForCreate(anyString(), eq(TEST_USER_GROUP_NAME), any(), eq(TEST_ORG_ID), eq(TEST_USER_ID)))
                .thenReturn(entity);
        when(validationService.validateCreateRequest(anyString(), anyList(), any())).thenReturn(true);
        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap())).thenReturn(null);
        doNothing().when(esService).indexUserGroup(any());
        when(dataTransformService.entityToResponseMap(entity)).thenReturn(Map.of(Constants.COL_USERGROUPID, TEST_USER_GROUP_ID));

        // Act
        ApiResponse response = userGroupService.createUserGroup(request, TEST_AUTH_TOKEN);

        // Assert
        assertNotNull(response);
        assertEquals(Constants.SUCCESSFUL, response.getParams().getStatus());
        assertEquals(HttpStatus.CREATED, response.getResponseCode());
        assertEquals(TEST_USER_GROUP_ID, response.get(Constants.COL_USERGROUPID));
        verify(cassandraOperation, times(1)).insertRecord(anyString(), anyString(), anyMap());
        verify(esService, times(1)).indexUserGroup(any());
    }

    @Test
    void createUserGroup_withEmptyUserId_shouldReturnFailedResponse() {
        // Arrange
        ApiRequest request = createApiRequest();
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any())).thenReturn("");

        // Act
        ApiResponse response = userGroupService.createUserGroup(request, TEST_AUTH_TOKEN);

        // Assert
        assertNotNull(response);
        verify(cassandraOperation, never()).insertRecord(anyString(), anyString(), anyMap());
        verify(esService, never()).indexUserGroup(any());
    }

    @Test
    void createUserGroup_withBlankRootOrgId_shouldReturnFailedResponse() {
        // Arrange
        ApiRequest request = createApiRequest();
        Map<String, String> profileWithoutOrg = new HashMap<>();
        profileWithoutOrg.put(Constants.USER_ROOT_ORG_ID, "");

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any())).thenReturn(TEST_USER_ID);
        when(userProfileUtil.buildUserProfile(eq(TEST_USER_ID), any())).thenReturn(profileWithoutOrg);

        // Act
        ApiResponse response = userGroupService.createUserGroup(request, TEST_AUTH_TOKEN);

        // Assert
        assertNotNull(response);
        verify(cassandraOperation, never()).insertRecord(anyString(), anyString(), anyMap());
    }

    @Test
    void readUserGroup_whenFound_shouldReturnUserGroup() {
        // Arrange
        Map<String, Object> cassandraRow = createCassandraRow();

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any())).thenReturn(TEST_USER_ID);
        when(userProfileUtil.buildUserProfile(eq(TEST_USER_ID), any())).thenReturn(createUserProfile());
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), any()))
                .thenReturn(List.of(cassandraRow));
        when(dataTransformService.entityToResponseMap(any())).thenReturn(new HashMap<>());
        when(validationService.validateUserGroupId(anyString(), any())).thenReturn(true);

        // Act
        ApiResponse response = userGroupService.readUserGroup(TEST_USER_GROUP_ID, TEST_AUTH_TOKEN);

        // Assert
        assertNotNull(response);
        assertEquals(Constants.SUCCESSFUL, response.getParams().getStatus());
        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), any());
    }

    @Test
    void readUserGroup_whenNotFound_shouldReturnFailedResponse() {
        // Arrange
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any())).thenReturn(TEST_USER_ID);
        when(userProfileUtil.buildUserProfile(eq(TEST_USER_ID), any())).thenReturn(createUserProfile());
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), any()))
                .thenReturn(Collections.emptyList());
        when(validationService.validateUserGroupId(anyString(), any())).thenReturn(true);

        // Act
        ApiResponse response = userGroupService.readUserGroup(TEST_USER_GROUP_ID, TEST_AUTH_TOKEN);

        // Assert
        assertNotNull(response);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.NOT_FOUND, response.getResponseCode());
        assertEquals(Constants.MSG_USER_GROUP_NOT_FOUND, response.getParams().getErr());
    }

    @Test
    void updateUserGroup_withValidRequest_shouldReturnSuccess() {
        // Arrange
        ApiRequest request = createApiRequest();
        UserGroupRequest userGroupRequest = new UserGroupRequest(TEST_USER_GROUP_ID, TEST_USER_GROUP_NAME, createCriteriaList());
        Map<String, Object> cassandraRow = createCassandraRow();
        Map<String, Object> updateProps = new HashMap<>();

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any())).thenReturn(TEST_USER_ID);
        when(userProfileUtil.buildUserProfile(eq(TEST_USER_ID), any())).thenReturn(createUserProfile());
        when(objectMapper.convertValue(any(), eq(UserGroupRequest.class))).thenReturn(userGroupRequest);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), any()))
                .thenReturn(List.of(cassandraRow));
        when(dataTransformService.buildUpdateProperties(anyString(), anyList(), anyString())).thenReturn(updateProps);
        when(validationService.validateUpdateRequest(anyString(), anyString(), anyList(), any())).thenReturn(true);
        when(validationService.validateUpdateAuthorization(anyString(), anyString(), anyString(), anyString(), anyString(), any())).thenReturn(true);
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap())).thenReturn(new HashMap<>());
        doNothing().when(esService).updateUserGroup(anyString(), anyMap());
        when(dataTransformService.entityToResponseMap(any())).thenReturn(Map.of(Constants.COL_USERGROUPID, TEST_USER_GROUP_ID));

        // Act
        ApiResponse response = userGroupService.updateUserGroup(request, TEST_AUTH_TOKEN);

        // Assert
        assertNotNull(response);
        assertEquals(Constants.SUCCESSFUL, response.getParams().getStatus());
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(TEST_USER_GROUP_ID, response.get(Constants.COL_USERGROUPID));
        verify(cassandraOperation, times(1)).updateRecord(anyString(), anyString(), anyMap(), anyMap());
        verify(esService, times(1)).updateUserGroup(anyString(), anyMap());
    }

    @Test
    void deleteUserGroup_whenFound_shouldArchiveSuccessfully() {
        // Arrange
        Map<String, Object> cassandraRow = createCassandraRow();

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any())).thenReturn(TEST_USER_ID);
        when(userProfileUtil.buildUserProfile(eq(TEST_USER_ID), any())).thenReturn(createUserProfile());
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), any()))
                .thenReturn(List.of(cassandraRow));
        when(validationService.validateUserGroupId(anyString(), any())).thenReturn(true);
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap())).thenReturn(new HashMap<>());
        doNothing().when(esService).updateUserGroup(anyString(), anyMap());

        // Act
        ApiResponse response = userGroupService.deleteUserGroup(TEST_USER_GROUP_ID, TEST_AUTH_TOKEN);

        // Assert
        assertNotNull(response);
        assertEquals(Constants.SUCCESSFUL, response.getParams().getStatus());
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.MSG_USER_GROUP_ARCHIVED, response.get(Constants.RESPONSE));
        verify(cassandraOperation, times(1)).updateRecord(anyString(), anyString(), anyMap(), anyMap());
    }

    @Test
    void searchUserGroups_withDefaultFilters_shouldReturnResults() {
        // Arrange
        ApiRequest request = createApiRequest();
        Map<String, Object> searchResult = new HashMap<>();
        searchResult.put("content", List.of());
        searchResult.put("totalHits", 0);

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(TEST_AUTH_TOKEN), any())).thenReturn(TEST_USER_ID);
        when(userProfileUtil.buildUserProfile(eq(TEST_USER_ID), any())).thenReturn(createUserProfile());
        when(esService.searchUserGroups(anyMap(), anyInt(), anyInt(), anyString(), anyString())).thenReturn(searchResult);

        // Act
        ApiResponse response = userGroupService.searchUserGroups(request, TEST_AUTH_TOKEN);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(esService, times(1)).searchUserGroups(anyMap(), anyInt(), anyInt(), anyString(), anyString());
    }

    // Helper methods

    private ApiRequest createApiRequest() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("usergroupname", TEST_USER_GROUP_NAME);
        requestMap.put("criteria", createCriteriaList());
        request.setRequest(requestMap);
        return request;
    }

    private List<CriteriaItem> createCriteriaList() {
        return List.of(
                new CriteriaItem("department", List.of("HR", "Finance")),
                new CriteriaItem("role", List.of("Manager"))
        );
    }

    private UserGroupEntity createUserGroupEntity() {
        return UserGroupEntity.builder()
                .orgId(TEST_ORG_ID)
                .userGroupId(TEST_USER_GROUP_ID)
                .userGroupName(TEST_USER_GROUP_NAME)
                .createdBy(TEST_USER_ID)
                .createdDate(String.valueOf(System.currentTimeMillis()))
                .updatedBy(TEST_USER_ID)
                .updatedDate(String.valueOf(System.currentTimeMillis()))
                .criteria(List.of())
                .status("ACTIVE")
                .build();
    }

    private Map<String, String> createUserProfile() {
        Map<String, String> profile = new HashMap<>();
        profile.put(Constants.USER_ROOT_ORG_ID, TEST_ORG_ID);
        profile.put(Constants.ROLES, TEST_ROLES);
        return profile;
    }

    private Map<String, Object> createCassandraRow() {
        Map<String, Object> row = new HashMap<>();
        row.put(Constants.ORG_ID, TEST_ORG_ID);
        row.put(Constants.COL_USERGROUPID, TEST_USER_GROUP_ID);
        row.put(Constants.COL_USERGROUPNAME, TEST_USER_GROUP_NAME);
        row.put(Constants.CREATED_BY, TEST_USER_ID);
        row.put(Constants.COL_CREATEDDATE, String.valueOf(System.currentTimeMillis()));
        row.put(Constants.UPDATED_BY, TEST_USER_ID);
        row.put(Constants.COL_UPDATEDDATE, String.valueOf(System.currentTimeMillis()));
        row.put(Constants.COL_CRITERIA, List.of());
        row.put(Constants.COL_STATUS, "ACTIVE");
        return row;
    }
}
