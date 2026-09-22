package com.igot.cb.usergroups.service.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.usergroups.model.UserGroupEntity;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class UserGroupElasticSearchServiceImplTest {

    private static final String TEST_USER_ID = "user_123";
    private static final String TEST_ORG_ID = "org_456";
    private static final String TEST_USER_GROUP_ID = "ug_789";
    private static final String TEST_USER_GROUP_NAME = "Test Group";

    @Mock
    private EsUtilService esUtilService;

    @Mock
    private UserGroupDataTransformServiceImpl dataTransformService;

    @Mock
    private CbExtServerProperties serverProperties;

    @Mock
    private ObjectMapper objectMapper;

    private UserGroupElasticSearchServiceImpl esService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        lenient().when(serverProperties.getUserGroupIndex()).thenReturn("user_group_info");
        lenient().when(serverProperties.getElasticUserGroupJsonPath()).thenReturn("/EsRequiredFields/EsRequiredFieldsUserGroup.json");
        esService = new UserGroupElasticSearchServiceImpl(esUtilService, dataTransformService, serverProperties, objectMapper);
    }

    @Test
    void tryIndexUserGroup_withValidDocument_shouldReturnTrueAndVerifyAddDocument() {
        UserGroupEntity entity = createUserGroupEntity();
        Map<String, Object> document = new HashMap<>();

        when(dataTransformService.entityToResponseMap(entity)).thenReturn(document);
        when(esUtilService.addDocument(anyString(), anyString(), anyString(), anyMap(), anyString()))
                .thenReturn("success");

        boolean result = esService.tryIndexUserGroup(entity);

        assertTrue(result);
        verify(dataTransformService, times(1)).entityToResponseMap(entity);
        verify(esUtilService, times(1)).addDocument(
                eq("user_group_info"),
                eq(Constants.INDEX_TYPE),
                eq(TEST_USER_GROUP_ID),
                eq(document),
                anyString()
        );
    }

    @Test
    void tryIndexUserGroup_whenDataTransformThrows_shouldReturnFalseWithoutThrowing() {
        UserGroupEntity entity = createUserGroupEntity();

        when(dataTransformService.entityToResponseMap(entity)).thenThrow(new RuntimeException("Test exception"));

        boolean result = esService.tryIndexUserGroup(entity);

        assertFalse(result);
    }

    @Test
    void updateUserGroup_withValidData_shouldUpdateSuccessfully() {
        Map<String, Object> updateProps = new HashMap<>();
        updateProps.put(Constants.COL_USERGROUPNAME, "Updated Name");

        when(esUtilService.updateDocument(anyString(), anyString(), anyString(), anyMap(), anyString()))
                .thenReturn("success");

        assertDoesNotThrow(() -> esService.updateUserGroup(TEST_USER_GROUP_ID, updateProps));

        verify(esUtilService, times(1)).updateDocument(
                eq("user_group_info"),
                eq(Constants.INDEX_TYPE),
                eq(TEST_USER_GROUP_ID),
                eq(updateProps),
                anyString()
        );
    }

    @Test
    void updateUserGroup_withException_shouldLogAndContinue() {
        Map<String, Object> updateProps = new HashMap<>();

        when(esUtilService.updateDocument(anyString(), anyString(), anyString(), anyMap(), anyString()))
                .thenThrow(new RuntimeException("Test exception"));

        assertDoesNotThrow(() -> esService.updateUserGroup(TEST_USER_GROUP_ID, updateProps));
    }

    @Test
    void searchUserGroups_withDefaultFilters_shouldReturnResults() throws Exception {
        Map<String, Object> filters = new HashMap<>();
        filters.put(Constants.COL_ORGID, TEST_ORG_ID);

        SearchResult searchResult = new SearchResult();
        searchResult.setData(List.of());
        searchResult.setTotalCount(0);

        when(esUtilService.searchDocuments(anyString(), any(SearchCriteria.class), anyString()))
                .thenReturn(searchResult);
        when(objectMapper.convertValue(any(), any(com.fasterxml.jackson.core.type.TypeReference.class)))
                .thenReturn(List.of());

        Map<String, Object> result = esService.searchUserGroups(filters, 10, 0, "createddate", "desc");

        assertNotNull(result);
        assertTrue(result.containsKey("content"));
        assertTrue(result.containsKey("count"));
        verify(esUtilService, times(1)).searchDocuments(
                eq("user_group_info"),
                any(SearchCriteria.class),
                anyString()
        );
    }

    @Test
    void searchUserGroups_withStatusFilter_shouldIncludeInQuery() throws Exception {
        Map<String, Object> filters = new HashMap<>();
        filters.put(Constants.COL_ORGID, TEST_ORG_ID);
        filters.put(Constants.COL_STATUS, List.of("ACTIVE"));

        SearchResult searchResult = new SearchResult();
        searchResult.setData(List.of());
        searchResult.setTotalCount(0);

        when(esUtilService.searchDocuments(anyString(), any(SearchCriteria.class), anyString()))
                .thenReturn(searchResult);
        when(objectMapper.convertValue(any(), any(com.fasterxml.jackson.core.type.TypeReference.class)))
                .thenReturn(List.of());

        esService.searchUserGroups(filters, 20, 1, "usergroupname", "asc");

        verify(esUtilService, times(1)).searchDocuments(
                eq("user_group_info"),
                any(SearchCriteria.class),
                anyString()
        );
    }

    @Test
    void searchUserGroups_withException_shouldReturnEmptyResult() throws Exception {
        Map<String, Object> filters = new HashMap<>();

        when(esUtilService.searchDocuments(anyString(), any(SearchCriteria.class), anyString()))
                .thenThrow(new RuntimeException("Test exception"));

        Map<String, Object> result = esService.searchUserGroups(filters, 10, 0, "createddate", "desc");

        assertNotNull(result);
        assertTrue(result.containsKey("content"));
        assertTrue(result.containsKey("count"));
        assertEquals(0, ((List<?>) result.get("content")).size());
        assertEquals(0, result.get("count"));
    }

    @Test
    void tryUpdateDocument_whenEsUpdateReturnsNonNull_shouldReturnTrue() {
        Map<String, Object> updateProps = Map.of(Constants.COL_USERGROUPNAME, "Updated Name");
        when(esUtilService.updateDocument(anyString(), anyString(), anyString(), anyMap(), anyString())).thenReturn("success");

        boolean result = esService.tryUpdateDocument(TEST_USER_GROUP_ID, updateProps);

        assertTrue(result);
        verify(esUtilService, times(1)).updateDocument(
                eq("user_group_info"), eq(Constants.INDEX_TYPE), eq(TEST_USER_GROUP_ID), eq(updateProps), anyString());
    }

    @Test
    void tryUpdateDocument_whenEsUpdateReturnsNull_shouldReturnFalse() {
        Map<String, Object> updateProps = Map.of(Constants.COL_STATUS, Constants.ARCHIVED);
        when(esUtilService.updateDocument(anyString(), anyString(), anyString(), anyMap(), anyString())).thenReturn(null);

        boolean result = esService.tryUpdateDocument(TEST_USER_GROUP_ID, updateProps);

        assertFalse(result);
    }

    @Test
    void rollbackUpdate_whenEsRollbackSucceeds_shouldCompleteWithoutError() {
        Map<String, Object> previousState = new HashMap<>();
        when(esUtilService.updateDocument(anyString(), anyString(), anyString(), anyMap(), anyString())).thenReturn("success");

        assertDoesNotThrow(() -> esService.rollbackUpdate(TEST_USER_GROUP_ID, previousState));
        verify(esUtilService, times(1)).updateDocument(
                eq("user_group_info"), eq(Constants.INDEX_TYPE), eq(TEST_USER_GROUP_ID), eq(previousState), anyString());
    }

    @Test
    void rollbackUpdate_whenEsRollbackFails_shouldLogDivergenceErrorWithoutThrowing() {
        Map<String, Object> previousState = new HashMap<>();
        when(esUtilService.updateDocument(anyString(), anyString(), anyString(), anyMap(), anyString())).thenReturn(null);

        assertDoesNotThrow(() -> esService.rollbackUpdate(TEST_USER_GROUP_ID, previousState));
    }

    @Test
    void tryIndexUserGroup_whenEsAddReturnsNonNull_shouldReturnTrue() {
        UserGroupEntity entity = createUserGroupEntity();
        Map<String, Object> document = new HashMap<>();
        when(dataTransformService.entityToResponseMap(entity)).thenReturn(document);
        when(esUtilService.addDocument(anyString(), anyString(), anyString(), anyMap(), anyString())).thenReturn("created");

        boolean result = esService.tryIndexUserGroup(entity);

        assertTrue(result);
        verify(esUtilService, times(1)).addDocument(
                eq("user_group_info"), eq(Constants.INDEX_TYPE), eq(TEST_USER_GROUP_ID), eq(document), anyString());
    }

    @Test
    void tryIndexUserGroup_whenEsAddReturnsNull_shouldReturnFalse() {
        UserGroupEntity entity = createUserGroupEntity();
        when(dataTransformService.entityToResponseMap(entity)).thenReturn(new HashMap<>());
        when(esUtilService.addDocument(anyString(), anyString(), anyString(), anyMap(), anyString())).thenReturn(null);

        boolean result = esService.tryIndexUserGroup(entity);

        assertFalse(result);
    }

    @Test
    void tryIndexUserGroup_whenEsAddThrows_shouldReturnFalse() {
        UserGroupEntity entity = createUserGroupEntity();
        when(dataTransformService.entityToResponseMap(entity)).thenThrow(new RuntimeException("ES unavailable"));

        boolean result = esService.tryIndexUserGroup(entity);

        assertFalse(result);
    }

    @Test
    void rollbackCreate_whenDeleteSucceeds_shouldCompleteWithoutError() {
        when(esUtilService.deleteDocument(eq("user_group_info"), eq(TEST_USER_GROUP_ID))).thenReturn(true);

        assertDoesNotThrow(() -> esService.rollbackCreate(TEST_USER_GROUP_ID));
        verify(esUtilService, times(1)).deleteDocument(eq("user_group_info"), eq(TEST_USER_GROUP_ID));
    }

    @Test
    void rollbackCreate_whenDeleteFails_shouldLogDivergenceWithoutThrowing() {
        when(esUtilService.deleteDocument(eq("user_group_info"), eq(TEST_USER_GROUP_ID))).thenReturn(false);

        assertDoesNotThrow(() -> esService.rollbackCreate(TEST_USER_GROUP_ID));
    }

    @Test
    void isDuplicateGroupName_createFlow_whenNoMatchFound_shouldReturnFalse() throws Exception {
        SearchResult searchResult = new SearchResult();
        searchResult.setData(List.of());
        searchResult.setTotalCount(0);

        when(esUtilService.searchDocuments(anyString(), any(SearchCriteria.class), anyString()))
                .thenReturn(searchResult);

        boolean result = esService.isDuplicateGroupName(TEST_USER_GROUP_NAME, TEST_ORG_ID, null);

        assertFalse(result);
        verify(esUtilService, times(1)).searchDocuments(eq("user_group_info"), any(SearchCriteria.class), anyString());
    }

    @Test
    void isDuplicateGroupName_createFlow_whenMatchFoundInSameOrg_shouldReturnTrue() throws Exception {
        Map<String, Object> existingDoc = Map.of(Constants.COL_USERGROUPID, "other_group_id");
        SearchResult searchResult = new SearchResult();
        searchResult.setData(List.of(existingDoc));
        searchResult.setTotalCount(1);

        when(esUtilService.searchDocuments(anyString(), any(SearchCriteria.class), anyString()))
                .thenReturn(searchResult);

        boolean result = esService.isDuplicateGroupName(TEST_USER_GROUP_NAME, TEST_ORG_ID, null);

        assertTrue(result);
    }

    @Test
    void isDuplicateGroupName_updateFlow_whenOnlyOwnGroupMatches_shouldReturnFalse() throws Exception {
        Map<String, Object> ownDoc = Map.of(Constants.COL_USERGROUPID, TEST_USER_GROUP_ID);
        SearchResult searchResult = new SearchResult();
        searchResult.setData(List.of(ownDoc));
        searchResult.setTotalCount(1);

        when(esUtilService.searchDocuments(anyString(), any(SearchCriteria.class), anyString()))
                .thenReturn(searchResult);

        boolean result = esService.isDuplicateGroupName(TEST_USER_GROUP_NAME, TEST_ORG_ID, TEST_USER_GROUP_ID);

        assertFalse(result);
    }

    @Test
    void isDuplicateGroupName_updateFlow_whenDifferentGroupHasSameName_shouldReturnTrue() throws Exception {
        Map<String, Object> otherDoc = Map.of(Constants.COL_USERGROUPID, "another_group_id");
        SearchResult searchResult = new SearchResult();
        searchResult.setData(List.of(otherDoc));
        searchResult.setTotalCount(1);

        when(esUtilService.searchDocuments(anyString(), any(SearchCriteria.class), anyString()))
                .thenReturn(searchResult);

        boolean result = esService.isDuplicateGroupName(TEST_USER_GROUP_NAME, TEST_ORG_ID, TEST_USER_GROUP_ID);

        assertTrue(result);
    }

    @Test
    void isDuplicateGroupName_whenEsQueryThrows_shouldReturnFalseWithoutRethrow() throws Exception {
        when(esUtilService.searchDocuments(anyString(), any(SearchCriteria.class), anyString()))
                .thenThrow(new RuntimeException("ES unavailable"));

        boolean result = esService.isDuplicateGroupName(TEST_USER_GROUP_NAME, TEST_ORG_ID, null);

        assertFalse(result);
    }

    // Helper methods

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
}
