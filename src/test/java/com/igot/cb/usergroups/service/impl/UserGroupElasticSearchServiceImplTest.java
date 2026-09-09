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
    private ObjectMapper objectMapper;

    private UserGroupElasticSearchServiceImpl esService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        esService = new UserGroupElasticSearchServiceImpl(esUtilService, dataTransformService, objectMapper);
    }

    @Test
    void indexUserGroup_withValidEntity_shouldIndexSuccessfully() {
        UserGroupEntity entity = createUserGroupEntity();
        Map<String, Object> document = new HashMap<>();

        when(dataTransformService.entityToResponseMap(entity)).thenReturn(document);
        when(esUtilService.addDocument(anyString(), anyString(), anyString(), anyMap(), anyString()))
                .thenReturn("success");

        assertDoesNotThrow(() -> esService.indexUserGroup(entity));

        verify(dataTransformService, times(1)).entityToResponseMap(entity);
        verify(esUtilService, times(1)).addDocument(
                eq(Constants.ES_INDEX_USER_GROUP_INFO),
                eq(Constants.INDEX_TYPE),
                eq(TEST_USER_GROUP_ID),
                eq(document),
                anyString()
        );
    }

    @Test
    void indexUserGroup_withException_shouldLogAndContinue() {
        // Arrange
        UserGroupEntity entity = createUserGroupEntity();

        when(dataTransformService.entityToResponseMap(entity)).thenThrow(new RuntimeException("Test exception"));

        // Act & Assert - should not throw
        assertDoesNotThrow(() -> esService.indexUserGroup(entity));
    }

    @Test
    void updateUserGroup_withValidData_shouldUpdateSuccessfully() {
        Map<String, Object> updateProps = new HashMap<>();
        updateProps.put(Constants.COL_USERGROUPNAME, "Updated Name");

        when(esUtilService.updateDocument(anyString(), anyString(), anyString(), anyMap(), anyString()))
                .thenReturn("success");

        assertDoesNotThrow(() -> esService.updateUserGroup(TEST_USER_GROUP_ID, updateProps));

        verify(esUtilService, times(1)).updateDocument(
                eq(Constants.ES_INDEX_USER_GROUP_INFO),
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
                eq(Constants.ES_INDEX_USER_GROUP_INFO),
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
                eq(Constants.ES_INDEX_USER_GROUP_INFO),
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
