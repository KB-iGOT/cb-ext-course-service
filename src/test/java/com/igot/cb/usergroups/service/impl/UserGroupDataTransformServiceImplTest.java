package com.igot.cb.usergroups.service.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import com.igot.cb.usergroups.model.CriteriaItem;
import com.igot.cb.usergroups.model.UserGroupEntity;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UserGroupDataTransformServiceImplTest {

    private static final String TEST_USER_ID = "user_123";
    private static final String TEST_ORG_ID = "org_456";
    private static final String TEST_USER_GROUP_ID = "ug_789";
    private static final String TEST_USER_GROUP_NAME = "Test Group";

    private UserGroupDataTransformServiceImpl dataTransformService;

    @BeforeEach
    void setUp() {
        dataTransformService = new UserGroupDataTransformServiceImpl();
    }

    @Test
    void criteriaItemsToDbFormat_withValidList_shouldConvertCorrectly() {
        List<CriteriaItem> criteriaItems = List.of(
                new CriteriaItem("department", List.of("HR", "Finance")),
                new CriteriaItem("role", List.of("Manager", "Lead"))
        );

        List<Map<String, List<String>>> result = dataTransformService.criteriaItemsToDbFormat(criteriaItems);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals(List.of("HR", "Finance"), result.get(0).get("department"));
        assertEquals(List.of("Manager", "Lead"), result.get(1).get("role"));
    }

    @Test
    void criteriaItemsToDbFormat_withEmptyList_shouldReturnEmptyList() {
        List<Map<String, List<String>>> result = dataTransformService.criteriaItemsToDbFormat(Collections.emptyList());

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void criteriaItemsToDbFormat_withNullList_shouldReturnEmptyList() {
        List<Map<String, List<String>>> result = dataTransformService.criteriaItemsToDbFormat(null);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void dbFormatToCriteriaItems_withValidList_shouldConvertCorrectly() {
        List<Map<String, List<String>>> dbCriteria = List.of(
                Map.of("department", List.of("HR", "Finance")),
                Map.of("role", List.of("Manager"))
        );

        List<CriteriaItem> result = dataTransformService.dbFormatToCriteriaItems(dbCriteria);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("department", result.get(0).criteriaKey());
        assertEquals(List.of("HR", "Finance"), result.get(0).criteriaValue());
        assertEquals("role", result.get(1).criteriaKey());
        assertEquals(List.of("Manager"), result.get(1).criteriaValue());
    }

    @Test
    void dbFormatToCriteriaItems_withEmptyList_shouldReturnEmptyList() {
        List<CriteriaItem> result = dataTransformService.dbFormatToCriteriaItems(Collections.emptyList());

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void dbFormatToCriteriaItems_withNullList_shouldReturnEmptyList() {
        List<CriteriaItem> result = dataTransformService.dbFormatToCriteriaItems(null);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void entityToResponseMap_withValidEntity_shouldConvertCorrectly() {
        UserGroupEntity entity = UserGroupEntity.builder()
                .orgid(TEST_ORG_ID)
                .usergroupid(TEST_USER_GROUP_ID)
                .usergroupname(TEST_USER_GROUP_NAME)
                .createdby(TEST_USER_ID)
                .createddate("1234567890")
                .updatedby(TEST_USER_ID)
                .updateddate("1234567890")
                .criteria(List.of(
                        Map.of(
                                Constants.CRITERIA_KEY, List.of("department"),
                                Constants.CRITERIA_VALUE, List.of("HR")
                        )
                ))
                .status("ACTIVE")
                .build();

        Map<String, Object> result = dataTransformService.entityToResponseMap(entity);

        assertNotNull(result);
        assertEquals(TEST_ORG_ID, result.get(Constants.COL_ORGID));
        assertEquals(TEST_USER_GROUP_ID, result.get(Constants.COL_USERGROUPID));
        assertEquals(TEST_USER_GROUP_NAME, result.get(Constants.COL_USERGROUPNAME));
        assertEquals("ACTIVE", result.get(Constants.COL_STATUS));
        assertTrue(result.get(Constants.COL_CRITERIA) instanceof List);
    }

    @Test
    void buildEntityForCreate_shouldCreateEntityWithAllFields() {
        List<CriteriaItem> criteria = List.of(
                new CriteriaItem("department", List.of("HR"))
        );

        UserGroupEntity result = dataTransformService.buildEntityForCreate(
                TEST_USER_GROUP_ID,
                TEST_USER_GROUP_NAME,
                criteria,
                TEST_ORG_ID,
                TEST_USER_ID
        );

        assertNotNull(result);
        assertEquals(TEST_USER_GROUP_ID, result.getUsergroupid());
        assertEquals(TEST_USER_GROUP_NAME, result.getUsergroupname());
        assertEquals(TEST_ORG_ID, result.getOrgid());
        assertEquals(TEST_USER_ID, result.getCreatedby());
        assertEquals(TEST_USER_ID, result.getUpdatedby());
        assertEquals(Constants.ACTIVE, result.getStatus());
        assertNotNull(result.getCreateddate());
        assertNotNull(result.getUpdateddate());
        assertEquals(result.getCreateddate(), result.getUpdateddate());
    }

    @Test
    void buildUpdateProperties_withNameOnly_shouldUpdateNameAndDate() {
        String newName = "Updated Group";

        Map<String, Object> result = dataTransformService.buildUpdateProperties(
                newName,
                null,
                TEST_USER_ID
        );

        assertNotNull(result);
        assertEquals(newName, result.get(Constants.COL_USERGROUPNAME));
        assertEquals(TEST_USER_ID, result.get(Constants.COL_UPDATEDBY));
        assertNotNull(result.get(Constants.COL_UPDATEDDATE));
        assertFalse(result.containsKey(Constants.COL_CRITERIA));
    }

    @Test
    void buildUpdateProperties_withCriteriaOnly_shouldUpdateCriteriaAndDate() {
        List<CriteriaItem> criteria = List.of(
                new CriteriaItem("role", List.of("Admin"))
        );

        Map<String, Object> result = dataTransformService.buildUpdateProperties(
                null,
                criteria,
                TEST_USER_ID
        );

        assertNotNull(result);
        assertEquals(TEST_USER_ID, result.get(Constants.COL_UPDATEDBY));
        assertNotNull(result.get(Constants.COL_UPDATEDDATE));
        assertTrue(result.containsKey(Constants.COL_CRITERIA));
        assertFalse(result.containsKey(Constants.COL_USERGROUPNAME));
    }

    @Test
    void buildUpdateProperties_withBothFields_shouldUpdateBoth() {
        String newName = "Updated Group";
        List<CriteriaItem> criteria = List.of(
                new CriteriaItem("role", List.of("Admin"))
        );

        Map<String, Object> result = dataTransformService.buildUpdateProperties(
                newName,
                criteria,
                TEST_USER_ID
        );

        assertNotNull(result);
        assertEquals(newName, result.get(Constants.COL_USERGROUPNAME));
        assertTrue(result.containsKey(Constants.COL_CRITERIA));
        assertEquals(TEST_USER_ID, result.get(Constants.COL_UPDATEDBY));
        assertNotNull(result.get(Constants.COL_UPDATEDDATE));
    }
}
