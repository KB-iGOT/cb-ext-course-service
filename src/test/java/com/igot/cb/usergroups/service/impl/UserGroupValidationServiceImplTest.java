package com.igot.cb.usergroups.service.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.ProjectUtil;
import com.igot.cb.usergroups.model.CriteriaItem;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

@ExtendWith(MockitoExtension.class)
class UserGroupValidationServiceImplTest {

    private static final String TEST_USER_ID = "user_123";
    private static final String TEST_ORG_ID = "org_456";
    private static final String TEST_USER_GROUP_ID = "ug_789";
    private static final String TEST_USER_GROUP_NAME = "Test Group";
    private static final String TEST_AUTHORIZED_ROLE = "MDO_LEADER";

    @Mock
    private CbExtServerProperties serverProperties;

    private UserGroupValidationServiceImpl validationService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        validationService = new UserGroupValidationServiceImpl(serverProperties);
    }

    @Test
    void validateCreateRequest_withValidInputs_shouldPass() {
        List<CriteriaItem> criteria = createValidCriteriaList();
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(TEST_USER_GROUP_NAME, criteria, response);

        assertTrue(result);
    }

    @Test
    void validateCreateRequest_withBlankName_shouldReturnFalse() {
        List<CriteriaItem> criteria = createValidCriteriaList();
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest("", criteria, response);

        assertFalse(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void validateCreateRequest_withNullName_shouldReturnFalse() {
        List<CriteriaItem> criteria = createValidCriteriaList();
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(null, criteria, response);

        assertFalse(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void validateCreateRequest_withEmptyCriteria_shouldReturnFalse() {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(TEST_USER_GROUP_NAME, Collections.emptyList(), response);

        assertFalse(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void validateCreateRequest_withNullCriteria_shouldReturnFalse() {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(TEST_USER_GROUP_NAME, null, response);

        assertFalse(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.MSG_CRITERIA_REQUIRED, response.getParams().getErr());
    }

    @Test
    void validateUpdateRequest_withValidInputs_shouldPass() {
        List<CriteriaItem> criteria = createValidCriteriaList();
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_UPDATE);

        boolean result = validationService.validateUpdateRequest(TEST_USER_GROUP_ID, TEST_USER_GROUP_NAME, criteria, response);

        assertTrue(result);
    }

    @Test
    void validateUpdateRequest_withBlankUserGroupId_shouldReturnFalse() {
        List<CriteriaItem> criteria = createValidCriteriaList();
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_UPDATE);

        boolean result = validationService.validateUpdateRequest("", TEST_USER_GROUP_NAME, criteria, response);

        assertFalse(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void validateUserGroupId_withValidId_shouldPass() {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_READ);

        boolean result = validationService.validateUserGroupId(TEST_USER_GROUP_ID, response);

        assertTrue(result);
    }

    @Test
    void validateUserGroupId_withBlankId_shouldReturnFalse() {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_READ);

        boolean result = validationService.validateUserGroupId("", response);

        assertFalse(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void validateUpdateAuthorization_asCreator_shouldPass() {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_UPDATE);

        boolean result = validationService.validateUpdateAuthorization(
                TEST_USER_ID,
                TEST_ORG_ID,
                "VIEWER",
                TEST_USER_ID,
                TEST_ORG_ID,
                response
        );

        assertTrue(result);
    }

    @Test
    void validateUpdateAuthorization_withAuthorizedRole_shouldPass() {
        when(serverProperties.getUserGroupUpdateAuthorizedRole()).thenReturn(TEST_AUTHORIZED_ROLE);
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_UPDATE);

        boolean result = validationService.validateUpdateAuthorization(
                TEST_USER_ID,
                TEST_ORG_ID,
                TEST_AUTHORIZED_ROLE,
                "other_user",
                TEST_ORG_ID,
                response
        );

        assertTrue(result);
    }

    @Test
    void validateUpdateAuthorization_withOrgMismatch_shouldReturnFalse() {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_UPDATE);

        boolean result = validationService.validateUpdateAuthorization(
                TEST_USER_ID,
                TEST_ORG_ID,
                TEST_AUTHORIZED_ROLE,
                "other_user",
                "different_org",
                response
        );

        assertFalse(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.FORBIDDEN, response.getResponseCode());
    }

    @Test
    void validateUpdateAuthorization_withoutAuthorizedRole_shouldReturnFalse() {
        when(serverProperties.getUserGroupUpdateAuthorizedRole()).thenReturn(TEST_AUTHORIZED_ROLE);
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_UPDATE);

        boolean result = validationService.validateUpdateAuthorization(
                TEST_USER_ID,
                TEST_ORG_ID,
                "VIEWER",
                "other_user",
                TEST_ORG_ID,
                response
        );

        assertFalse(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.FORBIDDEN, response.getResponseCode());
    }

    // Helper methods

    private List<CriteriaItem> createValidCriteriaList() {
        return List.of(
                new CriteriaItem("department", List.of("HR", "Finance")),
                new CriteriaItem("role", List.of("Manager"))
        );
    }
}
