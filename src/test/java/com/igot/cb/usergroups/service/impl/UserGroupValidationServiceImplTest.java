package com.igot.cb.usergroups.service.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.UserAndOrgServiceImpl;
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
    private static final String TEST_USER_ROLES = "MDO_LEADER,USER";

    @Mock
    private CbExtServerProperties serverProperties;

    @Mock
    private UserAndOrgServiceImpl userAndOrgService;

    private UserGroupValidationServiceImpl validationService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        validationService = new UserGroupValidationServiceImpl(serverProperties, userAndOrgService);

        Map<String, Object> orgMap = new HashMap<>();
        orgMap.put(Constants.IS_CCA, false);
        lenient().when(userAndOrgService.readOrgFromDB(eq(TEST_ORG_ID), any())).thenReturn(orgMap);

        lenient().when(serverProperties.getUserGroupUpdateAuthorizedRole()).thenReturn(TEST_AUTHORIZED_ROLE);
    }

    @Test
    void validateCreateRequest_withValidInputs_shouldPass() {
        List<CriteriaItem> criteria = createValidCriteriaList();
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(TEST_USER_GROUP_NAME, criteria, TEST_ORG_ID, TEST_USER_ROLES, response);

        assertTrue(result);
    }

    @Test
    void validateCreateRequest_withBlankName_shouldReturnFalse() {
        List<CriteriaItem> criteria = createValidCriteriaList();
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest("", criteria, TEST_ORG_ID, TEST_USER_ROLES, response);

        assertFalse(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void validateCreateRequest_withNullName_shouldReturnFalse() {
        List<CriteriaItem> criteria = createValidCriteriaList();
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(null, criteria, TEST_ORG_ID, TEST_USER_ROLES, response);

        assertFalse(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void validateCreateRequest_withEmptyCriteria_shouldReturnFalse() {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(TEST_USER_GROUP_NAME, Collections.emptyList(), TEST_ORG_ID, TEST_USER_ROLES, response);

        assertFalse(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void validateCreateRequest_withNullCriteria_shouldReturnFalse() {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(TEST_USER_GROUP_NAME, null, TEST_ORG_ID, TEST_USER_ROLES, response);

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

    @Test
    void validateCreateRequest_nonCCA_withoutRootOrgId_shouldFail() {
        List<CriteriaItem> criteriaWithoutRootOrgId = List.of(
                new CriteriaItem("department", List.of("HR"))
        );
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(
                TEST_USER_GROUP_NAME, criteriaWithoutRootOrgId, TEST_ORG_ID, "USER", response);

        assertFalse(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.MSG_ROOTORGID_REQUIRED_NON_CCA, response.getParams().getErr());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void validateCreateRequest_nonCCA_withMultipleRootOrgIds_shouldFail() {
        List<CriteriaItem> criteriaWithMultipleRootOrgIds = List.of(
                new CriteriaItem("rootOrgId", List.of(TEST_ORG_ID, "different_org"))
        );
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(
                TEST_USER_GROUP_NAME, criteriaWithMultipleRootOrgIds, TEST_ORG_ID, "USER", response);

        assertFalse(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.MSG_MULTIPLE_ROOTORGID_NON_CCA, response.getParams().getErr());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void validateCreateRequest_nonCCA_rootOrgIdMismatch_nonAdmin_shouldFail() {
        List<CriteriaItem> criteriaWithDifferentOrg = List.of(
                new CriteriaItem("rootOrgId", List.of("different_org_id"))
        );
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(
                TEST_USER_GROUP_NAME, criteriaWithDifferentOrg, TEST_ORG_ID, "USER", response);

        assertFalse(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.MSG_ROOTORGID_MISMATCH, response.getParams().getErr());
        assertEquals(HttpStatus.FORBIDDEN, response.getResponseCode());
    }

    @Test
    void validateCreateRequest_nonCCA_rootOrgIdMismatch_admin_shouldPass() {
        List<CriteriaItem> criteriaWithDifferentOrg = List.of(
                new CriteriaItem("rootOrgId", List.of("different_org_id"))
        );
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(
                TEST_USER_GROUP_NAME, criteriaWithDifferentOrg, TEST_ORG_ID, TEST_USER_ROLES, response);

        assertTrue(result);
    }

    @Test
    void validateCreateRequest_nonCCA_rootOrgIdMatches_shouldPass() {
        List<CriteriaItem> criteria = List.of(
                new CriteriaItem("rootOrgId", List.of(TEST_ORG_ID)),
                new CriteriaItem("department", List.of("HR"))
        );
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(
                TEST_USER_GROUP_NAME, criteria, TEST_ORG_ID, "USER", response);

        assertTrue(result);
    }

    @Test
    void validateCreateRequest_CCA_withoutRootOrgId_shouldPass() {
        Map<String, Object> ccaOrgMap = new HashMap<>();
        ccaOrgMap.put(Constants.IS_CCA, true);
        when(userAndOrgService.readOrgFromDB(eq(TEST_ORG_ID), any())).thenReturn(ccaOrgMap);

        List<CriteriaItem> criteriaWithoutRootOrgId = List.of(
                new CriteriaItem("department", List.of("HR"))
        );
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(
                TEST_USER_GROUP_NAME, criteriaWithoutRootOrgId, TEST_ORG_ID, "USER", response);

        assertTrue(result);
    }

    @Test
    void validateCreateRequest_CCA_withSingleRootOrgId_shouldPass() {
        Map<String, Object> ccaOrgMap = new HashMap<>();
        ccaOrgMap.put(Constants.IS_CCA, true);
        when(userAndOrgService.readOrgFromDB(eq(TEST_ORG_ID), any())).thenReturn(ccaOrgMap);

        List<CriteriaItem> criteria = List.of(
                new CriteriaItem("rootOrgId", List.of(TEST_ORG_ID))
        );
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(
                TEST_USER_GROUP_NAME, criteria, TEST_ORG_ID, "USER", response);

        assertTrue(result);
    }

    @Test
    void validateCreateRequest_CCA_withMultipleRootOrgIds_shouldPass() {
        Map<String, Object> ccaOrgMap = new HashMap<>();
        ccaOrgMap.put(Constants.IS_CCA, true);
        when(userAndOrgService.readOrgFromDB(eq(TEST_ORG_ID), any())).thenReturn(ccaOrgMap);

        List<CriteriaItem> criteriaWithMultipleOrgs = List.of(
                new CriteriaItem("rootOrgId", List.of(TEST_ORG_ID, "other_org_1", "other_org_2"))
        );
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(
                TEST_USER_GROUP_NAME, criteriaWithMultipleOrgs, TEST_ORG_ID, "USER", response);

        assertTrue(result);
    }

    @Test
    void validateCreateRequest_orgNotFoundInDB_shouldFail() {
        when(userAndOrgService.readOrgFromDB(eq(TEST_ORG_ID), any())).thenReturn(null);

        List<CriteriaItem> criteria = createValidCriteriaList();
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(
                TEST_USER_GROUP_NAME, criteria, TEST_ORG_ID, TEST_USER_ROLES, response);

        assertFalse(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErr().contains(Constants.ERR_FAILED_TO_READ_ORG_DETAILS));
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void validateCreateRequest_withTargetedOrganisation_shouldPass() {
        List<CriteriaItem> criteriaWithTargetedOrg = List.of(
                new CriteriaItem("targetedOrganisation", List.of(TEST_ORG_ID)),
                new CriteriaItem("department", List.of("HR"))
        );
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(
                TEST_USER_GROUP_NAME, criteriaWithTargetedOrg, TEST_ORG_ID, "USER", response);

        assertTrue(result);
    }

    @Test
    void validateCreateRequest_caseInsensitiveRootOrgId_shouldPass() {
        List<CriteriaItem> criteriaWithMixedCase = List.of(
                new CriteriaItem("ROOTORGID", List.of(TEST_ORG_ID)),
                new CriteriaItem("department", List.of("HR"))
        );
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(
                TEST_USER_GROUP_NAME, criteriaWithMixedCase, TEST_ORG_ID, "USER", response);

        assertTrue(result);
    }

    @Test
    void validateCreateRequest_userWithNoRoles_nonAdminBehavior() {
        List<CriteriaItem> criteria = List.of(
                new CriteriaItem("rootOrgId", List.of(TEST_ORG_ID))
        );
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(
                TEST_USER_GROUP_NAME, criteria, TEST_ORG_ID, null, response);

        assertTrue(result);
    }

    @Test
    void validateCreateRequest_userWithBlankRoles_nonAdminBehavior() {
        List<CriteriaItem> criteriaWithDifferentOrg = List.of(
                new CriteriaItem("rootOrgId", List.of("different_org"))
        );
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        boolean result = validationService.validateCreateRequest(
                TEST_USER_GROUP_NAME, criteriaWithDifferentOrg, TEST_ORG_ID, "   ", response);

        assertFalse(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.MSG_ROOTORGID_MISMATCH, response.getParams().getErr());
    }

    private List<CriteriaItem> createValidCriteriaList() {
        return List.of(
                new CriteriaItem("rootOrgId", List.of(TEST_ORG_ID)),
                new CriteriaItem("department", List.of("HR", "Finance")),
                new CriteriaItem("role", List.of("Manager"))
        );
    }
}
