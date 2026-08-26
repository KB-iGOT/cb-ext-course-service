package com.igot.cb.cbplan.service.impl;

import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.UserAndOrgServiceImpl;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanValidationServiceV3ImplTest {

    private static final String USER_ID = "user1";
    private static final String ORG_ID = "org1";
    private static final String PLAN_ID = "plan123";

    @Mock
    private AccessTokenValidator accessTokenValidator;

    @Mock
    private UserAndOrgServiceImpl userAndOrgService;

    @Mock
    private CbPlanRequestValidatorImpl requestValidator;

    @Mock
    private CbExtServerProperties serverProperties;

    @InjectMocks
    private CbPlanValidationServiceV3Impl validationService;

    @BeforeEach
    void setUp() {
        lenient().when(serverProperties.getCbPlanUpdatePublishAuthorizedRoles())
                .thenReturn(List.of("CONTENT_CREATOR", "ADMIN"));
    }

    private static ApiRequest apiRequestWithId(String planId) {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        if (planId != null) {
            requestMap.put(Constants.ID, planId);
        }
        request.setRequest(requestMap);
        return request;
    }

    @Test
    void testValidateAndExtractUserIdSuccess() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn(USER_ID);
        assertEquals(USER_ID, validationService.validateAndExtractUserId("token", new ApiResponse()));
    }

    @Test
    void testValidateAndExtractUserIdReturnsNullWhenBlank() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("");
        assertNull(validationService.validateAndExtractUserId("token", new ApiResponse()));
    }

    @Test
    void testValidateAndExtractUserIdReturnsNullWhenNull() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn(null);
        assertNull(validationService.validateAndExtractUserId("token", new ApiResponse()));
    }

    @Test
    void testValidateUserOrganizationSuccess() {
        Map<String, Object> userMap = new HashMap<>();
        userMap.put(Constants.ROOT_ORG_ID, ORG_ID);
        when(userAndOrgService.readUserProfileFromDB(anyString(), anyList())).thenReturn(userMap);
        assertEquals(ORG_ID, validationService.validateUserOrganization(USER_ID, new ApiResponse()));
    }

    @Test
    void testValidateUserOrganizationFailsWhenUserNotFound() {
        ApiResponse response = new ApiResponse();
        when(userAndOrgService.readUserProfileFromDB(anyString(), anyList())).thenReturn(new HashMap<>());
        assertNull(validationService.validateUserOrganization(USER_ID, response));
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testValidateOrgCCAReturnsTrue() {
        Map<String, Object> orgMap = new HashMap<>();
        orgMap.put(Constants.IS_CCA, "true");
        when(userAndOrgService.readOrgFromDB(anyString(), any())).thenReturn(orgMap);
        assertTrue(validationService.validateOrgCCA(ORG_ID, new ApiResponse()));
    }

    @Test
    void testValidateOrgCCAReturnsFalseWhenFlagFalse() {
        Map<String, Object> orgMap = new HashMap<>();
        orgMap.put(Constants.IS_CCA, false);
        when(userAndOrgService.readOrgFromDB(anyString(), any())).thenReturn(orgMap);
        assertFalse(validationService.validateOrgCCA(ORG_ID, new ApiResponse()));
    }

    @Test
    void testValidateOrgCCAFailsWhenOrgNotFound() {
        ApiResponse response = new ApiResponse();
        when(userAndOrgService.readOrgFromDB(anyString(), any())).thenReturn(new HashMap<>());
        assertFalse(validationService.validateOrgCCA(ORG_ID, response));
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testValidateOrgCCAReturnsFalseWhenKeyMissing() {
        Map<String, Object> orgMap = new HashMap<>();
        orgMap.put(Constants.NAME, "someOrg");
        when(userAndOrgService.readOrgFromDB(anyString(), any())).thenReturn(orgMap);
        assertFalse(validationService.validateOrgCCA(ORG_ID, new ApiResponse()));
    }

    @Test
    void testValidateRequestPassesWhenNoValidationErrors() {
        when(requestValidator.validateCbPlanCreateRequest(any(), anyBoolean(), anyString(), anyBoolean()))
                .thenReturn(List.of());
        assertTrue(validationService.validateRequest(new ApiRequest(), true, ORG_ID, new ApiResponse()));
    }

    @Test
    void testValidateRequestFailsWithValidationErrors() {
        ApiResponse response = new ApiResponse();
        when(requestValidator.validateCbPlanCreateRequest(any(), anyBoolean(), anyString(), anyBoolean()))
                .thenReturn(List.of("name is required"));
        assertFalse(validationService.validateRequest(new ApiRequest(), true, ORG_ID, response));
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertTrue(response.getParams().getErr().contains("name is required"));
    }

    @Test
    void testValidatePlanIdExistsWhenPresent() {
        assertTrue(validationService.validatePlanIdExists(apiRequestWithId(PLAN_ID), new ApiResponse()));
    }

    @Test
    void testValidatePlanIdExistsFailsWhenMissing() {
        ApiResponse response = new ApiResponse();
        assertFalse(validationService.validatePlanIdExists(apiRequestWithId(null), response));
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testValidatePlanIdExistsFailsWhenBlank() {
        ApiResponse response = new ApiResponse();
        assertFalse(validationService.validatePlanIdExists(apiRequestWithId("  "), response));
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testIsUnauthorizedToUpdateAllowsOwner() {
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, USER_ID);
        assertFalse(validationService.isUnauthorizedToUpdate(USER_ID, existingCbPlan, List.of(), new ApiResponse()));
    }

    @Test
    void testIsUnauthorizedToUpdateAllowsAuthorizedRole() {
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, "otherUser");
        assertFalse(validationService.isUnauthorizedToUpdate(USER_ID, existingCbPlan,
                List.of("CONTENT_CREATOR"), new ApiResponse()));
    }

    @Test
    void testIsUnauthorizedToUpdateBlocksNonOwnerNonAdmin() {
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, "otherUser");
        ApiResponse response = new ApiResponse();
        assertTrue(validationService.isUnauthorizedToUpdate(USER_ID, existingCbPlan, List.of("PUBLIC"), response));
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.FORBIDDEN, response.getResponseCode());
    }

    @Test
    void testIsUnauthorizedToUpdateBlocksWhenRolesNull() {
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CREATED_BY, "otherUser");
        ApiResponse response = new ApiResponse();
        assertTrue(validationService.isUnauthorizedToUpdate(USER_ID, existingCbPlan, null, response));
        assertEquals(HttpStatus.FORBIDDEN, response.getResponseCode());
    }

    @Test
    void testValidateContextDataForLivePlanPasses() {
        when(requestValidator.validateContextData(any(), anyBoolean(), anyString(), any(), anyBoolean())).thenReturn(List.of());
        assertTrue(validationService.validateContextDataForLivePlan(new HashMap<>(), true, ORG_ID,
                new HashSet<>(), new ApiResponse()));
    }

    @Test
    void testValidateContextDataForLivePlanFails() {
        ApiResponse response = new ApiResponse();
        when(requestValidator.validateContextData(any(), anyBoolean(), anyString(), any(), anyBoolean()))
                .thenReturn(List.of("bad context"));
        assertFalse(validationService.validateContextDataForLivePlan(new HashMap<>(), true, ORG_ID,
                new HashSet<>(), response));
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertTrue(response.getParams().getErr().contains("bad context"));
    }

    @Test
    void testValidateContextDataForLivePlanPopulatesOrgIdsFromValidator() {
        Set<String> rootOrgIds = new HashSet<>();
        when(requestValidator.validateContextData(any(), anyBoolean(), anyString(), any(), anyBoolean()))
                .thenAnswer(invocation -> {
                    Set<String> target = invocation.getArgument(3);
                    target.add(ORG_ID);
                    return List.of();
                });
        assertTrue(validationService.validateContextDataForLivePlan(new HashMap<>(), false, ORG_ID,
                rootOrgIds, new ApiResponse()));
        assertEquals(Set.of(ORG_ID), rootOrgIds);
    }

    @Test
    void testValidateAndExtractPlanIdWhenPresent() {
        Map<String, Object> incomingRequest = new HashMap<>();
        incomingRequest.put(Constants.ID, PLAN_ID);
        assertEquals(PLAN_ID, validationService.validateAndExtractPlanId(incomingRequest, new ApiResponse()));
    }

    @Test
    void testValidateAndExtractPlanIdFailsWhenMissing() {
        ApiResponse response = new ApiResponse();
        assertNull(validationService.validateAndExtractPlanId(new HashMap<>(), response));
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testIsAlreadyArchivedReturnsTrueForRetiredPlan() {
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.STATUS, Constants.CB_RETIRE);
        ApiResponse response = new ApiResponse();
        assertTrue(validationService.isAlreadyArchived(existingCbPlan, PLAN_ID, response));
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testIsAlreadyArchivedReturnsFalseForLivePlan() {
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.STATUS, Constants.LIVE);
        assertFalse(validationService.isAlreadyArchived(existingCbPlan, PLAN_ID, new ApiResponse()));
    }

    @Test
    void testIsAlreadyArchivedReturnsFalseWhenStatusMissing() {
        assertFalse(validationService.isAlreadyArchived(new HashMap<>(), PLAN_ID, new ApiResponse()));
    }

    @Test
    void testConstructor() {
        assertNotNull(new CbPlanValidationServiceV3Impl(accessTokenValidator, userAndOrgService, requestValidator, serverProperties));
    }
}
