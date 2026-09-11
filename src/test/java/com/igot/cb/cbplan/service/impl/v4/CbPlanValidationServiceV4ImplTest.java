package com.igot.cb.cbplan.service.impl.v4;

import com.igot.cb.cbplan.service.impl.CbPlanRequestValidatorImpl;
import com.igot.cb.cbplan.service.impl.CbPlanValidationServiceV3Impl;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;
import com.igot.cb.util.UserProfileUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanValidationServiceV4ImplTest {

    private static final String USER_ID = "user1";
    private static final String ORG_ID = "org1";
    private static final String TOKEN = "token";

    @Mock
    private CbPlanValidationServiceV3Impl validationServiceV3;

    @Mock
    private CbPlanRequestValidatorImpl cbPlanRequestValidator;

    @Mock
    private CbPlanOrgScopeServiceV4Impl orgScopeService;

    @Mock
    private UserProfileUtil userProfileUtil;

    @InjectMocks
    private CbPlanValidationServiceV4Impl validationService;

    private static Map<String, Object> requestMapWithValidContentList() {
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.IDENTIFIER, "do_1");
        content.put(Constants.MANDATORY, true);
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.CONTENT_LIST, List.of(content));
        return requestMap;
    }

    private static ApiRequest apiRequest(Map<String, Object> requestMap) {
        ApiRequest request = new ApiRequest();
        request.setRequest(requestMap);
        return request;
    }


    @Test
    void validateAndExtractUserId_delegatesToV3AndReturnsResult() {
        ApiResponse response = new ApiResponse();
        when(validationServiceV3.validateAndExtractUserId(TOKEN, response)).thenReturn(USER_ID);

        String result = validationService.validateAndExtractUserId(TOKEN, response);

        assertEquals(USER_ID, result);
    }

    @Test
    void validateOrgCCA_delegatesToV3AndReturnsResult() {
        ApiResponse response = new ApiResponse();
        when(validationServiceV3.validateOrgCCA(ORG_ID, response)).thenReturn(true);

        assertTrue(validationService.validateOrgCCA(ORG_ID, response));
    }

    @Test
    void validatePlanIdExists_delegatesToV3AndReturnsResult() {
        ApiRequest request = new ApiRequest();
        ApiResponse response = new ApiResponse();
        when(validationServiceV3.validatePlanIdExists(request, response)).thenReturn(true);

        assertTrue(validationService.validatePlanIdExists(request, response));
    }

    @Test
    void isUnauthorizedToUpdate_delegatesToV3AndReturnsResult() {
        Map<String, Object> existingCbPlan = new HashMap<>();
        ApiResponse response = new ApiResponse();
        when(validationServiceV3.isUnauthorizedToUpdate(USER_ID, existingCbPlan, List.of(), response)).thenReturn(true);

        assertTrue(validationService.isUnauthorizedToUpdate(USER_ID, existingCbPlan, List.of(), response));
    }

    @Test
    void validateAndExtractPlanId_delegatesToV3AndReturnsResult() {
        Map<String, Object> incomingRequest = new HashMap<>();
        ApiResponse response = new ApiResponse();
        when(validationServiceV3.validateAndExtractPlanId(incomingRequest, response)).thenReturn("plan1");

        assertEquals("plan1", validationService.validateAndExtractPlanId(incomingRequest, response));
    }


    @Test
    void validateUserOrganization_userProfileHasRootOrgId_returnsIt() {
        Map<String, String> profile = new HashMap<>();
        profile.put(Constants.USER_ROOT_ORG_ID, ORG_ID);
        when(userProfileUtil.buildUserProfile(eq(USER_ID), any())).thenReturn(profile);

        String result = validationService.validateUserOrganization(USER_ID, new ApiResponse());

        assertEquals(ORG_ID, result);
    }

    @Test
    void validateUserOrganization_emptyProfile_returnsNull() {
        when(userProfileUtil.buildUserProfile(eq(USER_ID), any())).thenReturn(new HashMap<>());

        assertNull(validationService.validateUserOrganization(USER_ID, new ApiResponse()));
    }


    @Test
    void validateContextDataForLivePlanV4_noErrors_returnsTrue() {
        when(orgScopeService.resolveOrgScope(anyMap(), anyBoolean(), anyString(), any(), any())).thenReturn(List.of());

        boolean result = validationService.validateContextDataForLivePlanV4(new HashMap<>(), false, ORG_ID,
                new HashSet<>(), new HashSet<>(), new ApiResponse());

        assertTrue(result);
    }

    @Test
    void validateContextDataForLivePlanV4_hasErrors_returnsFalseAndPopulatesResponse() {
        ApiResponse response = new ApiResponse();
        when(orgScopeService.resolveOrgScope(anyMap(), anyBoolean(), anyString(), any(), any()))
                .thenReturn(List.of("bad group"));

        boolean result = validationService.validateContextDataForLivePlanV4(new HashMap<>(), false, ORG_ID,
                new HashSet<>(), new HashSet<>(), response);

        assertFalse(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertTrue(response.getParams().getErr().contains("bad group"));
    }


    @Test
    void validateRequest_allValid_returnsTrue() {
        when(cbPlanRequestValidator.validateMandatoryFields(anyMap())).thenReturn(List.of());
        when(orgScopeService.resolveOrgScope(anyMap(), anyBoolean(), anyString(), any(), any())).thenReturn(List.of());

        boolean result = validationService.validateRequest(apiRequest(requestMapWithValidContentList()), false, ORG_ID, new ApiResponse());

        assertTrue(result);
    }

    @Test
    void validateRequest_contentListMissing_returnsFalse() {
        ApiResponse response = new ApiResponse();

        boolean result = validationService.validateRequest(apiRequest(new HashMap<>()), false, ORG_ID, response);

        assertFalse(result);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertTrue(response.getParams().getErr().contains(Constants.ERR_CONTENTS_REQUIRED));
    }

    @Test
    void validateRequest_mandatoryFieldsInvalid_returnsFalse() {
        when(cbPlanRequestValidator.validateMandatoryFields(anyMap())).thenReturn(List.of("name is required"));
        ApiResponse response = new ApiResponse();

        boolean result = validationService.validateRequest(apiRequest(requestMapWithValidContentList()), false, ORG_ID, response);

        assertFalse(result);
        assertTrue(response.getParams().getErr().contains("name is required"));
    }

    @Test
    void validateRequest_orgScopeResolutionFails_returnsFalse() {
        when(cbPlanRequestValidator.validateMandatoryFields(anyMap())).thenReturn(List.of());
        when(orgScopeService.resolveOrgScope(anyMap(), anyBoolean(), anyString(), any(), any()))
                .thenReturn(List.of("userGroupId not found"));
        ApiResponse response = new ApiResponse();

        boolean result = validationService.validateRequest(apiRequest(requestMapWithValidContentList()), false, ORG_ID, response);

        assertFalse(result);
        assertTrue(response.getParams().getErr().contains("userGroupId not found"));
    }

    @Test
    void validateRequest_normalizesContentListBeforeMandatoryFieldsCheck() {
        when(cbPlanRequestValidator.validateMandatoryFields(anyMap())).thenReturn(List.of());
        when(orgScopeService.resolveOrgScope(anyMap(), anyBoolean(), anyString(), any(), any())).thenReturn(List.of());
        Map<String, Object> requestMap = requestMapWithValidContentList();
        ApiRequest request = apiRequest(requestMap);

        validationService.validateRequest(request, false, ORG_ID, new ApiResponse());

        assertEquals(List.of("do_1"), requestMap.get(Constants.CONTENT_LIST));
        verify(cbPlanRequestValidator).validateMandatoryFields(requestMap);
    }


    @Test
    void validateAndNormalizeContentList_keyMissing_returnsContentsRequiredError() {
        List<String> errors = validationService.validateAndNormalizeContentList(new HashMap<>());

        assertEquals(List.of(Constants.ERR_CONTENTS_REQUIRED), errors);
    }

    @Test
    void validateAndNormalizeContentList_notAList_returnsContentsRequiredError() {
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.CONTENT_LIST, "not-a-list");

        List<String> errors = validationService.validateAndNormalizeContentList(requestMap);

        assertEquals(List.of(Constants.ERR_CONTENTS_REQUIRED), errors);
    }

    @Test
    void validateAndNormalizeContentList_emptyList_returnsContentsEmptyError() {
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.CONTENT_LIST, List.of());

        List<String> errors = validationService.validateAndNormalizeContentList(requestMap);

        assertEquals(List.of(Constants.ERR_CONTENTS_EMPTY), errors);
    }

    @Test
    void validateAndNormalizeContentList_validEntries_normalizesToFlatIdListWithNoErrors() {
        Map<String, Object> content1 = new HashMap<>();
        content1.put(Constants.IDENTIFIER, "do_1");
        content1.put(Constants.MANDATORY, true);
        Map<String, Object> content2 = new HashMap<>();
        content2.put(Constants.IDENTIFIER, "do_2");
        content2.put(Constants.MANDATORY, false);
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.CONTENT_LIST, List.of(content1, content2));

        List<String> errors = validationService.validateAndNormalizeContentList(requestMap);

        assertTrue(errors.isEmpty());
        assertEquals(List.of("do_1", "do_2"), requestMap.get(Constants.CONTENT_LIST));
    }

    @Test
    void validateAndNormalizeContentList_missingId_returnsErrorButStillNormalizes() {
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.MANDATORY, true);
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.CONTENT_LIST, List.of(content));

        List<String> errors = validationService.validateAndNormalizeContentList(requestMap);

        assertTrue(errors.stream().anyMatch(e -> e.contains(Constants.ERR_CONTENT_ID_REQUIRED)));
        assertEquals(Collections.singletonList(null), requestMap.get(Constants.CONTENT_LIST));
    }

    @Test
    void validateAndNormalizeContentList_blankId_returnsError() {
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.IDENTIFIER, "  ");
        content.put(Constants.MANDATORY, true);
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.CONTENT_LIST, List.of(content));

        List<String> errors = validationService.validateAndNormalizeContentList(requestMap);

        assertTrue(errors.stream().anyMatch(e -> e.contains(Constants.ERR_CONTENT_ID_REQUIRED)));
    }

    @Test
    void validateAndNormalizeContentList_missingMandatory_returnsError() {
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.IDENTIFIER, "do_1");
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.CONTENT_LIST, List.of(content));

        List<String> errors = validationService.validateAndNormalizeContentList(requestMap);

        assertTrue(errors.stream().anyMatch(e -> e.contains(Constants.ERR_CONTENT_MANDATORY_REQUIRED)));
    }

    @Test
    void validateAndNormalizeContentList_invalidMandatoryValue_returnsError() {
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.IDENTIFIER, "do_1");
        content.put(Constants.MANDATORY, "yes");
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.CONTENT_LIST, List.of(content));

        List<String> errors = validationService.validateAndNormalizeContentList(requestMap);

        assertTrue(errors.stream().anyMatch(e -> e.contains(Constants.ERR_CONTENT_MANDATORY_INVALID)));
    }

    @Test
    void validateAndNormalizeContentList_emptyEntryObject_returnsError() {
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.CONTENT_LIST, List.of(new HashMap<>()));

        List<String> errors = validationService.validateAndNormalizeContentList(requestMap);

        assertFalse(errors.isEmpty());
    }
}
