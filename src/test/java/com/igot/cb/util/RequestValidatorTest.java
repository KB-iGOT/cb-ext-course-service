package com.igot.cb.util;

import com.igot.cb.model.ApiRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class RequestValidatorTest {
    private RequestValidator requestValidator;
    private CbExtServerProperties cbExtServerProperties;

    @BeforeEach
    void setUp() {
        cbExtServerProperties = Mockito.mock(CbExtServerProperties.class);
        Mockito.when(cbExtServerProperties.getMsgOnUserGroupRestrictionForAllOrg())
                .thenReturn("User group restriction for all org");
        requestValidator = new RequestValidator(cbExtServerProperties);
    }

    @Test
    void testValidateCbPlanCreateRequest_missingContextData() {
        ApiRequest apiRequest = new ApiRequest();
        Map<String, Object> req = new HashMap<>();
        apiRequest.setRequest(req);
        List<String> errors = requestValidator.validateCbPlanCreateRequest(apiRequest, false, "org1");
        assertFalse(errors.stream().anyMatch(e -> e.contains("contextData is missing")));
    }

    @Test
    void testValidateContextData_invalidType() {
        Map<String, Object> req = new HashMap<>();
        req.put(Constants.CONTEXT_DATA_REQUEST, 123); // invalid type
        List<String> errors = requestValidator.validateContextData(req, false, "org1");
        assertTrue(errors.stream().anyMatch(e -> e.contains("contextData is of invalid type")));
    }

    @Test
    void testValidateContextData_missingAccessControl() {
        Map<String, Object> req = new HashMap<>();
        req.put(Constants.CONTEXT_DATA_REQUEST, new HashMap<>());
        List<String> errors = requestValidator.validateContextData(req, false, "org1");
        assertTrue(errors.stream().anyMatch(e -> e.contains("accessControl is missing")));
    }

    @Test
    void testValidateContextData_missingUserGroups() {
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, new HashMap<>());
        Map<String, Object> req = new HashMap<>();
        req.put(Constants.CONTEXT_DATA_REQUEST, contextData);
        List<String> errors = requestValidator.validateContextData(req, false, "org1");
        assertTrue(errors.stream().anyMatch(e -> e.contains("User groups are missing")));
    }

    @Test
    void testValidateContextData_validSingleRootOrgId() {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID);
        criteria.put(Constants.CRITERIA_VALUE, "org1");
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        criteriaList.add(criteria);
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        List<Map<String, Object>> userGroups = new ArrayList<>();
        userGroups.add(userGroup);
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, userGroups);
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        Map<String, Object> req = new HashMap<>();
        req.put(Constants.CONTEXT_DATA_REQUEST, contextData);
        List<String> errors = requestValidator.validateContextData(req, false, "org1");
        assertTrue(errors.isEmpty());
        assertEquals(Arrays.asList("org1"), req.get(Constants.ORG_ID_LIST));
        assertEquals(Constants.SINGLE, req.get(Constants.ORG_SCOPE));
    }

    @Test
    void testValidateContextData_multipleRootOrgIdsNotCCA() {
        Map<String, Object> criteria1 = new HashMap<>();
        criteria1.put(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID);
        criteria1.put(Constants.CRITERIA_VALUE, "org1");
        Map<String, Object> criteria2 = new HashMap<>();
        criteria2.put(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID);
        criteria2.put(Constants.CRITERIA_VALUE, "org2");
        List<Map<String, Object>> criteriaList = Arrays.asList(criteria1, criteria2);
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        List<Map<String, Object>> userGroups = new ArrayList<>();
        userGroups.add(userGroup);
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, userGroups);
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        Map<String, Object> req = new HashMap<>();
        req.put(Constants.CONTEXT_DATA_REQUEST, contextData);
        List<String> errors = requestValidator.validateContextData(req, false, "org1");
        assertTrue(errors.stream().anyMatch(e -> e.contains("Multiple ROOT_ORG_IDs found")));
    }

    @Test
    void testValidateContextData_rootOrgIdMismatch() {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID);
        criteria.put(Constants.CRITERIA_VALUE, "org2");
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        criteriaList.add(criteria);
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        List<Map<String, Object>> userGroups = new ArrayList<>();
        userGroups.add(userGroup);
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, userGroups);
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        Map<String, Object> req = new HashMap<>();
        req.put(Constants.CONTEXT_DATA_REQUEST, contextData);
        List<String> errors = requestValidator.validateContextData(req, false, "org1");
        assertTrue(errors.stream().anyMatch(e -> e.contains("does not match logged-in user's orgId")));
    }

    @Test
    void testValidateContextData_CCA_allOrg() {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, "otherKey");
        criteria.put(Constants.CRITERIA_VALUE, "value");
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        criteriaList.add(criteria);
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        List<Map<String, Object>> userGroups = new ArrayList<>();
        userGroups.add(userGroup);
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, userGroups);
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        Map<String, Object> req = new HashMap<>();
        req.put(Constants.CONTEXT_DATA_REQUEST, contextData);
        List<String> errors = requestValidator.validateContextData(req, true, "org1");
        assertTrue(errors.isEmpty());
        assertEquals(Constants.ALL, req.get(Constants.ORG_SCOPE));
    }

    @Test
    void testValidateCbPlanRequest_constraintViolation() {
        Map<String, Object> req = new HashMap<>();
        req.put("planName", ""); // Assuming planName is @NotBlank in CbPlanDto
        List<String> errors = requestValidator.validateCbPlanRequest(req);
        assertTrue(errors.stream().anyMatch(e -> e.contains("Validation Error:")));
    }

    @Test
    void testValidateContextData_contextDataStringParseFail() {
        Map<String, Object> req = new HashMap<>();
        req.put(Constants.CONTEXT_DATA_REQUEST, "not a json");
        List<String> errors = requestValidator.validateContextData(req, false, "org1");
        assertTrue(errors.stream().anyMatch(e -> e.contains("Failed to parse contextData")));
    }

    @Test
    void testValidateContextData_criteriaValueBoolean() {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID);
        criteria.put(Constants.CRITERIA_VALUE, true);
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        criteriaList.add(criteria);
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        List<Map<String, Object>> userGroups = new ArrayList<>();
        userGroups.add(userGroup);
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, userGroups);
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        Map<String, Object> req = new HashMap<>();
        req.put(Constants.CONTEXT_DATA_REQUEST, contextData);
        List<String> errors = requestValidator.validateContextData(req, false, "true");
        assertTrue(errors.isEmpty());
        assertEquals(Arrays.asList("true"), req.get(Constants.ORG_ID_LIST));
    }

    @Test
    void testValidateContextData_criteriaValueUnsupportedType() {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID);
        criteria.put(Constants.CRITERIA_VALUE, 123.45); // Double type
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        criteriaList.add(criteria);
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        List<Map<String, Object>> userGroups = new ArrayList<>();
        userGroups.add(userGroup);
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, userGroups);
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        Map<String, Object> req = new HashMap<>();
        req.put(Constants.CONTEXT_DATA_REQUEST, contextData);
        List<String> errors = requestValidator.validateContextData(req, false, "org1");
        assertTrue(errors.stream().anyMatch(e -> e.contains("Unsupported criteriaValue type")));
    }

    @Test
    void testValidateContextData_criteriaKeyMissingOrEmpty() {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_VALUE, "org1");
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        criteriaList.add(criteria);
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        List<Map<String, Object>> userGroups = new ArrayList<>();
        userGroups.add(userGroup);
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, userGroups);
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        Map<String, Object> req = new HashMap<>();
        req.put(Constants.CONTEXT_DATA_REQUEST, contextData);
        List<String> errors = requestValidator.validateContextData(req, false, "org1");
        assertTrue(errors.stream().anyMatch(e -> e.contains("criteriaKey is missing")));
    }

    @Test
    void testValidateContextData_criteriaValueMissing() {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID);
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        criteriaList.add(criteria);
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        List<Map<String, Object>> userGroups = new ArrayList<>();
        userGroups.add(userGroup);
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, userGroups);
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        Map<String, Object> req = new HashMap<>();
        req.put(Constants.CONTEXT_DATA_REQUEST, contextData);
        List<String> errors = requestValidator.validateContextData(req, false, "org1");
        assertTrue(errors.stream().anyMatch(e -> e.contains("criteriaValue is missing")));
    }

    @Test
    void testValidateContextData_criteriaListMissingOrEmpty() {
        Map<String, Object> userGroup = new HashMap<>();
        // Missing criteriaList
        List<Map<String, Object>> userGroups = new ArrayList<>();
        userGroups.add(userGroup);
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, userGroups);
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        Map<String, Object> req = new HashMap<>();
        req.put(Constants.CONTEXT_DATA_REQUEST, contextData);
        List<String> errors = requestValidator.validateContextData(req, false, "org1");
        assertTrue(errors.stream().anyMatch(e -> e.contains("criteriaList is missing")));

        // Empty criteriaList
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, new ArrayList<>());
        errors = requestValidator.validateContextData(req, false, "org1");
        assertTrue(errors.stream().anyMatch(e -> e.contains("criteriaList is empty")));
    }

    @Test
    void testValidateContextData_multipleUserGroupsSomeMissingRootOrgId() {
        Map<String, Object> criteria1 = new HashMap<>();
        criteria1.put(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID);
        criteria1.put(Constants.CRITERIA_VALUE, "org1");
        List<Map<String, Object>> criteriaList1 = new ArrayList<>();
        criteriaList1.add(criteria1);
        Map<String, Object> userGroup1 = new HashMap<>();
        userGroup1.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList1);

        Map<String, Object> criteria2 = new HashMap<>();
        criteria2.put(Constants.CRITERIA_KEY, "otherKey");
        criteria2.put(Constants.CRITERIA_VALUE, "value");
        List<Map<String, Object>> criteriaList2 = new ArrayList<>();
        criteriaList2.add(criteria2);
        Map<String, Object> userGroup2 = new HashMap<>();
        userGroup2.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList2);

        List<Map<String, Object>> userGroups = Arrays.asList(userGroup1, userGroup2);
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, userGroups);
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        Map<String, Object> req = new HashMap<>();
        req.put(Constants.CONTEXT_DATA_REQUEST, contextData);
        List<String> errors = requestValidator.validateContextData(req, false, "org1");
        assertTrue(errors.stream().anyMatch(e -> e.contains("ROOT_ORG_ID criteria is missing")));
    }

    @Test
    void testValidateContextData_CCA_rootOrgCriteriaNotFoundInUserGroup() {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, "otherKey");
        criteria.put(Constants.CRITERIA_VALUE, "value");
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        criteriaList.add(criteria);
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        List<Map<String, Object>> userGroups = new ArrayList<>();
        userGroups.add(userGroup);
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, userGroups);
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        Map<String, Object> req = new HashMap<>();
        req.put(Constants.CONTEXT_DATA_REQUEST, contextData);
        List<String> errors = requestValidator.validateContextData(req, true, "org1");
        assertFalse(errors.stream().anyMatch(e -> e.contains("User group restriction for all org")));
    }

    @Test
    void testValidateContextData_noRootOrgIdNotCCA() {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, "otherKey");
        criteria.put(Constants.CRITERIA_VALUE, "value");
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        criteriaList.add(criteria);
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        List<Map<String, Object>> userGroups = new ArrayList<>();
        userGroups.add(userGroup);
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, userGroups);
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        Map<String, Object> req = new HashMap<>();
        req.put(Constants.CONTEXT_DATA_REQUEST, contextData);
        List<String> errors = requestValidator.validateContextData(req, false, "org1");
        assertFalse(errors.stream().anyMatch(e -> e.contains("No ROOT_ORG_ID found in criteria")));
    }
}
