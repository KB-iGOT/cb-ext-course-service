package com.igot.cb.cbplan.service.impl;

import com.igot.cb.model.ApiRequest;
import com.igot.cb.service.UserAndOrgServiceImpl;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CbPlanRequestValidatorImplTest {

    private static final String ORG_ID = "org1";
    private static final String ORG_ID_2 = "org2";
    private static final String MINISTRY_ORG = "MIN_001";
    private static final String LEVEL_ZERO = "levelZero";
    private static final String ALL_ORG_MSG = "All org restriction message";

    @Mock
    private UserAndOrgServiceImpl userAndOrgService;

    @Mock
    private CbExtServerProperties serverProperties;

    private CbPlanRequestValidatorImpl validator;

    @BeforeEach
    void setUp() {
        validator = new CbPlanRequestValidatorImpl(userAndOrgService, serverProperties);
        lenient().when(serverProperties.getMsgOnUserGroupRestrictionForAllOrg()).thenReturn(ALL_ORG_MSG);
    }

    @Test
    void testValidateMandatoryFieldsWithAllFieldsPresent() {
        Map<String, Object> request = buildValidRequest();
        List<String> errors = validator.validateMandatoryFields(request);
        if (!errors.isEmpty()) {
            System.out.println("Validation errors: " + errors);
        }
        assertTrue(errors.isEmpty(), "Expected no errors but got: " + errors);
    }

    @Test
    void testValidateMandatoryFieldsWithMissingName() {
        Map<String, Object> request = buildValidRequest();
        request.remove(Constants.NAME);
        List<String> errors = validator.validateMandatoryFields(request);
        assertFalse(errors.isEmpty());
        assertTrue(errors.get(0).contains("name"));
    }

    @Test
    void testValidateMandatoryFieldsSetsDefaultIsApar() {
        Map<String, Object> request = buildValidRequest();
        request.remove(Constants.IS_APAR);
        validator.validateMandatoryFields(request);
        assertEquals(false, request.get(Constants.IS_APAR));
    }

    @Test
    void testValidateCbPlanCreateRequestWithMandatoryFieldErrors() {
        ApiRequest apiRequest = new ApiRequest();
        apiRequest.setRequest(new HashMap<>());

        List<String> errors = validator.validateCbPlanCreateRequest(apiRequest, false, ORG_ID, false);
        assertFalse(errors.isEmpty());
    }

    @Test
    void testValidateContextDataMissingContextData() {
        Map<String, Object> request = buildValidRequest();
        request.remove(Constants.CONTEXT_DATA_REQUEST);
        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertEquals(1, errors.size());
        assertEquals(Constants.ERR_CONTEXT_DATA_MISSING, errors.get(0));
    }

    @Test
    void testValidateContextDataWithInvalidJsonString() {
        Map<String, Object> request = buildValidRequest();
        request.put(Constants.CONTEXT_DATA_REQUEST, "{invalid-json");

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertEquals(1, errors.size());
        assertEquals(Constants.ERR_CONTEXT_DATA_UNPARSEABLE, errors.get(0));
    }

    @Test
    void testValidateContextDataWithInvalidType() {
        Map<String, Object> request = buildValidRequest();
        request.put(Constants.CONTEXT_DATA_REQUEST, 12345);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertEquals(1, errors.size());
        assertEquals(Constants.ERR_CONTEXT_DATA_INVALID_TYPE, errors.get(0));
    }

    @Test
    void testValidateContextDataWithMapContextData() {
        Map<String, Object> request = buildValidRequest();
        Map<String, Object> contextData = new HashMap<>();
        request.put(Constants.CONTEXT_DATA_REQUEST, contextData);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertFalse(errors.isEmpty());
        assertEquals(Constants.ERR_ACCESS_CONTROL_MISSING, errors.get(0));
    }

    @Test
    void testValidateContextDataWithEmptyAccessControl() {
        Map<String, Object> request = buildValidRequest();
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, new HashMap<>());
        request.put(Constants.CONTEXT_DATA_REQUEST, contextData);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertEquals(1, errors.size());
        assertEquals(Constants.ERR_ACCESS_CONTROL_MISSING, errors.get(0));
    }

    @Test
    void testValidateContextDataWithEmptyUserGroups() {
        Map<String, Object> request = buildValidRequest();
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, new ArrayList<>());
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        request.put(Constants.CONTEXT_DATA_REQUEST, contextData);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertEquals(1, errors.size());
        assertEquals(Constants.ERR_USER_GROUPS_MISSING, errors.get(0));
    }

    @Test
    void testValidateContextDataWithMissingCriteriaList() {
        Map<String, Object> request = buildRequestWithUserGroupNoCriteria();
        mockOrgAsNonL0(ORG_ID);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertFalse(errors.isEmpty());
        assertEquals(Constants.ERR_CRITERIA_LIST_MISSING, errors.get(0));
    }

    @Test
    void testValidateContextDataWithEmptyCriteriaList() {
        Map<String, Object> request = buildRequestWithEmptyCriteria();
        mockOrgAsNonL0(ORG_ID);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertFalse(errors.isEmpty());
        assertEquals(Constants.ERR_CRITERIA_LIST_EMPTY, errors.get(0));
    }

    @Test
    void testValidateContextDataWithMissingCriteriaKey() {
        Map<String, Object> request = buildRequestWithCriteriaMissingKey();
        mockOrgAsNonL0(ORG_ID);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertFalse(errors.isEmpty());
        assertEquals(Constants.ERR_CRITERIA_KEY_MISSING, errors.get(0));
    }

    @Test
    void testValidateContextDataWithMissingCriteriaValue() {
        Map<String, Object> request = buildRequestWithCriteriaMissingValue();
        mockOrgAsNonL0(ORG_ID);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertFalse(errors.isEmpty());
        assertTrue(errors.get(0).contains("criteriaValue is missing"));
    }

    @Test
    void testValidateContextDataWithUnsupportedCriteriaType() {
        Map<String, Object> request = buildRequestWithUnsupportedCriteriaValue();
        mockOrgAsNonL0(ORG_ID);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertFalse(errors.isEmpty());
        assertTrue(errors.get(0).contains("Unsupported criteriaValue type"));
    }

    @Test
    void testValidateContextDataNonCCAWithRootOrgId() {
        Map<String, Object> request = buildRequestWithRootOrgId(ORG_ID);
        mockOrgAsNonL0(ORG_ID);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertTrue(errors.isEmpty());
        assertEquals(Constants.SINGLE, request.get(Constants.ORG_SCOPE));
    }

    @Test
    void testValidateContextDataNonCCAWithMultipleRootOrgIds() {
        Map<String, Object> request = buildRequestWithMultipleRootOrgIds();
        mockOrgAsNonL0(ORG_ID);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertEquals(1, errors.size());
        assertEquals(Constants.ERR_MULTIPLE_ROOT_ORG_IDS, errors.get(0));
    }

    @Test
    void testValidateContextDataNonCCAWithNoRootOrgId() {
        Map<String, Object> request = buildRequestWithOtherCriteria();
        mockOrgAsNonL0(ORG_ID);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertEquals(1, errors.size());
        assertEquals(Constants.ERR_ROOT_ORG_CRITERIA_MISSING, errors.get(0));
    }

    @Test
    void testValidateContextDataNonCCAWithOrgMismatch() {
        Map<String, Object> request = buildRequestWithRootOrgId(ORG_ID_2);
        mockOrgAsNonL0(ORG_ID);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertEquals(1, errors.size());
        assertEquals(Constants.ERR_ROOT_ORG_ID_MISMATCH, errors.get(0));
    }

    @Test
    void testValidateContextDataNonCCAWithAdminBypass() {
        Map<String, Object> request = buildRequestWithRootOrgId(ORG_ID_2);
        mockOrgAsNonL0(ORG_ID);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, true);
        assertTrue(errors.isEmpty());
    }

    @Test
    void testValidateContextDataCCAWithNoRootOrgId() {
        Map<String, Object> request = buildRequestWithOtherCriteria();
        mockOrgAsNonL0(ORG_ID);

        List<String> errors = validator.validateContextData(request, true, ORG_ID, null, false);
        assertTrue(errors.isEmpty());
        assertEquals(Constants.ALL, request.get(Constants.ORG_SCOPE));
    }

    @Test
    void testValidateContextDataCCAWithSingleRootOrgId() {
        Map<String, Object> request = buildRequestWithRootOrgId(ORG_ID);
        mockOrgAsNonL0(ORG_ID);

        List<String> errors = validator.validateContextData(request, true, ORG_ID, null, false);
        assertTrue(errors.isEmpty());
        assertEquals(Constants.SINGLE, request.get(Constants.ORG_SCOPE));
    }

    @Test
    void testValidateContextDataCCAWithMultipleRootOrgIds() {
        Map<String, Object> request = buildRequestWithMultipleRootOrgIds();
        mockOrgAsNonL0(ORG_ID);

        List<String> errors = validator.validateContextData(request, true, ORG_ID, null, false);
        assertTrue(errors.isEmpty());
        assertEquals(Constants.CUSTOM, request.get(Constants.ORG_SCOPE));
    }

    @Test
    void testValidateContextDataCCAWithMixedUserGroups() {
        Map<String, Object> request = buildRequestWithMixedUserGroups();
        mockOrgAsNonL0(ORG_ID);

        List<String> errors = validator.validateContextData(request, true, ORG_ID, null, false);
        assertEquals(1, errors.size());
        assertEquals(ALL_ORG_MSG, errors.get(0));
    }

    @Test
    void testValidateContextDataL0WithSingleRootOrgId() {
        Map<String, Object> request = buildRequestWithRootOrgId(ORG_ID);
        mockOrgAsL0(ORG_ID);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertTrue(errors.isEmpty());
        assertEquals(Constants.SINGLE, request.get(Constants.ORG_SCOPE));
    }

    @Test
    void testValidateContextDataL0WithMultipleRootOrgIds() {
        Map<String, Object> request = buildRequestWithMultipleRootOrgIds();
        mockOrgAsL0(ORG_ID);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertTrue(errors.isEmpty());
        assertEquals(Constants.CUSTOM, request.get(Constants.ORG_SCOPE));
    }

    @Test
    void testValidateContextDataL0WithNoRootOrgId() {
        Map<String, Object> request = buildRequestWithOtherCriteria();
        mockOrgAsL0(ORG_ID);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertEquals(1, errors.size());
        assertEquals(Constants.ERR_ROOT_ORG_CRITERIA_MISSING, errors.get(0));
    }

    @Test
    void testValidateContextDataL0WithBothCriteriaTypes() {
        Map<String, Object> request = buildRequestWithBothRootOrgAndMinistry();
        mockOrgAsL0(ORG_ID);
        mockOrgAsL0(MINISTRY_ORG);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertEquals(1, errors.size());
        assertEquals(Constants.ERR_BOTH_ROOT_ORG_AND_MINISTRY_USED, errors.get(0));
    }

    @Test
    void testValidateContextDataWithMinistryOrgIdAsL0() {
        Map<String, Object> request = buildRequestWithMinistryOrgId(MINISTRY_ORG);
        mockOrgAsL0(ORG_ID);
        mockOrgAsL0(MINISTRY_ORG);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertTrue(errors.isEmpty());
        assertEquals(Constants.ALL, request.get(Constants.ORG_SCOPE));
    }

    @Test
    void testValidateContextDataWithMinistryOrgIdNotL0() {
        Map<String, Object> request = buildRequestWithMinistryOrgId(MINISTRY_ORG);
        mockOrgAsL0(ORG_ID);
        mockOrgAsNonL0(MINISTRY_ORG);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertFalse(errors.isEmpty());
        assertTrue(errors.get(0).contains("not a Level 0"));
    }

    @Test
    void testValidateContextDataWithMinistryOrgIdNotFound() {
        Map<String, Object> request = buildRequestWithMinistryOrgId(MINISTRY_ORG);
        mockOrgAsL0(ORG_ID);
        lenient().when(userAndOrgService.readOrgFromDB(eq(MINISTRY_ORG), anyList())).thenReturn(Collections.emptyMap());

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertFalse(errors.isEmpty());
        assertTrue(errors.get(0).contains("not found for L0 validation"));
    }

    @Test
    void testValidateContextDataWithEmptyMinistryList() {
        Map<String, Object> request = buildRequestWithMinistryOrgId("");
        mockOrgAsL0(ORG_ID);

        Map<String, Object> userGroup = getFirstUserGroup(request);
        List<Map<String, Object>> criteria = (List<Map<String, Object>>) userGroup.get(Constants.USER_GROUP_CRITERIA_LIST);
        criteria.get(0).put(Constants.CRITERIA_VALUE, Collections.emptyList());

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertTrue(errors.isEmpty());
    }

    @Test
    void testValidateContextDataPopulatesRootOrgIdsSet() {
        Map<String, Object> request = buildRequestWithRootOrgId(ORG_ID);
        Set<String> rootOrgIds = new HashSet<>();
        mockOrgAsNonL0(ORG_ID);

        validator.validateContextData(request, false, ORG_ID, rootOrgIds, false);
        assertTrue(rootOrgIds.contains(ORG_ID));
    }

    @Test
    void testValidateContextDataWithTargetedOrganisationCriteria() {
        Map<String, Object> request = buildRequestWithCriteria(Constants.TARGETED_ORGANISATION, ORG_ID);
        mockOrgAsNonL0(ORG_ID);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertTrue(errors.isEmpty());
        assertEquals(Constants.SINGLE, request.get(Constants.ORG_SCOPE));
    }

    @Test
    void testValidateContextDataWithBooleanCriteriaValue() {
        Map<String, Object> request = buildRequestWithCriteria(Constants.ROOT_ORG_ID, true);
        mockOrgAsNonL0(ORG_ID);

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertFalse(errors.isEmpty());
    }

    @Test
    void testValidateContextDataWithOrgNotFound() {
        Map<String, Object> request = buildRequestWithRootOrgId(ORG_ID);
        lenient().when(userAndOrgService.readOrgFromDB(eq(ORG_ID), anyList())).thenReturn(Collections.emptyMap());

        List<String> errors = validator.validateContextData(request, false, ORG_ID, null, false);
        assertTrue(errors.isEmpty());
    }

    private Map<String, Object> buildValidRequest() {
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.NAME, "Test CB Plan");
        request.put(Constants.DESCRIPTION, "Test Description");
        request.put(Constants.TYPE, "Mandatory");
        request.put(Constants.PLAN_YEAR, "2026");
        request.put(Constants.IS_APAR, false);
        request.put(Constants.CONTENT_TYPE, "Course");
        request.put(Constants.CONTENT_LIST, Arrays.asList("do_123"));
        request.put(Constants.END_DATE_REQUEST, new Date());

        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        request.put(Constants.CONTEXT_DATA_REQUEST, contextData);
        return request;
    }

    private Map<String, Object> buildRequestWithRootOrgId(String orgId) {
        return buildRequestWithCriteria(Constants.ROOT_ORG_ID, orgId);
    }

    private Map<String, Object> buildRequestWithMinistryOrgId(String orgId) {
        return buildRequestWithCriteria(Constants.MINISTRY_OR_STATEID, orgId);
    }

    private Map<String, Object> buildRequestWithCriteria(String criteriaKey, Object criteriaValue) {
        Map<String, Object> request = buildValidRequest();
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        Map<String, Object> criteria = new HashMap<>();

        criteria.put(Constants.CRITERIA_KEY, criteriaKey);
        criteria.put(Constants.CRITERIA_VALUE, criteriaValue instanceof String ?
                List.of((String) criteriaValue) : criteriaValue);
        criteriaList.add(criteria);
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        userGroups.add(userGroup);
        accessControl.put(Constants.USER_GROUPS, userGroups);
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        request.put(Constants.CONTEXT_DATA_REQUEST, contextData);

        return request;
    }

    private Map<String, Object> buildRequestWithMultipleRootOrgIds() {
        Map<String, Object> request = buildValidRequest();
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        Map<String, Object> criteria = new HashMap<>();

        criteria.put(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID);
        criteria.put(Constants.CRITERIA_VALUE, Arrays.asList(ORG_ID, ORG_ID_2));
        criteriaList.add(criteria);
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        userGroups.add(userGroup);
        accessControl.put(Constants.USER_GROUPS, userGroups);
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        request.put(Constants.CONTEXT_DATA_REQUEST, contextData);

        return request;
    }

    private Map<String, Object> buildRequestWithOtherCriteria() {
        return buildRequestWithCriteria("otherCriteria", "someValue");
    }

    private Map<String, Object> buildRequestWithUserGroupNoCriteria() {
        Map<String, Object> request = buildValidRequest();
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();

        userGroups.add(userGroup);
        accessControl.put(Constants.USER_GROUPS, userGroups);
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        request.put(Constants.CONTEXT_DATA_REQUEST, contextData);

        return request;
    }

    private Map<String, Object> buildRequestWithEmptyCriteria() {
        Map<String, Object> request = buildValidRequest();
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();

        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, new ArrayList<>());
        userGroups.add(userGroup);
        accessControl.put(Constants.USER_GROUPS, userGroups);
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        request.put(Constants.CONTEXT_DATA_REQUEST, contextData);

        return request;
    }

    private Map<String, Object> buildRequestWithCriteriaMissingKey() {
        Map<String, Object> request = buildValidRequest();
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        Map<String, Object> criteria = new HashMap<>();

        criteria.put(Constants.CRITERIA_VALUE, List.of(ORG_ID));
        criteriaList.add(criteria);
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        userGroups.add(userGroup);
        accessControl.put(Constants.USER_GROUPS, userGroups);
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        request.put(Constants.CONTEXT_DATA_REQUEST, contextData);

        return request;
    }

    private Map<String, Object> buildRequestWithCriteriaMissingValue() {
        Map<String, Object> request = buildValidRequest();
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        Map<String, Object> criteria = new HashMap<>();

        criteria.put(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID);
        criteriaList.add(criteria);
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        userGroups.add(userGroup);
        accessControl.put(Constants.USER_GROUPS, userGroups);
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        request.put(Constants.CONTEXT_DATA_REQUEST, contextData);

        return request;
    }

    private Map<String, Object> buildRequestWithUnsupportedCriteriaValue() {
        Map<String, Object> request = buildValidRequest();
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        Map<String, Object> criteria = new HashMap<>();

        criteria.put(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID);
        criteria.put(Constants.CRITERIA_VALUE, new HashMap<>());
        criteriaList.add(criteria);
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        userGroups.add(userGroup);
        accessControl.put(Constants.USER_GROUPS, userGroups);
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        request.put(Constants.CONTEXT_DATA_REQUEST, contextData);

        return request;
    }

    private Map<String, Object> buildRequestWithMixedUserGroups() {
        Map<String, Object> request = buildValidRequest();
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();

        Map<String, Object> userGroup1 = new HashMap<>();
        List<Map<String, Object>> criteriaList1 = new ArrayList<>();
        Map<String, Object> criteria1 = new HashMap<>();
        criteria1.put(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID);
        criteria1.put(Constants.CRITERIA_VALUE, List.of(ORG_ID));
        criteriaList1.add(criteria1);
        userGroup1.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList1);

        Map<String, Object> userGroup2 = new HashMap<>();
        List<Map<String, Object>> criteriaList2 = new ArrayList<>();
        Map<String, Object> criteria2 = new HashMap<>();
        criteria2.put(Constants.CRITERIA_KEY, "otherCriteria");
        criteria2.put(Constants.CRITERIA_VALUE, List.of("value"));
        criteriaList2.add(criteria2);
        userGroup2.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList2);

        userGroups.add(userGroup1);
        userGroups.add(userGroup2);
        accessControl.put(Constants.USER_GROUPS, userGroups);
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        request.put(Constants.CONTEXT_DATA_REQUEST, contextData);

        return request;
    }

    private Map<String, Object> buildRequestWithBothRootOrgAndMinistry() {
        Map<String, Object> request = buildValidRequest();
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();

        Map<String, Object> userGroup1 = new HashMap<>();
        List<Map<String, Object>> criteriaList1 = new ArrayList<>();
        Map<String, Object> criteria1 = new HashMap<>();
        criteria1.put(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID);
        criteria1.put(Constants.CRITERIA_VALUE, List.of(ORG_ID));
        criteriaList1.add(criteria1);
        userGroup1.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList1);

        Map<String, Object> userGroup2 = new HashMap<>();
        List<Map<String, Object>> criteriaList2 = new ArrayList<>();
        Map<String, Object> criteria2 = new HashMap<>();
        criteria2.put(Constants.CRITERIA_KEY, Constants.MINISTRY_OR_STATEID);
        criteria2.put(Constants.CRITERIA_VALUE, List.of(MINISTRY_ORG));
        criteriaList2.add(criteria2);
        userGroup2.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList2);

        userGroups.add(userGroup1);
        userGroups.add(userGroup2);
        accessControl.put(Constants.USER_GROUPS, userGroups);
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        request.put(Constants.CONTEXT_DATA_REQUEST, contextData);

        return request;
    }

    private Map<String, Object> getFirstUserGroup(Map<String, Object> request) {
        Map<String, Object> contextData = (Map<String, Object>) request.get(Constants.CONTEXT_DATA_REQUEST);
        Map<String, Object> accessControl = (Map<String, Object>) contextData.get(Constants.ACCESS_CONTROL);
        List<Map<String, Object>> userGroups = (List<Map<String, Object>>) accessControl.get(Constants.USER_GROUPS);
        return userGroups.get(0);
    }

    private void mockOrgAsL0(String orgId) {
        Map<String, Object> orgMap = new HashMap<>();
        orgMap.put(Constants.ID, orgId);
        orgMap.put(Constants.MINISTRY_OR_STATETYPE, Constants.SPV);
        lenient().when(userAndOrgService.readOrgFromDB(eq(orgId), anyList())).thenReturn(orgMap);
    }

    private void mockOrgAsNonL0(String orgId) {
        Map<String, Object> orgMap = new HashMap<>();
        orgMap.put(Constants.ID, orgId);
        orgMap.put(Constants.MINISTRY_OR_STATETYPE, "OTHER");
        lenient().when(userAndOrgService.readOrgFromDB(eq(orgId), anyList())).thenReturn(orgMap);
    }
}
