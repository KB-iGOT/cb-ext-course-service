package com.igot.cb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.CbPlanCacheMgr;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CbPlanLearnerServiceImplTest {

    @Mock(lenient = true)
    private AccessTokenValidator accessTokenValidator;

    @Mock(lenient = true)
    private CassandraOperation cassandraOperation;

    @Mock(lenient = true)
    private ContentInfoServiceImpl contentService;

    @Mock(lenient = true)
    private CbPlanCacheMgr cbPlanCacheMgr;

    @Mock(lenient = true)
    private RedisCacheMgr redisCacheMgr;


    private CbPlanLearnerServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new CbPlanLearnerServiceImpl(accessTokenValidator, cassandraOperation, cbPlanCacheMgr);
        ReflectionTestUtils.setField(service, "contentService", contentService);
        ReflectionTestUtils.setField(service, "redisCacheMgr", redisCacheMgr); // 👈 Add this line
    }


    @Test
    void testGetCBPlanListForUser_Success() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user123");

        Map<String, Object> userData = createUserData();
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER), any(), any(), any()))
                .thenReturn(Arrays.asList(userData));

        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ALL_ORG), any(), any(), any()))
                .thenReturn(new ArrayList<>());

        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ORG), any(), any(), any()))
                .thenReturn(new ArrayList<>());

        ApiResponse response = service.getCBPlanListForUser("org123", "token123", false);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testGetCBPlanListForUser_UserNotFound() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user123");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER), any(), any(), any()))
                .thenReturn(new ArrayList<>());

        ApiResponse response = service.getCBPlanListForUser("org123", "token123", false);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testGetCBPlanListForUser_WithActivePlans() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user123");

        Map<String, Object> userData = createUserData();
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER), any(), any(), any()))
                .thenReturn(Arrays.asList(userData));

        // Mock Redis cache to return nothing (so CBPlan fetches from cacheMgr)
        when(redisCacheMgr.getFromCache(anyString())).thenReturn("");

        // Prepare an active plan
        Map<String, Object> activePlan = new HashMap<>();
        activePlan.put(Constants.PLAN_ID, "plan1");
        activePlan.put(Constants.STATUS, Constants.LIVE);
        activePlan.put(Constants.CONTENT_LIST, Arrays.asList("course1"));
        activePlan.put(Constants.END_DATE_REQUEST, Instant.now());

        // ✅ Use argument matchers for AtomicBoolean
        when(cbPlanCacheMgr.getCbPlanForAllAndOrgId(eq("org123"), any(AtomicBoolean.class)))
                .thenReturn(Arrays.asList(activePlan));

        // Mock content details
        Map<String, Object> contentDetails = new HashMap<>();
        contentDetails.put(Constants.IDENTIFIER, "course1");
        when(contentService.readContent("course1", null)).thenReturn(contentDetails);

        // Execute
        ApiResponse response = service.getCBPlanListForUser("org123", "token123", false);

        // Verify
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(1, response.getResult().get(Constants.COUNT));
    }



    @Test
    void testGetCBPlanListForUser_PrivateMode() {
        Map<String, Object> userData = createUserData();
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER), any(), any(), any()))
                .thenReturn(Arrays.asList(userData));

        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ALL_ORG), any(), any(), any()))
                .thenReturn(new ArrayList<>());
        
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ORG), any(), any(), any()))
                .thenReturn(new ArrayList<>());

        ApiResponse response = service.getCBPlanListForUser("org123", "user123", true);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        verify(accessTokenValidator, never()).fetchUserIdFromAccessToken(anyString(), any());
    }

    @Test
    void testGetCBPlanListForUser_BlankUserId() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("");

        ApiResponse response = service.getCBPlanListForUser("org123", "token123", false);

        assertNotNull(response);
    }

    @Test
    void testGetCBPlanListForUser_Exception() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenThrow(new RuntimeException("Test exception"));

        ApiResponse response = service.getCBPlanListForUser("org123", "token123", false);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testRemoveDuplicateCourses_WithLanguageMap() {
        List<Map<String, Object>> courseList = new ArrayList<>();
        
        Map<String, Object> course1 = new HashMap<>();
        course1.put(Constants.IDENTIFIER, "course1");
        Map<String, Object> langMap1 = new HashMap<>();
        Map<String, Object> langDetails = new HashMap<>();
        langDetails.put(Constants.ID, "lang1");
        langMap1.put("en", langDetails);
        course1.put(Constants.LANGUAGE_MAP_V1, langMap1);
        
        Map<String, Object> course2 = new HashMap<>();
        course2.put(Constants.IDENTIFIER, "course2");
        course2.put(Constants.LANGUAGE_MAP_V1, new HashMap<>());
        
        courseList.add(course1);
        courseList.add(course2);

        List<Map<String, Object>> result = service.removeDuplicateCourses(courseList);

        assertEquals(2, result.size());
    }

    @Test
    void testRemoveDuplicateCourses_EmptyList() {
        List<Map<String, Object>> courseList = new ArrayList<>();
        List<Map<String, Object>> result = service.removeDuplicateCourses(courseList);
        assertTrue(result.isEmpty());
    }

    @Test
    void testParseContextData_ValidJson() throws Exception {
        String jsonData = "{\"accessControl\":{\"userGroups\":[]}}";
        
        Method parseMethod = CbPlanLearnerServiceImpl.class.getDeclaredMethod("parseContextData", Object.class);
        parseMethod.setAccessible(true);
        
        Map<String, Object> result = (Map<String, Object>) parseMethod.invoke(service, jsonData);
        
        assertNotNull(result);
        assertTrue(result.containsKey("accessControl"));
    }

    @Test
    void testParseContextData_InvalidJson() throws Exception {
        String invalidJson = "invalid json";
        
        Method parseMethod = CbPlanLearnerServiceImpl.class.getDeclaredMethod("parseContextData", Object.class);
        parseMethod.setAccessible(true);
        
        Map<String, Object> result = (Map<String, Object>) parseMethod.invoke(service, invalidJson);
        
        assertTrue(result.isEmpty());
    }

    @Test
    void testParseContextData_NonStringInput() throws Exception {
        Integer nonStringInput = 123;
        
        Method parseMethod = CbPlanLearnerServiceImpl.class.getDeclaredMethod("parseContextData", Object.class);
        parseMethod.setAccessible(true);
        
        Map<String, Object> result = (Map<String, Object>) parseMethod.invoke(service, nonStringInput);
        
        assertTrue(result.isEmpty());
    }

    @Test
    @Disabled
    void testSetUserProfile_ValidData() throws Exception {
        Map<String, String> userProfile = new HashMap<>();
        Map<String, Object> userBasicProfile = createUserData();
        
        Method setUserProfileMethod = CbPlanLearnerServiceImpl.class.getDeclaredMethod("setUserProfile", Map.class, Map.class);
        setUserProfileMethod.setAccessible(true);
        
        setUserProfileMethod.invoke(service, userProfile, userBasicProfile);
        
        assertEquals("user123", userProfile.get(Constants.USER));
        assertEquals("org123", userProfile.get(Constants.ROOT_ORG_ID));
    }

    @Test
    void testSetUserProfile_EmptyProfile() throws Exception {
        Map<String, String> userProfile = new HashMap<>();
        Map<String, Object> emptyProfile = new HashMap<>();
        
        Method setUserProfileMethod = CbPlanLearnerServiceImpl.class.getDeclaredMethod("setUserProfile", Map.class, Map.class);
        setUserProfileMethod.setAccessible(true);
        
        setUserProfileMethod.invoke(service, userProfile, emptyProfile);
        
        assertTrue(userProfile.isEmpty());
    }

    @Test
    void testEvaluateContextAccessRule_ValidAccess() throws Exception {
        Map<String, Object> accessSettingMap = createAccessSettingMap();
        Map<String, String> userProfile = createUserProfile();
        
        Method evaluateMethod = CbPlanLearnerServiceImpl.class.getDeclaredMethod("evaluateContextAccessRule", Map.class, Map.class);
        evaluateMethod.setAccessible(true);
        
        boolean result = (boolean) evaluateMethod.invoke(service, accessSettingMap, userProfile);
        
        assertTrue(result);
    }

    @Test
    void testEvaluateContextAccessRule_NoAccess() throws Exception {
        Map<String, Object> accessSettingMap = createAccessSettingMapNoAccess();
        Map<String, String> userProfile = createUserProfile();
        
        Method evaluateMethod = CbPlanLearnerServiceImpl.class.getDeclaredMethod("evaluateContextAccessRule", Map.class, Map.class);
        evaluateMethod.setAccessible(true);
        
        boolean result = (boolean) evaluateMethod.invoke(service, accessSettingMap, userProfile);
        
        assertFalse(result);
    }

    @Test
    void testEvaluateContextAccessRule_EmptyMaps() throws Exception {
        Method evaluateMethod = CbPlanLearnerServiceImpl.class.getDeclaredMethod("evaluateContextAccessRule", Map.class, Map.class);
        evaluateMethod.setAccessible(true);
        
        boolean result = (boolean) evaluateMethod.invoke(service, new HashMap<>(), new HashMap<>());
        
        assertFalse(result);
    }

    private Map<String, Object> createUserData() {
        Map<String, Object> userData = new HashMap<>();
        userData.put(Constants.ID, "user123");
        userData.put("rootorgid", "org123");
        userData.put("profiledetails", "{\"professionalDetails\":[{\"designation\":\"Test\",\"group\":\"TestGroup\"}],\"profileStatus\":\"VERIFIED\",\"cadreDetails\":{\"cadreName\":\"TestCadre\",\"civilServiceName\":\"TestService\",\"cadreBatch\":2020}}");
        return userData;
    }

    private Map<String, String> createUserProfile() {
        Map<String, String> userProfile = new HashMap<>();
        userProfile.put("designation", "Test");
        userProfile.put("group", "TestGroup");
        return userProfile;
    }

    private Map<String, Object> createAccessSettingMap() {
        Map<String, Object> accessControl = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_NAME, "TestGroup");
        
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, "designation");
        criteria.put(Constants.CRITERIA_VALUE, Arrays.asList("Test"));
        criteriaList.add(criteria);
        
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        userGroups.add(userGroup);
        
        accessControl.put(Constants.USER_GROUPS, userGroups);
        
        Map<String, Object> accessSettingMap = new HashMap<>();
        accessSettingMap.put(Constants.ACCESS_CONTROL, accessControl);
        
        return accessSettingMap;
    }

    private Map<String, Object> createAccessSettingMapNoAccess() {
        Map<String, Object> accessControl = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_NAME, "TestGroup");
        
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, "designation");
        criteria.put(Constants.CRITERIA_VALUE, Arrays.asList("NoMatch"));
        criteriaList.add(criteria);
        
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        userGroups.add(userGroup);
        
        accessControl.put(Constants.USER_GROUPS, userGroups);
        
        Map<String, Object> accessSettingMap = new HashMap<>();
        accessSettingMap.put(Constants.ACCESS_CONTROL, accessControl);
        
        return accessSettingMap;
    }
    @Test
    void testGetExistingContextData_WithCustomFields() throws Exception {
        Map<String, String> userProfile = new HashMap<>();
        Map<String, Object> userBasicProfile = createUserData();

        // Prepare custom field TEXT type
        Map<String, Object> customFieldText = new HashMap<>();
        customFieldText.put(Constants.TYPE, Constants.TEXT);
        customFieldText.put(Constants.ATTRIBUTE_NAME, "customText");
        customFieldText.put(Constants.VALUE, "CustomValue");

        // Prepare custom field MASTER_LIST type
        Map<String, Object> masterListValue = new HashMap<>();
        masterListValue.put(Constants.ATTRIBUTE_NAME, "skill");
        masterListValue.put(Constants.VALUE, "Java");

        Map<String, Object> customFieldMasterList = new HashMap<>();
        customFieldMasterList.put(Constants.TYPE, Constants.MASTER_LIST);
        customFieldMasterList.put(Constants.VALUES, List.of(masterListValue));

        Map<String, Object> orgAdditionalProperty = new HashMap<>();
        orgAdditionalProperty.put(Constants.ORGANISATION_ID, "org123");
        orgAdditionalProperty.put(Constants.CUSTOM_FIELD_VALUES, List.of(customFieldText, customFieldMasterList));

        String json = new ObjectMapper().writeValueAsString(List.of(orgAdditionalProperty));
        Map<String, Object> row = Map.of(Constants.CONTEXT_DATA_KEY, json);

        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER_EXTENDED_PROFILE), any(), any(), any()))
                .thenReturn(List.of(row));

        // Call private method via reflection
        Method method = CbPlanLearnerServiceImpl.class.getDeclaredMethod("getExistingContextData", String.class, String.class, Map.class);
        method.setAccessible(true);
        method.invoke(service, "user123", "org123", userProfile);

        assertEquals("CustomValue", userProfile.get("customText"));
        assertEquals("Java", userProfile.get("skill"));
    }
}