package com.igot.cb.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.lang.reflect.Field;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import com.igot.cb.cache.AccessSettingRuleCacheMgr;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.model.CachedAccessSettingRule;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
class CourseAccessServiceImplTest {

    private CourseAccessServiceImpl courseAccessService;
    
    @Mock
    private AccessTokenValidator mockAccessTokenValidator;
    
    @Mock
    private UserAndOrgServiceImpl mockUserProfileService;
    
    @Mock
    private AccessSettingRuleCacheMgr mockAccessSettingRuleCacheMgr;

    @Mock
    private ContentInfoServiceImpl contentInfoService;

    @Mock
    private RedisCacheMgr redisCacheMgr;

    @Mock
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    private final String authToken = "validToken";

    @BeforeEach
    void setUp() throws Exception {
        courseAccessService = new CourseAccessServiceImpl(
            mockAccessTokenValidator, 
            mockUserProfileService,
            mockAccessSettingRuleCacheMgr, contentInfoService, outboundRequestHandlerService
        );
        
        // Inject the mocked RedisCacheMgr using reflection
        Field redisCacheMgrField = CourseAccessServiceImpl.class.getDeclaredField("redisCacheMgr");
        redisCacheMgrField.setAccessible(true);
        redisCacheMgrField.set(courseAccessService, redisCacheMgr);
        
        // Inject contentReadFields using reflection
        Field contentReadFieldsField = CourseAccessServiceImpl.class.getDeclaredField("contentReadFields");
        contentReadFieldsField.setAccessible(true);
        contentReadFieldsField.set(courseAccessService, "identifier,name,description");
    }

    @Test
    void testGetCoursesForUser_InvalidToken() {
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq("invalid"), any(ApiResponse.class))).thenReturn("");
        
        ApiResponse result = courseAccessService.getCoursesForUser(Map.of("key", "value"), "invalid");
        
        assertEquals(HttpStatus.UNAUTHORIZED, result.getResponseCode());
    }

    @Test
    void testGetCoursesForUser_EmptyRequest() {
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq("token"), any(ApiResponse.class))).thenReturn("user123");
        
        ApiResponse result = courseAccessService.getCoursesForUser(null, "token");
        
        assertEquals(HttpStatus.BAD_REQUEST, result.getResponseCode());
    }

    @Test
    void testGetCoursesForUser_NoRules() {
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq("token"), any(ApiResponse.class))).thenReturn("user123");
        when(redisCacheMgr.getFromCache(Constants.ACCESS_KEY + "user123")).thenReturn(null);
        when(mockUserProfileService.getUserProfile("user123")).thenReturn(Map.of("cadre", 1));
        when(mockAccessSettingRuleCacheMgr.getAccessSettingRules()).thenReturn(Collections.emptyList());
        
        ApiResponse result = courseAccessService.getCoursesForUser(Map.of("key", "value"), "token");
        
        assertEquals(HttpStatus.OK, result.getResponseCode());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getResult().get(Constants.CONTENT);
        assertEquals(0, content.size());
    }

    @Test
    void testEvaluateAccessSettingRule_EmptyMaps() {
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq("token"), any(ApiResponse.class))).thenReturn("user123");
        when(redisCacheMgr.getFromCache(Constants.ACCESS_KEY + "user123")).thenReturn(null);
        when(mockUserProfileService.getUserProfile("user123")).thenReturn(Map.of());
        
        CachedAccessSettingRule rule = new CachedAccessSettingRule("course123", "Course", "{\"accessControlId\":{}}", false);
        when(mockAccessSettingRuleCacheMgr.getAccessSettingRules()).thenReturn(List.of(rule));
        
        ApiResponse result = courseAccessService.getCoursesForUser(Map.of("key", "value"), "token");
        
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getResult().get(Constants.CONTENT);
        assertEquals(0, content.size());
    }

    @Test
    void testEvaluateAccessSettingRule_NoUserGroups() {
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq("token"), any(ApiResponse.class))).thenReturn("user123");
        when(redisCacheMgr.getFromCache(Constants.ACCESS_KEY + "user123")).thenReturn(null);
        when(mockUserProfileService.getUserProfile("user123")).thenReturn(Map.of("cadre", 1));
        
        CachedAccessSettingRule rule = new CachedAccessSettingRule("course123", "Course", "{\"accessControlId\":{\"userGroups\":[]}}", false);
        when(mockAccessSettingRuleCacheMgr.getAccessSettingRules()).thenReturn(List.of(rule));
        
        ApiResponse result = courseAccessService.getCoursesForUser(Map.of("key", "value"), "token");
        
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getResult().get(Constants.CONTENT);
        assertEquals(0, content.size());
    }

    @Test
    void testEvaluateAccessSettingRule_NoCriteria() {
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq("token"), any(ApiResponse.class))).thenReturn("user123");
        when(redisCacheMgr.getFromCache(Constants.ACCESS_KEY + "user123")).thenReturn(null);
        when(mockUserProfileService.getUserProfile("user123")).thenReturn(Map.of("cadre", 1));
        
        CachedAccessSettingRule rule = new CachedAccessSettingRule("course123", "Course", "{\"accessControlId\":{\"userGroups\":[{\"userGroupId\":\"group1\",\"userGroupCriteriaList\":[]}]}}", false);
        when(mockAccessSettingRuleCacheMgr.getAccessSettingRules()).thenReturn(List.of(rule));
        
        ApiResponse result = courseAccessService.getCoursesForUser(Map.of("key", "value"), "token");
        
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getResult().get(Constants.CONTENT);
        assertEquals(0, content.size());
    }

    @Test
    void testEvaluateAccessSettingRule_UserCriteriaMissing() {
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq("token"), any(ApiResponse.class))).thenReturn("user123");
        when(redisCacheMgr.getFromCache(Constants.ACCESS_KEY + "user123")).thenReturn(null);
        when(mockUserProfileService.getUserProfile("user123")).thenReturn(Map.of());
        
        // Create a BitSet for criteria value
        BitSet criteriaValue = new BitSet();
        criteriaValue.set(1); // Set bit 1 to true
        
        // Create the rule with proper BitSet in contextData
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControlId = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put("userGroupId", "group1");
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("criteriaKey", "cadre");
        criteria.put("criteriaValue", criteriaValue);
        criteriaList.add(criteria);
        userGroup.put("userGroupCriteriaList", criteriaList);
        userGroups.add(userGroup);
        accessControlId.put("userGroups", userGroups);
        contextData.put("accessControlId", accessControlId);
        
        CachedAccessSettingRule rule = mock(CachedAccessSettingRule.class);
        when(rule.getContextData()).thenReturn(contextData);
        
        when(mockAccessSettingRuleCacheMgr.getAccessSettingRules()).thenReturn(List.of(rule));
        
        ApiResponse result = courseAccessService.getCoursesForUser(Map.of("key", "value"), "token");
        
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getResult().get(Constants.CONTENT);
        assertEquals(0, content.size());
    }

    @Test
    void testEvaluateAccessSettingRule_UserCriteriaMatches() {
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq("token"), any(ApiResponse.class))).thenReturn("user123");
        when(redisCacheMgr.getFromCache(Constants.ACCESS_KEY + "user123")).thenReturn(null);
        when(mockUserProfileService.getUserProfile("user123")).thenReturn(Map.of("cadre", 1));
        
        // Create a BitSet for criteria value
        BitSet criteriaValue = new BitSet();
        criteriaValue.set(1); // Set bit 1 to true to match user's cadre value
        
        // Create the rule with proper BitSet in contextData
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControlId = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put("userGroupId", "group1");
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("criteriaKey", "cadre");
        criteria.put("criteriaValue", criteriaValue);
        criteriaList.add(criteria);
        userGroup.put("userGroupCriteriaList", criteriaList);
        userGroups.add(userGroup);
        accessControlId.put("userGroups", userGroups);
        contextData.put("accessControlId", accessControlId);
        
        CachedAccessSettingRule rule = mock(CachedAccessSettingRule.class);
        when(rule.getContextId()).thenReturn("course123");
        when(rule.getContextData()).thenReturn(contextData);
        
        when(mockAccessSettingRuleCacheMgr.getAccessSettingRules()).thenReturn(List.of(rule));
        
        // Mock content service to return course details
        Map<String, Object> courseDetails = Map.of(
            "identifier", "course123",
            "name", "Test Course",
            "description", "Test Description"
        );
        when(contentInfoService.readContent(eq("course123"), any(List.class))).thenReturn(courseDetails);
        
        ApiResponse result = courseAccessService.getCoursesForUser(Map.of("key", "value"), "token");
        
        assertEquals(HttpStatus.OK, result.getResponseCode());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getResult().get(Constants.CONTENT);
        assertEquals(1, content.size());
        assertEquals("course123", content.get(0).get("identifier"));
    }

    @Test
    void testGetCoursesForUser_shouldThrowRuntimeException_onReadValueError() throws Exception {
        // Arrange
        Map<String, Object> request = Map.of("key", "value");
        ApiResponse.createDefaultResponse("test");

        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(any(), any(ApiResponse.class)))
                .thenReturn("user123");
        when(redisCacheMgr.getFromCache(Constants.ACCESS_KEY + "user123")).thenReturn("[{bad json}]");

        ObjectMapper mapperSpy = Mockito.spy(new ObjectMapper());
        ReflectionTestUtils.setField(courseAccessService, "mapper", mapperSpy);

        doThrow(JsonProcessingException.class)
                .when(mapperSpy)
                .readValue(anyString(), ArgumentMatchers.<TypeReference<List<Map<String, Object>>>>any());

        // Act & Assert
        assertThrows(RuntimeException.class, () ->
                courseAccessService.getCoursesForUser(request, "token"));
    }

    @Test
    void testGetCoursesForUser_1() {
        // Arrange
        Map<String, Object> request = Map.of("key", "value");

        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn("user123");
        when(redisCacheMgr.getFromCache(Constants.ACCESS_KEY + "user123")).thenReturn("No records found for this user");

        ObjectMapper mapperSpy = Mockito.spy(new ObjectMapper());
        ReflectionTestUtils.setField(courseAccessService, "mapper", mapperSpy);

        // Act & Assert
        assertDoesNotThrow(() ->
                courseAccessService.getCoursesForUser(request, authToken));
    }

    @Test
    void testGetCoursesForUser_shouldHandleExceptionFromRetrieveUserCourses() {
        // Arrange
        Map<String, Object> request = Map.of("key", "value");

        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn("user123");
        when(redisCacheMgr.getFromCache(Constants.ACCESS_KEY + "user123")).thenReturn(null);
        when(mockUserProfileService.getUserProfile("user123")).thenReturn(Map.of("k", 1));

        when(mockAccessSettingRuleCacheMgr.getAccessSettingRules())
                .thenThrow(new RuntimeException("Cache error"));

        // Act
        ApiResponse response = courseAccessService.getCoursesForUser(request, authToken);

        // Assert
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }
}