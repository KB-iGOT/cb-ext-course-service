package com.igot.cb.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.lang.reflect.Field;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.IdMapCacheMgr;
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

    @Mock
    private IdMapCacheMgr idMapCacheMgr;


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
        when(contentInfoService.readContent(eq("course123"), anyList()))
                .thenReturn(courseDetails);

        ApiResponse result = courseAccessService.getCoursesForUser(Map.of("key", "value"), "token");
        
        assertEquals(HttpStatus.OK, result.getResponseCode());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getResult().get(Constants.CONTENT);
        assertEquals(1, content.size());
        assertEquals("course123", content.get(0).get("identifier"));
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

    @SuppressWarnings("unchecked")
    @Test
    void testGetCoursesForUser_CacheHasValidJson() throws Exception {
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn("user1");

        String json = "[{\"identifier\":\"c1\"}]";
        when(redisCacheMgr.getFromCache(Constants.ACCESS_KEY + "user1")).thenReturn(json);

        ObjectMapper mapperSpy = spy(new ObjectMapper());
        ReflectionTestUtils.setField(courseAccessService, "mapper", mapperSpy);

        ApiResponse response = courseAccessService.getCoursesForUser(Map.of("x", "y"), authToken);

        List<Map<String, Object>> content =
                (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT);

        assertEquals(1, content.size());
        assertEquals("c1", content.get(0).get("identifier"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testGetCoursesForUser_CacheNoRecordsFound() {
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn("user1");

        when(redisCacheMgr.getFromCache(Constants.ACCESS_KEY + "user1"))
                .thenReturn(Constants.NO_RECORDS_FOUND);

        ApiResponse response = courseAccessService.getCoursesForUser(Map.of("x", "y"), authToken);

        List<Map<String, Object>> content =
                (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT);

        assertTrue(content.isEmpty());
    }

    @Test
    void testGetCoursesForUser_CacheInvalidJson_ThrowsException() throws Exception {
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn("u1");

        when(redisCacheMgr.getFromCache(Constants.ACCESS_KEY + "u1")).thenReturn("invalid JSON");

        ObjectMapper mapperSpy = spy(new ObjectMapper());
        doThrow(new JsonProcessingException("error") {})
                .when(mapperSpy)
                .readValue(anyString(), (TypeReference<?>) any());
        ReflectionTestUtils.setField(courseAccessService, "mapper", mapperSpy);

        assertThrows(RuntimeException.class, () ->
                courseAccessService.getCoursesForUser(Map.of("x", "y"), authToken));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testGetCoursesForUser_RetrieveUserCoursesReturnsFalse() {
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn("u1");

        when(redisCacheMgr.getFromCache(Constants.ACCESS_KEY + "u1")).thenReturn(null);
        when(mockUserProfileService.getUserProfile("u1")).thenReturn(Map.of("k", 1));
        when(mockAccessSettingRuleCacheMgr.getAccessSettingRules()).thenReturn(Collections.emptyList());

        ApiResponse response = courseAccessService.getCoursesForUser(Map.of("x", "y"), authToken);

        List<Map<String, Object>> content =
                (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT);

        assertTrue(content.isEmpty());
    }

    @Test
    void testGetCoursesForUser_ExceptionInRedisPut() throws Exception {
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn("u1");
        when(redisCacheMgr.getFromCache(Constants.ACCESS_KEY + "u1")).thenReturn(null);
        when(mockUserProfileService.getUserProfile("u1")).thenReturn(Map.of("cadre", 1));
        BitSet bit = new BitSet();
        bit.set(1);
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> ac = new HashMap<>();
        Map<String, Object> ug = new HashMap<>();
        ug.put("userGroupCriteriaList", List.of(Map.of("criteriaKey", "cadre", "criteriaValue", bit)));
        ac.put("userGroups", List.of(ug));
        contextData.put("accessControlId", ac);
        CachedAccessSettingRule rule = mock(CachedAccessSettingRule.class);
        when(rule.getContextId()).thenReturn("c1");
        when(rule.getContextData()).thenReturn(contextData);
        when(contentInfoService.readContent(eq("c1"), anyList()))
                .thenReturn(Map.of("identifier", "c1"));
        when(mockAccessSettingRuleCacheMgr.getAccessSettingRules()).thenReturn(List.of(rule));
        ObjectMapper mapperSpy = spy(new ObjectMapper());
        ReflectionTestUtils.setField(courseAccessService, "mapper", mapperSpy);

        doThrow(new RuntimeException("cache fail"))
                .when(redisCacheMgr)
                .putInCache(anyString(), anyString());
        ApiResponse response = courseAccessService.getCoursesForUser(Map.of("x", "y"), authToken);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testFetchFromRedisCache_InvalidJson() throws Exception {
        String key = "k1";
        when(redisCacheMgr.getFromCache(key)).thenReturn("invalid");

        ObjectMapper mapperSpy = spy(new ObjectMapper());
        ReflectionTestUtils.setField(courseAccessService, "mapper", mapperSpy);
        doThrow(new JsonProcessingException("error") {})
                .when(mapperSpy)
                .readValue(anyString(), (TypeReference<?>) any());
        List<Map<String, Object>> result =
                ReflectionTestUtils.invokeMethod(courseAccessService, "fetchFromRedisCache", key);

        assertNull(result);
    }

    @Test
    void testGetAssignedCoursesForUser_EmptyRequest() {
        ApiResponse response = courseAccessService.getAssignedCoursesForUser(null, authToken);
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertInstanceOf(Map.class, response.getResult());
        Map<String, Object> result = (Map<String, Object>) response.getResult();
        assertTrue(result.isEmpty() || result.containsKey("courses"));
    }



    @Test
    void testGetAssignedCoursesForUser_MissingCourseCategory() {
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn("u1");

        ApiResponse response = courseAccessService.getAssignedCoursesForUser(Map.of(), authToken);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testGetCoursesFromCacheOrService_NoIdentifiers() {
        Map<String, Object> result = Map.of(
                Constants.RESULT, Map.of(Constants.CONTENT, List.of())
        );

        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), anyMap(), isNull()))
                .thenReturn(result);

        List<String> list = ReflectionTestUtils.invokeMethod(
                courseAccessService, "getCoursesFromCacheOrService", "category1");

        assertNotNull(list);
        assertTrue(list.isEmpty());
    }

    @Test
    void testGetCoursesForUser_InvalidToken_ShouldReturnBadRequest() throws Exception {
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(null);
        ApiResponse response = courseAccessService.getCoursesForUser(Map.of(), authToken);
        assertEquals(HttpStatus.UNAUTHORIZED, response.getResponseCode());
    }

    @Test
    void testGetCoursesForUser_NoUserProfile() throws Exception {
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn("u1");
        when(redisCacheMgr.getFromCache(Constants.ACCESS_KEY + "u1")).thenReturn(null);
        when(mockUserProfileService.getUserProfile("u1")).thenReturn(Map.of());
        ApiResponse response = courseAccessService.getCoursesForUser(Map.of("dummy", "value"), authToken);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertTrue(((List<?>) response.getResult().get(Constants.CONTENT)).isEmpty());
    }

    @Test
    void testGetCoursesForUser_NoAccessRules() throws Exception {
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn("u1");
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(mockUserProfileService.getUserProfile("u1"))
                .thenReturn(Map.of("cadre", 1));
        when(mockAccessSettingRuleCacheMgr.getAccessSettingRules())
                .thenReturn(List.of());
        ApiResponse response = courseAccessService.getCoursesForUser(
                Map.of("dummy", "value"),  // must be NON-EMPTY
                authToken
        );
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }
}