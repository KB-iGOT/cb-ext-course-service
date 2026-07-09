package com.igot.cb.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.lang.reflect.Field;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.IdMapCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
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
    private CbPlanLearnerServiceImpl cbPlanLearnerServiceImpl;

    @Mock
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    private final String authToken = "validToken";

    @Mock
    private IdMapCacheMgr idMapCacheMgr;

    @Mock
    private CassandraOperation cassandraOperation;


    @BeforeEach
    void setUp() throws Exception {
        courseAccessService = new CourseAccessServiceImpl(
            mockAccessTokenValidator, 
            mockUserProfileService,
            mockAccessSettingRuleCacheMgr, contentInfoService, outboundRequestHandlerService, cbPlanLearnerServiceImpl, cassandraOperation
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

    @Test
    void testGetAssignedCoursesForUser_ValidFlow() throws Exception {
        String userId = "u1";
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        Map<String, Object> request = Map.of(Constants.COURSE_CATEGORY, "cat1");
        // Simulate Redis cache miss for the user-course assignment
        when(redisCacheMgr.getFromCache(startsWith(Constants.ACCESS_KEY + "_cat1_" + userId))).thenReturn(null);
        // Simulate Redis cache miss for course category list (fix key)
        when(redisCacheMgr.getFromCache("access_settings_enabled_cat1")).thenReturn(null);
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), anyMap(), isNull()))
                .thenReturn(Map.of(Constants.RESULT,
                        Map.of(Constants.CONTENT,
                                List.of(Map.of(Constants.IDENTIFIER, "C1")))));
        ReflectionTestUtils.setField(courseAccessService, "cacheTtlMs", 99999999L);
        ReflectionTestUtils.setField(courseAccessService, "accessCacheTtlSecods", 600); // Set TTL to avoid NPE
        BitSet bit = new BitSet();
        bit.set(1);
        Map<String,Object> accessControl = Map.of(
                Constants.USER_GROUPS,
                List.of(
                        Map.of(
                                Constants.USER_GROUP_ID, "G1",
                                Constants.USER_GROUP_CRITERIA_LIST,
                                List.of(
                                        Map.of(Constants.CRITERIA_KEY, "cadre",
                                                Constants.CRITERIA_VALUE, bit)
                                )
                        )
                )
        );
        CachedAccessSettingRule rule = mock(CachedAccessSettingRule.class);
        when(rule.getContextId()).thenReturn("C1");
        when(rule.getContextData()).thenReturn(
                Map.of(Constants.ACCESS_CONTROL_ID, accessControl)
        );
        when(mockAccessSettingRuleCacheMgr.getOrLoadAccessSettingRule(anyString(), anyString()))
                .thenReturn(rule);
        when(mockUserProfileService.getUserProfile(userId))
                .thenReturn(Map.of("cadre", 1));
        when(contentInfoService.readContent(eq("C1"), anyList()))
                .thenReturn(Map.of("identifier", "C1"));
        ApiResponse response = courseAccessService.getAssignedCoursesForUser(request, authToken);
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(1,
                ((List<?>)response.getResult().get(Constants.CONTENT)).size()
        );
    }


    @Test
    void testGetAssignedCoursesForUser_InvalidToken() {
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(null);
        ApiResponse response = courseAccessService.getAssignedCoursesForUser(
                Map.of(Constants.COURSE_CATEGORY, "c1"), authToken);
        assertEquals(HttpStatus.OK, response.getResponseCode()); // default response
    }

    @Test
    void testGetAssignedCoursesForUser_CacheHit() throws Exception {
        String userId = "u1";
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        List<Map<String, Object>> cached = List.of(Map.of("id","C1"));
        ObjectMapper spyMapper = spy(new ObjectMapper());
        ReflectionTestUtils.setField(courseAccessService, "mapper", spyMapper);
        when(redisCacheMgr.getFromCache(anyString()))
                .thenReturn("[{\"id\":\"C1\"}]");
        ApiResponse response = courseAccessService.getAssignedCoursesForUser(
                Map.of(Constants.COURSE_CATEGORY,"CAT"), authToken);
        assertEquals("C1", ((Map<?,?>)((List<?>)response.getResult().get(Constants.CONTENT)).get(0)).get("id"));
    }

    @Test
    void testFetchAccessSettingsEnabledCoursesForCategory() {
        Map<String,Object> mockResponse = Map.of("RES","OK");
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), anyMap(), isNull()))
                .thenReturn(mockResponse);
        Map<String,Object> result = courseAccessService.fetchAccessSettingsEnabledCoursesForCategory("cat");
        assertEquals("OK", result.get("RES"));
    }

    @Test
    void testGetCoursesFromCacheOrService_CacheHit() {
        // Simulate Redis cache hit for the course category
        when(redisCacheMgr.getFromCache("access_settings_enabled_cat"))
            .thenReturn("[\"C1\",\"C2\"]");
        List<String> result = ReflectionTestUtils.invokeMethod(courseAccessService,
                "getCoursesFromCacheOrService", "cat");
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains("C1"));
        assertTrue(result.contains("C2"));
    }
    @Test
    void testGetCoursesFromCacheOrService_Exception() {
        ReflectionTestUtils.setField(courseAccessService, "courseCategoryCache",
                new HashMap<>());
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), anyMap(), isNull()))
                .thenThrow(new RuntimeException("ERR"));
        List<String> result = ReflectionTestUtils.invokeMethod(courseAccessService,
                "getCoursesFromCacheOrService", "cat");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testEvaluateAccessSettingRule_FullMatchTrue() {
        BitSet bs = new BitSet();
        bs.set(1);
        Map<String,Object> group = Map.of(
                Constants.USER_GROUP_ID, "g1",
                Constants.USER_GROUP_CRITERIA_LIST,
                List.of(Map.of(Constants.CRITERIA_KEY,"cadre", Constants.CRITERIA_VALUE,bs))
        );
        Map<String,Object> access = Map.of(Constants.USER_GROUPS, List.of(group));
        boolean result = Boolean.TRUE.equals(ReflectionTestUtils.invokeMethod(
                courseAccessService,
                "evaluateAccessSettingRule",
                access,
                Map.of("cadre", 1)
        ));
        assertTrue(result);
    }
    @Test
    void testEvaluateAccessSettingRule_False() {
        BitSet bs = new BitSet();
        bs.set(1);
        Map<String,Object> group = Map.of(
                Constants.USER_GROUP_ID, "g1",
                Constants.USER_GROUP_CRITERIA_LIST,
                List.of(Map.of(Constants.CRITERIA_KEY,"grade", Constants.CRITERIA_VALUE,bs))
        );
        Map<String,Object> access = Map.of(Constants.USER_GROUPS, List.of(group));
        // does NOT match
        boolean result = Boolean.TRUE.equals(ReflectionTestUtils.invokeMethod(
                courseAccessService,
                "evaluateAccessSettingRule",
                access,
                Map.of("cadre", 1) // does NOT match
        ));
        assertFalse(result);
    }

    @Test
    void testRetrieveUserCourses_RuleMatches() {
        BitSet bs = new BitSet();
        bs.set(1);
        Map<String,Object> ruleData = Map.of(Constants.ACCESS_CONTROL_ID,
                Map.of(Constants.USER_GROUPS,
                        List.of(Map.of(Constants.USER_GROUP_ID,"G1",
                                Constants.USER_GROUP_CRITERIA_LIST,
                                List.of(Map.of(Constants.CRITERIA_KEY,"cadre", Constants.CRITERIA_VALUE,bs)))
                        )));
        CachedAccessSettingRule rule = mock(CachedAccessSettingRule.class);
        when(rule.getContextId()).thenReturn("C1");
        when(rule.getContextData()).thenReturn(ruleData);
        when(mockAccessSettingRuleCacheMgr.getAccessSettingRules())
                .thenReturn(List.of(rule));
        when(contentInfoService.readContent(eq("C1"), anyList()))
                .thenReturn(Map.of("id","C1"));
        List<Map<String,Object>> list = new ArrayList<>();
        boolean val = Boolean.TRUE.equals(ReflectionTestUtils.invokeMethod(courseAccessService,
                "retrieveUserCourses",
                Map.of("cadre", 1),
                list));
        assertTrue(val);
        assertEquals(1, list.size());
    }

    @Test
    void testGetAssignedExternalCoursesForUser_ContentDirectMap() throws Exception {
        String userId = "u_ext";
        when(mockAccessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        Map<String, Object> request = Map.of(Constants.PARTNER_ID, "partner1");
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        ReflectionTestUtils.setField(courseAccessService, "courseCategoryCache",
                new HashMap<>(Map.of("access_settings_enabled_partner1", List.of("C1"))));
        ReflectionTestUtils.setField(courseAccessService, "cacheTimestamps",
                new HashMap<>(Map.of("access_settings_enabled_partner1", System.currentTimeMillis())));
        ReflectionTestUtils.setField(courseAccessService, "cacheTtlMs", 99999999L);
        BitSet bit = new BitSet();
        bit.set(1);
        Map<String, Object> ug = new HashMap<>();
        ug.put(Constants.USER_GROUP_CRITERIA_LIST, List.of(Map.of(Constants.CRITERIA_KEY, "cadre", Constants.CRITERIA_VALUE, bit)));
        Map<String, Object> accessControl = Map.of(Constants.USER_GROUPS, List.of(ug));

        CachedAccessSettingRule rule = mock(CachedAccessSettingRule.class);
        when(rule.getContextId()).thenReturn("C1");
        when(rule.getContextData()).thenReturn(Map.of(Constants.ACCESS_CONTROL_ID, accessControl));
        when(mockAccessSettingRuleCacheMgr.getOrLoadAccessSettingRule(eq("C1"), eq(Constants.EXTERNAL_COURSES)))
                .thenReturn(rule);

        when(mockUserProfileService.getUserProfile(userId)).thenReturn(Map.of("cadre", 1));

        when(contentInfoService.readContent(eq("C1"), anyList()))
                .thenReturn(Map.of("content", Map.of(Constants.IDENTIFIER, "C1", "name", "External Course")));

        ApiResponse resp = courseAccessService.getAssignedExternalCoursesForUser(request, authToken);

        assertEquals(HttpStatus.OK, resp.getResponseCode());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> content = (List<Map<String, Object>>) resp.getResult().get(Constants.CONTENT);
        assertNotNull(content);
        assertEquals(1, content.size());
        assertEquals("C1", content.get(0).get(Constants.IDENTIFIER));
    }

    @Test
    void testGetCoursesFromCacheOrServiceForExternalCourse_CacheHit_viaReflection() throws Exception {
        String partnerId = "partnerCache";
        String cacheKey = "access_settings_enabled_" + partnerId;
        Map<String, List<String>> partnerCache = new HashMap<>();
        partnerCache.put(cacheKey, List.of("E1", "E2"));
        Map<String, Long> timestamps = new HashMap<>();
        timestamps.put(cacheKey, System.currentTimeMillis());

        ReflectionTestUtils.setField(courseAccessService, "courseCategoryCache", partnerCache);
        ReflectionTestUtils.setField(courseAccessService, "cacheTimestamps", timestamps);
        ReflectionTestUtils.setField(courseAccessService, "cacheTtlMs", 99999999L);

        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) ReflectionTestUtils.invokeMethod(
                courseAccessService, "getCoursesFromCacheOrServiceForExternalCourse", partnerId);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains("E1"));
        assertTrue(result.contains("E2"));
    }

    // Test cases for getPersonalContentInfo
    @Test
    void testGetPersonalContentInfo_ValidTokenAndSuccess() throws Exception {
        String authToken = "validToken123";
        String userId = "user123";
        String orgId = "org456";
        
        Map<String, Object> tokenData = new HashMap<>();
        tokenData.put("userId", userId);
        tokenData.put("org", orgId);
        
        when(mockAccessTokenValidator.fetchUserIdAndOrg(authToken)).thenReturn(tokenData);
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        
        ApiResponse cbPlanResponse = new ApiResponse();
        cbPlanResponse.setResult(new HashMap<>());
        when(cbPlanLearnerServiceImpl.getCBPlanListForUser(orgId, userId, true)).thenReturn(cbPlanResponse);
        
        doNothing().when(redisCacheMgr).putInCache(anyString(), anyString());
        
        ApiResponse result = courseAccessService.getPersonalContentInfo(authToken);
        
        assertNotNull(result);
        assertNotNull(result.getResult());
    }

    @Test
    void testGetPersonalContentInfo_InvalidTokenEmptyUserId() throws Exception {
        String authToken = "invalidToken";
        
        Map<String, Object> tokenData = new HashMap<>();
        tokenData.put("userId", "");
        tokenData.put("org", "org456");
        
        when(mockAccessTokenValidator.fetchUserIdAndOrg(authToken)).thenReturn(tokenData);
        
        ApiResponse result = courseAccessService.getPersonalContentInfo(authToken);
        
        assertNotNull(result);
        assertEquals(Constants.FAILED, result.getParams().getStatus());
        assertEquals(HttpStatus.UNAUTHORIZED, result.getResponseCode());
        assertEquals("Invalid auth token", result.getParams().getErrMsg());
    }

    @Test
    void testGetPersonalContentInfo_InvalidTokenNullUserId() throws Exception {
        String authToken = "invalidToken";
        
        Map<String, Object> tokenData = new HashMap<>();
        tokenData.put("userId", null);
        tokenData.put("org", "org456");
        
        when(mockAccessTokenValidator.fetchUserIdAndOrg(authToken)).thenReturn(tokenData);
        
        ApiResponse result = courseAccessService.getPersonalContentInfo(authToken);
        
        assertNotNull(result);
        assertEquals(Constants.FAILED, result.getParams().getStatus());
        assertEquals(HttpStatus.UNAUTHORIZED, result.getResponseCode());
        assertEquals("Invalid auth token", result.getParams().getErrMsg());
    }

    @Test
    void testGetPersonalContentInfo_ExceptionHandling() throws Exception {
        String authToken = "token123";
        
        when(mockAccessTokenValidator.fetchUserIdAndOrg(authToken))
                .thenThrow(new RuntimeException("Token validation failed"));
        
        ApiResponse result = courseAccessService.getPersonalContentInfo(authToken);
        
        assertNotNull(result);
        assertEquals(Constants.FAILED, result.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, result.getResponseCode());
        assertTrue(result.getParams().getErrMsg().contains("Failed to fetch personal content info"));
    }

    @Test
    void testGetPersonalContentInfo_CacheHitForPersonalContent() throws Exception {
        String authToken = "validToken123";
        String userId = "user123";
        String orgId = "org456";
        
        Map<String, Object> tokenData = new HashMap<>();
        tokenData.put("userId", userId);
        tokenData.put("org", orgId);
        
        Map<String, Object> cachedContent = new HashMap<>();
        cachedContent.put(Constants.TRAINING_PLAN, 5);
        cachedContent.put(Constants.APAR, 2);
        
        ObjectMapper objectMapper = new ObjectMapper();
        
        when(mockAccessTokenValidator.fetchUserIdAndOrg(authToken)).thenReturn(tokenData);
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        doNothing().when(redisCacheMgr).putInCache(anyString(), anyString());
        
        ReflectionTestUtils.setField(courseAccessService, "objectMapper", objectMapper);
        
        ApiResponse result = courseAccessService.getPersonalContentInfo(authToken);
        
        assertNotNull(result);
        verify(redisCacheMgr, atLeastOnce()).getFromCache(Constants.PERSONAL_CONTENT_INFO_REDIS_KEY_PREFIX + userId);
    }

    @Test
    void testGetPersonalContentInfo_CacheMissForPersonalContent() throws Exception {
        String authToken = "validToken123";
        String userId = "user123";
        String orgId = "org456";
        
        Map<String, Object> tokenData = new HashMap<>();
        tokenData.put("userId", userId);
        tokenData.put("org", orgId);
        
        when(mockAccessTokenValidator.fetchUserIdAndOrg(authToken)).thenReturn(tokenData);
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        
        ApiResponse cbPlanResponse = new ApiResponse();
        Map<String, Object> cbPlanResult = new HashMap<>();
        cbPlanResponse.setResult(cbPlanResult);
        when(cbPlanLearnerServiceImpl.getCBPlanListForUser(orgId, userId, true)).thenReturn(cbPlanResponse);
        
        doNothing().when(redisCacheMgr).putInCache(anyString(), anyString());
        
        ObjectMapper objectMapper = new ObjectMapper();
        ReflectionTestUtils.setField(courseAccessService, "objectMapper", objectMapper);
        
        ApiResponse result = courseAccessService.getPersonalContentInfo(authToken);
        
        assertNotNull(result);
    }

    @Test
    void testGetPersonalContentInfo_WithCachedModeratedContent() throws Exception {
        String authToken = "validToken123";
        String userId = "user123";
        String orgId = "org456";
        
        Map<String, Object> tokenData = new HashMap<>();
        tokenData.put("userId", userId);
        tokenData.put("org", orgId);
        
        Map<String, Object> moderatedCacheData = new HashMap<>();
        moderatedCacheData.put(orgId, 5);
        
        ObjectMapper objectMapper = new ObjectMapper();
        String moderatedCachedJson = objectMapper.writeValueAsString(moderatedCacheData);
        
        when(mockAccessTokenValidator.fetchUserIdAndOrg(authToken)).thenReturn(tokenData);
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        
        ApiResponse cbPlanResponse = new ApiResponse();
        cbPlanResponse.setResult(new HashMap<>());
        when(cbPlanLearnerServiceImpl.getCBPlanListForUser(orgId, userId, true)).thenReturn(cbPlanResponse);
        
        doNothing().when(redisCacheMgr).putInCache(anyString(), anyString());
        
        ReflectionTestUtils.setField(courseAccessService, "objectMapper", objectMapper);
        
        ApiResponse result = courseAccessService.getPersonalContentInfo(authToken);
        
        assertNotNull(result);
    }

    @Test
    void testGetPersonalContentInfo_WithCBPlansAPAR() throws Exception {
        String authToken = "validToken123";
        String userId = "user123";
        String orgId = "org456";
        
        Map<String, Object> tokenData = new HashMap<>();
        tokenData.put("userId", userId);
        tokenData.put("org", orgId);
        
        when(mockAccessTokenValidator.fetchUserIdAndOrg(authToken)).thenReturn(tokenData);
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        
        ApiResponse cbPlanResponse = new ApiResponse();
        Map<String, Object> cbPlanResult = new HashMap<>();
        
        List<Map<String, Object>> plans = new ArrayList<>();
        Map<String, Object> plan1 = new HashMap<>();
        plan1.put(Constants.IS_APAR, true);
        plans.add(plan1);
        
        Map<String, Object> plan2 = new HashMap<>();
        plan2.put(Constants.IS_APAR, false);
        plans.add(plan2);
        
        cbPlanResult.put(Constants.CONTENT, plans);
        cbPlanResponse.setResult(cbPlanResult);
        
        when(cbPlanLearnerServiceImpl.getCBPlanListForUser(orgId, userId, true)).thenReturn(cbPlanResponse);
        
        doNothing().when(redisCacheMgr).putInCache(anyString(), anyString());
        
        ObjectMapper objectMapper = new ObjectMapper();
        ReflectionTestUtils.setField(courseAccessService, "objectMapper", objectMapper);
        
        ApiResponse result = courseAccessService.getPersonalContentInfo(authToken);
        
        assertNotNull(result);
        assertNotNull(result.getResult());
    }

    @Test
    void testGetPersonalContentInfo_ResponseStructure() throws Exception {
        String authToken = "validToken123";
        String userId = "user123";
        String orgId = "org456";
        
        Map<String, Object> tokenData = new HashMap<>();
        tokenData.put("userId", userId);
        tokenData.put("org", orgId);
        
        when(mockAccessTokenValidator.fetchUserIdAndOrg(authToken)).thenReturn(tokenData);
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        
        ApiResponse cbPlanResponse = new ApiResponse();
        cbPlanResponse.setResult(new HashMap<>());
        when(cbPlanLearnerServiceImpl.getCBPlanListForUser(orgId, userId, true)).thenReturn(cbPlanResponse);
        
        doNothing().when(redisCacheMgr).putInCache(anyString(), anyString());
        
        ObjectMapper objectMapper = new ObjectMapper();
        ReflectionTestUtils.setField(courseAccessService, "objectMapper", objectMapper);
        
        ApiResponse result = courseAccessService.getPersonalContentInfo(authToken);
        
        assertNotNull(result);
        assertNotNull(result.getParams());
        assertNotNull(result.getResponseCode());
        assertNotNull(result.getResult());
    }

}

