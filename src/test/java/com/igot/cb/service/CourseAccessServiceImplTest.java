package com.igot.cb.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import com.igot.cb.cache.AccessSettingRuleCacheMgr;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.model.CachedAccessSettingRule;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;

@ExtendWith(MockitoExtension.class)
class CourseAccessServiceImplTest {

    private CourseAccessServiceImpl courseAccessService;
    
    @Mock
    private AccessTokenValidator mockAccessTokenValidator;
    
    @Mock
    private UserProfileServiceImpl mockUserProfileService;
    
    @Mock
    private AccessSettingRuleCacheMgr mockAccessSettingRuleCacheMgr;

    @BeforeEach
    void setUp() {
        courseAccessService = new CourseAccessServiceImpl(
            mockAccessTokenValidator, 
            mockUserProfileService, 
            mockAccessSettingRuleCacheMgr
        );
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
        when(mockUserProfileService.getUserProfile("user123")).thenReturn(Map.of());
        
        CachedAccessSettingRule rule = new CachedAccessSettingRule("course123", "Course", "{\"accessControlId\":{\"userGroups\":[{\"userGroupId\":\"group1\",\"userGroupCriteriaList\":[{\"criteriaKey\":\"cadre\",\"criteriaValue\":\"AgAAAAAAAAA=\"}]}]}}", false);
        when(mockAccessSettingRuleCacheMgr.getAccessSettingRules()).thenReturn(List.of(rule));
        
        ApiResponse result = courseAccessService.getCoursesForUser(Map.of("key", "value"), "token");
        
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getResult().get(Constants.CONTENT);
        assertEquals(0, content.size());
    }
}