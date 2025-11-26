package com.igot.cb.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.PromotionalContentRuleCacheMgr;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.model.CachedAccessSettingRule;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PayloadValidation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * JUnit test cases for PromotionalContentServiceImpl.
 * Tests cover all public methods and critical private methods via reflection.
 * Target coverage: >85%
 */
@ExtendWith(MockitoExtension.class)
class PromotionalContentServiceImplTest {

    @Mock
    private AccessTokenValidator accessTokenValidator;

    @Mock
    private PayloadValidation payloadValidation;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private AccessSettingMigrationServiceImpl accessSettingMigrationService;

    @Mock
    private RedisCacheMgr redisCacheMgr;

    @Mock
    private UserAndOrgServiceImpl userProfileServiceImpl;

    @Mock
    private PromotionalContentRuleCacheMgr promotionalContentRuleCacheMgr;

    @Mock
    private ContentInfoServiceImpl contentService;

    @InjectMocks
    private PromotionalContentServiceImpl promotionalContentService;

    private static final String AUTH_TOKEN = "valid-auth-token";
    private static final String USER_ID = "user-123";
    private static final String CONTENT_ID = "content-456";

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(promotionalContentService, "contentReadFields", "name,description,identifier");
    }


    @Test
    void testUpsertPromotionalContentMetadata_Success() throws Exception {
        Map<String, Object> userGroupDetails = createValidUserGroupDetails();
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(payloadValidation.validateAccessControlPayload(userGroupDetails)).thenReturn("");
        when(accessSettingMigrationService.processAccessSettingRule(any())).thenReturn(true);
        when(objectMapper.writeValueAsString(any())).thenReturn("{\"accessControl\":{}}");
        ApiResponse result = promotionalContentService.upsertPromotionalContentMetadata(userGroupDetails, AUTH_TOKEN);
        assertNotNull(result);
        verify(cassandraOperation).insertRecord(eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.PROMOTIONAL_CONTENT_RULES), any());
        verify(accessSettingMigrationService).processAccessSettingRule(any());
    }

    @Test
    void testUpsertPromotionalContentMetadata_InvalidAuthToken() {
        Map<String, Object> userGroupDetails = createValidUserGroupDetails();
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn("");
        ApiResponse result = promotionalContentService.upsertPromotionalContentMetadata(userGroupDetails, AUTH_TOKEN);
        assertNotNull(result);
        assertEquals(HttpStatus.UNAUTHORIZED, result.getResponseCode());
        assertEquals(Constants.FAILED, result.getParams().getStatus());
        assertTrue(result.getParams().getErrMsg().contains("Invalid or missing authentication token"));
    }

    @Test
    void testUpsertPromotionalContentMetadata_EmptyUserGroupDetails() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        ApiResponse result = promotionalContentService.upsertPromotionalContentMetadata(new HashMap<>(), AUTH_TOKEN);
        assertNotNull(result);
        assertEquals(HttpStatus.BAD_REQUEST, result.getResponseCode());
        assertEquals(Constants.FAILED, result.getParams().getStatus());
        assertTrue(result.getParams().getErrMsg().contains("User group details cannot be null or empty"));
    }

    @Test
    void testUpsertPromotionalContentMetadata_NullUserGroupDetails() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        ApiResponse result = promotionalContentService.upsertPromotionalContentMetadata(null, AUTH_TOKEN);
        assertNotNull(result);
        assertEquals(HttpStatus.BAD_REQUEST, result.getResponseCode());
        assertEquals(Constants.FAILED, result.getParams().getStatus());
    }

    @Test
    void testUpsertPromotionalContentMetadata_PayloadValidationFailure() {
        Map<String, Object> userGroupDetails = createValidUserGroupDetails();
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(payloadValidation.validateAccessControlPayload(userGroupDetails))
                .thenReturn("Invalid payload structure");
        ApiResponse result = promotionalContentService.upsertPromotionalContentMetadata(userGroupDetails, AUTH_TOKEN);
        assertNotNull(result);
        assertEquals(HttpStatus.BAD_REQUEST, result.getResponseCode());
        assertEquals(Constants.FAILED, result.getParams().getStatus());
        assertEquals("Invalid payload structure", result.getParams().getErrMsg());
    }

    @Test
    void testUpsertPromotionalContentMetadata_ProcessingFailure() throws Exception {
        Map<String, Object> userGroupDetails = createValidUserGroupDetails();
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(payloadValidation.validateAccessControlPayload(userGroupDetails)).thenReturn("");
        when(accessSettingMigrationService.processAccessSettingRule(any())).thenReturn(false);
        when(objectMapper.writeValueAsString(any())).thenReturn("{\"accessControl\":{}}");
        ApiResponse result = promotionalContentService.upsertPromotionalContentMetadata(userGroupDetails, AUTH_TOKEN);
        assertNotNull(result);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, result.getResponseCode());
        assertEquals(Constants.FAILED, result.getParams().getStatus());
        assertTrue(result.getParams().getErrMsg().contains("Failed to process access setting rule to id-map"));
    }

    @Test
    void testUpsertPromotionalContentMetadata_ExceptionDuringProcessing() throws Exception {
        Map<String, Object> userGroupDetails = createValidUserGroupDetails();
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(payloadValidation.validateAccessControlPayload(userGroupDetails)).thenReturn("");
        when(objectMapper.writeValueAsString(any())).thenThrow(new JsonProcessingException("Serialization failed") {});
        ApiResponse result = promotionalContentService.upsertPromotionalContentMetadata(userGroupDetails, AUTH_TOKEN);
        assertNotNull(result);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, result.getResponseCode());
        assertEquals(Constants.FAILED, result.getParams().getStatus());
        assertTrue(result.getParams().getErrMsg().contains("Failed to create access settings"));
    }

    @Test
    void testGetPromotionalContentForUsers_Success_FromCache() throws Exception {
        String cachedData = "[{\"id\":\"content1\",\"name\":\"Course 1\"}]";
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(redisCacheMgr.getFromCache(Constants.PROMOTIONAL_CONTENT_KEY + USER_ID)).thenReturn(cachedData);
        when(objectMapper.readValue(eq(cachedData), any(com.fasterxml.jackson.core.type.TypeReference.class)))
                .thenReturn(Collections.singletonList(Map.of("id", "content1", "name", "Course 1")));
        ApiResponse result = promotionalContentService.getPromotionalContentForUsers(AUTH_TOKEN);
        assertNotNull(result);
        assertEquals(HttpStatus.OK, result.getResponseCode());
        assertNotNull(result.getResult().get(Constants.CONTENT));
        verify(redisCacheMgr).getFromCache(Constants.PROMOTIONAL_CONTENT_KEY + USER_ID);
    }

    @Test
    void testGetPromotionalContentForUsers_CacheHit_NoRecordsFound() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(redisCacheMgr.getFromCache(Constants.PROMOTIONAL_CONTENT_KEY + USER_ID))
                .thenReturn(Constants.NO_RECORDS_FOUND);
        ApiResponse result = promotionalContentService.getPromotionalContentForUsers(AUTH_TOKEN);
        assertNotNull(result);
        assertEquals(HttpStatus.OK, result.getResponseCode());
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getResult().get(Constants.CONTENT);
        assertTrue(content.isEmpty());
    }

    @Test
    void testGetPromotionalContentForUsers_InvalidAuthToken() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn("");
        ApiResponse result = promotionalContentService.getPromotionalContentForUsers(AUTH_TOKEN);
        assertNotNull(result);
        assertEquals(HttpStatus.UNAUTHORIZED, result.getResponseCode());
        assertEquals(Constants.FAILED, result.getParams().getStatus());
    }

    @Test
    void testGetPromotionalContentForUsers_CacheMiss_Success() throws Exception {
        Map<String, Integer> userProfile = Map.of("designation", 1, "cadre", 2);
        List<CachedAccessSettingRule> rules = createMockAccessRules();
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(redisCacheMgr.getFromCache(Constants.PROMOTIONAL_CONTENT_KEY + USER_ID)).thenReturn(null);
        when(userProfileServiceImpl.getUserProfile(USER_ID)).thenReturn(userProfile);
        when(promotionalContentRuleCacheMgr.getAccessSettingRules()).thenReturn(rules);
        when(contentService.readContent(anyString(), anyList()))
                .thenReturn(Map.of("id", "content1", "name", "Course 1"));
        when(objectMapper.writeValueAsString(any())).thenReturn("[{\"id\":\"content1\"}]");
        ApiResponse result = promotionalContentService.getPromotionalContentForUsers(AUTH_TOKEN);
        assertNotNull(result);
        assertEquals(HttpStatus.OK, result.getResponseCode());
        verify(userProfileServiceImpl).getUserProfile(USER_ID);
        verify(promotionalContentRuleCacheMgr).getAccessSettingRules();
    }

    @Test
    void testGetPromotionalContentForUsers_NoAccessRules() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(redisCacheMgr.getFromCache(Constants.PROMOTIONAL_CONTENT_KEY + USER_ID)).thenReturn(null);
        when(userProfileServiceImpl.getUserProfile(USER_ID)).thenReturn(new HashMap<>());
        when(promotionalContentRuleCacheMgr.getAccessSettingRules()).thenReturn(Collections.emptyList());
        ApiResponse result = promotionalContentService.getPromotionalContentForUsers(AUTH_TOKEN);
        assertNotNull(result);
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getResult().get(Constants.CONTENT);
        assertTrue(content.isEmpty());
    }

    @Test
    void testGetPromotionalContentForUsers_JsonProcessingException() throws Exception {
        String cachedData = "[{\"invalid json";
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(redisCacheMgr.getFromCache(Constants.PROMOTIONAL_CONTENT_KEY + USER_ID)).thenReturn(cachedData);
        when(objectMapper.readValue(eq(cachedData), any(com.fasterxml.jackson.core.type.TypeReference.class)))
                .thenThrow(new JsonProcessingException("Parse error") {});
        ApiResponse result = promotionalContentService.getPromotionalContentForUsers(AUTH_TOKEN);
        assertNotNull(result);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, result.getResponseCode());
        assertEquals(Constants.FAILED, result.getParams().getStatus());
        assertTrue(result.getParams().getErrMsg().contains("Failed to parse cached promotional content data"));
    }

    @Test
    void testGetPromotionalContentForUsers_CachingException() throws Exception {
        Map<String, Integer> userProfile = Map.of("designation", 1);
        List<CachedAccessSettingRule> rules = createMockAccessRules();
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(redisCacheMgr.getFromCache(Constants.PROMOTIONAL_CONTENT_KEY + USER_ID)).thenReturn(null);
        when(userProfileServiceImpl.getUserProfile(USER_ID)).thenReturn(userProfile);
        when(promotionalContentRuleCacheMgr.getAccessSettingRules()).thenReturn(rules);
        when(contentService.readContent(anyString(), anyList()))
                .thenReturn(Map.of("id", "content1"));
        when(objectMapper.writeValueAsString(any())).thenThrow(new JsonProcessingException("Cache error") {});
        ApiResponse result = promotionalContentService.getPromotionalContentForUsers(AUTH_TOKEN);
        assertNotNull(result);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, result.getResponseCode());
    }

    @Test
    void testDelete_Success() {
        ApiResponse mockResponse = ApiResponse.createDefaultResponse("test");
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(mockResponse);
        ApiResponse result = promotionalContentService.delete(CONTENT_ID);
        assertNotNull(result);
        assertEquals(HttpStatus.OK, result.getResponseCode());
        assertTrue(result.getResult().get(Constants.MSG).toString()
                .contains("Promotional Content Metadata deleted successfully"));
        verify(cassandraOperation).insertRecord(eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.PROMOTIONAL_CONTENT_RULES), any());
    }

    @Test
    void testDelete_EmptyContentId() {
        ApiResponse result = promotionalContentService.delete("");
        assertNotNull(result);
        assertEquals(HttpStatus.BAD_REQUEST, result.getResponseCode());
        assertEquals(Constants.FAILED, result.getParams().getStatus());
        assertTrue(result.getParams().getErrMsg().contains("Content ID cannot be null or empty"));
    }

    @Test
    void testDelete_NullContentId() {
        ApiResponse result = promotionalContentService.delete(null);
        assertNotNull(result);
        assertEquals(HttpStatus.BAD_REQUEST, result.getResponseCode());
        assertEquals(Constants.FAILED, result.getParams().getStatus());
    }

    @Test
    void testDelete_ExceptionDuringDeletion() {
        when(cassandraOperation.insertRecord(anyString(), anyString(), any()))
                .thenThrow(new RuntimeException("Database error"));
        ApiResponse result = promotionalContentService.delete(CONTENT_ID);
        assertNotNull(result);
        assertEquals(HttpStatus.BAD_REQUEST, result.getResponseCode());
        assertEquals(Constants.FAILED, result.getParams().getStatus());
        assertTrue(result.getParams().getErrMsg().contains("Failed to delete access settings"));
    }

    @Test
    void testCreateUserGroupIds_WithExistingId() throws Exception {
        Map<String, Object> payload = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_ID, "existing-uuid");
        userGroups.add(userGroup);
        accessControl.put(Constants.USER_GROUPS, userGroups);
        payload.put(Constants.ACCESS_CONTROL, accessControl);
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(payloadValidation.validateAccessControlPayload(payload)).thenReturn("");
        when(accessSettingMigrationService.processAccessSettingRule(any())).thenReturn(true);
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        promotionalContentService.upsertPromotionalContentMetadata(payload, AUTH_TOKEN);
        assertEquals("existing-uuid", userGroup.get(Constants.USER_GROUP_ID));
    }

    @Test
    void testCreateUserGroupIds_WithoutId() throws Exception {
        Map<String, Object> payload = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();
        userGroups.add(userGroup);
        accessControl.put(Constants.USER_GROUPS, userGroups);
        payload.put(Constants.ACCESS_CONTROL, accessControl);
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(payloadValidation.validateAccessControlPayload(payload)).thenReturn("");
        when(accessSettingMigrationService.processAccessSettingRule(any())).thenReturn(true);
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        promotionalContentService.upsertPromotionalContentMetadata(payload, AUTH_TOKEN);
        assertNotNull(userGroup.get(Constants.USER_GROUP_ID));
    }

    @Test
    void testCreateUserGroupIds_InvalidAccessControl() throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.ACCESS_CONTROL, "invalid");
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(payloadValidation.validateAccessControlPayload(payload)).thenReturn("");
        when(accessSettingMigrationService.processAccessSettingRule(any())).thenReturn(true);
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        ApiResponse result = promotionalContentService.upsertPromotionalContentMetadata(payload, AUTH_TOKEN);
        assertNotNull(result);
    }

    @Test
    void testCreateUserGroupIds_InvalidUserGroups() throws Exception {
        Map<String, Object> payload = new HashMap<>();
        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.USER_GROUPS, "invalid");
        payload.put(Constants.ACCESS_CONTROL, accessControl);
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(payloadValidation.validateAccessControlPayload(payload)).thenReturn("");
        when(accessSettingMigrationService.processAccessSettingRule(any())).thenReturn(true);
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        ApiResponse result = promotionalContentService.upsertPromotionalContentMetadata(payload, AUTH_TOKEN);
        assertNotNull(result);
    }


    private Map<String, Object> createValidUserGroupDetails() {
        Map<String, Object> userGroupDetails = new HashMap<>();
        userGroupDetails.put(Constants.CONTENT_ID, CONTENT_ID);
        Map<String, Object> accessControl = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_ID, UUID.randomUUID().toString());
        userGroup.put(Constants.USER_GROUP_NAME, "Test Group");
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, "designation");
        criteria.put(Constants.CRITERIA_VALUE, Arrays.asList(1, 2, 3));
        criteriaList.add(criteria);
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        userGroups.add(userGroup);
        accessControl.put(Constants.USER_GROUPS, userGroups);
        userGroupDetails.put(Constants.ACCESS_CONTROL, accessControl);
        return userGroupDetails;
    }

    @Test
    void testGetPromotionalContentForUsers_NoMatchingRules() {
        Map<String, Integer> userProfile = Map.of("designation", 99);
        List<CachedAccessSettingRule> rules = createMockAccessRulesForNonMatchingTest();
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(redisCacheMgr.getFromCache(Constants.PROMOTIONAL_CONTENT_KEY + USER_ID)).thenReturn(null);
        when(userProfileServiceImpl.getUserProfile(USER_ID)).thenReturn(userProfile);
        when(promotionalContentRuleCacheMgr.getAccessSettingRules()).thenReturn(rules);
        ApiResponse result = promotionalContentService.getPromotionalContentForUsers(AUTH_TOKEN);
        assertNotNull(result);
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getResult().get(Constants.CONTENT);
        assertTrue(content.isEmpty());
        verify(redisCacheMgr).putInCache(Constants.ACCESS_KEY + USER_ID, Constants.NO_RECORDS_FOUND);
    }

    @Test
    void testEvaluateAccessSettingRule_EmptyCriteriaList() {
        Map<String, Integer> userProfile = Map.of("designation", 1);
        List<CachedAccessSettingRule> rules = createMockAccessRulesWithEmptyCriteria();
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(redisCacheMgr.getFromCache(Constants.PROMOTIONAL_CONTENT_KEY + USER_ID)).thenReturn(null);
        when(userProfileServiceImpl.getUserProfile(USER_ID)).thenReturn(userProfile);
        when(promotionalContentRuleCacheMgr.getAccessSettingRules()).thenReturn(rules);
        ApiResponse result = promotionalContentService.getPromotionalContentForUsers(AUTH_TOKEN);
        assertNotNull(result);
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getResult().get(Constants.CONTENT);
        assertTrue(content.isEmpty());
    }

    @Test
    void testEvaluateAccessSettingRule_NullUserProfile() {
        List<CachedAccessSettingRule> rules = createMockAccessRulesWithNullCheck();
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(redisCacheMgr.getFromCache(Constants.PROMOTIONAL_CONTENT_KEY + USER_ID)).thenReturn(null);
        when(userProfileServiceImpl.getUserProfile(USER_ID)).thenReturn(null);
        when(promotionalContentRuleCacheMgr.getAccessSettingRules()).thenReturn(rules);
        ApiResponse result = promotionalContentService.getPromotionalContentForUsers(AUTH_TOKEN);
        assertNotNull(result);
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getResult().get(Constants.CONTENT);
        assertTrue(content.isEmpty());
    }

    @Test
    void testEvaluateAccessSettingRule_UserProfileMissingCriteria() {
        Map<String, Integer> userProfile = Map.of("designation", 1);
        List<CachedAccessSettingRule> rules = createMockAccessRulesWithMultipleCriteria();
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(redisCacheMgr.getFromCache(Constants.PROMOTIONAL_CONTENT_KEY + USER_ID)).thenReturn(null);
        when(userProfileServiceImpl.getUserProfile(USER_ID)).thenReturn(userProfile);
        when(promotionalContentRuleCacheMgr.getAccessSettingRules()).thenReturn(rules);
        ApiResponse result = promotionalContentService.getPromotionalContentForUsers(AUTH_TOKEN);
        assertNotNull(result);
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getResult().get(Constants.CONTENT);
        assertTrue(content.isEmpty());
    }

    @Test
    void testEvaluateAccessSettingRule_EmptyAccessSettingIdMap() {
        List<CachedAccessSettingRule> rules = new ArrayList<>();
        CachedAccessSettingRule rule = mock(CachedAccessSettingRule.class);
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL_ID, new HashMap<>());
        when(rule.getContextData()).thenReturn(contextData);
        rules.add(rule);
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(redisCacheMgr.getFromCache(Constants.PROMOTIONAL_CONTENT_KEY + USER_ID)).thenReturn(null);
        when(userProfileServiceImpl.getUserProfile(USER_ID)).thenReturn(Map.of("designation", 1));
        when(promotionalContentRuleCacheMgr.getAccessSettingRules()).thenReturn(rules);
        ApiResponse result = promotionalContentService.getPromotionalContentForUsers(AUTH_TOKEN);
        assertNotNull(result);
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getResult().get(Constants.CONTENT);
        assertTrue(content.isEmpty());
    }

    @Test
    void testEvaluateAccessSettingRule_EmptyUserGroups() {
        List<CachedAccessSettingRule> rules = new ArrayList<>();
        CachedAccessSettingRule rule = mock(CachedAccessSettingRule.class);
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControlId = new HashMap<>();
        accessControlId.put(Constants.USER_GROUPS, new ArrayList<>());
        contextData.put(Constants.ACCESS_CONTROL_ID, accessControlId);
        when(rule.getContextData()).thenReturn(contextData);
        rules.add(rule);
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(redisCacheMgr.getFromCache(Constants.PROMOTIONAL_CONTENT_KEY + USER_ID)).thenReturn(null);
        when(userProfileServiceImpl.getUserProfile(USER_ID)).thenReturn(Map.of("designation", 1));
        when(promotionalContentRuleCacheMgr.getAccessSettingRules()).thenReturn(rules);
        ApiResponse result = promotionalContentService.getPromotionalContentForUsers(AUTH_TOKEN);
        assertNotNull(result);
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getResult().get(Constants.CONTENT);
        assertTrue(content.isEmpty());
    }

    @Test
    void testEvaluateAccessSettingRule_MultipleUserGroupsFirstFails() throws Exception {
        Map<String, Integer> userProfile = Map.of("designation", 1, "cadre", 2);
        List<CachedAccessSettingRule> rules = createMockAccessRulesWithMultipleGroups();
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(redisCacheMgr.getFromCache(Constants.PROMOTIONAL_CONTENT_KEY + USER_ID)).thenReturn(null);
        when(userProfileServiceImpl.getUserProfile(USER_ID)).thenReturn(userProfile);
        when(promotionalContentRuleCacheMgr.getAccessSettingRules()).thenReturn(rules);
        when(contentService.readContent(anyString(), anyList()))
                .thenReturn(Map.of("id", "content1"));
        when(objectMapper.writeValueAsString(any())).thenReturn("[]");
        ApiResponse result = promotionalContentService.getPromotionalContentForUsers(AUTH_TOKEN);
        assertNotNull(result);
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getResult().get(Constants.CONTENT);
        assertFalse(content.isEmpty());
    }

    @Test
    void testEvaluateAccessSettingRule_CriteriaValueNotInBitSet() {
        Map<String, Integer> userProfile = Map.of("designation", 5);
        List<CachedAccessSettingRule> rules = createMockAccessRulesForNonMatchingTest();
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(redisCacheMgr.getFromCache(Constants.PROMOTIONAL_CONTENT_KEY + USER_ID)).thenReturn(null);
        when(userProfileServiceImpl.getUserProfile(USER_ID)).thenReturn(userProfile);
        when(promotionalContentRuleCacheMgr.getAccessSettingRules()).thenReturn(rules);
        ApiResponse result = promotionalContentService.getPromotionalContentForUsers(AUTH_TOKEN);
        assertNotNull(result);
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.getResult().get(Constants.CONTENT);
        assertTrue(content.isEmpty());
    }

    private List<CachedAccessSettingRule> createMockAccessRulesForNonMatchingTest() {
        List<CachedAccessSettingRule> rules = new ArrayList<>();
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControlId = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_ID, "group-1");
        userGroup.put(Constants.USER_GROUP_NAME, "Test Group");
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, "designation");
        BitSet bitSet = new BitSet();
        bitSet.set(1);
        bitSet.set(2);
        criteria.put(Constants.CRITERIA_VALUE, bitSet);
        criteriaList.add(criteria);
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        userGroups.add(userGroup);
        accessControlId.put(Constants.USER_GROUPS, userGroups);
        contextData.put(Constants.ACCESS_CONTROL_ID, accessControlId);
        CachedAccessSettingRule rule = mock(CachedAccessSettingRule.class);
        when(rule.getContextData()).thenReturn(contextData);
        rules.add(rule);
        return rules;
    }

    private List<CachedAccessSettingRule> createMockAccessRules() {
        List<CachedAccessSettingRule> rules = new ArrayList<>();
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControlId = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_ID, "group-1");
        userGroup.put(Constants.USER_GROUP_NAME, "Test Group");
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, "designation");
        BitSet bitSet = new BitSet();
        bitSet.set(1);
        bitSet.set(2);
        criteria.put(Constants.CRITERIA_VALUE, bitSet);
        criteriaList.add(criteria);
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        userGroups.add(userGroup);
        accessControlId.put(Constants.USER_GROUPS, userGroups);
        contextData.put(Constants.ACCESS_CONTROL_ID, accessControlId);
        CachedAccessSettingRule rule = mock(CachedAccessSettingRule.class);
        when(rule.getContextData()).thenReturn(contextData);
        when(rule.getContextId()).thenReturn("content-123");
        rules.add(rule);
        return rules;
    }

    private List<CachedAccessSettingRule> createMockAccessRulesWithEmptyCriteria() {
        List<CachedAccessSettingRule> rules = new ArrayList<>();
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControlId = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_ID, "group-1");
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, new ArrayList<>());
        userGroups.add(userGroup);
        accessControlId.put(Constants.USER_GROUPS, userGroups);
        contextData.put(Constants.ACCESS_CONTROL_ID, accessControlId);
        CachedAccessSettingRule rule = mock(CachedAccessSettingRule.class);
        when(rule.getContextData()).thenReturn(contextData);
        rules.add(rule);
        return rules;
    }

    private List<CachedAccessSettingRule> createMockAccessRulesWithNullCheck() {
        List<CachedAccessSettingRule> rules = new ArrayList<>();
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControlId = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_ID, "group-1");
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(Constants.CRITERIA_KEY, "designation");
        BitSet bitSet = new BitSet();
        bitSet.set(1);
        criteria.put(Constants.CRITERIA_VALUE, bitSet);
        criteriaList.add(criteria);
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        userGroups.add(userGroup);
        accessControlId.put(Constants.USER_GROUPS, userGroups);
        contextData.put(Constants.ACCESS_CONTROL_ID, accessControlId);
        CachedAccessSettingRule rule = mock(CachedAccessSettingRule.class);
        when(rule.getContextData()).thenReturn(contextData);
        rules.add(rule);
        return rules;
    }

    private List<CachedAccessSettingRule> createMockAccessRulesWithMultipleCriteria() {
        List<CachedAccessSettingRule> rules = new ArrayList<>();
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControlId = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.USER_GROUP_ID, "group-1");
        List<Map<String, Object>> criteriaList = new ArrayList<>();
        Map<String, Object> criteria1 = new HashMap<>();
        criteria1.put(Constants.CRITERIA_KEY, "designation");
        BitSet bitSet1 = new BitSet();
        bitSet1.set(1);
        criteria1.put(Constants.CRITERIA_VALUE, bitSet1);
        criteriaList.add(criteria1);
        Map<String, Object> criteria2 = new HashMap<>();
        criteria2.put(Constants.CRITERIA_KEY, "cadre");
        BitSet bitSet2 = new BitSet();
        bitSet2.set(2);
        criteria2.put(Constants.CRITERIA_VALUE, bitSet2);
        criteriaList.add(criteria2);
        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
        userGroups.add(userGroup);
        accessControlId.put(Constants.USER_GROUPS, userGroups);
        contextData.put(Constants.ACCESS_CONTROL_ID, accessControlId);
        CachedAccessSettingRule rule = mock(CachedAccessSettingRule.class);
        when(rule.getContextData()).thenReturn(contextData);
        rules.add(rule);
        return rules;
    }

    private List<CachedAccessSettingRule> createMockAccessRulesWithMultipleGroups() {
        List<CachedAccessSettingRule> rules = new ArrayList<>();
        Map<String, Object> contextData = new HashMap<>();
        Map<String, Object> accessControlId = new HashMap<>();
        List<Map<String, Object>> userGroups = new ArrayList<>();
        Map<String, Object> userGroup1 = new HashMap<>();
        userGroup1.put(Constants.USER_GROUP_ID, "group-1");
        List<Map<String, Object>> criteriaList1 = new ArrayList<>();
        Map<String, Object> criteria1 = new HashMap<>();
        criteria1.put(Constants.CRITERIA_KEY, "designation");
        BitSet bitSet1 = new BitSet();
        bitSet1.set(99);
        criteria1.put(Constants.CRITERIA_VALUE, bitSet1);
        criteriaList1.add(criteria1);
        userGroup1.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList1);
        userGroups.add(userGroup1);
        Map<String, Object> userGroup2 = new HashMap<>();
        userGroup2.put(Constants.USER_GROUP_ID, "group-2");
        List<Map<String, Object>> criteriaList2 = new ArrayList<>();
        Map<String, Object> criteria2 = new HashMap<>();
        criteria2.put(Constants.CRITERIA_KEY, "designation");
        BitSet bitSet2 = new BitSet();
        bitSet2.set(1);
        criteria2.put(Constants.CRITERIA_VALUE, bitSet2);
        criteriaList2.add(criteria2);
        Map<String, Object> criteria3 = new HashMap<>();
        criteria3.put(Constants.CRITERIA_KEY, "cadre");
        BitSet bitSet3 = new BitSet();
        bitSet3.set(2);
        criteria3.put(Constants.CRITERIA_VALUE, bitSet3);
        criteriaList2.add(criteria3);
        userGroup2.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList2);
        userGroups.add(userGroup2);
        accessControlId.put(Constants.USER_GROUPS, userGroups);
        contextData.put(Constants.ACCESS_CONTROL_ID, accessControlId);
        CachedAccessSettingRule rule = mock(CachedAccessSettingRule.class);
        when(rule.getContextData()).thenReturn(contextData);
        when(rule.getContextId()).thenReturn("content-123");
        rules.add(rule);
        return rules;
    }
}
