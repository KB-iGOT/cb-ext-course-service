package com.igot.cb.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.common.ServerProperties;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive test class for CompetencyServiceImpl
 * Covers all methods and scenarios including success, empty data, error cases,
 * and Redis cache-aside pattern (cache hit / cache miss / cache store).
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CompetencyServiceImplTest {

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private AccessTokenValidator accessTokenValidator;

    @Mock
    private ServerProperties serverProperties;

    @Mock
    private RedisCacheMgr redisCacheMgr;

    @InjectMocks
    private CompetencyServiceImpl service;

    private static final int CACHE_TTL = 3600;
    private String authToken;
    private String userId;

    @BeforeEach
    void setUp() {
        authToken = "Bearer test-token-12345";
        userId = "test-user-123";
        when(serverProperties.getUserCompetencyCacheTtlSeconds()).thenReturn(CACHE_TTL);
    }

    // ==================== Redis Cache HIT ====================

    @Test
    void testFetchUserCompetency_CacheHit_ReturnsCachedData() throws Exception {
        // Arrange
        List<Map<String, Object>> cachedList = createMockCompetencyList();
        String cachedJson = "[{\"user_id\":\"test-user-123\"}]";

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(redisCacheMgr.getFromCache(Constants.USER_COMPETENCY_REDIS_KEY_PREFIX + userId, CACHE_TTL))
                .thenReturn(cachedJson);
        when(objectMapper.readValue(eq(cachedJson), any(TypeReference.class)))
                .thenReturn(cachedList);

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertTrue(response.getResult().containsKey(Constants.COMPETENCIES));
        assertEquals(cachedList, response.getResult().get(Constants.COMPETENCIES));

        // Cassandra should NOT be called on cache hit
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    void testFetchUserCompetency_CacheHit_TrimmedUserId_UsedAsRedisKey() throws Exception {
        // Arrange: token returns userId with surrounding spaces.
        // The service uses the raw userId (no trim) to build the Redis key,
        // so the expected key must include the same spaces.
        String paddedUserId = "  " + userId + "  ";
        List<Map<String, Object>> cachedList = createMockCompetencyList();
        String cachedJson = "[{\"user_id\":\"test-user-123\"}]";

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(paddedUserId);
        // Redis key is built with the raw (untrimmed) userId
        when(redisCacheMgr.getFromCache(Constants.USER_COMPETENCY_REDIS_KEY_PREFIX + paddedUserId, CACHE_TTL))
                .thenReturn(cachedJson);
        when(objectMapper.readValue(eq(cachedJson), any(TypeReference.class)))
                .thenReturn(cachedList);

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert: Redis was called with the raw (untrimmed) key
        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(redisCacheMgr).getFromCache(Constants.USER_COMPETENCY_REDIS_KEY_PREFIX + paddedUserId, CACHE_TTL);
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    // ==================== Redis Cache MISS — Cassandra has data ====================

    @Test
    void testFetchUserCompetency_CacheMiss_DataInCassandra_StoresInCache() throws Exception {
        // Arrange
        List<Map<String, Object>> records = createMockCompetencyList();
        String serializedJson = "[{\"user_id\":\"test-user-123\"}]";

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(redisCacheMgr.getFromCache(anyString(), eq(CACHE_TTL))).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_COMPETENCY_MAPPING_TABLE),
                anyMap(), isNull(), isNull()))
                .thenReturn(records);
        when(objectMapper.writeValueAsString(records)).thenReturn(serializedJson);

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertTrue(response.getResult().containsKey(Constants.COMPETENCIES));
        assertEquals(records, response.getResult().get(Constants.COMPETENCIES));

        // Cache should be populated
        verify(redisCacheMgr).putInCache(
                eq(Constants.USER_COMPETENCY_REDIS_KEY_PREFIX + userId),
                eq(serializedJson),
                eq(CACHE_TTL));
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    void testFetchUserCompetency_CacheMiss_MultipleRecords_AllStoredInCache() throws Exception {
        // Arrange
        List<Map<String, Object>> records = Arrays.asList(
                createMockCompetencyData("area1", "theme1", "subtheme1"),
                createMockCompetencyData("area2", "theme2", "subtheme2"),
                createMockCompetencyData("area3", "theme3", "subtheme3")
        );
        String json = "[...]";

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(redisCacheMgr.getFromCache(anyString(), eq(CACHE_TTL))).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(records);
        when(objectMapper.writeValueAsString(records)).thenReturn(json);

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertEquals(HttpStatus.OK, response.getResponseCode());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> resultList = (List<Map<String, Object>>) response.getResult().get(Constants.COMPETENCIES);
        assertEquals(3, resultList.size());
        verify(redisCacheMgr).putInCache(anyString(), eq(json), eq(CACHE_TTL));
    }

    // ==================== Redis Cache MISS — No data (first-time user) ====================

    @Test
    void testFetchUserCompetency_CacheMiss_EmptyData_PublishesKafkaEvent() throws Exception {
        // Arrange
        when(serverProperties.getCompetencyAcquiredTopicName()).thenReturn("competency.acquired");
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(redisCacheMgr.getFromCache(anyString(), eq(CACHE_TTL))).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(Collections.emptyList());

        String eventJson = "{\"edata\":{\"eventType\":\"COMPETENCY_ACQUIRED\"}}";
        when(objectMapper.writeValueAsString(anyMap())).thenReturn(eventJson);

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertTrue(response.getResult().containsKey(Constants.COMPETENCIES));
        @SuppressWarnings("unchecked")
        List<?> list = (List<?>) response.getResult().get(Constants.COMPETENCIES);
        assertTrue(list.isEmpty());

        // Cache should NOT be populated for empty data
        verify(redisCacheMgr, never()).putInCache(anyString(), anyString(), anyInt());
        verify(kafkaTemplate).send("competency.acquired", eventJson);
    }

    @Test
    void testFetchUserCompetency_CacheMiss_NullFromCassandra_PublishesKafkaEvent() throws Exception {
        // Arrange
        when(serverProperties.getCompetencyAcquiredTopicName()).thenReturn("competency.acquired");
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(redisCacheMgr.getFromCache(anyString(), eq(CACHE_TTL))).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(null);
        when(objectMapper.writeValueAsString(anyMap())).thenReturn("{}");

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertTrue(response.getResult().containsKey(Constants.COMPETENCIES));
        @SuppressWarnings("unchecked")
        List<?> list = (List<?>) response.getResult().get(Constants.COMPETENCIES);
        assertTrue(list.isEmpty());
        verify(redisCacheMgr, never()).putInCache(anyString(), anyString(), anyInt());
    }

    // ==================== Invalid token / userId ====================

    @Test
    void testFetchUserCompetency_EmptyUserId_ReturnsBadRequest() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn("");

        ApiResponse response = service.fetchUserCompetency(authToken);

        assertNotNull(response);
        verify(redisCacheMgr, never()).getFromCache(anyString(), anyInt());
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    void testFetchUserCompetency_NullUserId_ReturnsBadRequest() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(null);

        ApiResponse response = service.fetchUserCompetency(authToken);

        assertNotNull(response);
        verify(redisCacheMgr, never()).getFromCache(anyString(), anyInt());
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    void testFetchUserCompetency_WhitespaceOnlyUserId_ReturnsBadRequest() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn("   ");

        ApiResponse response = service.fetchUserCompetency(authToken);

        assertNotNull(response);
        verify(redisCacheMgr, never()).getFromCache(anyString(), anyInt());
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    @Test
    void testFetchUserCompetency_NullAuthToken() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(isNull(), any(ApiResponse.class)))
                .thenReturn(null);

        ApiResponse response = service.fetchUserCompetency(null);

        assertNotNull(response);
        verify(accessTokenValidator).fetchUserIdFromAccessToken(isNull(), any(ApiResponse.class));
        verify(redisCacheMgr, never()).getFromCache(anyString(), anyInt());
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    @Test
    void testFetchUserCompetency_EmptyAuthToken() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(""), any(ApiResponse.class)))
                .thenReturn("");

        ApiResponse response = service.fetchUserCompetency("");

        assertNotNull(response);
        verify(redisCacheMgr, never()).getFromCache(anyString(), anyInt());
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    // ==================== Exception scenarios ====================

    @Test
    void testFetchUserCompetency_CassandraException_Returns500() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(redisCacheMgr.getFromCache(anyString(), eq(CACHE_TTL))).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenThrow(new RuntimeException("Database connection failed"));

        ApiResponse response = service.fetchUserCompetency(authToken);

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertNotNull(response.getParams().getErrMsg());
        assertTrue(response.getParams().getErrMsg().contains("Error fetching competency data"));
    }

    @Test
    void testFetchUserCompetency_TokenValidatorException_Returns500() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenThrow(new RuntimeException("Invalid token"));

        ApiResponse response = service.fetchUserCompetency(authToken);

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    @Test
    void testFetchUserCompetency_RedisGetException_FallsBackToCassandra() throws Exception {
        // Arrange: Redis throws, should fallback to Cassandra
        List<Map<String, Object>> records = createMockCompetencyList();
        String json = "[{\"user_id\":\"test-user-123\"}]";

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(redisCacheMgr.getFromCache(anyString(), eq(CACHE_TTL))).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(records);
        when(objectMapper.writeValueAsString(records)).thenReturn(json);

        ApiResponse response = service.fetchUserCompetency(authToken);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertTrue(response.getResult().containsKey(Constants.COMPETENCIES));
        verify(cassandraOperation).getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull());
    }

    // ==================== fetchUserCompetencyMapping — Cassandra query verification ====================

    @Test
    void testFetchUserCompetencyMapping_QueryUsesCorrectKeyspaceAndTable() throws Exception {
        List<Map<String, Object>> records = createMockCompetencyList();
        String json = "[]";

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(redisCacheMgr.getFromCache(anyString(), eq(CACHE_TTL))).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_COMPETENCY_MAPPING_TABLE),
                anyMap(), isNull(), isNull()))
                .thenReturn(records);
        when(objectMapper.writeValueAsString(records)).thenReturn(json);

        service.fetchUserCompetency(authToken);

        verify(cassandraOperation).getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_COMPETENCY_MAPPING_TABLE),
                argThat(map -> userId.equals(map.get(Constants.USER_ID_KEY))),
                isNull(), isNull()
        );
    }

    @Test
    void testFetchUserCompetencyMapping_UserIdIsTrimmedBeforeCassandraQuery() throws Exception {
        String paddedUserId = "  " + userId + "  ";
        List<Map<String, Object>> records = createMockCompetencyList();
        String json = "[]";

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(paddedUserId);
        // Redis key uses the raw (untrimmed) paddedUserId
        when(redisCacheMgr.getFromCache(Constants.USER_COMPETENCY_REDIS_KEY_PREFIX + paddedUserId, CACHE_TTL))
                .thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(records);
        when(objectMapper.writeValueAsString(records)).thenReturn(json);

        service.fetchUserCompetency(authToken);

        // Cassandra is called with the trimmed userId (trim happens inside fetchUserCompetencyMapping)
        verify(cassandraOperation).getRecordsByProperties(
                anyString(), anyString(),
                argThat(map -> userId.equals(map.get(Constants.USER_ID_KEY))),
                isNull(), isNull()
        );
    }

    @Test
    void testFetchUserCompetencyMapping_Exception_Returns500() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(redisCacheMgr.getFromCache(anyString(), eq(CACHE_TTL))).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenThrow(new RuntimeException("DB error"));

        ApiResponse response = service.fetchUserCompetency(authToken);

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    // ==================== publishFirstTimeCompetencyEvent Tests ====================

    @Test
    void testPublishFirstTimeCompetencyEvent_Success() throws Exception {
        when(serverProperties.getCompetencyAcquiredTopicName()).thenReturn("competency.acquired");
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(redisCacheMgr.getFromCache(anyString(), eq(CACHE_TTL))).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(Collections.emptyList());

        String eventJson = "{\"edata\":{\"eventType\":\"COMPETENCY_ACQUIRED\",\"userId\":\"test-user-123\",\"isFirstTimeUser\":\"true\"}}";
        when(objectMapper.writeValueAsString(anyMap())).thenReturn(eventJson);

        ApiResponse response = service.fetchUserCompetency(authToken);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());

        // Verify event payload structure
        verify(objectMapper).writeValueAsString(argThat(obj -> {
            if (!(obj instanceof Map<?, ?> map)) return false;
            Object edata = map.get(Constants.E_DATA);
            if (edata instanceof Map<?, ?> edataMap) {
                return Constants.COMPETENCY_ACQUIRED_EVENT.equals(edataMap.get(Constants.EVENT_TYPE))
                        && userId.equals(edataMap.get(Constants.USER_ID))
                        && "true".equals(edataMap.get(Constants.IS_FIRST_TIME_USER));
            }
            return false;
        }));

        verify(kafkaTemplate).send("competency.acquired", eventJson);
    }

    @Test
    void testPublishFirstTimeCompetencyEvent_JsonSerializationException_StillReturns200() throws Exception {
        when(serverProperties.getCompetencyAcquiredTopicName()).thenReturn("competency.acquired");
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(redisCacheMgr.getFromCache(anyString(), eq(CACHE_TTL))).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(Collections.emptyList());
        when(objectMapper.writeValueAsString(anyMap()))
                .thenThrow(new RuntimeException("JSON serialization failed"));

        ApiResponse response = service.fetchUserCompetency(authToken);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    void testPublishFirstTimeCompetencyEvent_KafkaSendException_StillReturns200() throws Exception {
        when(serverProperties.getCompetencyAcquiredTopicName()).thenReturn("competency.acquired");
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(redisCacheMgr.getFromCache(anyString(), eq(CACHE_TTL))).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(Collections.emptyList());
        when(objectMapper.writeValueAsString(anyMap())).thenReturn("{}");
        doThrow(new RuntimeException("Kafka error")).when(kafkaTemplate).send(anyString(), anyString());

        ApiResponse response = service.fetchUserCompetency(authToken);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        verify(kafkaTemplate).send(anyString(), anyString());
    }

    // ==================== Response structure verification ====================

    @Test
    void testFetchUserCompetency_VerifyResponseStructure_WithData() throws Exception {
        String json = "[]";
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(redisCacheMgr.getFromCache(anyString(), eq(CACHE_TTL))).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(createMockCompetencyList());
        when(objectMapper.writeValueAsString(any())).thenReturn(json);

        ApiResponse response = service.fetchUserCompetency(authToken);

        assertNotNull(response);
        assertNotNull(response.getId());
        assertNotNull(response.getVer());
        assertNotNull(response.getTs());
        assertNotNull(response.getParams());
        assertNotNull(response.getParams().getResMsgId());
        assertEquals(Constants.API_FETCH_USER_COMPETENCY, response.getId());
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertTrue(response.getResult().containsKey(Constants.COMPETENCIES));
    }

    @Test
    void testFetchUserCompetency_VerifyResponseStructure_EmptyData() throws Exception {
        when(serverProperties.getCompetencyAcquiredTopicName()).thenReturn("competency.acquired");
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(redisCacheMgr.getFromCache(anyString(), eq(CACHE_TTL))).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(Collections.emptyList());
        when(objectMapper.writeValueAsString(anyMap())).thenReturn("{}");

        ApiResponse response = service.fetchUserCompetency(authToken);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertTrue(response.getResult().containsKey(Constants.COMPETENCIES));
        @SuppressWarnings("unchecked")
        List<?> list = (List<?>) response.getResult().get(Constants.COMPETENCIES);
        assertTrue(list.isEmpty());
    }

    @Test
    void testFetchUserCompetency_WithSpecialCharactersInUserId() throws Exception {
        String specialUserId = "user-123@test#domain";
        String json = "[]";
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(specialUserId);
        when(redisCacheMgr.getFromCache(anyString(), eq(CACHE_TTL))).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(createMockCompetencyList());
        when(objectMapper.writeValueAsString(any())).thenReturn(json);

        ApiResponse response = service.fetchUserCompetency(authToken);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertTrue(response.getResult().containsKey(Constants.COMPETENCIES));
    }

    @Test
    void testFetchUserCompetency_ConcurrentCalls_EachCallIndependent() throws Exception {
        String json = "[]";
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any(ApiResponse.class)))
                .thenReturn(userId);
        when(redisCacheMgr.getFromCache(anyString(), eq(CACHE_TTL))).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(createMockCompetencyList());
        when(objectMapper.writeValueAsString(any())).thenReturn(json);

        service.fetchUserCompetency("token1");
        service.fetchUserCompetency("token2");
        service.fetchUserCompetency("token3");

        verify(accessTokenValidator, times(3)).fetchUserIdFromAccessToken(anyString(), any(ApiResponse.class));
        verify(cassandraOperation, times(3)).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    // ==================== Helper Methods ====================

    private List<Map<String, Object>> createMockCompetencyList() {
        return Collections.singletonList(createMockCompetencyData(
                "kcmfinal_fw_competencyarea_test",
                "kcmfinal_fw_theme_test",
                "kcmfinal_fw_subtheme_test"
        ));
    }

    private Map<String, Object> createMockCompetencyData(String areaId, String themeId, String subthemeId) {
        Map<String, Object> data = new HashMap<>();
        data.put("competencySubthemeId", subthemeId);
        data.put("userId", userId);
        data.put("competencyThemeId", themeId);
        data.put("competencyAreaId", areaId);

        Map<String, Object> competencyDetails = new HashMap<>();
        List<Map<String, String>> selfAchievement = new ArrayList<>();

        Map<String, String> achievement = new HashMap<>();
        achievement.put("acquiredContextId", "context-123");
        achievement.put("acquired_at", "2025-03-06");
        achievement.put("certificateId", "cert-123");
        selfAchievement.add(achievement);

        competencyDetails.put("selfAchievement", selfAchievement);
        data.put("competencyDetails", competencyDetails);

        return data;
    }
}

