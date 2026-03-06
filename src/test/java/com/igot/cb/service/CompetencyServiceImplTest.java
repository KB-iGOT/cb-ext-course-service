package com.igot.cb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
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
 * Covers all methods and scenarios including success, empty data, and error cases
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

    @InjectMocks
    private CompetencyServiceImpl service;

    private String authToken;
    private String userId;

    @BeforeEach
    void setUp() {
        authToken = "Bearer test-token-12345";
        userId = "test-user-123";
    }

    // ==================== fetchUserCompetency - With Data ====================

    @Test
    void testFetchUserCompetency_Success_WithData() {
        // Arrange
        List<Map<String, Object>> records = createMockCompetencyList();

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_COMPETENCY_MAPPING_TABLE),
                anyMap(),
                isNull(),
                isNull()
        )).thenReturn(records);

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertNotNull(response.getResult());
        // Result is wrapped under "competencies" key containing the full list
        assertTrue(response.getResult().containsKey(Constants.COMPETENCIES));
        assertEquals(records, response.getResult().get(Constants.COMPETENCIES));

        verify(accessTokenValidator).fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class));
        verify(cassandraOperation).getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull());
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    void testFetchUserCompetency_Success_MultipleRecords() {
        // Arrange
        List<Map<String, Object>> records = Arrays.asList(
                createMockCompetencyData("area1", "theme1", "subtheme1"),
                createMockCompetencyData("area2", "theme2", "subtheme2"),
                createMockCompetencyData("area3", "theme3", "subtheme3")
        );

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(records);

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> resultList =
                (List<Map<String, Object>>) response.getResult().get(Constants.COMPETENCIES);
        assertEquals(3, resultList.size());
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    // ==================== fetchUserCompetency - Empty Data (first-time user) ====================

    @Test
    void testFetchUserCompetency_EmptyData_PublishesKafkaEvent() throws Exception {
        // Arrange
        when(serverProperties.getCompetencyAcquiredTopicName()).thenReturn("competency.acquired");
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
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
        // Result has "competencies" key with empty list
        assertTrue(response.getResult().containsKey(Constants.COMPETENCIES));
        @SuppressWarnings("unchecked")
        List<?> list = (List<?>) response.getResult().get(Constants.COMPETENCIES);
        assertTrue(list.isEmpty());

        verify(kafkaTemplate).send("competency.acquired", eventJson);
        verify(objectMapper).writeValueAsString(anyMap());
    }

    @Test
    void testFetchUserCompetency_CassandraReturnsNull_PublishesKafkaEvent() throws Exception {
        // Arrange
        when(serverProperties.getCompetencyAcquiredTopicName()).thenReturn("competency.acquired");
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
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
    }

    // ==================== fetchUserCompetency - Invalid token / userId ====================

    @Test
    void testFetchUserCompetency_EmptyUserId_ReturnsBadRequest() {
        // Arrange
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn("");

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    void testFetchUserCompetency_NullUserId_ReturnsBadRequest() {
        // Arrange
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(null);

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    void testFetchUserCompetency_WithWhitespaceUserId_ReturnsBadRequest() {
        // Arrange
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn("   ");

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    void testFetchUserCompetency_NullAuthToken() {
        // Arrange
        when(accessTokenValidator.fetchUserIdFromAccessToken(isNull(), any(ApiResponse.class)))
                .thenReturn(null);

        // Act
        ApiResponse response = service.fetchUserCompetency(null);

        // Assert
        assertNotNull(response);
        verify(accessTokenValidator).fetchUserIdFromAccessToken(isNull(), any(ApiResponse.class));
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    @Test
    void testFetchUserCompetency_EmptyAuthToken() {
        // Arrange
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(""), any(ApiResponse.class)))
                .thenReturn("");

        // Act
        ApiResponse response = service.fetchUserCompetency("");

        // Assert
        assertNotNull(response);
        verify(accessTokenValidator).fetchUserIdFromAccessToken(eq(""), any(ApiResponse.class));
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    // ==================== fetchUserCompetency - Exception scenarios ====================

    @Test
    void testFetchUserCompetency_CassandraException_Returns500() {
        // Arrange
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenThrow(new RuntimeException("Database connection failed"));

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertNotNull(response.getParams().getErrMsg());
        assertTrue(response.getParams().getErrMsg().contains("Error fetching competency data"));

        verify(cassandraOperation).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    @Test
    void testFetchUserCompetency_TokenValidatorException_Returns500() {
        // Arrange
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenThrow(new RuntimeException("Invalid token"));

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());

        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    // ==================== fetchUserCompetencyMapping - Cassandra query verification ====================

    @Test
    void testFetchUserCompetencyMapping_QueryUsesCorrectKeyspaceAndTable() {
        // Arrange
        List<Map<String, Object>> records = createMockCompetencyList();

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_COMPETENCY_MAPPING_TABLE),
                anyMap(),
                isNull(),
                isNull()
        )).thenReturn(records);

        // Act
        service.fetchUserCompetency(authToken);

        // Assert: verify the exact Cassandra call — keyspace, table, userId key, null columns, null limit
        verify(cassandraOperation).getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_COMPETENCY_MAPPING_TABLE),
                argThat(map -> userId.equals(map.get(Constants.USER_ID_KEY))),
                isNull(),
                isNull()
        );
    }

    @Test
    void testFetchUserCompetencyMapping_UserIdIsTrimmed() {
        // Arrange: token returns userId with surrounding spaces
        String paddedUserId = "  " + userId + "  ";
        List<Map<String, Object>> records = createMockCompetencyList();

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(paddedUserId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(records);

        // Act
        service.fetchUserCompetency(authToken);

        // Assert: Cassandra is called with the trimmed userId
        verify(cassandraOperation).getRecordsByProperties(
                anyString(), anyString(),
                argThat(map -> userId.equals(map.get(Constants.USER_ID_KEY))),
                isNull(), isNull()
        );
    }

    @Test
    void testFetchUserCompetencyMapping_EmptyResult_TriggersKafkaEvent() throws Exception {
        // Arrange
        when(serverProperties.getCompetencyAcquiredTopicName()).thenReturn("competency.acquired");
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(Collections.emptyList());
        when(objectMapper.writeValueAsString(anyMap())).thenReturn("{}");

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(cassandraOperation).getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull());
        verify(kafkaTemplate).send(anyString(), anyString());
    }

    @Test
    void testFetchUserCompetencyMapping_Exception_Returns500() {
        // Arrange
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenThrow(new RuntimeException("DB error"));

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    // ==================== publishFirstTimeCompetencyEvent Tests ====================

    @Test
    void testPublishFirstTimeCompetencyEvent_Success() throws Exception {
        // Arrange
        when(serverProperties.getCompetencyAcquiredTopicName()).thenReturn("competency.acquired");
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(Collections.emptyList());

        String eventJson = "{\"edata\":{\"eventType\":\"COMPETENCY_ACQUIRED\",\"userId\":\"test-user-123\",\"isFirstTimeUser\":\"true\"}}";
        when(objectMapper.writeValueAsString(anyMap())).thenReturn(eventJson);

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());

        // Verify the event payload structure passed to objectMapper
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
        // Arrange
        when(serverProperties.getCompetencyAcquiredTopicName()).thenReturn("competency.acquired");
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(Collections.emptyList());
        when(objectMapper.writeValueAsString(anyMap()))
                .thenThrow(new RuntimeException("JSON serialization failed"));

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert: Kafka publish error is swallowed — service still returns 200 with empty list
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        verify(objectMapper).writeValueAsString(anyMap());
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    void testPublishFirstTimeCompetencyEvent_KafkaSendException_StillReturns200() throws Exception {
        // Arrange
        when(serverProperties.getCompetencyAcquiredTopicName()).thenReturn("competency.acquired");
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(Collections.emptyList());
        when(objectMapper.writeValueAsString(anyMap())).thenReturn("{}");
        doThrow(new RuntimeException("Kafka error")).when(kafkaTemplate).send(anyString(), anyString());

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert: Kafka send error is swallowed — service still returns 200 with empty list
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        verify(kafkaTemplate).send(anyString(), anyString());
    }

    // ==================== Response structure verification ====================

    @Test
    void testFetchUserCompetency_VerifyResponseStructure_WithData() {
        // Arrange
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(createMockCompetencyList());

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert standard ApiResponse fields
        assertNotNull(response);
        assertNotNull(response.getId());
        assertNotNull(response.getVer());
        assertNotNull(response.getTs());
        assertNotNull(response.getParams());
        assertNotNull(response.getParams().getResMsgId());
        assertEquals(Constants.API_FETCH_USER_COMPETENCY, response.getId());
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        // Result contains the "competencies" list key
        assertTrue(response.getResult().containsKey(Constants.COMPETENCIES));
    }

    @Test
    void testFetchUserCompetency_VerifyResponseStructure_EmptyData() throws Exception {
        // Arrange
        when(serverProperties.getCompetencyAcquiredTopicName()).thenReturn("competency.acquired");
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(Collections.emptyList());
        when(objectMapper.writeValueAsString(anyMap())).thenReturn("{}");

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert: result has "competencies" with empty list
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertTrue(response.getResult().containsKey(Constants.COMPETENCIES));
        @SuppressWarnings("unchecked")
        List<?> list = (List<?>) response.getResult().get(Constants.COMPETENCIES);
        assertTrue(list.isEmpty());
    }

    @Test
    void testFetchUserCompetency_WithSpecialCharactersInUserId() {
        // Arrange
        String specialUserId = "user-123@test#domain";
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(specialUserId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(createMockCompetencyList());

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertTrue(response.getResult().containsKey(Constants.COMPETENCIES));
    }

    @Test
    void testFetchUserCompetency_LongUserId_DoesNotThrow() {
        // Arrange
        String longUserId = "a".repeat(1000);
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(longUserId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(Collections.emptyList());

        // Act & Assert - Should not throw exception
        assertDoesNotThrow(() -> service.fetchUserCompetency(authToken));
    }

    @Test
    void testFetchUserCompetency_ConcurrentCalls_EachCallIndependent() {
        // Arrange
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(createMockCompetencyList());

        // Act
        service.fetchUserCompetency("token1");
        service.fetchUserCompetency("token2");
        service.fetchUserCompetency("token3");

        // Assert
        verify(accessTokenValidator, times(3)).fetchUserIdFromAccessToken(anyString(), any(ApiResponse.class));
        verify(cassandraOperation, times(3)).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    // ==================== Helper Methods ====================

    /**
     * Creates a list with one mock competency record (as returned from Cassandra).
     */
    private List<Map<String, Object>> createMockCompetencyList() {
        return Collections.singletonList(createMockCompetencyData(
                "kcmfinal_fw_competencyarea_test",
                "kcmfinal_fw_theme_test",
                "kcmfinal_fw_subtheme_test"
        ));
    }

    /**
     * Creates a single mock competency record with snake_case keys (as returned from Cassandra).
     */
    private Map<String, Object> createMockCompetencyData(String areaId, String themeId, String subthemeId) {
        Map<String, Object> data = new HashMap<>();
        data.put("competency_subtheme_id", subthemeId);
        data.put("user_id", userId);
        data.put("competency_theme_id", themeId);
        data.put("competency_area_id", areaId);

        Map<String, Object> competencyDetails = new HashMap<>();
        List<Map<String, String>> selfAchievement = new ArrayList<>();

        Map<String, String> achievement = new HashMap<>();
        achievement.put("acquiredContextId", "context-123");
        achievement.put("acquired_at", "2025-03-06");
        achievement.put("certificateId", "cert-123");
        selfAchievement.add(achievement);

        competencyDetails.put("selfAchievement", selfAchievement);
        data.put("competency_details", competencyDetails);

        return data;
    }
}

