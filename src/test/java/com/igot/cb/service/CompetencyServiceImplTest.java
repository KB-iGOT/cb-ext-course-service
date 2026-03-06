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

        // Setup default mock behavior for serverProperties
        when(serverProperties.getCompetencyAcquiredTopicName()).thenReturn("competency.acquired");
    }

    // ==================== fetchUserCompetency Tests ====================

    @Test
    void testFetchUserCompetency_Success_WithData() throws Exception {
        // Arrange
        Map<String, Object> competencyData = createMockCompetencyData();

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_COMPETENCY_MAPPING_TABLE),
                anyMap(),
                isNull(),
                eq(1)
        )).thenReturn(Collections.singletonList(competencyData));

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertNotNull(response.getResult());
        assertEquals(competencyData, response.getResult());

        verify(accessTokenValidator, times(1)).fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class));
        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), eq(1));
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    void testFetchUserCompetency_Success_EmptyData_PublishesEvent() throws Exception {
        // Arrange
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_COMPETENCY_MAPPING_TABLE),
                anyMap(),
                isNull(),
                eq(1)
        )).thenReturn(Collections.emptyList());

        String eventJson = "{\"edata\":{\"eventType\":\"COMPETENCY_ACQUIRED\"}}";
        when(objectMapper.writeValueAsString(anyMap())).thenReturn(eventJson);

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertNotNull(response.getResult());
        assertTrue(response.getResult().isEmpty());

        verify(accessTokenValidator, times(1)).fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class));
        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(), anyString(), anyMap(), isNull(), eq(1));
        verify(kafkaTemplate, times(1)).send(eq("competency.acquired"), eq(eventJson));
        verify(objectMapper, times(1)).writeValueAsString(anyMap());
    }

    @Test
    void testFetchUserCompetency_EmptyUserId() {
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
    void testFetchUserCompetency_NullUserId() {
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
    void testFetchUserCompetency_CassandraException() {
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

        verify(accessTokenValidator, times(1)).fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class));
        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    @Test
    void testFetchUserCompetency_TokenValidatorException() {
        // Arrange
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenThrow(new RuntimeException("Invalid token"));

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());

        verify(accessTokenValidator, times(1)).fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class));
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    @Test
    void testFetchUserCompetency_KafkaPublishException() throws Exception {
        // Arrange
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());
        when(objectMapper.writeValueAsString(anyMap()))
                .thenThrow(new RuntimeException("JSON serialization failed"));

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());

        verify(objectMapper, times(1)).writeValueAsString(anyMap());
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    void testFetchUserCompetency_WithWhitespaceUserId() {
        // Arrange
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn("   ");

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    @Test
    void testFetchUserCompetency_CassandraReturnsNull() {
        // Arrange
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(null);

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertTrue(response.getResult().isEmpty());
    }

    @Test
    void testFetchUserCompetency_WithSpecialCharactersInUserId() throws Exception {
        // Arrange
        String specialUserId = "user-123@test#domain";
        Map<String, Object> competencyData = createMockCompetencyData();

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(specialUserId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.singletonList(competencyData));

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertNotNull(response.getResult());
    }

    // ==================== fetchUserCompetencyMapping Tests ====================

    @Test
    void testFetchUserCompetencyMapping_Success() throws Exception {
        // Arrange
        Map<String, Object> competencyData = createMockCompetencyData();

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_COMPETENCY_MAPPING_TABLE),
                anyMap(),
                isNull(),
                eq(1)
        )).thenReturn(Collections.singletonList(competencyData));

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(competencyData, response.getResult());

        verify(cassandraOperation, times(1)).getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_COMPETENCY_MAPPING_TABLE),
                argThat(map -> userId.equals(map.get(Constants.USER_ID_KEY))),
                isNull(),
                eq(1)
        );
    }

    @Test
    void testFetchUserCompetencyMapping_EmptyResult() throws Exception {
        // Arrange
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());
        when(objectMapper.writeValueAsString(anyMap())).thenReturn("{}");

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertTrue(response.getResult().isEmpty());

        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    @Test
    void testFetchUserCompetencyMapping_Exception() {
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
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());

        String eventJson = "{\"edata\":{\"eventType\":\"COMPETENCY_ACQUIRED\",\"userId\":\"test-user-123\",\"isFirstTimeUser\":\"true\"}}";
        when(objectMapper.writeValueAsString(anyMap())).thenReturn(eventJson);

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());

        verify(objectMapper, times(1)).writeValueAsString(argThat(obj -> {
            if (!(obj instanceof Map<?, ?> map)) return false;
            Object edata = map.get(Constants.E_DATA);
            if (edata instanceof Map<?, ?> edataMap) {
                return Constants.COMPETENCY_ACQUIRED_EVENT.equals(edataMap.get(Constants.EVENT_TYPE)) &&
                        userId.equals(edataMap.get(Constants.USER_ID)) &&
                        "true".equals(edataMap.get(Constants.IS_FIRST_TIME_USER));
            }
            return false;
        }));

        verify(kafkaTemplate, times(1)).send(eq("competency.acquired"), eq(eventJson));
    }

    @Test
    void testPublishFirstTimeCompetencyEvent_JsonException() throws Exception {
        // Arrange
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());
        when(objectMapper.writeValueAsString(anyMap()))
                .thenThrow(new RuntimeException("JSON error"));

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());

        verify(objectMapper, times(1)).writeValueAsString(anyMap());
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    void testPublishFirstTimeCompetencyEvent_KafkaException() throws Exception {
        // Arrange
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());

        String eventJson = "{}";
        when(objectMapper.writeValueAsString(anyMap())).thenReturn(eventJson);
        doThrow(new RuntimeException("Kafka error")).when(kafkaTemplate).send(anyString(), anyString());

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());

        verify(kafkaTemplate, times(1)).send(anyString(), anyString());
    }

    // ==================== Edge Cases and Integration Tests ====================

    @Test
    void testFetchUserCompetency_MultipleRecordsReturned() {
        // Arrange
        List<Map<String, Object>> multipleRecords = Arrays.asList(
                createMockCompetencyData(),
                createMockCompetencyData()
        );

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(multipleRecords);

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(multipleRecords.get(0), response.getResult());
    }

    @Test
    void testFetchUserCompetency_NullAuthToken() {
        // Act
        ApiResponse response = service.fetchUserCompetency(null);

        // Assert
        assertNotNull(response);
        verify(accessTokenValidator, times(1)).fetchUserIdFromAccessToken(isNull(), any(ApiResponse.class));
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
        verify(accessTokenValidator, times(1)).fetchUserIdFromAccessToken(eq(""), any(ApiResponse.class));
    }

    @Test
    void testFetchUserCompetency_VerifyResponseStructure() throws Exception {
        // Arrange
        Map<String, Object> competencyData = createMockCompetencyData();

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.singletonList(competencyData));

        // Act
        ApiResponse response = service.fetchUserCompetency(authToken);

        // Assert
        assertNotNull(response);
        assertNotNull(response.getId());
        assertNotNull(response.getVer());
        assertNotNull(response.getTs());
        assertNotNull(response.getParams());
        assertNotNull(response.getParams().getResMsgId());
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testFetchUserCompetency_LongUserId() {
        // Arrange
        String longUserId = "a".repeat(1000);

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(authToken), any(ApiResponse.class)))
                .thenReturn(longUserId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());

        // Act & Assert - Should not throw exception
        assertDoesNotThrow(() -> service.fetchUserCompetency(authToken));
    }

    @Test
    void testFetchUserCompetency_ConcurrentCalls() throws Exception {
        // Arrange
        Map<String, Object> competencyData = createMockCompetencyData();

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any(ApiResponse.class)))
                .thenReturn(userId);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.singletonList(competencyData));

        // Act - Simulate multiple calls
        service.fetchUserCompetency("token1");
        service.fetchUserCompetency("token2");
        service.fetchUserCompetency("token3");

        // Assert
        verify(accessTokenValidator, times(3)).fetchUserIdFromAccessToken(anyString(), any(ApiResponse.class));
        verify(cassandraOperation, times(3)).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    // ==================== Helper Methods ====================

    private Map<String, Object> createMockCompetencyData() {
        Map<String, Object> data = new HashMap<>();
        data.put("competency_subtheme_id", "kcmfinal_fw_subtheme_test");
        data.put("user_id", userId);
        data.put("competency_theme_id", "kcmfinal_fw_theme_test");
        data.put("competency_area_id", "kcmfinal_fw_competencyarea_test");

        Map<String, Object> competencyDetails = new HashMap<>();
        List<Map<String, String>> selfAchievement = new ArrayList<>();

        Map<String, String> achievement1 = new HashMap<>();
        achievement1.put("acquiredContextId", "context-123");
        achievement1.put("acquired_at", "2025-03-06");
        achievement1.put("certificateId", "cert-123");
        selfAchievement.add(achievement1);

        competencyDetails.put("selfAchievement", selfAchievement);
        data.put("competency_details", competencyDetails);

        return data;
    }
}

