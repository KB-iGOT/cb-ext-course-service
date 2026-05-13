package com.igot.cb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisDataCacheMgr;
import com.igot.cb.common.ServerProperties;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.impl.ContentHealthServiceImpl;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.AfterEach;
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
 * Comprehensive test class for ContentHealthServiceImpl.
 * Tests all methods including success, edge cases, and error scenarios.
 */
@ExtendWith(MockitoExtension.class)
class ContentHealthServiceImplTest {

    @Mock
    private RedisDataCacheMgr redisDataCacheMgr;

    @Mock
    private ServerProperties serverProperties;

    @InjectMocks
    private ContentHealthServiceImpl contentHealthService;

    private ObjectMapper objectMapper;

    private static final int DB_INDEX = 0;
    private static final String CONTENT_ID = "do_114189488264019968113";
    private static final String REDIS_KEY_PREFIX = "course_metrics:";

    private AutoCloseable mocks;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        ReflectionTestUtils.setField(contentHealthService, "objectMapper", objectMapper);
        lenient().when(serverProperties.getContentHealthDbIndex()).thenReturn(DB_INDEX);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mocks != null) {
            mocks.close();
        }
    }

    // ==================== getContentHealthReport Tests ====================

    @Test
    void testGetContentHealthReport_Success_WithMultipleMetrics() throws Exception {
        // Arrange
        Map<String, String> cachedData = new HashMap<>();
        cachedData.put("dropoff_rate", "{\"name\":\"Drop-off Rate\",\"overview\":\"Measures how many learners quit immediately after the first resource, indicating a poor hook or onboarding experience.\",\"maxWeight\":15,\"type\":\"dynamic\",\"score\":1,\"value\":20.0,\"points\":3.0,\"calculated_at\":\"2026-05-11T16:27:24Z\"}");
        cachedData.put("resource_length", "{\"name\":\"Resource Length Distribution\",\"overview\":\"Checks what proportion of resources fall under 10 minutes. Short modules significantly increase learner retention.\",\"maxWeight\":10,\"type\":\"dynamic\",\"score\":3,\"value\":0.0,\"points\":6.0,\"pct_optimal\":0.0,\"outliers\":0,\"calculated_at\":\"2026-05-11T16:27:24Z\"}");
        cachedData.put("avg_rating", "{\"name\":\"Average Rating\",\"overview\":\"Measures learner satisfaction relative to platform mean using standard deviation bands.\",\"maxWeight\":10,\"type\":\"dynamic\",\"score\":0,\"value\":0.0,\"points\":0.0,\"calculated_at\":\"2026-05-11T16:27:24Z\"}");
        cachedData.put("completion_rate", "{\"name\":\"Completion Rate\",\"overview\":\"Measures the ability of learners to complete the course. Low completion signals friction, unclear content, or technical issues.\",\"maxWeight\":5,\"type\":\"dynamic\",\"score\":0,\"value\":0.0,\"points\":0.0,\"calculated_at\":\"2026-05-11T16:27:24Z\"}");
        cachedData.put("health_score", "{\"total_health_score\":7.5,\"calculated_at\":\"2026-05-11T16:27:24Z\"}");

        when(redisDataCacheMgr.getAllHashFields(REDIS_KEY_PREFIX + CONTENT_ID, DB_INDEX))
                .thenReturn(cachedData);

        // Act
        ApiResponse response = contentHealthService.getContentHealthReport(CONTENT_ID);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.API_CONTENT_HEALTH_REPORT, response.getId());
        assertNotNull(response.get(Constants.CONTENT_LIST));
        
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.get(Constants.CONTENT_LIST);
        assertEquals(1, contentList.size());
        
        Map<String, Object> contentData = contentList.get(0);
        assertTrue(contentData.containsKey(CONTENT_ID));
        
        // Check the new structure
        Map<String, Object> structuredData = (Map<String, Object>) contentData.get(CONTENT_ID);
        assertTrue(structuredData.containsKey(Constants.INDICATORS));
        assertTrue(structuredData.containsKey("health_score"));
        
        // Verify indicators array contains metrics with "type" field
        List<Map<String, Object>> indicators = (List<Map<String, Object>>) structuredData.get(Constants.INDICATORS);
        assertEquals(4, indicators.size()); // 4 metrics with "type" field
        
        // Verify health_score is separate
        Map<String, Object> healthScore = (Map<String, Object>) structuredData.get("health_score");
        assertNotNull(healthScore);
        assertEquals(7.5, healthScore.get("total_health_score"));

        verify(redisDataCacheMgr).getAllHashFields(REDIS_KEY_PREFIX + CONTENT_ID, DB_INDEX);
        verify(serverProperties, atLeastOnce()).getContentHealthDbIndex();
    }

    @Test
    void testGetContentHealthReport_Success_WithSingleMetric() throws Exception {
        // Arrange
        Map<String, String> cachedData = new HashMap<>();
        cachedData.put("dropoff_rate", "{\"name\":\"Drop-off Rate\",\"score\":1,\"type\":\"dynamic\"}");

        when(redisDataCacheMgr.getAllHashFields(REDIS_KEY_PREFIX + CONTENT_ID, DB_INDEX))
                .thenReturn(cachedData);

        // Act
        ApiResponse response = contentHealthService.getContentHealthReport(CONTENT_ID);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.get(Constants.CONTENT_LIST);
        assertNotNull(contentList);
        assertEquals(1, contentList.size());
        
        Map<String, Object> contentData = contentList.get(0);
        Map<String, Object> structuredData = (Map<String, Object>) contentData.get(CONTENT_ID);
        List<Map<String, Object>> indicators = (List<Map<String, Object>>) structuredData.get(Constants.INDICATORS);
        assertEquals(1, indicators.size());

        verify(redisDataCacheMgr).getAllHashFields(REDIS_KEY_PREFIX + CONTENT_ID, DB_INDEX);
    }

    @Test
    void testGetContentHealthReport_EmptyData_ReturnsEmptyIndicators() {
        // Arrange
        when(redisDataCacheMgr.getAllHashFields(REDIS_KEY_PREFIX + CONTENT_ID, DB_INDEX))
                .thenReturn(new HashMap<>());

        // Act
        ApiResponse response = contentHealthService.getContentHealthReport(CONTENT_ID);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.get(Constants.CONTENT_LIST);
        assertNotNull(contentList);
        assertEquals(1, contentList.size());
        
        Map<String, Object> contentData = contentList.get(0);
        Map<String, Object> structuredData = (Map<String, Object>) contentData.get(CONTENT_ID);
        List<Map<String, Object>> indicators = (List<Map<String, Object>>) structuredData.get(Constants.INDICATORS);
        assertTrue(indicators.isEmpty());

        verify(redisDataCacheMgr).getAllHashFields(REDIS_KEY_PREFIX + CONTENT_ID, DB_INDEX);
    }

    @Test
    void testGetContentHealthReport_NullData_ReturnsEmptyIndicators() {
        // Arrange
        when(redisDataCacheMgr.getAllHashFields(REDIS_KEY_PREFIX + CONTENT_ID, DB_INDEX))
                .thenReturn(null);

        // Act
        ApiResponse response = contentHealthService.getContentHealthReport(CONTENT_ID);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.get(Constants.CONTENT_LIST);
        assertNotNull(contentList);
        assertEquals(1, contentList.size());
        
        Map<String, Object> contentData = contentList.get(0);
        Map<String, Object> structuredData = (Map<String, Object>) contentData.get(CONTENT_ID);
        List<Map<String, Object>> indicators = (List<Map<String, Object>>) structuredData.get(Constants.INDICATORS);
        assertTrue(indicators.isEmpty());

        verify(redisDataCacheMgr).getAllHashFields(REDIS_KEY_PREFIX + CONTENT_ID, DB_INDEX);
    }

    @Test
    void testGetContentHealthReport_BlankContentId() {
        // Act
        ApiResponse response = contentHealthService.getContentHealthReport("");

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.get(Constants.ERROR));
        assertEquals("Content ID is required and cannot be empty", response.get(Constants.ERROR_MESSAGE));
        
        verify(redisDataCacheMgr, never()).getAllHashFields(anyString(), anyInt());
    }

    @Test
    void testGetContentHealthReport_NullContentId() {
        // Act
        ApiResponse response = contentHealthService.getContentHealthReport(null);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.get(Constants.ERROR));
        assertEquals("Content ID is required and cannot be empty", response.get(Constants.ERROR_MESSAGE));
        
        verify(redisDataCacheMgr, never()).getAllHashFields(anyString(), anyInt());
    }

    @Test
    void testGetContentHealthReport_ContentIdWithSpaces() throws Exception {
        // Arrange
        String contentIdWithSpaces = "  " + CONTENT_ID + "  ";
        Map<String, String> cachedData = new HashMap<>();
        cachedData.put("dropoff_rate", "{\"name\":\"Drop-off Rate\",\"score\":1,\"type\":\"dynamic\"}");

        when(redisDataCacheMgr.getAllHashFields(REDIS_KEY_PREFIX + CONTENT_ID, DB_INDEX))
                .thenReturn(cachedData);

        // Act
        ApiResponse response = contentHealthService.getContentHealthReport(contentIdWithSpaces);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        
        verify(redisDataCacheMgr).getAllHashFields(REDIS_KEY_PREFIX + CONTENT_ID, DB_INDEX);
    }

    @Test
    void testGetContentHealthReport_InvalidJsonInRedisData() {
        // Arrange
        Map<String, String> cachedData = new HashMap<>();
        cachedData.put("dropoff_rate", "invalid-json");
        cachedData.put("valid_metric", "{\"name\":\"Valid Metric\",\"score\":1,\"type\":\"dynamic\"}");

        when(redisDataCacheMgr.getAllHashFields(REDIS_KEY_PREFIX + CONTENT_ID, DB_INDEX))
                .thenReturn(cachedData);

        // Act
        ApiResponse response = contentHealthService.getContentHealthReport(CONTENT_ID);

        // Assert - should skip invalid JSON and include valid one
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.get(Constants.CONTENT_LIST);
        Map<String, Object> contentData = contentList.get(0);
        Map<String, Object> structuredData = (Map<String, Object>) contentData.get(CONTENT_ID);
        List<Map<String, Object>> indicators = (List<Map<String, Object>>) structuredData.get(Constants.INDICATORS);
        assertEquals(1, indicators.size()); // Only valid metric should be included

        verify(redisDataCacheMgr).getAllHashFields(REDIS_KEY_PREFIX + CONTENT_ID, DB_INDEX);
    }

    @Test
    void testGetContentHealthReport_MetricsWithoutTypeField() {
        // Arrange
        Map<String, String> cachedData = new HashMap<>();
        cachedData.put("dropoff_rate", "{\"name\":\"Drop-off Rate\",\"score\":1,\"type\":\"dynamic\"}"); // has type
        cachedData.put("other_metric", "{\"name\":\"Other Metric\",\"score\":2}"); // no type, should not go to indicators

        when(redisDataCacheMgr.getAllHashFields(REDIS_KEY_PREFIX + CONTENT_ID, DB_INDEX))
                .thenReturn(cachedData);

        // Act
        ApiResponse response = contentHealthService.getContentHealthReport(CONTENT_ID);

        // Assert
        assertNotNull(response);
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.get(Constants.CONTENT_LIST);
        Map<String, Object> contentData = contentList.get(0);
        Map<String, Object> structuredData = (Map<String, Object>) contentData.get(CONTENT_ID);
        List<Map<String, Object>> indicators = (List<Map<String, Object>>) structuredData.get(Constants.INDICATORS);

        // Only metric with "type" should be in indicators - dropoff_rate
        assertEquals(1, indicators.size());
        // other_metric without type should be at top level
        assertTrue(structuredData.containsKey("other_metric"));
    }

    @Test
    void testGetContentHealthReport_RedisException() {
        // Arrange
        when(redisDataCacheMgr.getAllHashFields(REDIS_KEY_PREFIX + CONTENT_ID, DB_INDEX))
                .thenThrow(new RuntimeException("Redis connection error"));

        // Act
        ApiResponse response = contentHealthService.getContentHealthReport(CONTENT_ID);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.FAILED, response.get(Constants.ERROR));
        assertTrue(((String) response.get(Constants.ERROR_MESSAGE)).contains("Failed to fetch content health report"));

        verify(redisDataCacheMgr).getAllHashFields(REDIS_KEY_PREFIX + CONTENT_ID, DB_INDEX);
    }

    // ==================== getContentHealthSummary Tests ====================

    @Test
    void testGetContentHealthSummary_Success_SingleCourseId() throws Exception {
        // Arrange
        String healthScoreJson = "{\"total_health_score\":9.0,\"red_flag\":false,\"calculated_at\":\"2026-05-12T09:10:58Z\"}";
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.COURSE_ID, Arrays.asList("do_123"));

        when(redisDataCacheMgr.getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX))
                .thenReturn(healthScoreJson);

        // Act
        ApiResponse response = contentHealthService.getContentHealthSummary(requestBody);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.API_CONTENT_HEALTH_SUMMARY, response.getId());
        
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT_LIST);
        assertNotNull(contentList);
        assertEquals(1, contentList.size());
        
        Map<String, Object> courseData = contentList.get(0);
        assertTrue(courseData.containsKey("do_123"));
        
        // Verify the health score object is directly under courseId (not wrapped in a list)
        Map<String, Object> healthScore = (Map<String, Object>) courseData.get("do_123");
        assertEquals(9.0, healthScore.get("total_health_score"));
        assertEquals(false, healthScore.get("red_flag"));
        assertEquals("2026-05-12T09:10:58Z", healthScore.get("calculated_at"));

        verify(redisDataCacheMgr).getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX);
    }

    @Test
    void testGetContentHealthSummary_Success_MultipleCourseIds() throws Exception {
        // Arrange
        String healthScoreJson1 = "{\"total_health_score\":5.0,\"calculated_at\":\"2026-05-11T16:27:24Z\"}";
        String healthScoreJson2 = "{\"total_health_score\":6.0,\"calculated_at\":\"2026-05-11T16:27:24Z\"}";

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.COURSE_ID, Arrays.asList("do_123", "do_456"));

        when(redisDataCacheMgr.getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX))
                .thenReturn(healthScoreJson1);
        when(redisDataCacheMgr.getHashField(REDIS_KEY_PREFIX + "do_456", Constants.HEALTH_SCORE, DB_INDEX))
                .thenReturn(healthScoreJson2);

        // Act
        ApiResponse response = contentHealthService.getContentHealthSummary(requestBody);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT_LIST);
        assertNotNull(contentList);
        assertEquals(2, contentList.size());

        verify(redisDataCacheMgr).getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX);
        verify(redisDataCacheMgr).getHashField(REDIS_KEY_PREFIX + "do_456", Constants.HEALTH_SCORE, DB_INDEX);
    }

    @Test
    void testGetContentHealthSummary_CourseIdWithNoData() {
        // Arrange
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.COURSE_ID, Arrays.asList("do_123"));

        when(redisDataCacheMgr.getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX))
                .thenReturn(null);

        // Act
        ApiResponse response = contentHealthService.getContentHealthSummary(requestBody);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT_LIST);
        assertNotNull(contentList);
        // When no data is found, the course is not added to the result list
        assertEquals(0, contentList.size());

        verify(redisDataCacheMgr).getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX);
    }

    @Test
    void testGetContentHealthSummary_EmptyRequestBody() {
        // Act
        ApiResponse response = contentHealthService.getContentHealthSummary(new HashMap<>());

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.get(Constants.ERROR));
        assertEquals("Request body is empty", response.get(Constants.ERROR_MESSAGE));
        
        verify(redisDataCacheMgr, never()).getHashField(anyString(), anyString(), anyInt());
    }

    @Test
    void testGetContentHealthSummary_NullRequestBody() {
        // Act
        ApiResponse response = contentHealthService.getContentHealthSummary(null);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.get(Constants.ERROR));
        assertEquals("Request body is empty", response.get(Constants.ERROR_MESSAGE));
        
        verify(redisDataCacheMgr, never()).getHashField(anyString(), anyString(), anyInt());
    }

    @Test
    void testGetContentHealthSummary_MissingCourseIdField() {
        // Arrange
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("someOtherField", "value");

        // Act
        ApiResponse response = contentHealthService.getContentHealthSummary(requestBody);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.get(Constants.ERROR));
        assertTrue(((String) response.get(Constants.ERROR_MESSAGE)).contains("'courseId' is mandatory"));
        
        verify(redisDataCacheMgr, never()).getHashField(anyString(), anyString(), anyInt());
    }

    @Test
    void testGetContentHealthSummary_CourseIdNotList() {
        // Arrange
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.COURSE_ID, "not-a-list");

        // Act
        ApiResponse response = contentHealthService.getContentHealthSummary(requestBody);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.get(Constants.ERROR));
        assertTrue(((String) response.get(Constants.ERROR_MESSAGE)).contains("'courseId' is mandatory and should be a non-empty list"));
        
        verify(redisDataCacheMgr, never()).getHashField(anyString(), anyString(), anyInt());
    }

    @Test
    void testGetContentHealthSummary_EmptyCourseIdList() {
        // Arrange
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.COURSE_ID, new ArrayList<>());

        // Act
        ApiResponse response = contentHealthService.getContentHealthSummary(requestBody);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.get(Constants.ERROR));
        assertTrue(((String) response.get(Constants.ERROR_MESSAGE)).contains("'courseId' is mandatory and should be a non-empty list"));
        
        verify(redisDataCacheMgr, never()).getHashField(anyString(), anyString(), anyInt());
    }

    @Test
    void testGetContentHealthSummary_CourseIdListWithNullValues() throws Exception {
        // Arrange
        String healthScoreJson = "{\"total_health_score\":5.0,\"calculated_at\":\"2026-05-11T16:27:24Z\"}";

        List<Object> courseIds = new ArrayList<>();
        courseIds.add(null);
        courseIds.add("do_123");
        courseIds.add(null);
        
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.COURSE_ID, courseIds);

        when(redisDataCacheMgr.getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX))
                .thenReturn(healthScoreJson);

        // Act
        ApiResponse response = contentHealthService.getContentHealthSummary(requestBody);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT_LIST);
        assertNotNull(contentList);
        assertEquals(1, contentList.size()); // Only non-null course ID should be processed

        verify(redisDataCacheMgr).getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX);
    }

    @Test
    void testGetContentHealthSummary_CourseIdListWithEmptyStrings() throws Exception {
        // Arrange
        String healthScoreJson = "{\"total_health_score\":5.0,\"calculated_at\":\"2026-05-11T16:27:24Z\"}";

        List<Object> courseIds = new ArrayList<>();
        courseIds.add("");
        courseIds.add("do_123");
        courseIds.add("   ");
        
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.COURSE_ID, courseIds);

        when(redisDataCacheMgr.getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX))
                .thenReturn(healthScoreJson);

        ApiResponse response = contentHealthService.getContentHealthSummary(requestBody);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT_LIST);
        assertNotNull(contentList);
        assertEquals(1, contentList.size()); // Only non-blank course ID should be processed

        verify(redisDataCacheMgr).getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX);
    }

    @Test
    void testGetContentHealthSummary_CourseIdWithSpaces() throws Exception {
        // Arrange
        String healthScoreJson = "{\"total_health_score\":5.0,\"calculated_at\":\"2026-05-11T16:27:24Z\"}";

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.COURSE_ID, Arrays.asList("  do_123  "));

        when(redisDataCacheMgr.getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX))
                .thenReturn(healthScoreJson);

        // Act
        ApiResponse response = contentHealthService.getContentHealthSummary(requestBody);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT_LIST);
        Map<String, Object> courseData = contentList.get(0);
        assertTrue(courseData.containsKey("do_123")); // Should use trimmed ID as key

        verify(redisDataCacheMgr).getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX);
    }

    @Test
    void testGetContentHealthSummary_InvalidJsonInRedisData() throws Exception {
        // Arrange
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.COURSE_ID, Arrays.asList("do_123"));

        when(redisDataCacheMgr.getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX))
                .thenReturn("invalid-json");

        // Act
        ApiResponse response = contentHealthService.getContentHealthSummary(requestBody);

        // Assert - should handle error gracefully
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT_LIST);
        assertNotNull(contentList);
        // When JSON is invalid, the entry is not added to result list
        assertEquals(0, contentList.size());

        verify(redisDataCacheMgr).getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX);
    }

    @Test
    void testGetContentHealthSummary_RedisException() {
        // Arrange
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.COURSE_ID, Arrays.asList("do_123"));

        when(redisDataCacheMgr.getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX))
                .thenThrow(new RuntimeException("Redis connection error"));

        // Act
        ApiResponse response = contentHealthService.getContentHealthSummary(requestBody);

        // Assert - implementation handles individual course errors gracefully
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT_LIST);
        assertNotNull(contentList);
        // When there's an error, the course is not added to the result list
        assertEquals(0, contentList.size());

        verify(redisDataCacheMgr).getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX);
    }

    @Test
    void testGetContentHealthSummary_PartialFailure_SomeCoursesSucceedSomeFail() throws Exception {
        // Arrange
        String healthScoreJson1 = "{\"total_health_score\":5.0,\"calculated_at\":\"2026-05-11T16:27:24Z\"}";
        String healthScoreJson2 = "{\"total_health_score\":6.0,\"calculated_at\":\"2026-05-11T16:27:24Z\"}";

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.COURSE_ID, Arrays.asList("do_123", "do_456", "do_789"));

        when(redisDataCacheMgr.getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX))
                .thenReturn(healthScoreJson1);
        when(redisDataCacheMgr.getHashField(REDIS_KEY_PREFIX + "do_456", Constants.HEALTH_SCORE, DB_INDEX))
                .thenThrow(new RuntimeException("Error for do_456"));
        when(redisDataCacheMgr.getHashField(REDIS_KEY_PREFIX + "do_789", Constants.HEALTH_SCORE, DB_INDEX))
                .thenReturn(healthScoreJson2);

        // Act
        ApiResponse response = contentHealthService.getContentHealthSummary(requestBody);

        // Assert - should handle partial failures gracefully
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT_LIST);
        assertNotNull(contentList);
        // Only 2 courses should be present (do_123 and do_789), do_456 failed and is not added
        assertEquals(2, contentList.size());

        verify(redisDataCacheMgr).getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX);
        verify(redisDataCacheMgr).getHashField(REDIS_KEY_PREFIX + "do_456", Constants.HEALTH_SCORE, DB_INDEX);
        verify(redisDataCacheMgr).getHashField(REDIS_KEY_PREFIX + "do_789", Constants.HEALTH_SCORE, DB_INDEX);
    }

    @Test
    void testGetContentHealthSummary_AllCoursesWithHealthScoreOnly() throws Exception {
        // Arrange - only health_score
        String healthScoreJson = "{\"total_health_score\":5.0,\"red_flag\":false,\"calculated_at\":\"2026-05-11T16:27:24Z\"}";

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.COURSE_ID, Arrays.asList("do_123"));

        when(redisDataCacheMgr.getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX))
                .thenReturn(healthScoreJson);

        // Act
        ApiResponse response = contentHealthService.getContentHealthSummary(requestBody);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());

        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT_LIST);
        Map<String, Object> courseData = contentList.get(0);

        // Verify the health score object is directly under courseId
        Map<String, Object> healthScore = (Map<String, Object>) courseData.get("do_123");
        assertEquals(5.0, healthScore.get("total_health_score"));
        assertEquals(false, healthScore.get("red_flag"));

        verify(redisDataCacheMgr).getHashField(REDIS_KEY_PREFIX + "do_123", Constants.HEALTH_SCORE, DB_INDEX);
    }
}
