package com.igot.cb.service;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.impl.LearningPathwayRetireServiceImpl;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LearningPathwayRetireServiceImplTest {

    @Mock
    private AccessTokenValidator accessTokenValidator;

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private ContentInfoServiceImpl contentService;


    @InjectMocks
    private LearningPathwayRetireServiceImpl service;

    private static final String USER_TOKEN = "token";
    private static final String CONTENT_ID = "do_123";

    @BeforeEach
    void setup() {
        // No-op: constructor injection handled by @InjectMocks
    }

    @Test
    void retireLearningPathway_WhenUserIdBlank_ReturnsBadRequest() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(USER_TOKEN), any(ApiResponse.class))).thenReturn("");

        ApiResponse result = service.retireLearningPathway(USER_TOKEN, CONTENT_ID);

        assertEquals(HttpStatus.BAD_REQUEST, result.getResponseCode());
        assertEquals(Constants.FAILED, result.getParams().getStatus());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, result.getParams().getErr());
    }

    @Test
    void retireLearningPathway_WhenUserHasNoRole_ReturnsForbidden() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(USER_TOKEN), any(ApiResponse.class))).thenReturn("user-1");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER_ROLES), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());

        ApiResponse result = service.retireLearningPathway(USER_TOKEN, CONTENT_ID);

        assertEquals(HttpStatus.FORBIDDEN, result.getResponseCode());
        assertEquals("User does not have SPV_PUBLISHER role", result.get(Constants.MESSAGE));
    }

    @Test
    void retireLearningPathway_WhenContentEmpty_ReturnsNotFound() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(USER_TOKEN), any(ApiResponse.class))).thenReturn("user-1");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER_ROLES), anyMap(), any(), any()))
                .thenReturn(List.of(Map.of("dummy", "x"))); // role exists
        when(contentService.readContent(eq(CONTENT_ID), anyList()))
                .thenReturn(Collections.emptyMap());

        ApiResponse result = service.retireLearningPathway(USER_TOKEN, CONTENT_ID);

        assertEquals(HttpStatus.NOT_FOUND, result.getResponseCode());
        assertEquals("Content not found", result.get(Constants.MESSAGE));
    }

    @Test
    void retireLearningPathway_WhenCreatedByMismatch_ReturnsBadRequest() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(USER_TOKEN), any(ApiResponse.class))).thenReturn("user-1");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER_ROLES), anyMap(), any(), any()))
                .thenReturn(List.of(Map.of("role", "SPV_PUBLISHER")));
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.CREATED_BY, "user-2");
        content.put(Constants.COURSE_CATEGORY, Constants.LEARNING_PATHWAY);
        when(contentService.readContent(eq(CONTENT_ID), anyList())).thenReturn(content);

        ApiResponse result = service.retireLearningPathway(USER_TOKEN, CONTENT_ID);

        assertEquals(HttpStatus.BAD_REQUEST, result.getResponseCode());
        assertEquals("Content is not created by user", result.get(Constants.MESSAGE));
    }

    @Test
    void retireLearningPathway_WhenNotLearningPathway_ReturnsBadRequest() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(USER_TOKEN), any(ApiResponse.class))).thenReturn("user-1");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER_ROLES), anyMap(), any(), any()))
                .thenReturn(List.of(Map.of("role", "SPV_PUBLISHER")));
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.CREATED_BY, "user-1");
        content.put(Constants.COURSE_CATEGORY, "SomeOtherCategory");
        when(contentService.readContent(eq(CONTENT_ID), anyList())).thenReturn(content);

        ApiResponse result = service.retireLearningPathway(USER_TOKEN, CONTENT_ID);

        assertEquals(HttpStatus.BAD_REQUEST, result.getResponseCode());
        assertEquals("Content is not learning pathway", result.get(Constants.MESSAGE));
    }

    @Test
    void retireLearningPathway_WhenNoBatches_ProceedsRetirementSuccess() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(USER_TOKEN), any(ApiResponse.class))).thenReturn("user-1");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER_ROLES), anyMap(), any(), any()))
                .thenReturn(List.of(Map.of("role", "SPV_PUBLISHER")));
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.CREATED_BY, "user-1");
        content.put(Constants.COURSE_CATEGORY, Constants.LEARNING_PATHWAY);
        content.put(Constants.BATCHES, Collections.emptyList());
        when(contentService.readContent(eq(CONTENT_ID), anyList())).thenReturn(content);
        when(contentService.retireContent(CONTENT_ID)).thenReturn(Map.of("status", "success"));

        ApiResponse result = service.retireLearningPathway(USER_TOKEN, CONTENT_ID);

        assertEquals(HttpStatus.OK, result.getResponseCode());
        assertEquals("Content retired successfully", result.get(Constants.MESSAGE));
    }

    @Test
    void retireLearningPathway_WhenActiveEnrollmentsFound_ReturnsBadRequest() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(USER_TOKEN), any(ApiResponse.class))).thenReturn("user-1");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER_ROLES), anyMap(), any(), any()))
                .thenReturn(List.of(Map.of("role", "SPV_PUBLISHER")));
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.CREATED_BY, "user-1");
        content.put(Constants.COURSE_CATEGORY, Constants.LEARNING_PATHWAY);
        content.put(Constants.BATCHES, List.of(Map.of(Constants.BATCH_ID, "batch-1")));
        when(contentService.readContent(eq(CONTENT_ID), anyList())).thenReturn(content);
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD_COURSE), eq(Constants.ENROLLMENT_BATCH_LOOKUP), anyMap(), any(), any()))
                .thenReturn(List.of(Map.of(Constants.USER_ID, "u1", Constants.ACTIVE, true)));

        ApiResponse result = service.retireLearningPathway(USER_TOKEN, CONTENT_ID);

        assertEquals(HttpStatus.BAD_REQUEST, result.getResponseCode());
        assertEquals("Active enrollments available. Cannot retire the content.", result.get(Constants.MESSAGE));
    }

    @Test
    void retireLearningPathway_WhenNoActiveEnrollments_ProceedsRetirement() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(USER_TOKEN), any(ApiResponse.class))).thenReturn("user-1");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER_ROLES), anyMap(), any(), any()))
                .thenReturn(List.of(Map.of("role", "SPV_PUBLISHER")));
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.CREATED_BY, "user-1");
        content.put(Constants.COURSE_CATEGORY, Constants.LEARNING_PATHWAY);
        content.put(Constants.BATCHES, List.of(Map.of(Constants.BATCH_ID, "batch-1")));
        when(contentService.readContent(eq(CONTENT_ID), anyList())).thenReturn(content);
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD_COURSE), eq(Constants.ENROLLMENT_BATCH_LOOKUP), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());
        when(contentService.retireContent(CONTENT_ID)).thenReturn(Map.of("status", "success"));

        ApiResponse result = service.retireLearningPathway(USER_TOKEN, CONTENT_ID);

        assertEquals(HttpStatus.OK, result.getResponseCode());
        assertEquals("Content retired successfully", result.get(Constants.MESSAGE));
    }

    @Test
    void retireLearningPathway_WhenBatchIdIsBlank_SkipsBatchAndProceedsRetirement() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(USER_TOKEN), any(ApiResponse.class))).thenReturn("user-1");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER_ROLES), anyMap(), any(), any()))
                .thenReturn(List.of(Map.of("role", "SPV_PUBLISHER")));
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.CREATED_BY, "user-1");
        content.put(Constants.COURSE_CATEGORY, Constants.LEARNING_PATHWAY);
        content.put(Constants.BATCHES, List.of(Map.of(Constants.BATCH_ID, ""), Map.of(Constants.BATCH_ID, "batch-2")));
        when(contentService.readContent(eq(CONTENT_ID), anyList())).thenReturn(content);
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD_COURSE), eq(Constants.ENROLLMENT_BATCH_LOOKUP), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());
        when(contentService.retireContent(CONTENT_ID)).thenReturn(Map.of("status", "success"));

        ApiResponse result = service.retireLearningPathway(USER_TOKEN, CONTENT_ID);

        assertEquals(HttpStatus.OK, result.getResponseCode());
        assertEquals("Content retired successfully", result.get(Constants.MESSAGE));
    }

    @Test
    void retireLearningPathway_WhenRetireContentReturnsEmpty_ReturnsServerError() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(USER_TOKEN), any(ApiResponse.class))).thenReturn("user-1");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER_ROLES), anyMap(), any(), any()))
                .thenReturn(List.of(Map.of("role", "SPV_PUBLISHER")));
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.CREATED_BY, "user-1");
        content.put(Constants.COURSE_CATEGORY, Constants.LEARNING_PATHWAY);
        content.put(Constants.BATCHES, Collections.emptyList());
        when(contentService.readContent(eq(CONTENT_ID), anyList())).thenReturn(content);
        when(contentService.retireContent(CONTENT_ID)).thenReturn(Collections.emptyMap());

        ApiResponse result = service.retireLearningPathway(USER_TOKEN, CONTENT_ID);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, result.getResponseCode());
        assertEquals("Retirement API returned empty response", result.get(Constants.MESSAGE));
    }

    @Test
    void retireLearningPathway_WhenRetireContentThrowsException_ReturnsServerError() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(USER_TOKEN), any(ApiResponse.class))).thenReturn("user-1");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER_ROLES), anyMap(), any(), any()))
                .thenReturn(List.of(Map.of("role", "SPV_PUBLISHER")));
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.CREATED_BY, "user-1");
        content.put(Constants.COURSE_CATEGORY, Constants.LEARNING_PATHWAY);
        content.put(Constants.BATCHES, Collections.emptyList());
        when(contentService.readContent(eq(CONTENT_ID), anyList())).thenReturn(content);
        when(contentService.retireContent(CONTENT_ID)).thenThrow(new RuntimeException("Retirement failed"));

        ApiResponse result = service.retireLearningPathway(USER_TOKEN, CONTENT_ID);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, result.getResponseCode());
        assertTrue(((String) result.get(Constants.MESSAGE)).contains("Failed to retire content:"));
    }

    @Test
    void retireLearningPathway_WhenRoleCheckThrowsException_ReturnsForbidden() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(USER_TOKEN), any(ApiResponse.class))).thenReturn("user-1");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER_ROLES), anyMap(), any(), any()))
                .thenThrow(new RuntimeException("Database error"));

        ApiResponse result = service.retireLearningPathway(USER_TOKEN, CONTENT_ID);

        assertEquals(HttpStatus.FORBIDDEN, result.getResponseCode());
        assertEquals("User does not have SPV_PUBLISHER role", result.get(Constants.MESSAGE));
    }

    @Test
    void retireLearningPathway_WhenReadContentThrowsException_ReturnsServerError() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(USER_TOKEN), any(ApiResponse.class))).thenReturn("user-1");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER_ROLES), anyMap(), any(), any()))
                .thenReturn(List.of(Map.of("role", "SPV_PUBLISHER")));
        when(contentService.readContent(eq(CONTENT_ID), anyList())).thenThrow(new RuntimeException("Content service error"));

        ApiResponse result = service.retireLearningPathway(USER_TOKEN, CONTENT_ID);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, result.getResponseCode());
        assertTrue(((String) result.get(Constants.MESSAGE)).contains("Failed to retire content:"));
    }

    @Test
    void retireLearningPathway_WhenMultipleBatchesWithMixedEnrollments_ChecksAllBatches() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(USER_TOKEN), any(ApiResponse.class))).thenReturn("user-1");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER_ROLES), anyMap(), any(), any()))
                .thenReturn(List.of(Map.of("role", "SPV_PUBLISHER")));
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.CREATED_BY, "user-1");
        content.put(Constants.COURSE_CATEGORY, Constants.LEARNING_PATHWAY);
        content.put(Constants.BATCHES, List.of(
                Map.of(Constants.BATCH_ID, "batch-1"),
                Map.of(Constants.BATCH_ID, "batch-2")
        ));
        when(contentService.readContent(eq(CONTENT_ID), anyList())).thenReturn(content);
        // First batch returns empty, second batch returns active enrollments
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD_COURSE), eq(Constants.ENROLLMENT_BATCH_LOOKUP), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList())
                .thenReturn(List.of(Map.of(Constants.USER_ID, "u1", Constants.ACTIVE, true)));

        ApiResponse result = service.retireLearningPathway(USER_TOKEN, CONTENT_ID);

        assertEquals(HttpStatus.BAD_REQUEST, result.getResponseCode());
        assertEquals("Active enrollments available. Cannot retire the content.", result.get(Constants.MESSAGE));
        verify(cassandraOperation, times(2)).getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.ENROLLMENT_BATCH_LOOKUP),
                anyMap(),
                any(),
                any()
        );
    }

    @Test
    void retireLearningPathway_WhenUserIdIsNull_ReturnsBadRequest() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(USER_TOKEN), any(ApiResponse.class))).thenReturn(null);

        ApiResponse result = service.retireLearningPathway(USER_TOKEN, CONTENT_ID);

        assertEquals(HttpStatus.BAD_REQUEST, result.getResponseCode());
        assertEquals(Constants.FAILED, result.getParams().getStatus());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, result.getParams().getErr());
    }

    @Test
    void retireLearningPathway_WhenContentHasNullCreatedBy_ProceedsWithValidation() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(USER_TOKEN), any(ApiResponse.class))).thenReturn("user-1");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER_ROLES), anyMap(), any(), any()))
                .thenReturn(List.of(Map.of("role", "SPV_PUBLISHER")));
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.CREATED_BY, null);
        content.put(Constants.COURSE_CATEGORY, Constants.LEARNING_PATHWAY);
        content.put(Constants.BATCHES, Collections.emptyList());
        when(contentService.readContent(eq(CONTENT_ID), anyList())).thenReturn(content);
        when(contentService.retireContent(CONTENT_ID)).thenReturn(Map.of("status", "success"));

        ApiResponse result = service.retireLearningPathway(USER_TOKEN, CONTENT_ID);

        assertEquals(HttpStatus.OK, result.getResponseCode());
        assertEquals("Content retired successfully", result.get(Constants.MESSAGE));
    }
}
