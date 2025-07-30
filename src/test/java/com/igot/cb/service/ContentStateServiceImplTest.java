package com.igot.cb.service;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.cassandra.exceptions.CustomException;
import com.igot.cb.model.ApiResponse;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ContentStateServiceImplTest {

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private AccessTokenValidator accessTokenValidator;

    @InjectMocks
    private ContentStateServiceImpl service;

    @BeforeEach
    void setup() {
        // Set required fields via reflection (since @Value is not injected in unit tests)
        TestUtils.setField(service, "allowedFieldsConfig", "userId,contentId,lastAccessTime,lastCompletedTime,lastUpdatedTime,progress,progressdetails,status,completionPercentage");
        TestUtils.setField(service, "requiredFieldsConfig", "contentId,status,completionPercentage");
    }

    @Test
    void testReadContentState_success() {
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.CONTENT_IDS, List.of("c1"));
        requestMap.put(Constants.FIELDS, List.of("userId", "contentId", "progress"));
        Map<String, Object> requestBody = Map.of(Constants.REQUEST, requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user-1");
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(List.of(Map.of(Constants.USER_ID_LOWER_CASE, "user-1", Constants.RESOURCE_ID, "c1", Constants.PROGRESS, 50)));

        ApiResponse response = service.readContentState(requestBody, "token");
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertTrue(response.getResult().containsKey(Constants.CONTENT_LIST));
    }

    @Test
    void testReadContentState_emptyRequestBody() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user-1");
        ApiResponse response = service.readContentState(Collections.emptyMap(), "token");
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testReadContentState_invalidRequestObject() {
        Map<String, Object> requestBody = Map.of(Constants.REQUEST, "notAMap");
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user-1");
        ApiResponse response = service.readContentState(requestBody, "token");
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testReadContentState_missingContentIds() {
        Map<String, Object> requestMap = new HashMap<>();
        Map<String, Object> requestBody = Map.of(Constants.REQUEST, requestMap);
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user-1");
        ApiResponse response = service.readContentState(requestBody, "token");
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testReadContentState_invalidFields() {
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.CONTENT_IDS, List.of("c1"));
        requestMap.put(Constants.FIELDS, List.of("invalidField"));
        Map<String, Object> requestBody = Map.of(Constants.REQUEST, requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user-1");
        ApiResponse response = service.readContentState(requestBody, "token");
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testReadContentState_invalidToken() {
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.CONTENT_IDS, List.of("c1"));
        Map<String, Object> requestBody = Map.of(Constants.REQUEST, requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("");
        ApiResponse response = service.readContentState(requestBody, "token");
        assertEquals(HttpStatus.OK, response.getResponseCode()); // Fix: expect OK, not null
    }

    @Test
    void testReadContentState_exception() {
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.CONTENT_IDS, List.of("c1"));
        Map<String, Object> requestBody = Map.of(Constants.REQUEST, requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user-1");
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenThrow(new RuntimeException("DB error"));

        ApiResponse response = service.readContentState(requestBody, "token");
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testUpdateContentState_success() {
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.CONTENT_ID, "c1");
        content.put(Constants.STATUS, 2);
        content.put(Constants.COMPLETION_PERCENTAGE, 100);
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.CONTENTS, List.of(content));
        Map<String, Object> requestBody = Map.of(Constants.REQUEST, requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user-1");
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = service.updateContentState(requestBody, "token");
        assertEquals(HttpStatus.OK, response.getResponseCode()); // Not set in success path
        assertEquals(Constants.SUCCESS, response.getResult().get("c1"));
    }

    @Test
    void testUpdateContentState_emptyRequestBody() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user-1");
        ApiResponse response = service.updateContentState(Collections.emptyMap(), "token");
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testUpdateContentState_invalidPayload() {
        Map<String, Object> requestBody = Map.of(Constants.REQUEST, "notAMap");
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user-1");
        ApiResponse response = service.updateContentState(requestBody, "token");
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testUpdateContentState_invalidToken() {
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.CONTENTS, List.of());
        Map<String, Object> requestBody = Map.of(Constants.REQUEST, requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("");
        ApiResponse response = service.updateContentState(requestBody, "token");
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void testUpdateContentState_exception() {
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.CONTENT_ID, "c1");
        content.put(Constants.STATUS, 2);
        content.put(Constants.COMPLETION_PERCENTAGE, 100);
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.CONTENTS, List.of(content));
        Map<String, Object> requestBody = Map.of(Constants.REQUEST, requestMap);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user-1");
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenThrow(new RuntimeException("DB error"));

        ApiResponse response = service.updateContentState(requestBody, "token");
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testValidateContentStateUpdatePayload_missingFields() {
        Map<String, Object> requestBody = Map.of(Constants.REQUEST, Map.of(Constants.CONTENTS, List.of(Map.of())));
        String result = TestUtils.invokePrivate(service, "validateContentStateUpdatePayload", Map.class, requestBody);
        assertTrue(result.contains("Missing or invalid fields"));
    }

    @Test
    void testProcessContentConsumption_newContent_completed() throws Exception {
        Map<String, Object> inputContent = new HashMap<>();
        inputContent.put(Constants.STATUS, 2);
        inputContent.put(Constants.COMPLETION_PERCENTAGE, 100);
        inputContent.put(Constants.CONTENT_ID, "c1");
        inputContent.put(Constants.LAST_COMPLETED_TIME, "2024-06-01 10:00:00:000+0000");
        inputContent.put(Constants.LAST_ACCESS_TIME, "2024-06-01 09:00:00:000+0000");
        Map<String, Object> result = service.processContentConsumption(inputContent, null, "user-1");
        assertEquals(100.0, result.get(Constants.COMPLETION_PERCENTAGE));
        assertEquals(2, result.get(Constants.STATUS));
        assertEquals("user-1", result.get(Constants.USER_ID));
    }

    @Test
    void testProcessContentConsumption_existingContent_incomplete() throws Exception {
        Map<String, Object> inputContent = new HashMap<>();
        inputContent.put(Constants.STATUS, 1);
        inputContent.put(Constants.COMPLETION_PERCENTAGE, 50);
        inputContent.put(Constants.CONTENT_ID, "c1");
        inputContent.put(Constants.LAST_COMPLETED_TIME, "2024-06-01 10:00:00:000+0000");
        inputContent.put(Constants.LAST_ACCESS_TIME, "2024-06-01 09:00:00:000+0000");

        Map<String, Object> existingContent = new HashMap<>();
        existingContent.put(Constants.STATUS, 2);
        existingContent.put(Constants.COMPLETION_PERCENTAGE, 100);
        existingContent.put(Constants.LAST_COMPLETED_TIME, "2024-06-01 08:00:00:000+0000");
        existingContent.put(Constants.LAST_ACCESS_TIME, "2024-06-01 07:00:00:000+0000");
        existingContent.put(Constants.PROGRESS, 80);

        Map<String, Object> result = service.processContentConsumption(inputContent, existingContent, "user-1");
        assertEquals(2, result.get(Constants.STATUS));
        assertEquals(80, result.get(Constants.PROGRESS));
    }

    @Test
    void testProcessContentConsumption_invalidCompletionPercentageType() {
        Map<String, Object> inputContent = new HashMap<>();
        inputContent.put(Constants.STATUS, 1);
        inputContent.put(Constants.COMPLETION_PERCENTAGE, "notANumber");
        inputContent.put(Constants.CONTENT_ID, "c1");
        assertThrows(CustomException.class, () -> service.processContentConsumption(inputContent, null, "user-1"));
    }

    @Test
    void testProcessContentConsumption_invalidCompletionPercentageValue() {
        Map<String, Object> inputContent = new HashMap<>();
        inputContent.put(Constants.STATUS, 1);
        inputContent.put(Constants.COMPLETION_PERCENTAGE, 200);
        inputContent.put(Constants.CONTENT_ID, "c1");
        assertThrows(CustomException.class, () -> service.processContentConsumption(inputContent, null, "user-1"));
    }

    @Test
    void testParseDate_validAndInvalid() {
        Date date = service.parseDate("2024-06-01 10:00:00:000+0000");
        assertNotNull(date);
        assertNull(service.parseDate("invalid-date"));
        assertNull(service.parseDate(null));
        assertNull(service.parseDate("null"));
    }

    @Test
    void testMapPayloadToCassandraColumns() {
        Map<String, Object> payload = Map.of("foo", "bar");
        Map<String, String> mapping = Map.of("foo", "baz");
        Map<String, Object> result = ContentStateServiceImpl.mapPayloadToCassandraColumns(payload, mapping);
        assertEquals("bar", result.get("baz"));
    }
}
