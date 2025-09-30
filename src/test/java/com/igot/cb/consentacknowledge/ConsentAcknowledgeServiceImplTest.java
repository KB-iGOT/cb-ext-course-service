package com.igot.cb.consentacknowledge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import org.springframework.http.HttpStatus;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ConsentAcknowledgeServiceImplTest {

    @Mock
    private AccessTokenValidator accessTokenValidator;

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private ConsentAcknowledgeServiceImpl service;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testAcknowledgeDeclaration_Success() throws Exception {
        Map<String, Object> requestData = new HashMap<>();
        requestData.put(Constants.CONTENT_ID, "content1");
        requestData.put(Constants.CONSENT_ID, "consent1");
        requestData.put(Constants.ADDITIONAL_ATTRIBUTES, Map.of("k", "v"));
        Map<String, Object> body = Map.of(Constants.REQUEST, requestData);
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any()))
                .thenReturn("user1");
        when(objectMapper.writeValueAsString(any()))
                .thenReturn("{\"k\":\"v\"}");
        ApiResponse response = service.acknowledgeDeclaration(body, "token");
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.OK, response.getParams().getStatus());
        Map<String, Object> result = response.getResult();
        assertNotNull(result);

        Map<String, Object> consentAckDetails =
                (Map<String, Object>) result.get(Constants.RESPONSE);
        assertNotNull(consentAckDetails);
        assertEquals("content1", consentAckDetails.get(Constants.CONTENT_ID));
        assertEquals("consent1", consentAckDetails.get(Constants.CONSENT_ID));
        assertEquals("user1", consentAckDetails.get(Constants.USER_ID));
        assertEquals("Declaration acknowledged successfully", consentAckDetails.get(Constants.MESSAGE));
        verify(cassandraOperation, times(1))
                .insertRecord(any(), any(), any(), any(), any(), any());
    }

    @Test
    void testAcknowledgeDeclaration_InvalidUser() {
        Map<String, Object> body = Map.of(Constants.REQUEST, new HashMap<>());
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("");
        ApiResponse response = service.acknowledgeDeclaration(body, "token");
        assertNotEquals(Constants.OK, response.getParams().getStatus());
    }

    @Test
    void testAcknowledgeDeclaration_MissingContentId() {
        Map<String, Object> requestData = new HashMap<>();
        requestData.put(Constants.CONSENT_ID, "consent1");
        Map<String, Object> body = Map.of(Constants.REQUEST, requestData);
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user1");
        try (MockedStatic<ProjectUtil> mocked = mockStatic(ProjectUtil.class)) {
            mocked.when(() -> ProjectUtil.createDefaultResponse(anyString()))
                    .thenCallRealMethod();
            mocked.when(() -> ProjectUtil.errorResponse(any(ApiResponse.class), anyString(), any()))
                    .thenAnswer(invocation -> {
                        ApiResponse resp = invocation.getArgument(0);
                        resp.getParams().setErr(invocation.getArgument(1));
                        resp.setResponseCode(invocation.getArgument(2));
                        return null;
                    });
            ApiResponse response = service.acknowledgeDeclaration(body, "token");
            assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
            assertEquals("Missing or invalid contentId.", response.getParams().getErr());
        }
    }

    @Test
    void testAcknowledgeDeclaration_MissingConsentId() {
        Map<String, Object> requestData = new HashMap<>();
        requestData.put(Constants.CONTENT_ID, "content1");
        Map<String, Object> body = Map.of(Constants.REQUEST, requestData);
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user1");
        try (MockedStatic<ProjectUtil> mocked = mockStatic(ProjectUtil.class)) {
            mocked.when(() -> ProjectUtil.createDefaultResponse(anyString()))
                    .thenCallRealMethod();
            mocked.when(() -> ProjectUtil.errorResponse(any(ApiResponse.class), anyString(), any()))
                    .thenAnswer(invocation -> {
                        ApiResponse resp = invocation.getArgument(0);
                        resp.getParams().setErr(invocation.getArgument(1));
                        resp.setResponseCode(invocation.getArgument(2));
                        return null;
                    });
            ApiResponse response = service.acknowledgeDeclaration(body, "token");
            assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
            assertEquals("Missing or invalid consentId.", response.getParams().getErr());
        }
    }

    @Test
    void testAcknowledgeDeclaration_CassandraInsertFails() throws Exception {
        Map<String, Object> requestData = new HashMap<>();
        requestData.put(Constants.CONTENT_ID, "content1");
        requestData.put(Constants.CONSENT_ID, "consent1");
        requestData.put(Constants.ADDITIONAL_ATTRIBUTES, Map.of("k", "v"));
        Map<String, Object> body = Map.of(Constants.REQUEST, requestData);
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user1");
        when(objectMapper.writeValueAsString(any())).thenReturn("{\"k\":\"v\"}");
        doThrow(new RuntimeException("DB error"))
                .when(cassandraOperation).insertRecord(any(), any(), any(), any(), any(), any());
        try (MockedStatic<ProjectUtil> mocked = mockStatic(ProjectUtil.class)) {
            mocked.when(() -> ProjectUtil.createDefaultResponse(anyString()))
                    .thenCallRealMethod();
            mocked.when(() -> ProjectUtil.errorResponse(any(ApiResponse.class), anyString(), any()))
                    .thenAnswer(invocation -> {
                        ApiResponse resp = invocation.getArgument(0);
                        resp.getParams().setErr(invocation.getArgument(1)); // force set error
                        resp.setResponseCode(invocation.getArgument(2));
                        return null;
                    });

            ApiResponse response = service.acknowledgeDeclaration(body, "token");
            assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
            assertEquals("Failed to acknowledge declaration. Please try again later.",
                    response.getParams().getErr());
        }
    }


    @Test
    void testGetConsentDetails_Success() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user1");
        List<Map<String, Object>> fakeResult = List.of(Map.of(Constants.CONSENT_ID, "consent1", Constants.DESCRIPTION, "desc"));
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any())).thenReturn(fakeResult);
        ApiResponse response = service.getConsentDetails("consent1", "token");
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertTrue(response.getResult().containsKey(Constants.RESPONSE));
    }

    @Test
    void testGetConsentDetails_Failure_DBError() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user1");
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenThrow(new RuntimeException("DB error"));
        try (MockedStatic<ProjectUtil> mocked = mockStatic(ProjectUtil.class)) {
            mocked.when(() -> ProjectUtil.createDefaultResponse(anyString()))
                    .thenCallRealMethod();
            mocked.when(() -> ProjectUtil.errorResponse(any(ApiResponse.class), anyString(), any()))
                    .thenAnswer(invocation -> {
                        ApiResponse resp = invocation.getArgument(0);
                        resp.getParams().setErr(invocation.getArgument(1)); // force populate err
                        resp.setResponseCode(invocation.getArgument(2));
                        return null;
                    });
            ApiResponse response = service.getConsentDetails("consent1", "token");
            assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
            assertEquals("Failed to fetch consent details. Please try again later.",
                    response.getParams().getErr());
        }
    }


    @Test
    void testGetConsentDetails_InvalidUser() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("");
        ApiResponse response = service.getConsentDetails("consent1", "token");
        assertNotEquals(Constants.OK, response.getParams().getStatus());
    }


    @Test
    void testAcknowledgeDeclaration_EmptyAdditionalData() {
        Map<String, Object> requestData = new HashMap<>();
        requestData.put(Constants.CONTENT_ID, "content1");
        requestData.put(Constants.CONSENT_ID, "consent1");
        requestData.put(Constants.ADDITIONAL_ATTRIBUTES, new HashMap<>()); // empty map
        Map<String, Object> body = Map.of(Constants.REQUEST, requestData);
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user1");
        try (MockedStatic<ProjectUtil> mocked = mockStatic(ProjectUtil.class)) {
            mocked.when(() -> ProjectUtil.createDefaultResponse(anyString()))
                    .thenCallRealMethod();
            mocked.when(() -> ProjectUtil.errorResponse(any(ApiResponse.class), anyString(), any()))
                    .thenAnswer(invocation -> {
                        ApiResponse resp = invocation.getArgument(0);
                        resp.getParams().setErr(invocation.getArgument(1));
                        resp.setResponseCode(invocation.getArgument(2));
                        return null;
                    });
            ApiResponse response = service.acknowledgeDeclaration(body, "token");
            assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
            assertEquals("Missing or invalid additionalData.", response.getParams().getErr());
        }
    }

    @Test
    void testAcknowledgeDeclaration_UserIdEmpty() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("");
        Map<String, Object> body = Map.of(Constants.REQUEST,
                Map.of(Constants.CONTENT_ID, "c1", Constants.CONSENT_ID, "x1"));
        try (MockedStatic<ProjectUtil> mocked = mockStatic(ProjectUtil.class)) {
            mocked.when(() -> ProjectUtil.createDefaultResponse(anyString()))
                    .thenAnswer(inv -> {
                        ApiResponse resp = new ApiResponse();
                        resp.getParams().setStatus(Constants.FAILED);
                        return resp;
                    });
            ApiResponse response = service.acknowledgeDeclaration(body, "token");
            assertEquals(Constants.FAILED, response.getParams().getStatus());
        }
    }


    @Test
    void testAcknowledgeDeclaration_RequestDataNull() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user1");
        Map<String, Object> body = new HashMap<>();
        try (MockedStatic<ProjectUtil> mocked = mockStatic(ProjectUtil.class)) {
            mocked.when(() -> ProjectUtil.createDefaultResponse(anyString())).thenCallRealMethod();
            mocked.when(() -> ProjectUtil.errorResponse(any(), anyString(), any()))
                    .thenAnswer(inv -> {
                        ApiResponse resp = inv.getArgument(0);
                        resp.getParams().setErr(inv.getArgument(1));
                        resp.setResponseCode(inv.getArgument(2));
                        return null;
                    });

            ApiResponse response = service.acknowledgeDeclaration(body, "token");
            assertEquals("Request data is missing.", response.getParams().getErr());
        }
    }

    @Test
    void testAcknowledgeDeclaration_EmptyAdditionalAttributes() {
        Map<String, Object> requestData = new HashMap<>();
        requestData.put(Constants.CONTENT_ID, "c1");
        requestData.put(Constants.CONSENT_ID, "x1");
        requestData.put(Constants.ADDITIONAL_ATTRIBUTES, new HashMap<>());
        Map<String, Object> body = Map.of(Constants.REQUEST, requestData);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user1");

        try (MockedStatic<ProjectUtil> mocked = mockStatic(ProjectUtil.class)) {
            mocked.when(() -> ProjectUtil.createDefaultResponse(anyString())).thenCallRealMethod();
            mocked.when(() -> ProjectUtil.errorResponse(any(), anyString(), any()))
                    .thenAnswer(inv -> {
                        ApiResponse resp = inv.getArgument(0);
                        resp.getParams().setErr(inv.getArgument(1));
                        resp.setResponseCode(inv.getArgument(2));
                        return null;
                    });

            ApiResponse response = service.acknowledgeDeclaration(body, "token");
            assertEquals("Missing or invalid additionalData.", response.getParams().getErr());
        }
    }

    @Test
    void testAcknowledgeDeclaration_JsonProcessingException() throws Exception {
        Map<String, Object> requestData = new HashMap<>();
        requestData.put(Constants.CONTENT_ID, "c1");
        requestData.put(Constants.CONSENT_ID, "x1");
        requestData.put(Constants.ADDITIONAL_ATTRIBUTES, Map.of("k", "v"));
        Map<String, Object> body = Map.of(Constants.REQUEST, requestData);
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user1");
        when(objectMapper.writeValueAsString(any())).thenThrow(new JsonProcessingException("boom") {
        });
        ApiResponse response = service.acknowledgeDeclaration(body, "token");
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertNull(((Map<?, ?>) response.getResult()).get(Constants.ADDITIONAL_ATTRIBUTES));
    }

    @Test
    void testGetConsentDetails_UserIdEmpty() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("");
        try (MockedStatic<ProjectUtil> mocked = mockStatic(ProjectUtil.class)) {
            mocked.when(() -> ProjectUtil.createDefaultResponse(anyString()))
                    .thenAnswer(inv -> {
                        ApiResponse resp = new ApiResponse();
                        resp.getParams().setStatus(Constants.FAILED);
                        resp.setResponseCode(HttpStatus.BAD_REQUEST);
                        return resp;
                    });
            ApiResponse response = service.getConsentDetails("c1", "token");
            assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
            assertEquals(Constants.FAILED, response.getParams().getStatus());
        }
    }


    @Test
    void testGetConsentDetails_EmptyListThrows() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user1");
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(List.of());
        assertThrows(IndexOutOfBoundsException.class,
                () -> service.getConsentDetails("c1", "token"));
    }

    @Test
    void testAcknowledgeDeclaration_JsonProcessingFails() throws Exception {
        Map<String, Object> requestData = new HashMap<>();
        requestData.put(Constants.CONTENT_ID, "content1");
        requestData.put(Constants.CONSENT_ID, "consent1");
        requestData.put(Constants.ADDITIONAL_ATTRIBUTES, Map.of("k", "v"));
        Map<String, Object> body = Map.of(Constants.REQUEST, requestData);
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user1");
        when(objectMapper.writeValueAsString(any()))
                .thenThrow(new JsonProcessingException("boom") {});
        try (MockedStatic<ProjectUtil> mocked = mockStatic(ProjectUtil.class)) {
            mocked.when(() -> ProjectUtil.createDefaultResponse(anyString()))
                    .thenCallRealMethod();
            mocked.when(() -> ProjectUtil.errorResponse(any(ApiResponse.class), anyString(), any()))
                    .thenAnswer(invocation -> {
                        ApiResponse resp = invocation.getArgument(0);
                        resp.getParams().setErr(invocation.getArgument(1));
                        resp.setResponseCode(invocation.getArgument(2));
                        return null;
                    });
            ApiResponse response = service.acknowledgeDeclaration(body, "token");
            assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
            assertEquals("Failed to Parse the additional attributes",
                    response.getParams().getErr());
        }
    }
}
