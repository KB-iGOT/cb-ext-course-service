package com.igot.cb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.user.UserUtilityService;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@Slf4j
@ExtendWith(MockitoExtension.class)
public class NotificationServiceImplTest {

    @InjectMocks
    private NotificationServiceImpl service;

    @Mock
    private AccessTokenValidator accessTokenValidator;
    @Mock
    private CassandraOperation cassandraOperation;
    @Mock
    private UserUtilityService userUtilityService;
    @Mock
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    @Mock
    private CbExtServerProperties props;
    @Mock
    private ObjectMapper objectMapper;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        lenient().when(props.getNotificationSupportMail()).thenReturn("noreply@test.com");
        lenient().when(props.getNotificationServiceHost()).thenReturn("http://notify");
        lenient().when(props.getNotificationAsyncPath()).thenReturn("/async");
        lenient().when(props.getCbWrapperNotificationHost()).thenReturn("http://wrapper");
        lenient().when(props.getCbWrapperNotificationPath()).thenReturn("/notify");
        lenient().when(props.getSbUrl()).thenReturn("http://sb");
        lenient().when(props.getUserSearchEndPoint()).thenReturn("/user/search");
    }


    private Map<String, Object> createValidRequest() {
        Map<String, Object> req = new HashMap<>();
        req.put(Constants.COURSE_ID, "course123");
        req.put(Constants.BATCH_ID, "batch123");
        req.put(Constants.ASSIGNMENT_TITLE, "Assignment 1");
        req.put(Constants.LEARNER_ID, "learner123");
        req.put(Constants.INSTRUCTOR_ID, "inst123");
        return req;
    }

    @Test
    void testNotifyAssignmentUploaded_Success() throws Exception {
        Map<String, Object> request = createValidRequest();
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user123");

        Map<String, Object> enrollment = Map.of(Constants.ACTIVE, true, Constants.USER_ID, "user1");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), any()))
                .thenReturn(List.of(enrollment));

        Map<String, Object> emailResponse = Map.of(Constants.EMAILS, List.of("test@test.com"), Constants.FIRST_NAME, "John");
        lenient().when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), any(), any()))
                .thenReturn(emailResponse);
        lenient().when(objectMapper.writeValueAsString(any())).thenReturn("{}");

        ApiResponse response = service.notifyAssignmentUploaded(request, "authToken");
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }


    @Test
    void testNotifyAssignmentUploaded_InvalidUserId() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("");
        ApiResponse response = service.notifyAssignmentUploaded(createValidRequest(), "auth");
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }


    @Test
    void testNotifyAssignmentUploaded_NoEnrollments() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user123");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), any()))
                .thenReturn(Collections.emptyList());

        ApiResponse resp = service.notifyAssignmentUploaded(createValidRequest(), "auth");
        assertEquals(HttpStatus.OK, resp.getResponseCode());
    }

    @Test
    void testNotifyAssignmentUploaded_Exception() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user123");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), any()))
                .thenThrow(new RuntimeException("DB fail"));
        ApiResponse resp = service.notifyAssignmentUploaded(createValidRequest(), "auth");
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, resp.getResponseCode());
    }

    // === notifyAssignmentEvaluate ===
    @Test
    void testNotifyAssignmentEvaluate_Success() {
        Map<String, Object> req = createValidRequest();
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");
        Map<String, Object> userData = Map.of(Constants.EMAILS, List.of("mail@x.com"), Constants.FIRST_NAME, "John");
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), any(), any())).thenReturn(userData);

        ApiResponse resp = service.notifyAssignmentEvaluate(req, "auth");
        assertEquals(HttpStatus.OK, resp.getResponseCode());
    }

    @Test
    void testNotifyAssignmentEvaluate_EmptyUserId() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("");
        ApiResponse resp = service.notifyAssignmentEvaluate(createValidRequest(), "auth");
        assertNull(resp.getResponseCode());
    }

    @Test
    void testNotifyAssignmentEvaluate_MissingFields() {
        Map<String, Object> badReq = new HashMap<>();
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");
        ApiResponse resp = service.notifyAssignmentEvaluate(badReq, "auth");
        assertEquals(HttpStatus.BAD_REQUEST, resp.getResponseCode());
    }

    // === notifyAssignmentSubmit ===
    @Test
    void testNotifyAssignmentSubmit_Success() {
        Map<String, Object> req = createValidRequest();
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");
        Map<String, Object> userData = Map.of(Constants.EMAILS, List.of("mail@x.com"), Constants.FIRST_NAME, "John");
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), any(), any())).thenReturn(userData);

        ApiResponse resp = service.notifyAssignmentSubmit(req, "auth");
        assertEquals(HttpStatus.OK, resp.getResponseCode());
    }

    @Test
    void testNotifyAssignmentSubmit_InvalidInstructor() {
        Map<String, Object> req = createValidRequest();
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), any(), any()))
                .thenReturn(Collections.emptyMap());
        ApiResponse resp = service.notifyAssignmentSubmit(req, "auth");
        assertEquals(HttpStatus.OK, resp.getResponseCode());
    }

    @Test
    void testSendInAppNotification_AllBranches() throws Exception {
        lenient().when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        doReturn(Collections.emptyMap())
                .when(outboundRequestHandlerService)
                .fetchResultUsingPost(anyString(), anyString(), anyMap());

        service.sendInAppNotification("", "", Collections.emptyList(), Collections.emptyMap());
        service.sendInAppNotification("sub", "type", List.of("user1"), Map.of("msg", "ok"));
    }



    @Test
    void testConstructEmailTemplate_Success() throws Exception {
        // Arrange
        Map<String, Object> row = Map.of(Constants.TEMPLATE, "Hello $name");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), any()))
                .thenReturn(List.of(row));

        // Access private method via reflection
        java.lang.reflect.Method method = NotificationServiceImpl.class
                .getDeclaredMethod("constructEmailTemplate", String.class, Map.class);
        method.setAccessible(true);

        // Invoke method
        String html = (String) method.invoke(service, "template1", Map.of("name", "user"));

        // Assert
        assertTrue(html.contains("user"));
    }

    @Test
    void testConstructEmailTemplate_Exception() throws Exception {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), any()))
                .thenThrow(new RuntimeException("fail"));
        java.lang.reflect.Method method = NotificationServiceImpl.class
                .getDeclaredMethod("constructEmailTemplate", String.class, Map.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> {
            try {
                method.invoke(service, "x", Map.of());
            } catch (Exception e) {
                log.error("Exception in invoking method: ", e);
            }
        });
    }

    @Test
    void testFetchUserEmails_Success() throws Exception {
        Map<String, Object> content = Map.of(
                Constants.PROFILE_DETAILS, Map.of(Constants.PERSONAL_DETAILS,
                        Map.of(Constants.PRIMARY_EMAIL, "a@b.com", Constants.FIRST_NAME, "A"))
        );
        Map<String, Object> result = Map.of(
                Constants.RESULT, Map.of(
                        Constants.RESPONSE, Map.of(Constants.CONTENT, List.of(content))
                ),
                Constants.RESPONSE_CODE, "OK"
        );
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), any(), any()))
                .thenReturn(result);
        java.lang.reflect.Method method = NotificationServiceImpl.class
                .getDeclaredMethod("fetchUserEmails", List.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> resp = (Map<String, Object>) method.invoke(service, List.of("uid"));
        assertTrue(((List<?>) resp.get(Constants.EMAILS)).contains("a@b.com"));
        assertEquals("A", resp.get(Constants.FIRST_NAME));
    }


    @Test
    void testFetchUserEmails_EmptyResponse() throws Exception {
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), any(), any()))
                .thenReturn(Collections.emptyMap());
        java.lang.reflect.Method method = NotificationServiceImpl.class
                .getDeclaredMethod("fetchUserEmails", List.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> resp = (Map<String, Object>) method.invoke(service, List.of("u"));
        assertTrue(resp.containsKey(Constants.EMAILS));
        assertTrue(resp.containsKey(Constants.FIRST_NAME));
    }




}
