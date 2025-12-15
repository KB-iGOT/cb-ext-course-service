package com.igot.cb.service;

import org.igot.common.auth.AccessTokenValidator;
import org.igot.common.cassandra.CassandraOperation;
import org.igot.common.service.OutboundRequestHandlerServiceImpl;

import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class NotificationServiceImplTest {

    // Use a concrete test implementation for OutboundRequestHandlerServiceImpl
    private static class TestOutboundRequestHandlerService extends OutboundRequestHandlerServiceImpl {
        private final java.util.Queue<Map<String, Object>> responses = new java.util.LinkedList<>();

        public TestOutboundRequestHandlerService() {
            super(null); // Pass null for RestTemplate since we're overriding the method
        }

        public void addResponse(Map<String, Object> response) {
            responses.add(response);
        }

        @Override
        public Map<String, Object> fetchResultUsingPost(String uri, Object request, Map<String, String> headersValues) {
            if (responses.isEmpty()) {
                return Collections.emptyMap();
            }
            return responses.poll();
        }
    }

    // Use a concrete test implementation for CbExtServerProperties
    private static class TestCbExtServerProperties extends CbExtServerProperties {
        @Override
        public String getNotificationServiceHost() {
            return "http://notify";
        }

        @Override
        public String getNotificationAsyncPath() {
            return "/async";
        }

        @Override
        public String getNotificationSupportMail() {
            return "support@example.com";
        }

        @Override
        public String getSbUrl() {
            return "http://sb";
        }

        @Override
        public String getUserSearchEndPoint() {
            return "/users/search";
        }

        @Override
        public String getCbWrapperNotificationHost() {
            return "http://wrapper";
        }

        @Override
        public String getCbWrapperNotificationPath() {
            return "/notify";
        }
    }

    private NotificationServiceImpl notificationService;

    @Mock
    private AccessTokenValidator accessTokenValidator;

    private final TestOutboundRequestHandlerService testOutboundRequestHandler = new TestOutboundRequestHandlerService();
    private final TestCbExtServerProperties testProps = new TestCbExtServerProperties();

    // Still need to mock these as they are interfaces and easier to mock
    private CassandraOperation cassandraOperation;

    private final String authToken = "validToken";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        // Create mocks manually to avoid @Mock annotation
        cassandraOperation = mock(CassandraOperation.class);

        // Setup default mock behavior for accessTokenValidator
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any()))
                .thenReturn("invokerUser");

        // Use the constructor to create the service with all dependencies
        notificationService = new NotificationServiceImpl(
                accessTokenValidator,
                cassandraOperation,
                testOutboundRequestHandler,
                testProps,
                new com.fasterxml.jackson.databind.ObjectMapper()
        );
    }

    private Map<String, Object> buildUserSearchResponse(String email, String firstName) {
        Map<String, Object> personal = new HashMap<>();
        personal.put(Constants.PRIMARY_EMAIL, email);
        personal.put(Constants.FIRST_NAME, firstName);

        Map<String, Object> profileDetails = Map.of(Constants.PERSONAL_DETAILS, personal);
        Map<String, Object> contentEntry = Map.of(Constants.PROFILE_DETAILS, profileDetails);

        List<Object> contentList = List.of(contentEntry);
        Map<String, Object> top = new HashMap<>();
        top.put(Constants.RESPONSE_CODE, "OK");
        top.put(Constants.RESULT, Map.of(Constants.RESPONSE, Map.of(Constants.CONTENT, contentList)));
        return top;
    }

    @Test
    void testNotifyAssignmentUploaded_success() {
        // arrange
        Map<String, Object> request = Map.of(
                Constants.COURSE_ID, "course1",
                Constants.BATCH_ID, "batch1",
                Constants.ASSIGNMENT_TITLE, "Assignment A"
        );

        // enrollments: one active user
        Map<String, Object> enrollment = new HashMap<>();
        enrollment.put(Constants.USER_ID, "learner1");
        enrollment.put(Constants.ACTIVE, true);
        List<Map<String, Object>> enrollments = List.of(enrollment);

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.ENROLLMENT_BATCH_LOOKUP),
                anyMap(), anyList(), isNull()))
                .thenReturn(enrollments);

        // email template fetch
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_EMAIL_TEMPLATE),
                anyMap(), anyList(), isNull()))
                .thenReturn(List.of(Map.of(Constants.TEMPLATE, "<html>Hi $assignmentTitle</html>")));

        // outbound: user search responses
        testOutboundRequestHandler.addResponse(buildUserSearchResponse("learner@example.com", "Learner"));
        testOutboundRequestHandler.addResponse(Collections.emptyMap());

        // act
        var response = notificationService.notifyAssignmentUploaded(request, authToken);

        // assert
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void testNotifyAssignmentUploaded_noEnrollments() {
        // arrange
        Map<String, Object> request = Map.of(
                Constants.COURSE_ID, "course1",
                Constants.BATCH_ID, "batch-no",
                Constants.ASSIGNMENT_TITLE, "Assignment A"
        );

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.ENROLLMENT_BATCH_LOOKUP),
                anyMap(), anyList(), isNull()))
                .thenReturn(Collections.emptyList());

        // act
        var response = notificationService.notifyAssignmentUploaded(request, authToken);

        // assert
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals("No enrolled users found for given courseId/batchId", response.getResult().get(Constants.MESSAGE));
    }

    @Test
    void testNotifyAssignmentEvaluate_missingLearner() {
        // arrange: missing learnerId
        Map<String, Object> request = Map.of(
                Constants.COURSE_ID, "course1",
                Constants.BATCH_ID, "batch1",
                Constants.ASSIGNMENT_TITLE, "Assignment A"
        );

        // act
        var response = notificationService.notifyAssignmentEvaluate(request, authToken);

        // assert -> bad request due to missing learnerId
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testNotifyAssignmentEvaluate_success() {
        // arrange
        Map<String, Object> request = Map.of(
                Constants.COURSE_ID, "course1",
                Constants.BATCH_ID, "batch1",
                Constants.ASSIGNMENT_TITLE, "Assignment A",
                Constants.LEARNER_ID, "learner1"
        );

        // email template fetch
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_EMAIL_TEMPLATE),
                anyMap(), anyList(), isNull()))
                .thenReturn(List.of(Map.of(Constants.TEMPLATE, "<html>Hi $firstName, Assignment: $assignment</html>")));

        // outbound: learner user search, notification send
        testOutboundRequestHandler.addResponse(buildUserSearchResponse("learner@example.com", "Learner"));
        testOutboundRequestHandler.addResponse(Collections.emptyMap()); // For in-app notification
        testOutboundRequestHandler.addResponse(Collections.emptyMap()); // For email notification

        // act
        var response = notificationService.notifyAssignmentEvaluate(request, authToken);

        // assert
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void testNotifyAssignmentSubmit_success() {
        Map<String, Object> request = Map.of(
                Constants.COURSE_ID, "course1",
                Constants.BATCH_ID, "batch1",
                Constants.ASSIGNMENT_TITLE, "Assignment A",
                Constants.INSTRUCTOR_ID, "instructor1"
        );

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_EMAIL_TEMPLATE),
                anyMap(), anyList(), isNull()))
                .thenReturn(List.of(Map.of(Constants.TEMPLATE, "<html>Hi $learnerName</html>")));

        // outbound: instructor user search, learner name lookup, notification send
        testOutboundRequestHandler.addResponse(buildUserSearchResponse("instructor@example.com", "Instructor"));
        testOutboundRequestHandler.addResponse(buildUserSearchResponse("learner@example.com", "Learner"));
        testOutboundRequestHandler.addResponse(Collections.emptyMap()); // For in-app notification
        testOutboundRequestHandler.addResponse(Collections.emptyMap()); // For email notification

        // act
        var response = notificationService.notifyAssignmentSubmit(request, authToken);

        // assert
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }
    @Test
    void notifyAssignmentSubmit_missingInstructor() {
        Map<String, Object> request = Map.of(
                Constants.COURSE_ID, "course1",
                Constants.BATCH_ID, "batch1",
                Constants.ASSIGNMENT_TITLE, "A"
        );

        var response = notificationService.notifyAssignmentSubmit(request, authToken);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void notifyAssignmentUploaded_noActiveUsers() {
        Map<String, Object> request = Map.of(
                Constants.COURSE_ID, "course1",
                Constants.BATCH_ID, "batch1",
                Constants.ASSIGNMENT_TITLE, "A"
        );

        Map<String, Object> enrollment = new HashMap<>();
        enrollment.put(Constants.USER_ID, "u1");
        enrollment.put(Constants.ACTIVE, false);

        when(cassandraOperation.getRecordsByProperties(any(), any(), anyMap(), anyList(), isNull()))
                .thenReturn(List.of(enrollment));

        var response = notificationService.notifyAssignmentUploaded(request, authToken);

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }
    @Test
    void notifyAssignmentUploaded_validationFailure() {
        Map<String, Object> request = Map.of(
                Constants.COURSE_ID, "course1"
                // batchId & assignment missing
        );

        var response = notificationService.notifyAssignmentUploaded(request, authToken);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }
    @Test
    void notifyAssignmentUploaded_invalidToken() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any()))
                .thenReturn("");

        Map<String, Object> request = Map.of(
                Constants.COURSE_ID, "course1",
                Constants.BATCH_ID, "batch1",
                Constants.ASSIGNMENT_TITLE, "A"
        );

        var response = notificationService.notifyAssignmentUploaded(request, authToken);

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }


}