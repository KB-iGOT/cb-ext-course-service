package com.igot.cb.service;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.user.UserUtilityService;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

class NotificationServiceImplTest {

    // Use a concrete test implementation instead of mocking to avoid ByteBuddy issues on Java 23
    private static class TestAccessTokenValidator extends AccessTokenValidator {
        private String userIdToReturn = "invokerUser";

        public TestAccessTokenValidator() {
            super(null);
        }

        public void setUserIdToReturn(String userId) {
            this.userIdToReturn = userId;
        }

        @Override
        public String fetchUserIdFromAccessToken(String accessToken, ApiResponse response) {
            return userIdToReturn;
        }
    }

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

    private final TestAccessTokenValidator testAccessTokenValidator = new TestAccessTokenValidator();
    private final TestOutboundRequestHandlerService testOutboundRequestHandler = new TestOutboundRequestHandlerService();
    private final TestCbExtServerProperties testProps = new TestCbExtServerProperties();

    // Still need to mock these as they are interfaces and easier to mock
    private CassandraOperation cassandraOperation;
    private UserUtilityService userUtilityService;

    private final String authToken = "validToken";

    @BeforeEach
    void setUp() throws Exception {
        notificationService = new NotificationServiceImpl();

        // Create mocks manually to avoid @Mock annotation
        cassandraOperation = mock(CassandraOperation.class);
        userUtilityService = mock(UserUtilityService.class);

        // Inject test AccessTokenValidator using reflection
        Field accessTokenValidatorField = NotificationServiceImpl.class.getDeclaredField("accessTokenValidator");
        accessTokenValidatorField.setAccessible(true);
        accessTokenValidatorField.set(notificationService, testAccessTokenValidator);

        Field cassandraOperationField = NotificationServiceImpl.class.getDeclaredField("cassandraOperation");
        cassandraOperationField.setAccessible(true);
        cassandraOperationField.set(notificationService, cassandraOperation);

        Field userUtilityServiceField = NotificationServiceImpl.class.getDeclaredField("userUtilityService");
        userUtilityServiceField.setAccessible(true);
        userUtilityServiceField.set(notificationService, userUtilityService);

        Field outboundRequestHandlerField = NotificationServiceImpl.class.getDeclaredField("outboundRequestHandlerService");
        outboundRequestHandlerField.setAccessible(true);
        outboundRequestHandlerField.set(notificationService, testOutboundRequestHandler);

        Field propsField = NotificationServiceImpl.class.getDeclaredField("props");
        propsField.setAccessible(true);
        propsField.set(notificationService, testProps);

        // Inject ObjectMapper to avoid NullPointerException
        Field objectMapperField = NotificationServiceImpl.class.getDeclaredField("objectMapper");
        objectMapperField.setAccessible(true);
        objectMapperField.set(notificationService, new com.fasterxml.jackson.databind.ObjectMapper());
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
    void sendNotificationForContentRetirement_emptyUsers_shouldReturn() {

        notificationService.sendNotificationForContentRetirement(
                "do_1",
                "Course A",
                LocalDate.now(),
                Collections.emptyList(),
                Constants.CONTENT_RETIREMENT_APPROVED_NOTIFICATION
        );
    }

    @Test
    void sendNotificationForContentRetirement_approved_shouldSendApprovedTemplate() {

        notificationService.sendNotificationForContentRetirement(
                "do_2",
                "Course Approved",
                LocalDate.of(2026, 1, 10),
                List.of("user1"),
                Constants.CONTENT_RETIREMENT_APPROVED_NOTIFICATION
        );
    }

    @Test
    void sendNotificationForContentRetirement_sevenDayReminder_shouldSendReminder() {

        notificationService.sendNotificationForContentRetirement(
                "do_3",
                "Course Reminder",
                LocalDate.of(2026, 1, 10),
                List.of("user1"),
                Constants.REMINDER_NOTIFICATION_SEVEN_DAY
        );
    }

    @Test
    void sendNotificationForContentRetirement_oneDayReminder_shouldSendReminder() {

        notificationService.sendNotificationForContentRetirement(
                "do_4",
                "Course Reminder",
                LocalDate.of(2026, 1, 10),
                List.of("user1"),
                Constants.REMINDER_NOTIFICATION_ONE_DAY
        );
    }

    @Test
    void sendNotificationForContentRetirement_finalRetired_shouldSendFinalNotification() {

        notificationService.sendNotificationForContentRetirement(
                "do_5",
                "Course Retired",
                LocalDate.of(2026, 1, 10),
                List.of("user1"),
                "UNKNOWN_TYPE"
        );
    }

    @Test
    void sendNotificationForContentRetirement_exceptionThrown_shouldBeCaught() {
        NotificationServiceImpl spyService = spy(notificationService);

        doThrow(new RuntimeException("Boom"))
                .when(spyService)
                .sendInAppNotification(
                        anyString(),
                        anyString(),
                        anyList(),
                        anyMap()
                );

        assertDoesNotThrow(() ->
                spyService.sendNotificationForContentRetirement(
                        "do_999",
                        "Crash Course",
                        LocalDate.now(),
                        List.of("user1"),
                        Constants.CONTENT_RETIREMENT_APPROVED_NOTIFICATION
                )
        );
    }

    @Test
    void sendNotificationForContentRetirementSpv_validInput_shouldSendNotification() {

        NotificationServiceImpl spyService = spy(notificationService);

        ArrayList<String> users = new ArrayList<>(List.of("user1", "user2"));
        LocalDate date = LocalDate.now();

        // Act
        spyService.sendNotificationForContentRetirementSpv(
                "do_123",
                "Sample Course",
                users,
                Constants.CONTENT_RETIREMENT_SCHEDULED_NOTIFICATION,
                date
        );

        // Assert
        verify(spyService).sendInAppNotification(
                eq(Constants.CONTENT_RETIREMENT_SCHEDULED_NOTIFICATION),
                eq(Constants.ALERT),
                eq(users),
                argThat(message -> {
                    Map<String, String> placeholders =
                            (Map<String, String>) message.get(Constants.PLACE_HOLDERS);
                    Map<String, Object> data =
                            (Map<String, Object>) message.get(Constants.DATA);

                    return "Sample Course".equals(placeholders.get(Constants.TITLE))
                            && date.toString().equals(placeholders.get(Constants.DATE_KEY))
                            && "do_123".equals(data.get(Constants.ID));
                })
        );
    }

    @Test
    void sendNotificationForContentRetirementSpv_emptyUsers_shouldReturnEarly() {

        NotificationServiceImpl spyService = spy(notificationService);

        // Act
        spyService.sendNotificationForContentRetirementSpv(
                "do_124",
                "Course X",
                new ArrayList<>(),
                Constants.CONTENT_RETIREMENT_SCHEDULED_NOTIFICATION,
                LocalDate.now()
        );

        // Assert
        verify(spyService, never()).sendInAppNotification(any(), any(), any(), any());
    }

    @Test
    void sendNotificationForContentRetirementSpv_exceptionThrown_shouldBeCaught() {

        NotificationServiceImpl spyService = spy(notificationService);

        doThrow(new RuntimeException("Boom"))
                .when(spyService)
                .sendInAppNotification(any(), any(), any(), any());

        ArrayList<String> users = new ArrayList<>(List.of("user1"));

        assertDoesNotThrow(() ->
                spyService.sendNotificationForContentRetirementSpv(
                        "do_500",
                        "Crash Course",
                        users,
                        Constants.CONTENT_RETIREMENT_SCHEDULED_NOTIFICATION,
                        LocalDate.now()
                )
        );
    }


}
