package com.igot.cb.service;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ContentRetirementServiceTest {

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private ContentInfoServiceImpl contentService;


    @Mock
    private NotificationService notificationService;

    @Mock
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @Mock
    private CbExtServerProperties props;

    @InjectMocks
    private ContentRetirementService contentRetirementService;

    @Test
    void processDueRetirements_NoRecords_ShouldReturnEmptyList() {
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = contentRetirementService.processDueRetirements();

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertTrue(response.getResult().containsKey(Constants.CONTENT));
        assertTrue(((List<?>) response.getResult().get(Constants.CONTENT)).isEmpty());
        verifyNoInteractions(contentService);
    }

    @Test
    void processDueRetirements_WithDueContent_ShouldRetireContent() {
        Map<String, Object> record = createRetirementRecord("content123", "request123", LocalDate.now().minusDays(1));
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(Arrays.asList(record));
        when(contentService.retireContent("content123"))
                .thenReturn(Map.of("status", "success"));

        ApiResponse response = contentRetirementService.processDueRetirements();

        assertNotNull(response);
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT);
        assertEquals(1, contentList.size());
        assertEquals("content123", contentList.get(0).get(Constants.CONTENT_ID));
        assertEquals(true, contentList.get(0).get(Constants.RETIRED));
        
        verify(contentService).retireContent("content123");
        verify(cassandraOperation).updateRecord(eq(Constants.KEYSPACE_SUNBIRD_COURSE), 
                eq(Constants.CONTENT_RETIREMENT_REQUEST_TABLE), any(Map.class), any(Map.class));
    }

    @Test
    void processDueRetirements_WithFutureRetirementDate_ShouldNotRetire() {
        Map<String, Object> record = createRetirementRecord("content123", "request123", LocalDate.now().plusDays(1));
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(Arrays.asList(record));

        ApiResponse response = contentRetirementService.processDueRetirements();

        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT);
        assertTrue(contentList.isEmpty());
        verifyNoInteractions(contentService);
        verify(cassandraOperation, never()).updateRecord(any(), any(), any(), any());
    }

    @Test
    void processDueRetirements_WithNullRetirementDate_ShouldNotRetire() {
        Map<String, Object> record = createRetirementRecord("content123", "request123", null);
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(Arrays.asList(record));

        ApiResponse response = contentRetirementService.processDueRetirements();

        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT);
        assertTrue(contentList.isEmpty());
        verifyNoInteractions(contentService);
    }

    @Test
    void processDueRetirements_RetireContentReturnsEmpty_ShouldNotUpdateRecord() {
        Map<String, Object> record = createRetirementRecord("content123", "request123", LocalDate.now());
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(Arrays.asList(record));
        when(contentService.retireContent("content123"))
                .thenReturn(Collections.emptyMap());

        ApiResponse response = contentRetirementService.processDueRetirements();

        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT);
        assertEquals(1, contentList.size());
        assertEquals(false, contentList.get(0).get(Constants.RETIRED));
        assertEquals("Retirement API returned empty response", contentList.get(0).get(Constants.MESSAGE));
        
        verify(contentService).retireContent("content123");
        verify(cassandraOperation, never()).updateRecord(any(), any(), any(), any());
    }

    @Test
    void processDueRetirements_ExceptionDuringRetirement_ShouldHandleGracefully() {
        Map<String, Object> record = createRetirementRecord("content123", "request123", LocalDate.now());
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(Arrays.asList(record));
        when(contentService.retireContent("content123"))
                .thenThrow(new RuntimeException("Service error"));

        ApiResponse response = contentRetirementService.processDueRetirements();

        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT);
        assertEquals(1, contentList.size());
        assertEquals(false, contentList.get(0).get(Constants.RETIRED));
        assertEquals("Service error", contentList.get(0).get(Constants.MESSAGE));
        
        verify(cassandraOperation, never()).updateRecord(any(), any(), any(), any());
    }

    @Test
    void processDueRetirements_MultipleRecords_ShouldProcessAll() {
        Map<String, Object> record1 = createRetirementRecord("content1", "request1", LocalDate.now().minusDays(1));
        Map<String, Object> record2 = createRetirementRecord("content2", "request2", LocalDate.now());
        Map<String, Object> record3 = createRetirementRecord("content3", "request3", LocalDate.now().plusDays(1));
        
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(Arrays.asList(record1, record2, record3));
        when(contentService.retireContent(anyString()))
                .thenReturn(Map.of("status", "success"));

        ApiResponse response = contentRetirementService.processDueRetirements();

        List<Map<String, Object>> contentList = (List<Map<String, Object>>) response.getResult().get(Constants.CONTENT);
        assertEquals(2, contentList.size()); // Only content1 and content2 should be processed
        
        verify(contentService).retireContent("content1");
        verify(contentService).retireContent("content2");
        verify(contentService, never()).retireContent("content3");
    }

    @Test
    void processDueRetirements_ShouldCallCassandraWithCorrectParameters() {
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(Collections.emptyList());

        contentRetirementService.processDueRetirements();

        verify(cassandraOperation).getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.CONTENT_RETIREMENT_BY_RETIREMENT_DATE_TABLE),
                argThat(map -> map.containsKey(Constants.RETIREMENT_DATE_KEY)),
                argThat(fields ->
                        fields.contains(Constants.CONTENT_ID_KEY) &&
                                fields.contains(Constants.REQUEST_ID_KEY) &&
                                fields.contains(Constants.RETIREMENT_DATE_KEY) &&
                                fields.contains(Constants.STATUS)
                ),
                isNull()
        );
    }

    private Map<String, Object> createRetirementRecord(String contentId, String requestId, LocalDate retirementDate) {
        Map<String, Object> record = new HashMap<>();
        record.put(Constants.CONTENT_ID, contentId);
        record.put(Constants.REQUEST_ID, requestId);
        record.put(Constants.RETIREMENT_DATE, retirementDate);
        record.put(Constants.STATUS, Constants.APPROVED);
        return record;
    }

    @Test
    void sendContentRetirementNotifications_NoRetirementRequests_ShouldDoNothing() {
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(Collections.emptyList());

        contentRetirementService.sendContentRetirementNotifications();

        verifyNoInteractions(contentService);
        verifyNoInteractions(notificationService);
    }

    @Test
    void sendContentRetirementNotifications_ApprovedToday_ShouldSendApprovedNotification() {
        LocalDate today = LocalDate.now();

        Map<String, Object> record = new HashMap<>();
        record.put(Constants.CONTENT_ID, "content1");
        record.put(Constants.STATUS, Constants.APPROVED);
        record.put(Constants.APPROVED_DATE, today);
        record.put(Constants.RETIREMENT_DATE, today.plusDays(10));

        // Approved-date lookup
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.CONTENT_RETIREMENT_BY_APPROVED_DATE_TABLE),
                any(), any(), any()
        )).thenReturn(List.of(record));

        // Retirement-date lookup (not used here, but called)
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.CONTENT_RETIREMENT_BY_RETIREMENT_DATE_TABLE),
                any(), any(), any()
        )).thenReturn(Collections.emptyList());

        // REQUIRED: content with batches
        when(contentService.readContent(eq("content1"), any()))
                .thenReturn(Map.of(
                        Constants.NAME, "Test Course",
                        "batches", List.of(Map.of(Constants.BATCH_ID, "batch1"))
                ));

        //REQUIRED: batch users
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.ENROLLMENT_BATCH_LOOKUP),
                any(), any(), any()
        )).thenReturn(List.of(Map.of(Constants.USER_ID, "user1")));

        //REQUIRED: eligible enrolment
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.USER_ENROLMENTS_V2_TABLE),
                any(), any(), any()
        )).thenReturn(List.of(Map.of(
                Constants.STATUS, 1,
                Constants.ACTIVE, true,
                Constants.ISSUED_CERTIFICATES, Collections.emptyList()
        )));

        contentRetirementService.sendContentRetirementNotifications();
        verify(notificationService).sendNotificationForContentRetirement(
                eq("content1"),
                eq("Test Course"),
                eq(today.plusDays(10)),
                eq(List.of("user1")),
                eq(Constants.CONTENT_RETIREMENT_APPROVED_NOTIFICATION)
        );
    }

    @Test
    void sendContentRetirementNotifications_SevenDaysBefore_ShouldSendSevenDayReminder() {
        LocalDate today = LocalDate.now();

        Map<String, Object> record = new HashMap<>();
        record.put(Constants.CONTENT_ID, "content2");
        record.put(Constants.STATUS, Constants.APPROVED);
        record.put(Constants.RETIREMENT_DATE, today.plusDays(7));

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.CONTENT_RETIREMENT_BY_RETIREMENT_DATE_TABLE),
                any(), any(), any()
        )).thenReturn(List.of(record));

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.CONTENT_RETIREMENT_BY_APPROVED_DATE_TABLE),
                any(), any(), any()
        )).thenReturn(Collections.emptyList());

        when(contentService.readContent(eq("content2"), any()))
                .thenReturn(Map.of(
                        Constants.NAME, "Test Course",
                        "batches", List.of(Map.of(Constants.BATCH_ID, "batch1"))
                ));

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.ENROLLMENT_BATCH_LOOKUP),
                any(), any(), any()
        )).thenReturn(List.of(Map.of(Constants.USER_ID, "user1")));

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.USER_ENROLMENTS_V2_TABLE),
                any(), any(), any()
        )).thenReturn(List.of(Map.of(
                Constants.STATUS, 1,
                Constants.ACTIVE, true,
                Constants.ISSUED_CERTIFICATES, Collections.emptyList()
        )));

        contentRetirementService.sendContentRetirementNotifications();
        verify(notificationService).sendNotificationForContentRetirement(
                eq("content2"),
                eq("Test Course"),
                eq(today.plusDays(7)),
                eq(List.of("user1")),
                eq(Constants.REMINDER_NOTIFICATION_SEVEN_DAY)
        );
    }

    @Test
    void sendContentRetirementNotifications_OneDayBefore_ShouldSendOneDayReminder() {
        LocalDate today = LocalDate.now();

        Map<String, Object> record = new HashMap<>();
        record.put(Constants.CONTENT_ID, "content3");
        record.put(Constants.STATUS, Constants.APPROVED);
        record.put(Constants.RETIREMENT_DATE, today.plusDays(1));

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.CONTENT_RETIREMENT_BY_RETIREMENT_DATE_TABLE),
                any(), any(), any()
        )).thenReturn(List.of(record));

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.CONTENT_RETIREMENT_BY_APPROVED_DATE_TABLE),
                any(), any(), any()
        )).thenReturn(Collections.emptyList());

        when(contentService.readContent(eq("content3"), any()))
                .thenReturn(Map.of(
                        Constants.NAME, "Test Course",
                        "batches", List.of(Map.of(Constants.BATCH_ID, "batch1"))
                ));

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.ENROLLMENT_BATCH_LOOKUP),
                any(), any(), any()
        )).thenReturn(List.of(Map.of(Constants.USER_ID, "user1")));

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.USER_ENROLMENTS_V2_TABLE),
                any(), any(), any()
        )).thenReturn(List.of(Map.of(
                Constants.STATUS, 1,
                Constants.ACTIVE, true,
                Constants.ISSUED_CERTIFICATES, Collections.emptyList()
        )));

        contentRetirementService.sendContentRetirementNotifications();
        verify(notificationService).sendNotificationForContentRetirement(
                eq("content3"),
                eq("Test Course"),
                eq(today.plusDays(1)),
                eq(List.of("user1")),
                eq(Constants.REMINDER_NOTIFICATION_ONE_DAY)
        );
    }

    @Test
    void sendContentRetirementNotifications_NoEligibleEnrolment_ShouldNotNotify() {
        LocalDate today = LocalDate.now();

        Map<String, Object> retirementRecord = new HashMap<>();
        retirementRecord.put(Constants.CONTENT_ID, "content4");
        retirementRecord.put(Constants.STATUS, Constants.APPROVED); // ✅ REQUIRED
        retirementRecord.put(Constants.RETIREMENT_DATE, today.plusDays(7));

        // Retirement-date table
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.CONTENT_RETIREMENT_BY_RETIREMENT_DATE_TABLE),
                any(), any(), any()
        )).thenReturn(List.of(retirementRecord));

        // Approved-date table
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.CONTENT_RETIREMENT_BY_APPROVED_DATE_TABLE),
                any(), any(), any()
        )).thenReturn(Collections.emptyList());

        // Content has batches
        when(contentService.readContent(any(), any()))
                .thenReturn(Map.of(
                        Constants.NAME, "Test Course",
                        "batches", List.of(Map.of(Constants.BATCH_ID, "batch1"))
                ));

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.ENROLLMENT_BATCH_LOOKUP),
                any(), any(), any()))
                .thenReturn(List.of(Map.of(Constants.USER_ID, "user1")));

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.USER_ENROLMENTS_V2_TABLE),
                any(), any(), any()))
                .thenReturn(List.of(Map.of(
                        Constants.STATUS, 2,              // completed
                        Constants.ACTIVE, true,
                        Constants.ISSUED_CERTIFICATES, List.of("cert")
                )));

        contentRetirementService.sendContentRetirementNotifications();
        verifyNoInteractions(notificationService);
    }

    private void mockHappyPath(Map<String, Object> retirementRecord) {
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(List.of(retirementRecord));

        when(contentService.readContent(any(), any()))
                .thenReturn(Map.of(
                        Constants.NAME, "Test Course",
                        "batches", List.of(Map.of(Constants.BATCH_ID, "batch1"))
                ));

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.ENROLLMENT_BATCH_LOOKUP),
                any(), any(), any()))
                .thenReturn(List.of(Map.of(Constants.USER_ID, "user1")));

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.USER_ENROLMENTS_V2_TABLE),
                any(), any(), any()))
                .thenReturn(List.of(Map.of(
                        Constants.STATUS, 1,
                        Constants.ACTIVE, true,
                        Constants.ISSUED_CERTIFICATES, Collections.emptyList()
                )));
    }

    @Test
    void sendContentRetirementNotificationsToSpv_NoRequests_ShouldReturn() {
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(Collections.emptyList());

        contentRetirementService.sendContentRetirementNotificationsToSpv();
        verifyNoInteractions(contentService);
        verifyNoInteractions(notificationService);
    }

    @Test
    void sendContentRetirementNotificationsToSpv_CreatedDateNotToday_ShouldSkip() {
        Map<String, Object> record = Map.of(
                Constants.CONTENT_ID, "do_123",
                Constants.CREATED_AT_FIELD, Instant.now().minus(1, ChronoUnit.DAYS),
                Constants.USER_ID_RAISED_FIELD, "user-1",
                Constants.RETIREMENT_DATE, LocalDate.now().plusDays(10)
        );
        when(cassandraOperation.getRecordsByProperties(
                any(), any(), any(), any(), any()))
                .thenReturn(List.of(record));

        when(props.getSbUrl()).thenReturn("http://localhost");
        when(props.getUserSearchEndPoint()).thenReturn("/user/search");

        Map<String, Object> user1 = Map.of(Constants.USER_ID, "spv-1");
        Map<String, Object> user2 = Map.of(Constants.USER_ID, "spv-2");

        Map<String, Object> spvResponse =
                Map.of(
                        Constants.RESPONSE_CODE, "OK",
                        Constants.RESULT, Map.of(
                                Constants.RESPONSE, Map.of(
                                        Constants.CONTENT, List.of(user1, user2)
                                )
                        )
                );
        when(outboundRequestHandlerService.fetchResultUsingPost(
                anyString(), any(), any()))
                .thenReturn(spvResponse);

        contentRetirementService.sendContentRetirementNotificationsToSpv();

        verify(notificationService, never())
                .sendNotificationForContentRetirementSpv(
                        any(), any(), any(), any(), any(), any(), any());

        verify(contentService, never())
                .readContent(anyString(), anyList());
    }

    @Test
    void sendContentRetirementNotificationsToSpv_ValidRequest_ShouldNotify() {
        LocalDate today = LocalDate.now();

        Map<String, Object> record = new HashMap<>();
        record.put(Constants.CONTENT_ID, "do_123");
        record.put(Constants.CREATED_DATE, today); // ✅ FIX
        record.put(Constants.USER_ID_RAISED_FIELD, "requester-1");
        record.put(Constants.RETIREMENT_DATE, today.plusDays(5));

        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(List.of(record));
        when(contentService.readContent(eq("do_123"), any()))
                .thenReturn(Map.of("name", "Sample Course"));
        mockSpvUsers(List.of("spv-1", "spv-2"));
        contentRetirementService.sendContentRetirementNotificationsToSpv();
        verify(notificationService).sendNotificationForContentRetirementSpv(
                eq("do_123"),
                eq("Sample Course"),
                argThat(list -> list.contains("spv-1") && list.contains("spv-2")),
                eq(Constants.CONTENT_RETIREMENT_SCHEDULED_NOTIFICATION),
                eq(today.plusDays(5)),
                argThat(emails -> emails.contains("spv-1@test.com")),
                eq("requester-1")
        );
    }

    @Test
    void sendContentRetirementNotificationsToSpv_NoRequester_ShouldNotifyOnlySpv() {
        LocalDate today = LocalDate.now();

        Map<String, Object> record = new HashMap<>();
        record.put(Constants.CONTENT_ID, "do_124");
        record.put(Constants.CREATED_DATE, today); // ✅ FIX
        record.put(Constants.RETIREMENT_DATE, today.plusDays(7));
        // NOTE: no USER_ID_RAISED_FIELD on purpose

        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(List.of(record));
        when(contentService.readContent(eq("do_124"), any()))
                .thenReturn(Map.of("name", "Course X"));
        mockSpvUsers(List.of("spv-1"));
        contentRetirementService.sendContentRetirementNotificationsToSpv();
        verify(notificationService).sendNotificationForContentRetirementSpv(
                eq("do_124"),
                eq("Course X"),
                argThat(list -> list.contains("spv-1")),
                eq(Constants.CONTENT_RETIREMENT_SCHEDULED_NOTIFICATION),
                eq(today.plusDays(7)),
                argThat(emails -> emails.contains("spv-1@test.com")),
                isNull()   // requester is null → correct expectation
        );
    }

    @Test
    void sendContentRetirementNotificationsToSpv_NoSpvUsers_ShouldNotNotifyAnyone() {
        LocalDate today = LocalDate.now();
        Map<String, Object> record = Map.of(
                Constants.CONTENT_ID, "do_125",
                Constants.CREATED_AT_FIELD, today,
                Constants.USER_ID_RAISED_FIELD, "requester-2",
                Constants.RETIREMENT_DATE, today.plusDays(3)
        );
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(List.of(record));
        mockSpvUsers(Collections.emptyList());
        contentRetirementService.sendContentRetirementNotificationsToSpv();
        verifyNoInteractions(notificationService);
    }


    @Test
    void sendContentRetirementNotificationsToSpv_RetirementDateInstant_ShouldConvert() {
        Map<String, Object> record = new HashMap<>();
        record.put(Constants.CONTENT_ID, "do_126");
        record.put(Constants.CREATED_DATE, Instant.now()); // ✅ FIX + Instant
        record.put(Constants.USER_ID_RAISED_FIELD, "user-x");
        record.put(Constants.RETIREMENT_DATE, LocalDate.now().plusDays(10));

        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(List.of(record));
        when(contentService.readContent(any(), any()))
                .thenReturn(Map.of("name", "Course Z"));
        mockSpvUsers(List.of("spv"));

        contentRetirementService.sendContentRetirementNotificationsToSpv();
        verify(notificationService).sendNotificationForContentRetirementSpv(
                eq("do_126"),
                eq("Course Z"),
                argThat(list -> list.contains("spv")),
                eq(Constants.CONTENT_RETIREMENT_SCHEDULED_NOTIFICATION),
                any(LocalDate.class),        // ← converted from Instant
                argThat(emails -> emails.contains("spv@test.com")),
                eq("user-x")
        );
    }


    private void mockSpvUsers(List<String> spvUserIds) {
        when(props.getSbUrl()).thenReturn("http://test");
        when(props.getUserSearchEndPoint()).thenReturn("/search");
        List<Map<String, Object>> contents = spvUserIds.stream()
                .map(id -> {
                    Map<String, Object> personalDetails = new HashMap<>();
                    personalDetails.put(Constants.PRIMARY_EMAIL, id + "@test.com");

                    Map<String, Object> profileDetails = new HashMap<>();
                    profileDetails.put(Constants.PERSONAL_DETAILS, personalDetails);

                    Map<String, Object> user = new HashMap<>();
                    user.put(Constants.USER_ID, id);
                    user.put(Constants.PROFILE_DETAILS, profileDetails);

                    return user;
                }).toList();
        Map<String, Object> response = new HashMap<>();
        response.put(Constants.RESPONSE_CODE, "OK");
        response.put(Constants.RESULT, Map.of(
                Constants.RESPONSE, Map.of(
                        Constants.CONTENT, contents
                )
        ));
        when(outboundRequestHandlerService.fetchResultUsingPost(
                eq("http://test/search"),
                any(),
                any()
        )).thenReturn(response);
    }


}