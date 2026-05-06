package com.igot.cb.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiRespParam;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.NotificationService;
import com.igot.cb.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.service.impl.ExternalTrainingCertificateServiceImpl;
import com.igot.cb.storage.service.StorageService;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.lang.reflect.Method;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ExternalTrainingBulkUploadConsumerTest {

    private ExternalTrainingBulkUploadConsumer consumer;

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private StorageService storageService;

    @Mock
    private OutboundRequestHandlerServiceImpl outboundService;

    @Mock
    private CbExtServerProperties props;

    @Mock
    private NotificationService notificationService;

    @Mock
    private ExternalTrainingCertificateServiceImpl certService;

    @BeforeEach
    void setup() throws Exception {

        consumer = Mockito.spy(new ExternalTrainingBulkUploadConsumer(notificationService));

        inject("cassandraOperation", cassandraOperation);
        inject("storageService", storageService);
        inject("outboundRequestHandlerService", outboundService);
        inject("serverProperties", props);
        inject("externalTrainingCertificateService", certService);
        inject("objectMapper", new ObjectMapper());
    }

    private void inject(String field, Object value) throws Exception {
        var f = ExternalTrainingBulkUploadConsumer.class.getDeclaredField(field);
        f.setAccessible(true);
        f.set(consumer, value);
    }

    private Object invokePrivate(String method, Class<?>[] types, Object... args) throws Exception {
        Method m = ExternalTrainingBulkUploadConsumer.class.getDeclaredMethod(method, types);
        m.setAccessible(true);
        return m.invoke(consumer, args);
    }

    // ===========================
    // PRIVATE METHODS
    // ===========================

    @Test
    void testCleanHeaders() throws Exception {
        List<String> headers = new ArrayList<>(List.of("\"Email\"", "\"Name\""));
        invokePrivate("cleanHeaders", new Class[]{List.class}, headers);

        assertEquals("Email", headers.get(0));
        assertEquals("Name", headers.get(1));
    }

    @Test
    void testValidateNotNullOrEmpty_valid() throws Exception {
        Map<String, Object> map = Map.of("key", "value");

        assertDoesNotThrow(() ->
                invokePrivate("validateNotNullOrEmpty", new Class[]{Map.class}, map)
        );
    }

    @Test
    void testValidateNotNullOrEmpty_null() {
        Map<String, Object> map = new HashMap<>();
        map.put("key", null);

        Exception ex = assertThrows(Exception.class, () ->
                invokePrivate("validateNotNullOrEmpty", new Class[]{Map.class}, map)
        );

        assertTrue(ex.getCause().getMessage().contains("null"));
    }

    // ===========================
    // FINALIZE STATUS
    // ===========================

    @Test
    void testFinalizeStatus_success() throws Exception {

        File file = File.createTempFile("test", ".csv");

        ApiResponse response = new ApiResponse();
        response.setResponseCode(org.springframework.http.HttpStatus.OK);

        when(storageService.uploadFile(any(), any(), any()))
                .thenReturn(response);

        when(props.getExternalTrainingBulkUploadContainerName()).thenReturn("container");
        when(props.getCloudContainerName()).thenReturn("cloud");

        String result = (String) invokePrivate(
                "finalizeStatus",
                new Class[]{int.class, int.class, int.class, File.class},
                10, 10, 0, file
        );

        assertEquals(Constants.SUCCESS, result);
    }

    @Test
    void testFinalizeStatus_failed() throws Exception {

        File file = File.createTempFile("test", ".csv");

        ApiResponse response = new ApiResponse();
        response.setResponseCode(org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR);

        ApiRespParam params = new ApiRespParam();
        params.setErrMsg("Upload failed");
        response.setParams(params);

        when(storageService.uploadFile(any(), any(), any()))
                .thenReturn(response);

        when(props.getExternalTrainingBulkUploadContainerName()).thenReturn("container");
        when(props.getCloudContainerName()).thenReturn("cloud");

        String result = (String) invokePrivate(
                "finalizeStatus",
                new Class[]{int.class, int.class, int.class, File.class},
                10, 5, 5, file
        );

        assertEquals(Constants.FAILED, result);
    }

    // ===========================
    // PROCESS RECORD
    // ===========================

    @Test
    void testProcessRecord_successFlow() throws Exception {

        CSVRecord record = mock(CSVRecord.class);
        when(record.size()).thenReturn(1);
        when(record.get("Email")).thenReturn("test@mail.com");
        when(record.toMap()).thenReturn(new HashMap<>());

        Map<String, Object> userInfo = Map.of(Constants.USER_ID, "user1");
        Map<String, Object> emailMap = Map.of("test@mail.com", userInfo);

        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(Collections.emptyList());

        ApiResponse success = new ApiResponse();
        success.put(Constants.RESPONSE, Constants.SUCCESS);

        when(cassandraOperation.insertRecord(any(), any(), any()))
                .thenReturn(success);

        doNothing().when(certService)
                .generateCertificateEventAndPushToKafka(any(), any());

        Map<String, Object> eventDetails = new HashMap<>();
        eventDetails.put(Constants.DURATION, 100);
        eventDetails.put(Constants.START_DATE, new Date());
        eventDetails.put(Constants.END_DATE_CAMEL, new Date());

        List<String> notifications = new ArrayList<>();

        Map<String, String> result = (Map<String, String>) invokePrivate(
                "processRecord",
                new Class[]{CSVRecord.class, int.class, String.class, String.class, Map.class, Map.class, List.class},
                record, 5, "event", "batch", emailMap, eventDetails, notifications
        );

        assertFalse(result.containsKey("Status"));
        assertEquals(1, notifications.size());
    }

    @Test
    void testProcessRecord_invalidEmail() throws Exception {

        CSVRecord record = mock(CSVRecord.class);
        when(record.get("Email")).thenReturn("invalid");
        when(record.toMap()).thenReturn(new HashMap<>());
        when(record.size()).thenReturn(1);

        Map<String, String> result = (Map<String, String>) invokePrivate(
                "processRecord",
                new Class[]{CSVRecord.class, int.class, String.class, String.class, Map.class, Map.class, List.class},
                record, 5, "event", "batch", new HashMap<>(), new HashMap<>(), new ArrayList<>()
        );

        assertEquals("FAILED", result.get("Status"));
    }

    // ===========================
    // ERROR CASE
    // ===========================

    @Test
    void testGetUserIdList_nullParser() {

        Exception ex = assertThrows(Exception.class, () ->
                invokePrivate("getUserIdList",
                        new Class[]{org.apache.commons.csv.CSVParser.class, String.class, Map.class},
                        null, "Email", new HashMap<>())
        );

        assertTrue(ex.getCause().getMessage().contains("Invalid input"));
    }
}