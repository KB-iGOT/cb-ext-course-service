package com.igot.cb.service;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ContentRetirementServiceTest {

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private ContentInfoServiceImpl contentService;

    private ContentRetirementService contentRetirementService;

    @BeforeEach
    void setUp() {
        contentRetirementService = new ContentRetirementService(cassandraOperation, contentService);
    }

    @Test
    void processDueRetirements_NoRecords_ShouldReturnEarly() {
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(Collections.emptyList());

        contentRetirementService.processDueRetirements();

        verify(cassandraOperation).getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.CONTENT_RETIREMENT_REQUEST_TABLE),
                any(Map.class),
                any(List.class),
                isNull()
        );
        verifyNoInteractions(contentService);
    }

    @Test
    void processDueRetirements_WithDueContent_ShouldRetireContent() {
        Map<String, Object> record = new HashMap<>();
        record.put(Constants.CONTENT_ID, "content123");
        record.put(Constants.REQUEST_ID, "request123");
        record.put(Constants.RETIREMENT_DATE, LocalDate.now().minusDays(1));

        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(Arrays.asList(record));
        when(contentService.retireContent("content123"))
                .thenReturn(Map.of("status", "success"));

        contentRetirementService.processDueRetirements();

        verify(contentService).retireContent("content123");
        verify(cassandraOperation).updateRecord(
                eq(Constants.KEYSPACE_SUNBIRD_COURSE),
                eq(Constants.CONTENT_RETIREMENT_REQUEST_TABLE),
                any(Map.class),
                any(Map.class)
        );
    }

    @Test
    void processDueRetirements_WithFutureRetirementDate_ShouldNotRetire() {
        Map<String, Object> record = new HashMap<>();
        record.put(Constants.CONTENT_ID, "content123");
        record.put(Constants.REQUEST_ID, "request123");
        record.put(Constants.RETIREMENT_DATE, LocalDate.now().plusDays(1));

        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(Arrays.asList(record));

        contentRetirementService.processDueRetirements();

        verifyNoInteractions(contentService);
        verify(cassandraOperation, never()).updateRecord(any(), any(), any(), any());
    }

    @Test
    void processDueRetirements_WithNullRetirementDate_ShouldNotRetire() {
        Map<String, Object> record = new HashMap<>();
        record.put(Constants.CONTENT_ID, "content123");
        record.put(Constants.REQUEST_ID, "request123");
        record.put(Constants.RETIREMENT_DATE, null);

        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(Arrays.asList(record));

        contentRetirementService.processDueRetirements();

        verifyNoInteractions(contentService);
        verify(cassandraOperation, never()).updateRecord(any(), any(), any(), any());
    }

    @Test
    void processDueRetirements_RetireContentFails_ShouldNotUpdateRecord() {
        Map<String, Object> record = new HashMap<>();
        record.put(Constants.CONTENT_ID, "content123");
        record.put(Constants.REQUEST_ID, "request123");
        record.put(Constants.RETIREMENT_DATE, LocalDate.now());

        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(Arrays.asList(record));
        when(contentService.retireContent("content123"))
                .thenReturn(Collections.emptyMap());

        contentRetirementService.processDueRetirements();

        verify(contentService).retireContent("content123");
        verify(cassandraOperation, never()).updateRecord(any(), any(), any(), any());
    }

    @Test
    void processDueRetirements_ExceptionDuringRetirement_ShouldContinue() {
        Map<String, Object> record = new HashMap<>();
        record.put(Constants.CONTENT_ID, "content123");
        record.put(Constants.REQUEST_ID, "request123");
        record.put(Constants.RETIREMENT_DATE, LocalDate.now());

        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(Arrays.asList(record));
        when(contentService.retireContent("content123"))
                .thenThrow(new RuntimeException("Service error"));

        contentRetirementService.processDueRetirements();

        verify(contentService).retireContent("content123");
        verify(cassandraOperation, never()).updateRecord(any(), any(), any(), any());
    }
}