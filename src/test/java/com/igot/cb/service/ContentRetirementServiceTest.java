package com.igot.cb.service;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.time.LocalDate;
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

    private ContentRetirementService contentRetirementService;

    @BeforeEach
    void setUp() {
        contentRetirementService = new ContentRetirementService(cassandraOperation, contentService);
    }

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
                eq(Constants.CONTENT_RETIREMENT_REQUEST_TABLE),
                argThat(map -> Constants.APPROVED.equals(map.get(Constants.STATUS))),
                argThat(fields -> fields.contains(Constants.CONTENT_ID_KEY) && 
                                fields.contains(Constants.REQUEST_ID_KEY) &&
                                fields.contains(Constants.RETIREMENT_DATE_KEY) &&
                                fields.contains(Constants.STATUS)),
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
}