package com.igot.cb.cbplan.service.impl;

import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanContentLookupServiceV3ImplTest {

    private static final String PLAN_ID = "plan1";
    private static final String CONTENT_ID = "content1";

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private RedisCacheMgr redisCacheMgr;

    @Mock
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @InjectMocks
    private CbPlanContentLookupServiceV3Impl contentLookupService;

    private static Map<String, Object> lookupRow(String... planIds) {
        Map<String, Object> row = new HashMap<>();
        row.put(Constants.PLAN_ID, new HashSet<>(Set.of(planIds)));
        return row;
    }

    private void stubLookupRows(List<Map<String, Object>> rows) {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(rows);
    }

    @Test
    void testUpdateContentLookupWithContent() {
        Map<String, Object> planData = new HashMap<>();
        planData.put(Constants.CONTENT_LIST, List.of("content1", "content2"));
        stubLookupRows(List.of());
        contentLookupService.updateContentLookup(PLAN_ID, planData);
        verify(cassandraOperation, times(2)).updateRecord(anyString(), anyString(), anyMap(), anyMap());
    }

    @Test
    void testUpdateContentLookupSkipsWhenNoContent() {
        contentLookupService.updateContentLookup(PLAN_ID, new HashMap<>());
        verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap());
    }

    @Test
    void testUpsertAddsPlanIdWhenAbsent() {
        stubLookupRows(List.of());
        contentLookupService.upsertCbPlanContentLookup(PLAN_ID, List.of(CONTENT_ID));
        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(cassandraOperation).updateRecord(anyString(), anyString(), captor.capture(), anyMap());
        Set<String> planIds = (Set<String>) captor.getValue().get(Constants.PLAN_ID_COLUMN);
        assertTrue(planIds.contains(PLAN_ID));
    }

    @Test
    void testUpsertMergesWithExistingPlanIds() {
        stubLookupRows(List.of(lookupRow("existingPlan")));
        contentLookupService.upsertCbPlanContentLookup(PLAN_ID, List.of(CONTENT_ID));
        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(cassandraOperation).updateRecord(anyString(), anyString(), captor.capture(), anyMap());
        Set<String> planIds = (Set<String>) captor.getValue().get(Constants.PLAN_ID_COLUMN);
        assertEquals(Set.of(PLAN_ID, "existingPlan"), planIds);
    }

    @Test
    void testUpsertSkipsWhenPlanIdAlreadyPresent() {
        stubLookupRows(List.of(lookupRow(PLAN_ID)));
        contentLookupService.upsertCbPlanContentLookup(PLAN_ID, List.of(CONTENT_ID));
        verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap());
    }

    @Test
    void testUpdateContentLookupForModifiedPlanHandlesAddsAndDeletes() {
        Map<String, Object> updatedRequest = new HashMap<>();
        updatedRequest.put(Constants.CONTENT_LIST, List.of("content1", "content2"));
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CONTENT_LIST, List.of("content2", "content3"));
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenAnswer(invocation -> {
                    Map<String, Object> where = invocation.getArgument(2);
                    String contentId = (String) where.get(Constants.CONTENT_ID_COLUMN);
                    return "content1".equals(contentId)
                            ? List.of(lookupRow("otherPlan"))
                            : List.of(lookupRow(PLAN_ID));
                });
        contentLookupService.updateContentLookupForModifiedPlan(PLAN_ID, updatedRequest, existingCbPlan);
        verify(cassandraOperation, times(1)).updateRecord(anyString(), anyString(), anyMap(), anyMap());
        verify(cassandraOperation, times(1)).deleteRecord(anyString(), anyString(), anyMap());
    }

    @Test
    void testUpdateContentLookupForModifiedPlanSkipsWhenNoUpdatedContent() {
        contentLookupService.updateContentLookupForModifiedPlan(PLAN_ID, new HashMap<>(), new HashMap<>());
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    @Test
    void testRemoveFromContentLookupWithContent() {
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CONTENT_LIST, List.of(CONTENT_ID));
        stubLookupRows(List.of(lookupRow(PLAN_ID)));
        contentLookupService.removeFromContentLookup(PLAN_ID, existingCbPlan);
        verify(cassandraOperation).deleteRecord(anyString(), anyString(), anyMap());
    }

    @Test
    void testRemoveFromContentLookupSkipsWhenNoContent() {
        contentLookupService.removeFromContentLookup(PLAN_ID, new HashMap<>());
        verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), anyMap());
    }

    @Test
    void testRemoveDeletesRowWhenLastPlan() {
        stubLookupRows(List.of(lookupRow(PLAN_ID)));
        contentLookupService.removeCbPlanInfoForUpdateOrDeleteCbPlan(PLAN_ID, List.of(CONTENT_ID));
        verify(cassandraOperation).deleteRecord(anyString(), anyString(), anyMap());
        verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap());
    }

    @Test
    void testRemoveUpdatesRowWhenOtherPlansRemain() {
        stubLookupRows(List.of(lookupRow(PLAN_ID, "plan2")));
        contentLookupService.removeCbPlanInfoForUpdateOrDeleteCbPlan(PLAN_ID, List.of(CONTENT_ID));
        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(cassandraOperation).updateRecord(anyString(), anyString(), captor.capture(), anyMap());
        assertEquals(Set.of("plan2"), captor.getValue().get(Constants.PLAN_ID_COLUMN));
        verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), anyMap());
    }

    @Test
    void testRemoveSkipsWhenPlanNotInSet() {
        stubLookupRows(List.of(lookupRow("otherPlan")));
        contentLookupService.removeCbPlanInfoForUpdateOrDeleteCbPlan(PLAN_ID, List.of(CONTENT_ID));
        verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), anyMap());
        verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap());
    }

    @Test
    void testRemoveSkipsWhenNoRowFound() {
        stubLookupRows(List.of());
        contentLookupService.removeCbPlanInfoForUpdateOrDeleteCbPlan(PLAN_ID, List.of(CONTENT_ID));
        verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), anyMap());
    }

    @Test
    void testRemoveSkipsWhenPlanIdColumnHasWrongType() {
        Map<String, Object> row = new HashMap<>();
        row.put(Constants.PLAN_ID, "not-a-set");
        stubLookupRows(List.of(row));
        contentLookupService.removeCbPlanInfoForUpdateOrDeleteCbPlan(PLAN_ID, List.of(CONTENT_ID));
        verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), anyMap());
        verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap());
    }

    @Test
    void testGetAddedContent() {
        assertEquals(List.of("content2"),
                contentLookupService.getAddedContent(List.of("content1"), List.of("content1", "content2")));
    }

    @Test
    void testGetAddedContentWithNullExisting() {
        assertEquals(List.of("content1"), contentLookupService.getAddedContent(null, List.of("content1")));
    }

    @Test
    void testGetAddedContentWithBothNull() {
        assertTrue(contentLookupService.getAddedContent(null, null).isEmpty());
    }

    @Test
    void testGetDeletedContent() {
        assertEquals(List.of("content2"),
                contentLookupService.getDeletedContent(List.of("content1", "content2"), List.of("content1")));
    }

    @Test
    void testGetDeletedContentWithNullUpdated() {
        assertEquals(List.of("content1"), contentLookupService.getDeletedContent(List.of("content1"), null));
    }

    @Test
    void testGetContentMetadataReturnsCachedValueOnRedisHit() throws Exception {
        when(redisCacheMgr.getFromCache(Constants.EXTENDED_READ_CONTENT_CACHE_KEY_PREFIX + CONTENT_ID))
                .thenReturn("{\"identifier\":\"content1\",\"name\":\"Course 1\"}");
        Map<String, Object> result = contentLookupService.getContentMetadata(CONTENT_ID);
        assertEquals("Course 1", result.get("name"));
        verify(outboundRequestHandlerService, never()).fetchResult(anyString());
    }

    @Test
    void testGetContentMetadataFallsBackToExtendedReadApiOnCacheMiss() throws Exception {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put(Constants.RESPONSE_CODE, Constants.OK);
        apiResponse.put(Constants.RESULT, Map.of(Constants.CONTENT, Map.of("name", "Course 1")));
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(apiResponse);
        Map<String, Object> result = contentLookupService.getContentMetadata(CONTENT_ID);
        assertEquals("Course 1", result.get("name"));
    }

    @Test
    void testGetContentMetadataReturnsEmptyMapOnNonOkApiResponse() throws Exception {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put(Constants.RESPONSE_CODE, "SERVER_ERROR");
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(apiResponse);
        assertTrue(contentLookupService.getContentMetadata(CONTENT_ID).isEmpty());
    }

    @Test
    void testGetContentMetadataReturnsEmptyMapWhenApiReturnsNull() throws Exception {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(null);
        assertTrue(contentLookupService.getContentMetadata(CONTENT_ID).isEmpty());
    }

    @Test
    void testGetContentMetadataUsesExternalContentApiForExtPrefix() throws Exception {
        String externalContentId = Constants.EXTERNAL_CONTENT_PREFIX + "event1";
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put(Constants.RESULT, Map.of(Constants.EVENT, Map.of("name", "Event 1")));
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(apiResponse);
        Map<String, Object> result = contentLookupService.getContentMetadata(externalContentId);
        assertEquals("Event 1", result.get("name"));
    }

    @Test
    void testGetContentMetadataReturnsEmptyMapWhenExternalEventMissing() throws Exception {
        String externalContentId = Constants.EXTERNAL_CONTENT_PREFIX + "event1";
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put(Constants.RESULT, new HashMap<String, Object>());
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(apiResponse);
        assertTrue(contentLookupService.getContentMetadata(externalContentId).isEmpty());
    }

    @Test
    void testGetContentMetadataSwallowsApiExceptions() throws Exception {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(outboundRequestHandlerService.fetchResult(anyString()))
                .thenThrow(new RuntimeException("content service down"));
        assertTrue(contentLookupService.getContentMetadata(CONTENT_ID).isEmpty());
    }

    @Test
    void testConstructor() {
        assertNotNull(new CbPlanContentLookupServiceV3Impl(
                cassandraOperation, redisCacheMgr, outboundRequestHandlerService));
    }
}
