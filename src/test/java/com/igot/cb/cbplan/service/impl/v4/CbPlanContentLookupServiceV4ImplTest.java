package com.igot.cb.cbplan.service.impl.v4;

import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanContentLookupServiceV4ImplTest {

    private static final String PLAN_ID = "plan1";
    private static final String CONTENT_ID = "content1";
    private static final String V4_TABLE = "cb_plan_v4_content_lookup";

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private RedisCacheMgr redisCacheMgr;

    @Mock
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @Mock
    private CbExtServerProperties serverProperties;

    @InjectMocks
    private CbPlanContentLookupServiceV4Impl contentLookupService;

    @BeforeEach
    void setUp() {
        when(serverProperties.getCbPlanV4ContentLookupTable()).thenReturn(V4_TABLE);
    }

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
    void updateContentLookup_withContent_updatesLookupTable() {
        Map<String, Object> planData = new HashMap<>();
        planData.put(Constants.CONTENT_LIST, List.of("content1", "content2"));
        stubLookupRows(List.of());
        contentLookupService.updateContentLookup(PLAN_ID, planData);
        verify(cassandraOperation, times(2)).updateRecord(anyString(), anyString(), anyMap(), anyMap());
    }

    @Test
    void updateContentLookup_withNoContent_skipsUpdate() {
        contentLookupService.updateContentLookup(PLAN_ID, new HashMap<>());
        verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap());
    }

    @Test
    void updateContentLookup_usesV4TableName() {
        Map<String, Object> planData = new HashMap<>();
        planData.put(Constants.CONTENT_LIST, List.of(CONTENT_ID));
        stubLookupRows(List.of());
        contentLookupService.updateContentLookup(PLAN_ID, planData);
        verify(cassandraOperation).updateRecord(anyString(), eq(V4_TABLE), anyMap(), anyMap());
    }

    @Test
    void upsertCbPlanContentLookup_addsPlanIdWhenAbsent() {
        stubLookupRows(List.of());
        contentLookupService.upsertCbPlanContentLookup(PLAN_ID, List.of(CONTENT_ID));
        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(cassandraOperation).updateRecord(anyString(), anyString(), captor.capture(), anyMap());
        Set<String> planIds = (Set<String>) captor.getValue().get(Constants.PLAN_ID_COLUMN);
        assertTrue(planIds.contains(PLAN_ID));
    }

    @Test
    void upsertCbPlanContentLookup_mergesWithExistingPlanIds() {
        stubLookupRows(List.of(lookupRow("existingPlan")));
        contentLookupService.upsertCbPlanContentLookup(PLAN_ID, List.of(CONTENT_ID));
        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(cassandraOperation).updateRecord(anyString(), anyString(), captor.capture(), anyMap());
        Set<String> planIds = (Set<String>) captor.getValue().get(Constants.PLAN_ID_COLUMN);
        assertEquals(Set.of(PLAN_ID, "existingPlan"), planIds);
    }

    @Test
    void upsertCbPlanContentLookup_skipsWhenPlanIdAlreadyPresent() {
        stubLookupRows(List.of(lookupRow(PLAN_ID)));
        contentLookupService.upsertCbPlanContentLookup(PLAN_ID, List.of(CONTENT_ID));
        verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap());
    }

    @Test
    void updateContentLookupForModifiedPlan_handlesAddsAndDeletes() {
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
    void updateContentLookupForModifiedPlan_skipsWhenNoUpdatedContent() {
        contentLookupService.updateContentLookupForModifiedPlan(PLAN_ID, new HashMap<>(), new HashMap<>());
        verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any());
    }

    @Test
    void removeFromContentLookup_withContent_deletesRow() {
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.CONTENT_LIST, List.of(CONTENT_ID));
        stubLookupRows(List.of(lookupRow(PLAN_ID)));
        contentLookupService.removeFromContentLookup(PLAN_ID, existingCbPlan);
        verify(cassandraOperation).deleteRecord(anyString(), anyString(), anyMap());
    }

    @Test
    void removeFromContentLookup_withNoContent_skipsDelete() {
        contentLookupService.removeFromContentLookup(PLAN_ID, new HashMap<>());
        verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), anyMap());
    }

    @Test
    void removeCbPlanInfoForUpdateOrDeleteCbPlan_deletesRowWhenLastPlan() {
        stubLookupRows(List.of(lookupRow(PLAN_ID)));
        contentLookupService.removeCbPlanInfoForUpdateOrDeleteCbPlan(PLAN_ID, List.of(CONTENT_ID));
        verify(cassandraOperation).deleteRecord(anyString(), eq(V4_TABLE), anyMap());
        verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap());
    }

    @Test
    void removeCbPlanInfoForUpdateOrDeleteCbPlan_updatesRowWhenOtherPlansRemain() {
        stubLookupRows(List.of(lookupRow(PLAN_ID, "plan2")));
        contentLookupService.removeCbPlanInfoForUpdateOrDeleteCbPlan(PLAN_ID, List.of(CONTENT_ID));
        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(cassandraOperation).updateRecord(anyString(), eq(V4_TABLE), captor.capture(), anyMap());
        assertEquals(Set.of("plan2"), captor.getValue().get(Constants.PLAN_ID_COLUMN));
        verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), anyMap());
    }

    @Test
    void removeCbPlanInfoForUpdateOrDeleteCbPlan_skipsWhenPlanNotInSet() {
        stubLookupRows(List.of(lookupRow("otherPlan")));
        contentLookupService.removeCbPlanInfoForUpdateOrDeleteCbPlan(PLAN_ID, List.of(CONTENT_ID));
        verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), anyMap());
        verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap());
    }

    @Test
    void removeCbPlanInfoForUpdateOrDeleteCbPlan_skipsWhenNoRowFound() {
        stubLookupRows(List.of());
        contentLookupService.removeCbPlanInfoForUpdateOrDeleteCbPlan(PLAN_ID, List.of(CONTENT_ID));
        verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), anyMap());
    }

    @Test
    void removeCbPlanInfoForUpdateOrDeleteCbPlan_skipsWhenPlanIdColumnHasWrongType() {
        Map<String, Object> row = new HashMap<>();
        row.put(Constants.PLAN_ID, "not-a-set");
        stubLookupRows(List.of(row));
        contentLookupService.removeCbPlanInfoForUpdateOrDeleteCbPlan(PLAN_ID, List.of(CONTENT_ID));
        verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), anyMap());
        verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap());
    }

    @Test
    void getAddedContent_returnsNewContentIds() {
        assertEquals(List.of("content2"),
                contentLookupService.getAddedContent(List.of("content1"), List.of("content1", "content2")));
    }

    @Test
    void getAddedContent_withNullExisting_returnsAllUpdated() {
        assertEquals(List.of("content1"), contentLookupService.getAddedContent(null, List.of("content1")));
    }

    @Test
    void getAddedContent_withBothNull_returnsEmpty() {
        assertTrue(contentLookupService.getAddedContent(null, null).isEmpty());
    }

    @Test
    void getDeletedContent_returnsRemovedContentIds() {
        assertEquals(List.of("content2"),
                contentLookupService.getDeletedContent(List.of("content1", "content2"), List.of("content1")));
    }

    @Test
    void getDeletedContent_withNullUpdated_returnsAllExisting() {
        assertEquals(List.of("content1"), contentLookupService.getDeletedContent(List.of("content1"), null));
    }

    @Test
    void getContentMetadata_returnsCachedValueOnRedisHit() throws Exception {
        when(redisCacheMgr.getFromCache(Constants.EXTENDED_READ_CONTENT_CACHE_KEY_PREFIX + CONTENT_ID))
                .thenReturn("{\"identifier\":\"content1\",\"name\":\"Course 1\"}");
        Map<String, Object> result = contentLookupService.getContentMetadata(CONTENT_ID);
        assertEquals("Course 1", result.get("name"));
        verify(outboundRequestHandlerService, never()).fetchResult(anyString());
    }

    @Test
    void getContentMetadata_fallsBackToExtendedReadApiOnCacheMiss() throws Exception {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put(Constants.RESPONSE_CODE, Constants.OK);
        apiResponse.put(Constants.RESULT, Map.of(Constants.CONTENT, Map.of("name", "Course 1")));
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(apiResponse);
        Map<String, Object> result = contentLookupService.getContentMetadata(CONTENT_ID);
        assertEquals("Course 1", result.get("name"));
    }

    @Test
    void getContentMetadata_returnsEmptyMapOnNonOkApiResponse() throws Exception {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put(Constants.RESPONSE_CODE, "SERVER_ERROR");
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(apiResponse);
        assertTrue(contentLookupService.getContentMetadata(CONTENT_ID).isEmpty());
    }

    @Test
    void getContentMetadata_returnsEmptyMapWhenApiReturnsNull() throws Exception {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(null);
        assertTrue(contentLookupService.getContentMetadata(CONTENT_ID).isEmpty());
    }

    @Test
    void getContentMetadata_usesExternalContentApiForExtPrefix() throws Exception {
        String externalContentId = Constants.EXTERNAL_CONTENT_PREFIX + "event1";
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put(Constants.RESULT, Map.of(Constants.EVENT, Map.of("name", "Event 1")));
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(apiResponse);
        Map<String, Object> result = contentLookupService.getContentMetadata(externalContentId);
        assertEquals("Event 1", result.get("name"));
    }

    @Test
    void getContentMetadata_returnsEmptyMapWhenExternalEventMissing() throws Exception {
        String externalContentId = Constants.EXTERNAL_CONTENT_PREFIX + "event1";
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put(Constants.RESULT, new HashMap<String, Object>());
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(apiResponse);
        assertTrue(contentLookupService.getContentMetadata(externalContentId).isEmpty());
    }

    @Test
    void getContentMetadata_swallowsApiExceptions() throws Exception {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(outboundRequestHandlerService.fetchResult(anyString()))
                .thenThrow(new RuntimeException("content service down"));
        assertTrue(contentLookupService.getContentMetadata(CONTENT_ID).isEmpty());
    }

    @Test
    void enrichContentListForRead_withEmptyList_returnsEmpty() {
        List<Map<String, Object>> result = contentLookupService.enrichContentListForRead(
                List.of(), List.of("name", "identifier"));
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void enrichContentListForRead_withLiveContent_returnsFilteredFields() {
        String contentId = "do_123";
        String cachedJson = "{\"identifier\":\"do_123\",\"name\":\"Test Course\",\"status\":\"Live\",\"description\":\"Desc\"}";
        when(redisCacheMgr.getFromCache(Constants.EXTENDED_READ_CONTENT_CACHE_KEY_PREFIX + contentId))
                .thenReturn(cachedJson);
        List<Map<String, Object>> result = contentLookupService.enrichContentListForRead(
                List.of(contentId), List.of("identifier", "name", "status"));
        assertEquals(1, result.size());
        Map<String, Object> enriched = result.get(0);
        assertEquals("do_123", enriched.get("identifier"));
        assertEquals("Test Course", enriched.get("name"));
        assertEquals("Live", enriched.get("status"));
        assertTrue(!enriched.containsKey("description"));
    }

    @Test
    void enrichContentListForRead_filtersNonLiveContent() {
        String contentId = "do_456";
        String cachedJson = "{\"identifier\":\"do_456\",\"name\":\"Draft Course\",\"status\":\"Draft\"}";
        when(redisCacheMgr.getFromCache(Constants.EXTENDED_READ_CONTENT_CACHE_KEY_PREFIX + contentId))
                .thenReturn(cachedJson);
        List<Map<String, Object>> result = contentLookupService.enrichContentListForRead(
                List.of(contentId), List.of("identifier", "name"));
        assertTrue(result.isEmpty());
    }

    @Test
    void enrichContentListForRead_skipsContentNotFound() {
        String contentId = "do_missing";
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(outboundRequestHandlerService.fetchResult(anyString()))
                .thenReturn(Map.of(Constants.RESPONSE_CODE, "CLIENT_ERROR"));
        List<Map<String, Object>> result = contentLookupService.enrichContentListForRead(
                List.of(contentId), List.of("identifier"));
        assertTrue(result.isEmpty());
    }

    @Test
    void enrichContentListForRead_mixedContent_returnsOnlyLive() {
        String liveId = "do_live";
        String draftId = "do_draft";
        String missingId = "do_missing";
        when(redisCacheMgr.getFromCache(Constants.EXTENDED_READ_CONTENT_CACHE_KEY_PREFIX + liveId))
                .thenReturn("{\"identifier\":\"do_live\",\"name\":\"Live Course\",\"status\":\"Live\"}");
        when(redisCacheMgr.getFromCache(Constants.EXTENDED_READ_CONTENT_CACHE_KEY_PREFIX + draftId))
                .thenReturn("{\"identifier\":\"do_draft\",\"name\":\"Draft Course\",\"status\":\"Draft\"}");
        when(redisCacheMgr.getFromCache(Constants.EXTENDED_READ_CONTENT_CACHE_KEY_PREFIX + missingId))
                .thenReturn(null);
        when(outboundRequestHandlerService.fetchResult(anyString()))
                .thenReturn(Map.of(Constants.RESPONSE_CODE, "CLIENT_ERROR"));
        List<Map<String, Object>> result = contentLookupService.enrichContentListForRead(
                List.of(liveId, draftId, missingId), List.of("identifier", "name"));
        assertEquals(1, result.size());
        assertEquals("do_live", result.get(0).get("identifier"));
    }

    @Test
    void enrichContentListForRead_handlesExceptionsGracefully() {
        String contentId = "do_error";
        when(redisCacheMgr.getFromCache(anyString())).thenThrow(new RuntimeException("Redis error"));
        List<Map<String, Object>> result = contentLookupService.enrichContentListForRead(
                List.of(contentId), List.of("identifier"));
        assertTrue(result.isEmpty());
    }

    @Test
    void enrichContentListForRead_withEmptyAllowedFields_returnsAllFields() {
        String contentId = "do_123";
        when(redisCacheMgr.getFromCache(Constants.EXTENDED_READ_CONTENT_CACHE_KEY_PREFIX + contentId))
                .thenReturn("{\"identifier\":\"do_123\",\"name\":\"Test Course\",\"status\":\"Live\"}");
        List<Map<String, Object>> result = contentLookupService.enrichContentListForRead(
                List.of(contentId), List.of());
        assertEquals(1, result.size());
        assertTrue(result.get(0).containsKey("identifier"));
        assertTrue(result.get(0).containsKey("name"));
    }

    @Test
    void constructor_withValidDependencies_createsInstance() {
        assertNotNull(new CbPlanContentLookupServiceV4Impl(
                cassandraOperation, redisCacheMgr, outboundRequestHandlerService, serverProperties));
    }
}
