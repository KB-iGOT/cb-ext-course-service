package com.igot.cb.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PropertiesCache;

@ExtendWith(MockitoExtension.class)
class ContentInfoServiceImplTest {

    @Mock
    private RedisCacheMgr redisCacheMgr;

    @Mock
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private ContentInfoServiceImpl contentService;

    @BeforeEach
    void init() throws Exception {
        Properties testProps = new Properties();
        testProps.setProperty(Constants.CONTENT_SERVICE_HOST, "http://mock-content");

        PropertiesCache cache = PropertiesCache.getInstance();

        // Use reflection to set private final field
        var field = PropertiesCache.class.getDeclaredField("configProp");
        field.setAccessible(true);
        field.set(cache, testProps);
    }

    @Test
    void testReadContent_cacheHit_returnsFilteredFields() throws Exception {
        String contentId = "content-123";
        List<String> fields = List.of("name", "type");

        Map<String, Object> fullData = new HashMap<>();
        fullData.put("name", "Course A");
        fullData.put("type", "Course");
        fullData.put("irrelevant", "data");

        String redisValue = new ObjectMapper().writeValueAsString(fullData);
        when(redisCacheMgr.getFromCache(contentId)).thenReturn(redisValue);

        Map<String, Object> result = contentService.readContent(contentId, fields);

        assertEquals(2, result.size());
        assertEquals("Course A", result.get("name"));
        assertEquals("Course", result.get("type"));
    }

    @Test
    void testReadContent_cacheMiss_callsService() {
        String contentId = "content-456";
        List<String> fields = List.of("name");

        when(redisCacheMgr.getFromCache(contentId)).thenReturn(null);

        Map<String, Object> content = Map.of("name", "New Course");
        Map<String, Object> resultMap = Map.of(Constants.CONTENT, content);
        Map<String, Object> serviceResponse = Map.of(Constants.RESPONSE_CODE, "OK", Constants.RESULT, resultMap);

        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(serviceResponse);

        Map<String, Object> result = contentService.readContent(contentId, fields);

        assertEquals("New Course", result.get("name"));
    }

    @Test
    void testReadContent_invalidJson_returnsEmpty() {
        String contentId = "invalid-json";
        when(redisCacheMgr.getFromCache(contentId)).thenReturn("bad-json");

        Map<String, Object> result = contentService.readContent(contentId, List.of("name"));
        assertTrue(result.isEmpty());
    }

    @Test
    void testReadContent_nullContentId_returnsEmpty() {
        Map<String, Object> result = contentService.readContent(null, List.of("name"));
        assertTrue(result.isEmpty());
    }

    @Test
    void testReadContentFromService_success() {
        String contentId = "service-id";
        List<String> fields = List.of("field1");

        Map<String, Object> content = Map.of("field1", "value");
        Map<String, Object> resultMap = Map.of(Constants.CONTENT, content);
        Map<String, Object> serviceResponse = Map.of(Constants.RESPONSE_CODE, "OK", Constants.RESULT, resultMap);

        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(serviceResponse);

        Map<String, Object> result = contentService.readContentFromService(contentId, fields);

        assertEquals("value", result.get("field1"));
    }

    @Test
    void testReadContentFromService_invalidResponse_returnsEmpty() {
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(Collections.emptyMap());

        Map<String, Object> result = contentService.readContentFromService("x", List.of("a"));
        assertTrue(result.isEmpty());
    }

    @Test
    void testReadCourseCategoryForContent_returnsValue() throws Exception {
        String contentId = "cat-id";
        Map<String, Object> redisMap = Map.of(Constants.COURSE_CATEGORY, "Leadership");

        String json = new ObjectMapper().writeValueAsString(redisMap);
        when(redisCacheMgr.getFromCache(contentId)).thenReturn(json);

        String category = contentService.readCourseCategoryForContent(contentId);
        assertEquals("Leadership", category);
    }

    @Test
    void testReadCourseCategoryForContent_notFound_returnsEmpty() {
        when(redisCacheMgr.getFromCache("missing")).thenReturn(null);

        String result = contentService.readCourseCategoryForContent("missing");
        assertEquals("", result);
    }

    @Test
    void testReadContent_cacheHitFilteredFields() throws Exception {
        Map<String, Object> content = Map.of("name", "course", "type", "video");
        when(redisCacheMgr.getFromCache("cid")).thenReturn(new ObjectMapper().writeValueAsString(content));
        Map<String, Object> result = contentService.readContent("cid", List.of("name"));
        assertEquals("course", result.get("name"));
        assertFalse(result.containsKey("type"));
    }

    @Test
    void testReadContent_cacheEmpty_thenServiceCalled() {
        when(redisCacheMgr.getFromCache("cid")).thenReturn(null);
        Map<String, Object> inner = Map.of(Constants.CONTENT, Map.of("name", "x"));
        Map<String, Object> resp = Map.of(Constants.RESPONSE_CODE, "OK", Constants.RESULT, inner);
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(resp);
        Map<String, Object> result = contentService.readContent("cid", List.of("name"));
        assertEquals("x", result.get("name"));
    }

    @Test
    void testReadContent_cacheThrowsException_returnsEmpty() throws Exception {
        when(redisCacheMgr.getFromCache("cid")).thenReturn("invalid-json");
        Map<String, Object> result = contentService.readContent("cid", List.of("name"));
        assertTrue(result.isEmpty());
    }

    @Test
    void testReadContent_nullId_returnsEmpty() {
        Map<String, Object> result = contentService.readContent("", List.of("x"));
        assertTrue(result.isEmpty());
    }

    @Test
    void testReadContentFromCache_returnsAllWhenFieldsNull() throws Exception {
        Map<String, Object> content = Map.of("a", 1, "b", 2);
        when(redisCacheMgr.getFromCache("cid")).thenReturn(new ObjectMapper().writeValueAsString(content));
        Map<String, Object> result = contentService.readContentFromCache("cid", null);
        assertEquals(2, result.size());
    }

    @Test
    void testReadContentFromCache_returnsEmptyForUnknownField() throws Exception {
        Map<String, Object> content = Map.of("known", 1);
        when(redisCacheMgr.getFromCache("cid")).thenReturn(new ObjectMapper().writeValueAsString(content));
        Map<String, Object> result = contentService.readContentFromCache("cid", List.of("missing"));
        assertTrue(result.isEmpty());
    }

    @Test
    void testReadContentFromCache_emptyRedisValue_returnsEmpty() throws Exception {
        when(redisCacheMgr.getFromCache("cid")).thenReturn("");
        Map<String, Object> result = contentService.readContentFromCache("cid", List.of("x"));
        assertTrue(result.isEmpty());
    }

    @Test
    void testReadContentFromService_nullResponse_returnsEmpty() {
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(null);
        assertTrue(contentService.readContentFromService("x", List.of("a")).isEmpty());
    }

    @Test
    void testReadContentFromService_wrongResponseCode_returnsEmpty() {
        Map<String, Object> badResponse = Map.of(Constants.RESPONSE_CODE, "FAIL");
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(badResponse);
        assertTrue(contentService.readContentFromService("id", List.of("f1")).isEmpty());
    }

    @Test
    void testReadCourseCategoryForContent_valuePresent() throws Exception {
        String json = new ObjectMapper().writeValueAsString(Map.of(Constants.COURSE_CATEGORY, "Leadership"));
        when(redisCacheMgr.getFromCache("cid")).thenReturn(json);
        assertEquals("Leadership", contentService.readCourseCategoryForContent("cid"));
    }

    @Test
    void testReadCourseCategoryForContent_missing_returnsEmptyString() {
        when(redisCacheMgr.getFromCache("missing")).thenReturn(null);
        assertEquals("", contentService.readCourseCategoryForContent("missing"));
    }

    @Test
    void testEnrichContentInfoForCBPlan_withLiveAndNonLive() {
        Map<String, Object> liveContent = new HashMap<>();
        liveContent.put(Constants.STATUS, Constants.LIVE);
        liveContent.put(Constants.NAME, "Live Course");
        liveContent.put(Constants.IDENTIFIER, "ID-1");
        liveContent.put(Constants.COURSE_APP_ICON, "icon.png");

        Map<String, Object> nonLiveContent = new HashMap<>();
        nonLiveContent.put(Constants.STATUS, "Draft");

        ContentInfoServiceImpl spyService = spy(contentService);
        doReturn(liveContent).when(spyService).readContent(eq("id1"), any());
        doReturn(nonLiveContent).when(spyService).readContent(eq("id2"), any());

        List<Map<String, Object>> result = spyService.enrichContentInfoForCBPlan(List.of("id1", "id2"));
        assertEquals(1, result.size());
        assertEquals("Live Course", result.get(0).get(Constants.NAME));
    }

    @Test
    void testEnrichContentInfoForCBPlan_emptyInput() {
        List<Map<String, Object>> result = contentService.enrichContentInfoForCBPlan(Collections.emptyList());
        assertTrue(result.isEmpty());
    }

    @Test
    void testReadExternalContent_success_returnsResponse() throws Exception {
        Properties testProps = new Properties();
        testProps.setProperty(Constants.CB_PORES_SERVICE_HOST, "http://mock-pores");
        testProps.setProperty(Constants.EXTERNAL_CONTENT_READ_END_POINT, "/external/read");
        PropertiesCache cache = PropertiesCache.getInstance();
        Field cfgField = PropertiesCache.class.getDeclaredField("configProp");
        cfgField.setAccessible(true);
        cfgField.set(cache, testProps);

        String contentId = "ext-1";
        Map<String, Object> content = Map.of(Constants.IDENTIFIER, contentId, Constants.NAME, "Ext Course");
        Map<String, Object> response = Map.of(Constants.CONTENT, content);

        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(response);

        Map<String, Object> result = contentService.readExternalContent(contentId);

        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertTrue(result.containsKey(Constants.CONTENT));
        assertEquals(contentId, ((Map<?,?>) result.get(Constants.CONTENT)).get(Constants.IDENTIFIER));
    }

    @Test
    void testReadExternalContent_missingContentKey_returnsEmpty() throws Exception {
        Properties testProps = new Properties();
        testProps.setProperty(Constants.CB_PORES_SERVICE_HOST, "http://mock-pores");
        testProps.setProperty(Constants.EXTERNAL_CONTENT_READ_END_POINT, "/external/read");
        PropertiesCache cache = PropertiesCache.getInstance();
        Field cfgField = PropertiesCache.class.getDeclaredField("configProp");
        cfgField.setAccessible(true);
        cfgField.set(cache, testProps);

        String contentId = "ext-2";
        Map<String, Object> response = Map.of(Constants.RESPONSE_CODE, "OK"); // no CONTENT key

        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(response);

        Map<String, Object> result = contentService.readExternalContent(contentId);

        assertNotNull(result);
        assertTrue(result.isEmpty(), "Expected empty map when response does not contain CONTENT");
    }

    @Test
    void testReadExternalContent_outboundThrowsException_returnsEmpty() throws Exception {
        Properties testProps = new Properties();
        testProps.setProperty(Constants.CB_PORES_SERVICE_HOST, "http://mock-pores");
        testProps.setProperty(Constants.EXTERNAL_CONTENT_READ_END_POINT, "/external/read");
        PropertiesCache cache = PropertiesCache.getInstance();
        Field cfgField = PropertiesCache.class.getDeclaredField("configProp");
        cfgField.setAccessible(true);
        cfgField.set(cache, testProps);

        String contentId = "ext-3";
        when(outboundRequestHandlerService.fetchResult(anyString())).thenThrow(new RuntimeException("network"));

        Map<String, Object> result = contentService.readExternalContent(contentId);

        assertNotNull(result);
        assertTrue(result.isEmpty(), "Expected empty map when outbound call throws");
    }

}
