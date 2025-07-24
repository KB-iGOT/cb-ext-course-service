package com.igot.cb.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

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
    void testReadContent_cacheMiss_callsService() throws Exception {
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
    void testReadContent_invalidJson_returnsEmpty() throws Exception {
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
}
