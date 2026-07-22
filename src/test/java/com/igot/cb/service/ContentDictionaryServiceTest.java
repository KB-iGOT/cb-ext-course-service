package com.igot.cb.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for ContentDictionaryService.
 */
@ExtendWith(MockitoExtension.class)
class ContentDictionaryServiceTest {

    @Mock
    private RedisCacheMgr redisCacheMgr;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private ContentDictionaryService contentDictionaryService;

    @Test
    void testGetContentDictionary_Success() throws Exception {
        Map<String, Object> item1Data = Map.of("name", "Course 1", "identifier", "do_123");
        Map<String, Object> item2Data = Map.of("name", "Course 2", "identifier", "do_456");
        String item1Json = objectMapper.writeValueAsString(item1Data);
        String item2Json = objectMapper.writeValueAsString(item2Data);
        Map<String, String> hashData = new HashMap<>();
        hashData.put("do_123", item1Json);
        hashData.put("do_456", item2Json);
        when(redisCacheMgr.getAllCachedAccessRules(Constants.REDIS_CONTENT_DICTIONARY_KEY)).thenReturn(hashData);
        ApiResponse response = contentDictionaryService.getContentDictionary();
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        Map<String, Object> resultDict = (Map<String, Object>) response.getResult();
        assertEquals(2, resultDict.size());
        Map<String, Object> item1 = (Map<String, Object>) resultDict.get("do_123");
        assertEquals("Course 1", item1.get("name"));
        Map<String, Object> item2 = (Map<String, Object>) resultDict.get("do_456");
        assertEquals("Course 2", item2.get("name"));
    }

    @Test
    void testGetContentDictionary_CacheEmpty() {
        when(redisCacheMgr.getAllCachedAccessRules(Constants.REDIS_CONTENT_DICTIONARY_KEY)).thenReturn(null);

        ApiResponse response = contentDictionaryService.getContentDictionary();

        assertNotNull(response);
        assertEquals(HttpStatus.NOT_FOUND, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("Content dictionary not found in cache", response.getParams().getErrMsg());
    }

    @Test
    void testGetContentDictionary_EmptyHashMap() {
        when(redisCacheMgr.getAllCachedAccessRules(Constants.REDIS_CONTENT_DICTIONARY_KEY)).thenReturn(new HashMap<>());

        ApiResponse response = contentDictionaryService.getContentDictionary();

        assertNotNull(response);
        assertEquals(HttpStatus.NOT_FOUND, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("Content dictionary not found in cache", response.getParams().getErrMsg());
    }

    @Test
    void testGetContentDictionary_InvalidJson() throws Exception {
        Map<String, String> hashData = new HashMap<>();
        hashData.put("do_123", "invalid-json");
        when(redisCacheMgr.getAllCachedAccessRules(Constants.REDIS_CONTENT_DICTIONARY_KEY)).thenReturn(hashData);
        ApiResponse response = contentDictionaryService.getContentDictionary();
        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("Content dictionary is empty or corrupted", response.getParams().getErrMsg());
    }

    @Test
    void testGetContentDictionary_PartialInvalidJson() throws Exception {
        Map<String, Object> validItemData = Map.of("name", "Valid Course", "identifier", "do_123");
        String validJson = objectMapper.writeValueAsString(validItemData);
        Map<String, String> hashData = new HashMap<>();
        hashData.put("do_123", validJson);
        hashData.put("do_456", "invalid-json");
        when(redisCacheMgr.getAllCachedAccessRules(Constants.REDIS_CONTENT_DICTIONARY_KEY)).thenReturn(hashData);
        ApiResponse response = contentDictionaryService.getContentDictionary();
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        Map<String, Object> resultDict = response.getResult();
        assertEquals(1, resultDict.size());
        Map<String, Object> validItem = (Map<String, Object>) resultDict.get("do_123");
        assertEquals("Valid Course", validItem.get("name"));
    }
}
