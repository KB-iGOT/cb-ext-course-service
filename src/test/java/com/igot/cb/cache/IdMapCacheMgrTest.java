package com.igot.cb.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.core.ParameterizedTypeReference;

import com.igot.cb.model.CachedIdMap;
import com.igot.cb.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PropertiesCache;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class IdMapCacheMgrTest {

    @Mock
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @InjectMocks
    private IdMapCacheMgr idMapCacheMgr;

    @BeforeEach
    void setup() throws Exception {
        // Inject mock properties
        Properties testProps = new Properties();
        testProps.setProperty(Constants.ID_MAP_SERVICE_URL, "http://mock-idmap/");
        testProps.setProperty(Constants.ID_MAP_SERVICE_READ_ENDPOINT, "read/");

        PropertiesCache instance = PropertiesCache.getInstance();
        Field field = PropertiesCache.class.getDeclaredField("configProp");
        field.setAccessible(true);
        field.set(instance, testProps);

        // Reset internal cache map
        Field cacheMapField = IdMapCacheMgr.class.getDeclaredField("cacheMap");
        cacheMapField.setAccessible(true);
        cacheMapField.set(idMapCacheMgr, new ConcurrentHashMap<>());
    }

    private void putInCache(String key, CachedIdMap entry) throws Exception {
        Field cacheMapField = IdMapCacheMgr.class.getDeclaredField("cacheMap");
        cacheMapField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, CachedIdMap> cacheMap = (Map<String, CachedIdMap>) cacheMapField.get(idMapCacheMgr);
        cacheMap.put(key, entry);
    }

    @Test
    void testGetId_allInCache_valid() throws Exception {
        String key = "designation";
        CachedIdMap validEntry = new CachedIdMap(1, System.currentTimeMillis());

        putInCache(key, validEntry);

        Map<String, Integer> result = idMapCacheMgr.getId(List.of(key));

        assertEquals(1, result.size());
        assertEquals(1, result.get(key));
        verify(outboundRequestHandlerService, never()).fetchResult(anyString());
    }

    @Test
    void testGetId_expiredCache_callsService() throws Exception {
        String key = "designation";
        Long expiredTime = System.currentTimeMillis() - (2 * 60 * 60 * 1000); // 2 hours ago
        CachedIdMap expiredEntry = new CachedIdMap(2, expiredTime);

        Map<String, Integer> mockResponse = new HashMap<>();
        mockResponse.put(key, 10);

        putInCache(key, expiredEntry);

        when(outboundRequestHandlerService.fetchResultUsingExchange(
                anyString(),
                ArgumentMatchers.<ParameterizedTypeReference<List<Map<String, Integer>>>>any()))
                .thenReturn(List.of(mockResponse));

        Map<String, Integer> result = idMapCacheMgr.getId(List.of(key));

        assertEquals(1, result.size());
        assertEquals(10, result.get(key));
    }

    @Test
    void testGetId_notInCache_callsService() {
        String key = "newKey";
        String normalizedKey = key.trim().toLowerCase();

        Map<String, Integer> mockResponse = new HashMap<>();
        mockResponse.put(normalizedKey, 42);

        when(outboundRequestHandlerService.fetchResultUsingExchange(
                anyString(),
                ArgumentMatchers.<ParameterizedTypeReference<List<Map<String, Integer>>>>any()))
                .thenReturn(List.of(mockResponse));

        Map<String, Integer> result = idMapCacheMgr.getId(List.of(key));

        assertEquals(1, result.size());
        assertEquals(42, result.get(normalizedKey)); // check using normalized key
    }

    @Test
    void testGetId_serviceReturnsEmpty() {
        String key = "missing";
        lenient().when(outboundRequestHandlerService.fetchResult(anyString()))
                .thenReturn(Collections.emptyMap());

        Map<String, Integer> result = idMapCacheMgr.getId(List.of(key));
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetId_withEmptyInput_returnsEmptyMap() {
        Map<String, Integer> result = idMapCacheMgr.getId(Collections.emptyList());
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetId_serviceReturnsNull() {
        String key = "missing";
        when(outboundRequestHandlerService.fetchResultUsingExchange(
                anyString(),
                ArgumentMatchers.<ParameterizedTypeReference<List<Map<String, Integer>>>>any()))
                .thenReturn(null); // simulate null

        Map<String, Integer> result = idMapCacheMgr.getId(List.of(key));
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetId_serviceReturnsMultipleMappings() {
        String key1 = "k1";
        String key2 = "k2";

        Map<String, Integer> responseMap1 = new HashMap<>();
        responseMap1.put(key1, 1);
        Map<String, Integer> responseMap2 = new HashMap<>();
        responseMap2.put(key2, 2);

        when(outboundRequestHandlerService.fetchResultUsingExchange(
                anyString(),
                ArgumentMatchers.<ParameterizedTypeReference<List<Map<String, Integer>>>>any()))
                .thenReturn(List.of(responseMap1, responseMap2));

        Map<String, Integer> result = idMapCacheMgr.getId(List.of(key1, key2));

        assertEquals(2, result.size());
        assertEquals(1, result.get(key1));
        assertEquals(2, result.get(key2));
    }

    @Test
    void testGetId_withUrlEncodedSpaces_decodesCorrectly() {
        String encodedKey = "assistant%20engineer"; // Encoded: "assistant engineer"
        String expectedDecodedKey = "assistant engineer";
        Map<String, Integer> mockResponse = new HashMap<>();
        mockResponse.put(encodedKey, 100);
        when(outboundRequestHandlerService.fetchResultUsingExchange(
                anyString(),
                ArgumentMatchers.<ParameterizedTypeReference<List<Map<String, Integer>>>>any()))
                .thenReturn(List.of(mockResponse));
        Map<String, Integer> result = idMapCacheMgr.getId(List.of("Assistant Engineer"));
        assertEquals(1, result.size());
        assertEquals(100, result.get(expectedDecodedKey));
    }

    @Test
    void testGetId_withUrlEncodedParentheses_decodesCorrectly() {
        String encodedKey = "assistant%20%28signal%20and%20s%26t%29"; // "assistant (signal and s&t)"
        String expectedDecodedKey = "assistant (signal and s&t)";
        Map<String, Integer> mockResponse = new HashMap<>();
        mockResponse.put(encodedKey, 200);
        when(outboundRequestHandlerService.fetchResultUsingExchange(
                anyString(),
                ArgumentMatchers.<ParameterizedTypeReference<List<Map<String, Integer>>>>any()))
                .thenReturn(List.of(mockResponse));
        Map<String, Integer> result = idMapCacheMgr.getId(List.of("Assistant (Signal and S&T)"));
        assertEquals(1, result.size());
        assertEquals(200, result.get(expectedDecodedKey));
    }

    @Test
    void testGetId_withUrlEncodedPlusSign_decodesCorrectly() {
        String encodedKey = "grade%2Ba"; // "grade+a"
        String expectedDecodedKey = "grade+a";
        Map<String, Integer> mockResponse = new HashMap<>();
        mockResponse.put(encodedKey, 300);
        when(outboundRequestHandlerService.fetchResultUsingExchange(
                anyString(),
                ArgumentMatchers.<ParameterizedTypeReference<List<Map<String, Integer>>>>any()))
                .thenReturn(List.of(mockResponse));
        Map<String, Integer> result = idMapCacheMgr.getId(List.of("Grade+A"));
        assertEquals(1, result.size());
        assertEquals(300, result.get(expectedDecodedKey));
    }

    @Test
    void testGetId_withUrlEncodedSpecialChars_decodesCorrectly() {
        String encodedKey = "path%2Fto%3Aresource%2Citem%3Bvalue"; // "path/to:resource,item;value"
        String expectedDecodedKey = "path/to:resource,item;value";
        Map<String, Integer> mockResponse = new HashMap<>();
        mockResponse.put(encodedKey, 400);
        when(outboundRequestHandlerService.fetchResultUsingExchange(
                anyString(),
                ArgumentMatchers.<ParameterizedTypeReference<List<Map<String, Integer>>>>any()))
                .thenReturn(List.of(mockResponse));
        Map<String, Integer> result = idMapCacheMgr.getId(List.of("Path/To:Resource,Item;Value"));
        assertEquals(1, result.size());
        assertEquals(400, result.get(expectedDecodedKey));
    }

    @Test
    void testGetId_withUrlEncodedAmpersand_decodesCorrectly() {
        String encodedKey = "research%20%26%20development"; // "research & development"
        String expectedDecodedKey = "research & development";
        Map<String, Integer> mockResponse = new HashMap<>();
        mockResponse.put(encodedKey, 500);
        when(outboundRequestHandlerService.fetchResultUsingExchange(
                anyString(),
                ArgumentMatchers.<ParameterizedTypeReference<List<Map<String, Integer>>>>any()))
                .thenReturn(List.of(mockResponse));
        Map<String, Integer> result = idMapCacheMgr.getId(List.of("Research & Development"));
        assertEquals(1, result.size());
        assertEquals(500, result.get(expectedDecodedKey));
    }

    @Test
    void testGetId_withUrlEncodedPercentSign_decodesCorrectly() {
        String encodedKey = "discount%2550"; // "discount%50"
        String expectedDecodedKey = "discount%50";
        Map<String, Integer> mockResponse = new HashMap<>();
        mockResponse.put(encodedKey, 600);
        when(outboundRequestHandlerService.fetchResultUsingExchange(
                anyString(),
                ArgumentMatchers.<ParameterizedTypeReference<List<Map<String, Integer>>>>any()))
                .thenReturn(List.of(mockResponse));
        Map<String, Integer> result = idMapCacheMgr.getId(List.of("Discount%50"));
        assertEquals(1, result.size());
        assertEquals(600, result.get(expectedDecodedKey));
    }

    @Test
    void testGetId_withMixedEncodedAndPlainKeys_decodesCorrectly() {
        String encodedKey1 = "senior%20manager"; // "senior manager"
        String plainKey2 = "director"; // plain key
        Map<String, Integer> mockResponse1 = new HashMap<>();
        mockResponse1.put(encodedKey1, 700);
        Map<String, Integer> mockResponse2 = new HashMap<>();
        mockResponse2.put(plainKey2, 800);
        when(outboundRequestHandlerService.fetchResultUsingExchange(
                anyString(),
                ArgumentMatchers.<ParameterizedTypeReference<List<Map<String, Integer>>>>any()))
                .thenReturn(List.of(mockResponse1, mockResponse2));
        Map<String, Integer> result = idMapCacheMgr.getId(List.of("Senior Manager", "Director"));
        assertEquals(2, result.size());
        assertEquals(700, result.get("senior manager"));
        assertEquals(800, result.get("director"));
    }

    @Test
    void testGetId_withEncodedKeyInCache_returnsFromCache() throws Exception {
        String decodedKey = "assistant (signal and s&t)";
        CachedIdMap cachedEntry = new CachedIdMap(999, System.currentTimeMillis());
        putInCache(decodedKey, cachedEntry);
        Map<String, Integer> result = idMapCacheMgr.getId(List.of("Assistant (Signal and S&T)"));
        assertEquals(1, result.size());
        assertEquals(999, result.get("Assistant (Signal and S&T)"));
        verify(outboundRequestHandlerService, never()).fetchResultUsingExchange(anyString(), 
                ArgumentMatchers.<ParameterizedTypeReference<List<Map<String, Integer>>>>any());
    }

    @Test
    void testGetId_withUrlEncodedHashSymbol_decodesCorrectly() {
        String encodedKey = "department%23001"; // "department#001"
        String expectedDecodedKey = "department#001";
        Map<String, Integer> mockResponse = new HashMap<>();
        mockResponse.put(encodedKey, 1100);
        when(outboundRequestHandlerService.fetchResultUsingExchange(
                anyString(),
                ArgumentMatchers.<ParameterizedTypeReference<List<Map<String, Integer>>>>any()))
                .thenReturn(List.of(mockResponse));
        Map<String, Integer> result = idMapCacheMgr.getId(List.of("Department#001"));
        assertEquals(1, result.size());
        assertEquals(1100, result.get(expectedDecodedKey));
    }

    @Test
    void testGetId_withUrlEncodedAtSymbol_decodesCorrectly() {
        String encodedKey = "user%40domain"; // "user@domain"
        String expectedDecodedKey = "user@domain";
        Map<String, Integer> mockResponse = new HashMap<>();
        mockResponse.put(encodedKey, 1200);
        when(outboundRequestHandlerService.fetchResultUsingExchange(
                anyString(),
                ArgumentMatchers.<ParameterizedTypeReference<List<Map<String, Integer>>>>any()))
                .thenReturn(List.of(mockResponse));
        Map<String, Integer> result = idMapCacheMgr.getId(List.of("User@Domain"));
        assertEquals(1, result.size());
        assertEquals(1200, result.get(expectedDecodedKey));
    }

    @Test
    void testGetId_streamProcessing_handlesMultipleResponseObjects() {
        String key1 = "encoded%20key1";
        String key2 = "encoded%20key2";
        String key3 = "encoded%20key3";
        Map<String, Integer> response1 = new HashMap<>();
        response1.put(key1, 10);
        response1.put(key2, 20);
        Map<String, Integer> response2 = new HashMap<>();
        response2.put(key3, 30);
        when(outboundRequestHandlerService.fetchResultUsingExchange(
                anyString(),
                ArgumentMatchers.<ParameterizedTypeReference<List<Map<String, Integer>>>>any()))
                .thenReturn(List.of(response1, response2));
        Map<String, Integer> result = idMapCacheMgr.getId(List.of("Encoded Key1", "Encoded Key2", "Encoded Key3"));
        assertEquals(3, result.size());
        assertEquals(10, result.get("encoded key1"));
        assertEquals(20, result.get("encoded key2"));
        assertEquals(30, result.get("encoded key3"));
    }

    @Test
    void testGetId_withWhitespaceAndEncoding_normalizesProperly() {
        String encodedKey = "%20%20assistant%20engineer%20%20"; // "  assistant engineer  "
        String expectedDecodedKey = "assistant engineer"; // trimmed and lowercase
        Map<String, Integer> mockResponse = new HashMap<>();
        mockResponse.put(encodedKey, 1300);
        when(outboundRequestHandlerService.fetchResultUsingExchange(
                anyString(),
                ArgumentMatchers.<ParameterizedTypeReference<List<Map<String, Integer>>>>any()))
                .thenReturn(List.of(mockResponse));
        Map<String, Integer> result = idMapCacheMgr.getId(List.of("  Assistant Engineer  "));
        assertEquals(1, result.size());
        assertEquals(1300, result.get(expectedDecodedKey));
    }

    @Test
    void testGetId_batchProcessing_withEncodedKeys() {
        List<String> keys = new java.util.ArrayList<>();
        Map<String, Integer> mockResponse1 = new HashMap<>();
        Map<String, Integer> mockResponse2 = new HashMap<>();
        for (int i = 1; i <= 60; i++) {
            keys.add("Key" + i);
            String encodedKey = "key" + i; // Service returns lowercase
            if (i <= 50) {
                mockResponse1.put(encodedKey, i);
            } else {
                mockResponse2.put(encodedKey, i);
            }
        }
        when(outboundRequestHandlerService.fetchResultUsingExchange(
                anyString(),
                ArgumentMatchers.<ParameterizedTypeReference<List<Map<String, Integer>>>>any()))
                .thenReturn(List.of(mockResponse1))
                .thenReturn(List.of(mockResponse2));
        Map<String, Integer> result = idMapCacheMgr.getId(keys);
        assertEquals(60, result.size());
        verify(outboundRequestHandlerService, times(2)).fetchResultUsingExchange(
                anyString(),
                ArgumentMatchers.<ParameterizedTypeReference<List<Map<String, Integer>>>>any());
    }

    @Test
    void testGetId_withEncodedEqualsSign_decodesCorrectly() {
        String encodedKey = "formula%3Da%2Bb"; // "formula=a+b"
        String expectedDecodedKey = "formula=a+b";
        Map<String, Integer> mockResponse = new HashMap<>();
        mockResponse.put(encodedKey, 1400);
        when(outboundRequestHandlerService.fetchResultUsingExchange(
                anyString(),
                ArgumentMatchers.<ParameterizedTypeReference<List<Map<String, Integer>>>>any()))
                .thenReturn(List.of(mockResponse));
        Map<String, Integer> result = idMapCacheMgr.getId(List.of("Formula=A+B"));
        assertEquals(1, result.size());
        assertEquals(1400, result.get(expectedDecodedKey));
    }
}
