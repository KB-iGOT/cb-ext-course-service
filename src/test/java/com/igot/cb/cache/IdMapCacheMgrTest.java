package com.igot.cb.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
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
}
