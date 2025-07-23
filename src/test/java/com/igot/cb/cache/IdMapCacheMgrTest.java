package com.igot.cb.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Collections;
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

        Map<String, Long> result = idMapCacheMgr.getId(List.of(key));

        assertEquals(1, result.size());
        assertEquals(1, result.get(key));
        verify(outboundRequestHandlerService, never()).fetchResult(anyString());
    }

    @Test
    void testGetId_expiredCache_callsService() throws Exception {
        String key = "designation";
        long expiredTime = System.currentTimeMillis() - (2 * 60 * 60 * 1000); // 2 hours ago
        CachedIdMap expiredEntry = new CachedIdMap(2, expiredTime);

        putInCache(key, expiredEntry);

        when(outboundRequestHandlerService.fetchResult(anyString()))
                .thenReturn(Map.of(key, 10));

        Map<String, Long> result = idMapCacheMgr.getId(List.of(key));

        assertEquals(1, result.size());
        assertEquals(10, result.get(key));
    }

    @Test
    void testGetId_notInCache_callsService() {
        String key = "newKey";
        when(outboundRequestHandlerService.fetchResult(
                ArgumentMatchers.contains("/read/" + key + "?paramSeparator=#")))
                .thenReturn(Map.of(key, 42));

        Map<String, Long> result = idMapCacheMgr.getId(List.of(key));

        assertEquals(1, result.size());
        assertEquals(42, result.get(key));
    }

    @Test
    void testGetId_serviceReturnsEmpty() {
        String key = "missing";
        lenient().when(outboundRequestHandlerService.fetchResult(anyString()))
                .thenReturn(Collections.emptyMap());

        Map<String, Long> result = idMapCacheMgr.getId(List.of(key));
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetId_partialHit_cacheAndService() throws Exception {
        String cachedKey = "cachedKey";
        String remoteKey = "remoteKey";

        CachedIdMap cachedEntry = new CachedIdMap(5, System.currentTimeMillis());
        putInCache(cachedKey, cachedEntry);

        when(outboundRequestHandlerService.fetchResult(
                ArgumentMatchers.contains("/read/" + remoteKey + "?paramSeparator=#")))
                .thenReturn(Map.of(remoteKey, 9));

        Map<String, Long> result = idMapCacheMgr.getId(List.of(cachedKey, remoteKey));

        assertEquals(2, result.size());
        assertEquals(5, result.get(cachedKey));
        assertEquals(9, result.get(remoteKey));
    }
}
