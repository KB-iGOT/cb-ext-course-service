package com.igot.cb.cache;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.igot.cb.util.CbExtServerProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

@ExtendWith(MockitoExtension.class)
class CbPlanRedisCacheMgrTest {

    private static final int DB_INDEX = 3;
    private static final String CACHE_KEY = "cbplan:v4:dict:org1:2026-27";
    private static final String CACHE_VALUE = "{\"planId\":\"plan-001\"}";
    private static final int TTL = 3600;
    private static final String PATTERN = "cbplan:v4:dict:org1:*";

    @Mock
    private JedisPool jedisPool;

    @Mock
    private Jedis jedis;

    @Mock
    private CbExtServerProperties serverProperties;

    private CbPlanRedisCacheMgr cacheMgr;

    @BeforeEach
    void setUp() {
        cacheMgr = new CbPlanRedisCacheMgr(jedisPool, serverProperties);
    }

    private void stubDbIndex() {
        when(serverProperties.getCbPlanRedisDbIndex()).thenReturn(DB_INDEX);
    }

    @Test
    void getFromCache_hit_returnsValue() {
        stubDbIndex();
        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.get(CACHE_KEY)).thenReturn(CACHE_VALUE);

        String result = cacheMgr.getFromCache(CACHE_KEY);

        assertEquals(CACHE_VALUE, result);
        verify(jedis).select(DB_INDEX);
        verify(jedis).get(CACHE_KEY);
        verify(jedis).close();
    }

    @Test
    void getFromCache_miss_returnsNull() {
        stubDbIndex();
        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.get(CACHE_KEY)).thenReturn(null);

        assertNull(cacheMgr.getFromCache(CACHE_KEY));
        verify(jedis).select(DB_INDEX);
    }

    @Test
    void getFromCache_poolException_returnsNull() {
        when(jedisPool.getResource()).thenThrow(new RuntimeException("pool exhausted"));

        assertNull(cacheMgr.getFromCache(CACHE_KEY));
    }

    @Test
    void getFromCache_selectException_returnsNull() {
        stubDbIndex();
        when(jedisPool.getResource()).thenReturn(jedis);
        doThrow(new RuntimeException("select failed")).when(jedis).select(DB_INDEX);

        assertNull(cacheMgr.getFromCache(CACHE_KEY));
        verify(jedis).close();
    }

    @Test
    void putInCache_success_selectsDbAndSetsEx() {
        stubDbIndex();
        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.setex(CACHE_KEY, TTL, CACHE_VALUE)).thenReturn("OK");

        assertDoesNotThrow(() -> cacheMgr.putInCache(CACHE_KEY, CACHE_VALUE, TTL));

        verify(jedis).select(DB_INDEX);
        verify(jedis).setex(CACHE_KEY, TTL, CACHE_VALUE);
        verify(jedis).close();
    }

    @Test
    void putInCache_poolException_doesNotThrow() {
        when(jedisPool.getResource()).thenThrow(new RuntimeException("pool exhausted"));

        assertDoesNotThrow(() -> cacheMgr.putInCache(CACHE_KEY, CACHE_VALUE, TTL));
    }

    @Test
    void putInCache_selectException_doesNotThrow() {
        stubDbIndex();
        when(jedisPool.getResource()).thenReturn(jedis);
        doThrow(new RuntimeException("select failed")).when(jedis).select(DB_INDEX);

        assertDoesNotThrow(() -> cacheMgr.putInCache(CACHE_KEY, CACHE_VALUE, TTL));
        verify(jedis).close();
    }

    @Test
    void deleteKeysByPatternAsync_runsOnBackgroundThread() throws InterruptedException {
        stubDbIndex();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> threadName = new AtomicReference<>();

        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.scan(anyString(), any(ScanParams.class))).thenAnswer(inv -> {
            threadName.set(Thread.currentThread().getName());
            latch.countDown();
            return new ScanResult<>(ScanParams.SCAN_POINTER_START, List.<String>of());
        });

        cacheMgr.deleteKeysByPatternAsync(PATTERN);

        assertTrue(latch.await(5, TimeUnit.SECONDS), "background scan did not execute");
        assertEquals("cbplan-redis-cache-invalidator", threadName.get());
    }

    @Test
    void deleteKeysByPatternAsync_samePatternCoalesced() throws InterruptedException {
        stubDbIndex();
        CountDownLatch firstScanStarted = new CountDownLatch(1);
        CountDownLatch allowFirstScanToFinish = new CountDownLatch(1);

        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.scan(anyString(), any(ScanParams.class))).thenAnswer(inv -> {
            firstScanStarted.countDown();
            allowFirstScanToFinish.await();
            return new ScanResult<>(ScanParams.SCAN_POINTER_START, List.<String>of());
        });

        cacheMgr.deleteKeysByPatternAsync(PATTERN);
        assertTrue(firstScanStarted.await(5, TimeUnit.SECONDS));

        cacheMgr.deleteKeysByPatternAsync(PATTERN);
        cacheMgr.deleteKeysByPatternAsync(PATTERN);

        allowFirstScanToFinish.countDown();
    }

    @Test
    void deleteKeysByPatternAsync_poolExceptionDuringExecution_doesNotThrow() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        when(jedisPool.getResource()).thenAnswer(inv -> {
            latch.countDown();
            throw new RuntimeException("pool exhausted");
        });

        cacheMgr.deleteKeysByPatternAsync(PATTERN);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    void deleteKeysByPattern_multiPage_deletesAllKeys() throws InterruptedException {
        stubDbIndex();
        CountDownLatch done = new CountDownLatch(1);

        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.scan(eq(ScanParams.SCAN_POINTER_START), any(ScanParams.class)))
                .thenReturn(new ScanResult<>("cursor1", List.of("key1", "key2")));
        when(jedis.scan(eq("cursor1"), any(ScanParams.class)))
                .thenReturn(new ScanResult<>(ScanParams.SCAN_POINTER_START, List.of("key3")));
        when(jedis.del(any(String[].class))).thenReturn(1L);
        doAnswer(inv -> { done.countDown(); return null; }).when(jedis).close();

        cacheMgr.deleteKeysByPatternAsync(PATTERN);

        assertTrue(done.await(5, TimeUnit.SECONDS));
        verify(jedis, times(2)).scan(anyString(), any(ScanParams.class));
        verify(jedis, times(2)).del(any(String[].class));
    }

    @Test
    void deleteKeysByPattern_emptyResult_noDelCalled() throws InterruptedException {
        stubDbIndex();
        CountDownLatch done = new CountDownLatch(1);

        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.scan(anyString(), any(ScanParams.class)))
                .thenReturn(new ScanResult<>(ScanParams.SCAN_POINTER_START, List.<String>of()));
        doAnswer(inv -> { done.countDown(); return null; }).when(jedis).close();

        cacheMgr.deleteKeysByPatternAsync(PATTERN);

        assertTrue(done.await(5, TimeUnit.SECONDS));
        verify(jedis, never()).del(any(String[].class));
    }

    @Test
    void shutdownInvalidationExecutor_doesNotThrow() {
        assertDoesNotThrow(() -> cacheMgr.shutdownInvalidationExecutor());
    }
}
