package com.igot.cb.cache;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
class CbExtRedisCacheMgrTest {

    private static final int DB_INDEX = 3;
    private static final String CACHE_KEY = "cbplan:v4:dict:org1:2026-27";
    private static final String CACHE_VALUE = "{\"planId\":\"plan-001\"}";
    private static final int TTL = 3600;
    private static final String PATTERN = "cbplan:v4:dict:org1:*";
    private static final String INVALIDATOR_THREAD = "cbplan-redis-cache-invalidator";
    private static final String POOL_EXHAUSTED = "pool exhausted";
    private static final String SELECT_FAILED = "select failed";
    private static final String CURSOR_PAGE_TWO = "cursor1";
    private static final long AWAIT_SECONDS = 5L;

    @Mock
    private JedisPool jedisPool;

    @Mock
    private Jedis jedis;

    private CbExtRedisCacheMgr cacheMgr;

    @BeforeEach
    void setUp() {
        cacheMgr = new CbExtRedisCacheMgr(jedisPool, DB_INDEX);
    }

    @Test
    void getFromCache_hit_returnsValue() {
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
        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.get(CACHE_KEY)).thenReturn(null);

        assertNull(cacheMgr.getFromCache(CACHE_KEY));

        verify(jedis).select(DB_INDEX);
        verify(jedis).close();
    }

    @Test
    void getFromCache_poolException_returnsNull() {
        when(jedisPool.getResource()).thenThrow(new RuntimeException(POOL_EXHAUSTED));

        assertNull(cacheMgr.getFromCache(CACHE_KEY));
    }

    @Test
    void getFromCache_selectException_returnsNull() {
        when(jedisPool.getResource()).thenReturn(jedis);
        doThrow(new RuntimeException(SELECT_FAILED)).when(jedis).select(DB_INDEX);

        assertNull(cacheMgr.getFromCache(CACHE_KEY));

        verify(jedis, never()).get(CACHE_KEY);
        verify(jedis).close();
    }

    @Test
    void putInCache_success_selectsDbAndSetsEx() {
        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.setex(CACHE_KEY, TTL, CACHE_VALUE)).thenReturn("OK");

        assertDoesNotThrow(() -> cacheMgr.putInCache(CACHE_KEY, CACHE_VALUE, TTL));

        verify(jedis).select(DB_INDEX);
        verify(jedis).setex(CACHE_KEY, TTL, CACHE_VALUE);
        verify(jedis).close();
    }

    @Test
    void putInCache_poolException_doesNotThrow() {
        when(jedisPool.getResource()).thenThrow(new RuntimeException(POOL_EXHAUSTED));

        assertDoesNotThrow(() -> cacheMgr.putInCache(CACHE_KEY, CACHE_VALUE, TTL));
    }

    @Test
    void putInCache_selectException_doesNotThrow() {
        when(jedisPool.getResource()).thenReturn(jedis);
        doThrow(new RuntimeException(SELECT_FAILED)).when(jedis).select(DB_INDEX);

        assertDoesNotThrow(() -> cacheMgr.putInCache(CACHE_KEY, CACHE_VALUE, TTL));

        verify(jedis, never()).setex(CACHE_KEY, TTL, CACHE_VALUE);
        verify(jedis).close();
    }

    @Test
    void deleteKeysByPatternAsync_runsOnBackgroundThread() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> threadName = new AtomicReference<>();

        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.scan(anyString(), any(ScanParams.class))).thenAnswer(inv -> {
            threadName.set(Thread.currentThread().getName());
            latch.countDown();
            return new ScanResult<>(ScanParams.SCAN_POINTER_START, List.<String>of());
        });

        cacheMgr.deleteKeysByPatternAsync(PATTERN);

        assertTrue(latch.await(AWAIT_SECONDS, TimeUnit.SECONDS), "background scan did not execute");
        assertEquals(INVALIDATOR_THREAD, threadName.get());
    }

    @Test
    void deleteKeysByPatternAsync_samePatternQueuedTwice_coalescesIntoOnePass() throws InterruptedException {
        CountDownLatch firstScanStarted = new CountDownLatch(1);
        CountDownLatch allowFirstScanToFinish = new CountDownLatch(1);
        CountDownLatch secondScanDone = new CountDownLatch(1);
        AtomicInteger scanCount = new AtomicInteger();

        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.scan(anyString(), any(ScanParams.class))).thenAnswer(inv -> {
            if (scanCount.incrementAndGet() == 1) {
                firstScanStarted.countDown();
                allowFirstScanToFinish.await();
            } else {
                secondScanDone.countDown();
            }
            return new ScanResult<>(ScanParams.SCAN_POINTER_START, List.<String>of());
        });

        cacheMgr.deleteKeysByPatternAsync(PATTERN);
        assertTrue(firstScanStarted.await(AWAIT_SECONDS, TimeUnit.SECONDS), "first scan did not start");

        cacheMgr.deleteKeysByPatternAsync(PATTERN);
        cacheMgr.deleteKeysByPatternAsync(PATTERN);
        cacheMgr.deleteKeysByPatternAsync(PATTERN);
        allowFirstScanToFinish.countDown();

        assertTrue(secondScanDone.await(AWAIT_SECONDS, TimeUnit.SECONDS), "queued scan did not execute");
        verify(jedis, times(2)).scan(anyString(), any(ScanParams.class));
    }

    @Test
    void deleteKeysByPatternAsync_executorRejectsTask_swallowsRejection() {
        cacheMgr.shutdownInvalidationExecutor();

        assertDoesNotThrow(() -> cacheMgr.deleteKeysByPatternAsync(PATTERN));

        verifyNoInteractions(jedisPool);
    }

    @Test
    void deleteKeysByPatternAsync_poolExceptionDuringExecution_doesNotThrow() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        when(jedisPool.getResource()).thenAnswer(inv -> {
            latch.countDown();
            throw new RuntimeException(POOL_EXHAUSTED);
        });

        assertDoesNotThrow(() -> cacheMgr.deleteKeysByPatternAsync(PATTERN));

        assertTrue(latch.await(AWAIT_SECONDS, TimeUnit.SECONDS), "background scan did not execute");
    }

    @Test
    void deleteKeysByPattern_multiPage_deletesAllKeys() throws InterruptedException {
        CountDownLatch done = new CountDownLatch(1);

        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.scan(eq(ScanParams.SCAN_POINTER_START), any(ScanParams.class)))
                .thenReturn(new ScanResult<>(CURSOR_PAGE_TWO, List.of("key1", "key2")));
        when(jedis.scan(eq(CURSOR_PAGE_TWO), any(ScanParams.class)))
                .thenReturn(new ScanResult<>(ScanParams.SCAN_POINTER_START, List.of("key3")));
        when(jedis.del(any(String[].class))).thenReturn(1L);
        doAnswer(inv -> {
            done.countDown();
            return null;
        }).when(jedis).close();

        cacheMgr.deleteKeysByPatternAsync(PATTERN);

        assertTrue(done.await(AWAIT_SECONDS, TimeUnit.SECONDS), "background scan did not complete");
        verify(jedis).select(DB_INDEX);
        verify(jedis, times(2)).scan(anyString(), any(ScanParams.class));
        verify(jedis, times(2)).del(any(String[].class));
    }

    @Test
    void deleteKeysByPattern_emptyResult_noDelCalled() throws InterruptedException {
        CountDownLatch done = new CountDownLatch(1);

        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.scan(anyString(), any(ScanParams.class)))
                .thenReturn(new ScanResult<>(ScanParams.SCAN_POINTER_START, List.<String>of()));
        doAnswer(inv -> {
            done.countDown();
            return null;
        }).when(jedis).close();

        cacheMgr.deleteKeysByPatternAsync(PATTERN);

        assertTrue(done.await(AWAIT_SECONDS, TimeUnit.SECONDS), "background scan did not complete");
        verify(jedis, never()).del(any(String[].class));
    }

    @Test
    void deleteKeysByPattern_delThrows_doesNotPropagate() throws InterruptedException {
        CountDownLatch done = new CountDownLatch(1);

        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.scan(anyString(), any(ScanParams.class)))
                .thenReturn(new ScanResult<>(ScanParams.SCAN_POINTER_START, List.of("key1")));
        when(jedis.del(any(String[].class))).thenThrow(new RuntimeException(POOL_EXHAUSTED));
        doAnswer(inv -> {
            done.countDown();
            return null;
        }).when(jedis).close();

        assertDoesNotThrow(() -> cacheMgr.deleteKeysByPatternAsync(PATTERN));

        assertTrue(done.await(AWAIT_SECONDS, TimeUnit.SECONDS), "background scan did not complete");
    }

    @Test
    void shutdownInvalidationExecutor_doesNotThrow() {
        assertDoesNotThrow(() -> cacheMgr.shutdownInvalidationExecutor());
    }
}
