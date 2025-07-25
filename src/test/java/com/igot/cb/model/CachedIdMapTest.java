package com.igot.cb.model;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class CachedIdMapTest {

    @Test
    void testConstructorAndGetValue() {
        CachedIdMap cached = new CachedIdMap(123, System.currentTimeMillis());
        assertNotNull(cached);
        assertEquals(123, cached.getValue());
    }

    @Test
    void testIsExpired_false_whenWithinTTL() {
        CachedIdMap cached = new CachedIdMap(123, System.currentTimeMillis());
        assertFalse(cached.isExpired(10000), "Should not be expired within 10 seconds");
    }

    @Test
    void testIsExpired_true_whenTTLExceeded() throws InterruptedException {
        long ttl = 10; // 10 ms
        CachedIdMap cached = new CachedIdMap(999, System.currentTimeMillis() - 100);
        assertTrue(cached.isExpired(ttl), "Should be expired if TTL is exceeded");
    }

    @Test
    void testIsExpired_zeroTTLNotExpiredImmediately() {
        CachedIdMap cached = new CachedIdMap(456, System.currentTimeMillis());
        assertFalse(cached.isExpired(0), "Should not be expired immediately when TTL is 0");
    }

    @Test
    void testIsExpired_negativeTTLAlwaysExpired() {
        CachedIdMap cached = new CachedIdMap(789, System.currentTimeMillis());
        assertTrue(cached.isExpired(-100), "Should always be expired for negative TTL");
    }
}
