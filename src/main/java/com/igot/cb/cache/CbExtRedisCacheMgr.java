package com.igot.cb.cache;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class CbExtRedisCacheMgr {

    private static final long NANOS_PER_MILLIS = 1_000_000L;

    private final JedisPool jedisPool;
    private final int dbIndex;

    private final ExecutorService cacheInvalidationExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "cbplan-redis-cache-invalidator");
        t.setDaemon(true);
        return t;
    });
    private final Set<String> queuedInvalidationPatterns = ConcurrentHashMap.newKeySet();

    /**
     * Creates a cache manager bound to a single Redis endpoint and logical database.
     *
     * @param jedisPool connection pool for the target Redis endpoint
     * @param dbIndex   logical Redis database index selected on every operation
     */
    public CbExtRedisCacheMgr(JedisPool jedisPool, int dbIndex) {
        this.jedisPool = jedisPool;
        this.dbIndex = dbIndex;
    }

    /**
     * Reads a value from the cache.
     * Treated as best-effort: a Redis failure degrades to a cache miss rather than
     * propagating to the caller.
     *
     * @param key cache key to look up
     * @return the cached value, or {@code null} if absent or the lookup failed
     */
    public String getFromCache(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.select(dbIndex);
            return jedis.get(key);
        } catch (Exception e) {
            log.error("CbExtRedisCacheMgr.getFromCache: key={}", key, e);
            return null;
        }
    }

    /**
     * Writes a value to the cache with an explicit expiry.
     * Failures are logged and swallowed so that a Redis outage never fails the write path.
     *
     * @param key        cache key to write
     * @param value      value to store
     * @param ttlSeconds time-to-live in seconds
     */
    public void putInCache(String key, String value, int ttlSeconds) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.select(dbIndex);
            jedis.setex(key, ttlSeconds, value);
            log.debug("CbExtRedisCacheMgr.putInCache: key={}, ttl={}s", key, ttlSeconds);
        } catch (Exception e) {
            log.error("CbExtRedisCacheMgr.putInCache: key={}", key, e);
        }
    }

    /**
     * Schedules invalidation of all keys matching the pattern on a background thread,
     * keeping the SCAN off the request path.
     * Repeated requests for a pattern already queued are coalesced into the single
     * pending pass, so a burst of invalidations on a hot pattern cannot pile up scans.
     *
     * @param pattern Redis key pattern to invalidate, e.g. {@code cbplan:v4:org_001:*}
     */
    public void deleteKeysByPatternAsync(String pattern) {
        if (!queuedInvalidationPatterns.add(pattern)) {
            log.debug("CbExtRedisCacheMgr.deleteKeysByPatternAsync: coalescing pattern={}", pattern);
            return;
        }
        try {
            cacheInvalidationExecutor.execute(() -> {
                queuedInvalidationPatterns.remove(pattern);
                deleteKeysByPattern(pattern);
            });
        } catch (Exception e) {
            queuedInvalidationPatterns.remove(pattern);
            log.error("CbExtRedisCacheMgr.deleteKeysByPatternAsync: failed to schedule pattern={}", pattern, e);
        }
    }

    /**
     * Deletes every key matching the pattern using a cursor-based SCAN and batched DEL.
     * SCAN is used rather than KEYS so the Redis server is never blocked under production load.
     *
     * @param pattern Redis key pattern to invalidate
     */
    private void deleteKeysByPattern(String pattern) {
        long startNanos = System.nanoTime();
        long deleted = 0;
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.select(dbIndex);
            ScanParams params = new ScanParams().match(pattern).count(500);
            String cursor = ScanParams.SCAN_POINTER_START;
            do {
                ScanResult<String> scan = jedis.scan(cursor, params);
                List<String> keys = scan.getResult();
                if (!keys.isEmpty()) {
                    deleted += jedis.del(keys.toArray(String[]::new));
                }
                cursor = scan.getCursor();
            } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
            log.info("CbExtRedisCacheMgr.deleteKeysByPattern: deleted={}, pattern={}, elapsed={}ms",
                    deleted, pattern, (System.nanoTime() - startNanos) / NANOS_PER_MILLIS);
        } catch (Exception e) {
            log.error("CbExtRedisCacheMgr.deleteKeysByPattern: pattern={}", pattern, e);
        }
    }

    /**
     * Stops the background invalidation executor when the bean is destroyed, so pod
     * shutdown is not held up by the daemon thread.
     */
    @PreDestroy
    void shutdownInvalidationExecutor() {
        cacheInvalidationExecutor.shutdown();
    }
}
