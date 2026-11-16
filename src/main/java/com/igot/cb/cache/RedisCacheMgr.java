package com.igot.cb.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

/**
 * Cache manager for Redis operations.
 * It provides methods to get and set data in Redis cache.
 */
@Component
@Slf4j
public class RedisCacheMgr {
    private final JedisPool jedisPool;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${cb.cache.ttl:600}") 
    private int ttlSeconds;

    /**
     * Single background thread for pattern deletes, so a keyspace-wide SCAN never runs on an
     * API or Kafka listener thread and at most one scan runs at a time.
     */
    private final ExecutorService cacheInvalidationExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "redis-cache-invalidator");
        t.setDaemon(true);
        return t;
    });
    /** Patterns queued but not yet started; a pattern already queued is not queued again. */
    private final Set<String> queuedInvalidationPatterns = ConcurrentHashMap.newKeySet();

    /**
     * Constructor for RedisCacheMgr.
     *
     * @param jedisPool Jedis connection pool for Redis operations.
     */
    public RedisCacheMgr(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    /**
     * Sets a key-value pair in the Redis cache with a TTL.
     *
     * @param key   The key under which the value is stored.
     * @param value The value to be stored.
     * @return true if the operation was successful, false otherwise.
     */
    public String getFromCache(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key);
        } catch (Exception e) {
            log.error("Failed to read data from Redis: ", e);
            return null;
        }
    }

    /**
     * Sets a key-value pair in the Redis cache with a TTL.
     *
     * @param key   The key under which the value is stored.
     * @param value The value to be stored.
     * @return true if the operation was successful, false otherwise.
     */
    public boolean setAccessSettingRuleCache(String redisKey, String fieldKey, Map<String, Object> fieldData) {
        try (Jedis jedis = jedisPool.getResource()) {
            String fieldValue = objectMapper.writeValueAsString(fieldData);
            jedis.hset(redisKey, fieldKey, fieldValue);
            jedis.expire(redisKey, ttlSeconds);
            log.info("Cached field '{}' under Redis key '{}'", fieldKey, redisKey);
            return true;
        } catch (Exception e) {
            log.error("Failed to set access setting rule cache for key: {}, field: {}", redisKey, fieldKey, e);
            return false;
        }
    }

    /**
     * Get a single record from the Redis HSET cache
     */
    public String getCachedAccessRule(String redisKey, String contextid, String contextidtype) {
        String fieldKey = contextid + "|" + contextidtype;
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hget(redisKey, fieldKey);
        } catch (Exception e) {
            log.error("Failed to fetch cached rule from Redis for key: {}, field: {}", redisKey, fieldKey, e);
            return null;
        }
    }

    /**
     * Get all records from the HSET cache
     */
    public Map<String, String> getAllCachedAccessRules(String redisKey) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hgetAll(redisKey);
        } catch (Exception e) {
            log.error("Failed to fetch all cached rules from Redis key: {}", redisKey, e);
            return new HashMap<>();
        }
    }

    public void putInCache(String key, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.setex(key, ttlSeconds, value);
        } catch (Exception e) {
            log.error("Failed to write data to Redis with expiry: ", e);
        }
    }

    /**
     * Sets a key-value pair in the Redis cache with a custom TTL in seconds.
     *
     * @param key        The key under which the value is stored.
     * @param value      The value to be stored.
     * @param ttlSeconds The time-to-live in seconds for this cache entry.
     */
    public void putInCache(String key, String value, int ttlSeconds) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.setex(key, ttlSeconds, value);
            log.debug("Cached key '{}' with custom TTL: {} seconds", key, ttlSeconds);
        } catch (Exception e) {
            log.error("Failed to write data to Redis with custom expiry: ", e);
        }
    }

    /**
     * Gets a value from the Redis cache with a custom TTL applied on retrieval.
     * Note: This retrieves the value and does NOT modify the existing TTL.
     * If you need to refresh TTL on read, use getFromCacheAndRefreshTTL instead.
     *
     * @param key The key to retrieve.
     * @return The cached value, or null if not found or error occurred.
     */
    public String getFromCache(String key, int ttlSeconds) {
        try (Jedis jedis = jedisPool.getResource()) {
            String value = jedis.get(key);
            if (value != null && ttlSeconds > 0) {
                jedis.expire(key, ttlSeconds);
                log.debug("Retrieved and refreshed TTL for key '{}' to {} seconds", key, ttlSeconds);
            }
            return value;
        } catch (Exception e) {
            log.error("Failed to read data from Redis: ", e);
            return null;
        }
    }

    /**
     * Schedules deletion of every key matching the given glob pattern on the background
     * invalidation thread and returns immediately. The caller is never blocked, regardless of
     * keyspace size.
     * <p>
     * Requests are coalesced: if the same pattern is already queued and has not started yet, the
     * new request is dropped because the queued run will cover it. A request arriving while a
     * scan for that pattern is <em>running</em> is queued again, since keys written during the
     * scan may have been missed.
     *
     * @param pattern glob pattern of keys to delete
     */
    public void deleteKeysByPatternAsync(String pattern) {
        if (!queuedInvalidationPatterns.add(pattern)) {
            log.debug("Redis invalidation for pattern '{}' already queued, coalescing", pattern);
            return;
        }
        try {
            cacheInvalidationExecutor.execute(() -> {
                queuedInvalidationPatterns.remove(pattern);
                deleteKeysByPattern(pattern);
            });
        } catch (Exception e) {
            queuedInvalidationPatterns.remove(pattern);
            log.error("Failed to schedule Redis invalidation for pattern '{}': ", pattern, e);
        }
    }

    /**
     * Deletes every key matching the given glob pattern (e.g. "cbplan:v4:userlookup:*").
     * Uses SCAN + DEL in batches so it is safe to run against a live Redis, but it walks the
     * whole keyspace and BLOCKS the calling thread for the duration; prefer
     * {@link #deleteKeysByPatternAsync(String)} from request or listener threads.
     *
     * @param pattern glob pattern of keys to delete
     * @return number of keys deleted, or -1 on error
     */
    public long deleteKeysByPattern(String pattern) {
        long startNanos = System.nanoTime();
        long deleted = 0;
        try (Jedis jedis = jedisPool.getResource()) {
            ScanParams params = new ScanParams().match(pattern).count(500);
            String cursor = ScanParams.SCAN_POINTER_START;
            do {
                ScanResult<String> scan = jedis.scan(cursor, params);
                List<String> keys = scan.getResult();
                if (!keys.isEmpty()) {
                    deleted += jedis.del(keys.toArray(new String[0]));
                }
                cursor = scan.getCursor();
            } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
            log.info("Deleted {} Redis keys matching pattern '{}' in {} ms", deleted, pattern,
                    (System.nanoTime() - startNanos) / 1_000_000);
            return deleted;
        } catch (Exception e) {
            log.error("Failed to delete Redis keys matching pattern '{}': ", pattern, e);
            return -1;
        }
    }

    @PreDestroy
    void shutdownInvalidationExecutor() {
        cacheInvalidationExecutor.shutdown();
    }

}
