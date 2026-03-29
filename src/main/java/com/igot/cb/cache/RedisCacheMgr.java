package com.igot.cb.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

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

    public boolean setHashValue(String redisKey, String fieldKey, String fieldValue) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hset(redisKey, fieldKey, fieldValue);
            jedis.expire(redisKey, ttlSeconds);
            log.info("Cached field '{}' under Redis key '{}'", fieldKey, redisKey);
            return true;
        } catch (Exception e) {
            log.error("Failed to set Redis hash value for key: {}, field: {}", redisKey, fieldKey, e);
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

    public boolean deleteHashField(String redisKey, String fieldKey) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hdel(redisKey, fieldKey);
            log.info("Deleted field '{}' from Redis key '{}'", fieldKey, redisKey);
            return true;
        } catch (Exception e) {
            log.error("Failed to delete Redis hash field for key: {}, field: {}", redisKey, fieldKey, e);
            return false;
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

}
