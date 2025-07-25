package com.igot.cb.cache;

import java.util.HashMap;
import java.util.Map;

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
    private static final int TTL_SECONDS = 7200; // 2 hours

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
            jedis.expire(redisKey, TTL_SECONDS);
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
}
