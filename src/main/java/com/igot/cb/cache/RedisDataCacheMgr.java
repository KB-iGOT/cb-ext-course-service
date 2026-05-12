package com.igot.cb.cache;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
/**
 * Cache manager for Redis data operations with database index support.
 * Provides READ-ONLY methods to fetch data from Redis.
 */
@Component
@Slf4j
public class RedisDataCacheMgr {
    private final JedisPool jedisDataPool;
    /**
     * Constructor for RedisDataCacheMgr.
     *
     * @param jedisDataPool Jedis connection pool for Redis data operations.
     */
    public RedisDataCacheMgr(@Qualifier("jedisDataPool") JedisPool jedisDataPool) {
        this.jedisDataPool = jedisDataPool;
    }
    /**
     * Get all fields from Redis HASH from default database (0).
     * Uses HGETALL command.
     *
     * @param key The Redis hash key.
     * @return Map of all field-value pairs, or empty map if error occurred.
     */
    public Map<String, String> getAllHashFields(String key) {
        return getAllHashFields(key, 0);
    }
    /**
     * Get all fields from Redis HASH from a specific database.
     * Uses HGETALL command.
     *
     * @param key     The Redis hash key.
     * @param dbIndex The database index (0-15).
     * @return Map of all field-value pairs, or empty map if error occurred.
     */
    public Map<String, String> getAllHashFields(String key, int dbIndex) {
        try (Jedis jedis = jedisDataPool.getResource()) {
            jedis.select(dbIndex);
            Map<String, String> result = jedis.hgetAll(key);
            log.debug("Retrieved {} fields from hash key '{}' in db {}", 
                     result != null ? result.size() : 0, key, dbIndex);
            return result;
        } catch (Exception e) {
            log.error("Failed to fetch all hash fields from Redis key: {} in db: {}", key, dbIndex, e);
            return new HashMap<>();
        }
    }
    /**
     * Get a single field from Redis HASH from default database (0).
     * Uses HGET command.
     *
     * @param key      The Redis hash key.
     * @param field    The field name within the hash.
     * @return The field value, or null if not found or error occurred.
     */
    public String getHashField(String key, String field) {
        return getHashField(key, field, 0);
    }
    /**
     * Get a single field from Redis HASH from a specific database.
     * Uses HGET command.
     *
     * @param key      The Redis hash key.
     * @param field    The field name within the hash.
     * @param dbIndex  The database index (0-15).
     * @return The field value, or null if not found or error occurred.
     */
    public String getHashField(String key, String field, int dbIndex) {
        try (Jedis jedis = jedisDataPool.getResource()) {
            jedis.select(dbIndex);
            String result = jedis.hget(key, field);
            log.debug("Retrieved field '{}' from hash key '{}' in db {}: {}", 
                     field, key, dbIndex, result != null ? "found" : "not found");
            return result;
        } catch (Exception e) {
            log.error("Failed to fetch hash field from Redis key: {}, field: {} in db: {}", key, field, dbIndex, e);
            return null;
        }
    }
}
