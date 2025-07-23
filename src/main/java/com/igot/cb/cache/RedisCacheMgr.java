package com.igot.cb.cache;

import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@Component
@Slf4j
public class RedisCacheMgr {
    private final JedisPool jedisPool;

    public RedisCacheMgr(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public String getFromCache(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key);
        } catch (Exception e) {
            log.error("Failed to read data from Redis: ", e);
            return null;
        }
    }
}
