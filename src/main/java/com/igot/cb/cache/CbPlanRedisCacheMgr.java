package com.igot.cb.cache;

import com.igot.cb.util.CbExtServerProperties;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@Slf4j
public class CbPlanRedisCacheMgr {

    private static final long NANOS_PER_MILLIS = 1_000_000L;

    private final JedisPool jedisPool;
    private final CbExtServerProperties serverProperties;

    private final ExecutorService cacheInvalidationExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "cbplan-redis-cache-invalidator");
        t.setDaemon(true);
        return t;
    });
    private final Set<String> queuedInvalidationPatterns = ConcurrentHashMap.newKeySet();

    public CbPlanRedisCacheMgr(@Qualifier("jedisPoolCbPlan") JedisPool jedisPool,
                               CbExtServerProperties serverProperties) {
        this.jedisPool = jedisPool;
        this.serverProperties = serverProperties;
    }

    public String getFromCache(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.select(serverProperties.getCbPlanRedisDbIndex());
            return jedis.get(key);
        } catch (Exception e) {
            log.error("CbPlanRedisCacheMgr.getFromCache: key={}", key, e);
            return null;
        }
    }

    public void putInCache(String key, String value, int ttlSeconds) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.select(serverProperties.getCbPlanRedisDbIndex());
            jedis.setex(key, ttlSeconds, value);
            log.debug("CbPlanRedisCacheMgr.putInCache: key={}, ttl={}s", key, ttlSeconds);
        } catch (Exception e) {
            log.error("CbPlanRedisCacheMgr.putInCache: key={}", key, e);
        }
    }

    public void deleteKeysByPatternAsync(String pattern) {
        if (!queuedInvalidationPatterns.add(pattern)) {
            log.debug("CbPlanRedisCacheMgr.deleteKeysByPatternAsync: coalescing pattern={}", pattern);
            return;
        }
        try {
            cacheInvalidationExecutor.execute(() -> {
                queuedInvalidationPatterns.remove(pattern);
                deleteKeysByPattern(pattern);
            });
        } catch (Exception e) {
            queuedInvalidationPatterns.remove(pattern);
            log.error("CbPlanRedisCacheMgr.deleteKeysByPatternAsync: failed to schedule pattern={}", pattern, e);
        }
    }

    private void deleteKeysByPattern(String pattern) {
        long startNanos = System.nanoTime();
        long deleted = 0;
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.select(serverProperties.getCbPlanRedisDbIndex());
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
            log.info("CbPlanRedisCacheMgr.deleteKeysByPattern: deleted={}, pattern={}, elapsed={}ms",
                    deleted, pattern, (System.nanoTime() - startNanos) / NANOS_PER_MILLIS);
        } catch (Exception e) {
            log.error("CbPlanRedisCacheMgr.deleteKeysByPattern: pattern={}", pattern, e);
        }
    }

    @PreDestroy
    void shutdownInvalidationExecutor() {
        cacheInvalidationExecutor.shutdown();
    }
}
