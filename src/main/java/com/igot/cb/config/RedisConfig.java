package com.igot.cb.config;

import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.igot.cb.cache.CbExtRedisCacheMgr;
import com.igot.cb.common.ServerProperties;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PropertiesCache;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Configuration
@EnableCaching
@Slf4j
public class RedisConfig {

    private static final String POOL2_REGISTER_MBEANS = "org.apache.commons.pool2.registerMbeans";

    private final PropertiesCache propertiesCache;
    private final ServerProperties serverProperties;
    private final CbExtServerProperties cbExtServerProperties;

    /**
     * Initialises the Redis configuration and disables commons-pool2 MBean registration,
     * which would otherwise clash when several Jedis pools are created in the same JVM.
     *
     * @param serverProperties      core properties supplying the shared Redis data endpoint
     * @param cbExtServerProperties CB Ext properties supplying per-cache host, port and db index
     */
    public RedisConfig(ServerProperties serverProperties, CbExtServerProperties cbExtServerProperties) {
        this.propertiesCache = PropertiesCache.getInstance();
        this.serverProperties = serverProperties;
        this.cbExtServerProperties = cbExtServerProperties;
        System.setProperty(POOL2_REGISTER_MBEANS, "false");
    }

    /**
     * Creates a JedisPool bean for Redis connection pooling.
     * It sets the pool configurations and connects to the Redis server using host and port from properties.
     *
     * @return JedisPool instance configured with Redis settings.
     */
    @Bean(name = "jedisPool")
    public JedisPool jedisPool() {
        JedisPoolConfig poolConfig = buildPoolConfig();
        return new JedisPool(poolConfig, propertiesCache.getProperty(Constants.REDIS_HOST),
                Integer.parseInt(propertiesCache.getProperty(Constants.REDIS_PORT)));
    }

    /**
     * Creates a JedisPool bean for Redis data connection pooling.
     * This bean connects to a separate Redis instance for data operations.
     *
     * @return JedisPool instance configured with Redis data settings.
     */
    @Bean(name = "jedisDataPool")
    public JedisPool jedisDataPool() {
        JedisPoolConfig poolConfig = buildPoolConfig();
        return new JedisPool(poolConfig, serverProperties.getRedisDataHost(),
                Integer.parseInt(serverProperties.getRedisDataPort()));
    }

    /**
     * Creates the connection pool for the CB Plan cache endpoint.
     *
     * @return JedisPool targeting the configured CB Plan Redis host and port
     */
    @Bean(name = "jedisPoolCbPlan")
    public JedisPool jedisPoolCbPlan() {
        JedisPoolConfig poolConfig = buildPoolConfig();
        log.info("RedisConfig: Creating jedisPoolCbPlan - host={}, port={}, dbIndex={}",
                cbExtServerProperties.getCbPlanRedisHost(), cbExtServerProperties.getCbPlanRedisPort(),
                cbExtServerProperties.getCbPlanRedisDbIndex());
        return new JedisPool(poolConfig, cbExtServerProperties.getCbPlanRedisHost(),
                cbExtServerProperties.getCbPlanRedisPort());
    }

    /**
     * Creates the connection pool for the user profile cache endpoint.
     *
     * @return JedisPool targeting the configured user profile Redis host and port
     */
    @Bean(name = "jedisPoolUserProfile")
    public JedisPool jedisPoolUserProfile() {
        JedisPoolConfig poolConfig = buildPoolConfig();
        log.info("RedisConfig: Creating jedisPoolUserProfile - host={}, port={}, dbIndex={}",
                cbExtServerProperties.getUserProfileRedisHost(), cbExtServerProperties.getUserProfileRedisPort(),
                cbExtServerProperties.getUserProfileRedisDbIndex());
        return new JedisPool(poolConfig, cbExtServerProperties.getUserProfileRedisHost(),
                cbExtServerProperties.getUserProfileRedisPort());
    }

    /**
     * Creates the connection pool for the external content cache endpoint.
     *
     * @return JedisPool targeting the configured external content Redis host and port
     */
    @Bean(name = "jedisPoolExtContent")
    public JedisPool jedisPoolExtContent() {
        JedisPoolConfig poolConfig = buildPoolConfig();
        log.info("RedisConfig: Creating jedisPoolExtContent - host={}, port={}, dbIndex={}",
                cbExtServerProperties.getExtContentRedisHost(), cbExtServerProperties.getExtContentRedisPort(),
                cbExtServerProperties.getExtContentRedisDbIndex());
        return new JedisPool(poolConfig, cbExtServerProperties.getExtContentRedisHost(),
                cbExtServerProperties.getExtContentRedisPort());
    }

    /**
     * Cache manager for user profile lookups.
     * Consumed through {@code @Qualifier("userProfileRedisCacheMgr")}.
     *
     * @return cache manager bound to the user profile pool and db index
     */
    @Bean(name = "userProfileRedisCacheMgr")
    public CbExtRedisCacheMgr userProfileRedisCacheMgr() {
        return new CbExtRedisCacheMgr(jedisPoolUserProfile(), cbExtServerProperties.getUserProfileRedisDbIndex());
    }

    /**
     * Cache manager for CB Plan dictionary and plan data.
     * Consumed through {@code @Qualifier("cbPlanRedisCacheMgr")}.
     *
     * @return cache manager bound to the CB Plan pool and db index
     */
    @Bean(name = "cbPlanRedisCacheMgr")
    public CbExtRedisCacheMgr cbPlanRedisCacheMgr() {
        return new CbExtRedisCacheMgr(jedisPoolCbPlan(), cbExtServerProperties.getCbPlanRedisDbIndex());
    }

    /**
     * Cache manager for external content metadata lookups.
     * Consumed through {@code @Qualifier("extContentRedisCacheMgr")}.
     *
     * @return cache manager bound to the external content pool and db index
     */
    @Bean(name = "extContentRedisCacheMgr")
    public CbExtRedisCacheMgr extContentRedisCacheMgr() {
        return new CbExtRedisCacheMgr(jedisPoolExtContent(), cbExtServerProperties.getExtContentRedisDbIndex());
    }

    private JedisPoolConfig buildPoolConfig() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(128);
        poolConfig.setMaxTotal(3000);
        poolConfig.setMinIdle(100);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleTimeMillis(120000);
        poolConfig.setTimeBetweenEvictionRunsMillis(30000);
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);
        return poolConfig;
    }
}
