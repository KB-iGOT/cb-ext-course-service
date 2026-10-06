package com.igot.cb.config;

import com.igot.cb.util.CbExtServerProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Configuration
@Slf4j
public class CbPlanRedisConfig {

    private final CbExtServerProperties serverProperties;

    public CbPlanRedisConfig(CbExtServerProperties serverProperties) {
        this.serverProperties = serverProperties;
    }

    @Bean(name = "jedisPoolCbPlan")
    public JedisPool jedisPoolCbPlan() {
        System.setProperty("org.apache.commons.pool2.registerMbeans", "false");
        JedisPoolConfig poolConfig = buildPoolConfig();
        log.info("CbPlanRedisConfig: Creating jedisPoolCbPlan - host={}, port={}, dbIndex={}",
                serverProperties.getCbPlanRedisHost(), serverProperties.getCbPlanRedisPort(),
                serverProperties.getCbPlanRedisDbIndex());
        return new JedisPool(poolConfig, serverProperties.getCbPlanRedisHost(),
                serverProperties.getCbPlanRedisPort());
    }

    private JedisPoolConfig buildPoolConfig() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(128);
        poolConfig.setMaxTotal(3000);
        poolConfig.setMinIdle(100);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);
        return poolConfig;
    }
}
