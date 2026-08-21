package com.igot.cb.common;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
public class ServerProperties {

    @Value("${learning_service_vm_base_url}")
    private String learningServiceVmBaseUrl;

    @Value("${system.content.update.url}")
    private String systemUpdateAPI;

    @Value("${vod.bucket.prefix}")
    private String vodBucketPrefix;

    @Value("${vod.stream.url.prefix}")
    private String vodStreamUrlPrefix;

    @Value("${kafka.topic.competency-acquired}")
    private String competencyAcquiredTopicName;

    @Value("${user.competency.cache.ttl.seconds:3600}")
    private int userCompetencyCacheTtlSeconds;

    @Value("${spring.redis.data.host:localhost}")
    private String redisDataHost;

    @Value("${spring.redis.data.port:6379}")
    private String redisDataPort;

    @Value("${spring.redis.data.password:}")
    private String redisDataPassword; 

    @Value("${content.health.redis.db.index:12}")
    private int contentHealthDbIndex;
}
