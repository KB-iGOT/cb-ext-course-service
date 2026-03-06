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
}
