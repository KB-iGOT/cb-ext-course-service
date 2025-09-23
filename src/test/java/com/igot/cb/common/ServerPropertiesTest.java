package com.igot.cb.common;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = ServerProperties.class)
@TestPropertySource(properties = {
    "learning_service_vm_base_url=http://test-learning-service",
    "system.content.update.url=/api/system/v1/content/update/",
    "vod.bucket.prefix=test-bucket-prefix",
    "vod.stream.url.prefix=test-stream-prefix"
})
class ServerPropertiesTest {

    @Autowired
    private ServerProperties serverProperties;

    @Test
    void testLearningServiceVmBaseUrl() {
        assertEquals("http://test-learning-service", serverProperties.getLearningServiceVmBaseUrl());
    }

    @Test
    void testSystemUpdateAPI() {
        assertEquals("/api/system/v1/content/update/", serverProperties.getSystemUpdateAPI());
    }

    @Test
    void testVodBucketPrefix() {
        assertEquals("test-bucket-prefix", serverProperties.getVodBucketPrefix());
    }

    @Test
    void testVodStreamUrlPrefix() {
        assertEquals("test-stream-prefix", serverProperties.getVodStreamUrlPrefix());
    }

    @Test
    void testSettersAndGetters() {
        ServerProperties props = new ServerProperties();
        
        props.setLearningServiceVmBaseUrl("http://new-service");
        props.setSystemUpdateAPI("/new/api/");
        props.setVodBucketPrefix("new-bucket");
        props.setVodStreamUrlPrefix("new-stream");
        
        assertEquals("http://new-service", props.getLearningServiceVmBaseUrl());
        assertEquals("/new/api/", props.getSystemUpdateAPI());
        assertEquals("new-bucket", props.getVodBucketPrefix());
        assertEquals("new-stream", props.getVodStreamUrlPrefix());
    }
}