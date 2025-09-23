package com.igot.cb.config;

import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class ConfigTest {

    @Mock
    private ConsumerConfiguration consumerConfiguration;

    @Mock
    private ProducerConfiguration producerConfiguration;

    @Mock
    private RedisConfig redisConfig;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    // @Test
    // void testConsumerConfiguration() {
    //     // Updated to mock a valid getBootstrapServers method
    //     when(consumerConfiguration.getBootstrapServers()).thenReturn("localhost:9092");
    //     String bootstrapServers = consumerConfiguration.getBootstrapServers();
    //     assertEquals("localhost:9092", bootstrapServers);
    // }

    // @Test
    // void testProducerConfiguration() {
    //     // Updated to mock a valid getBootstrapServers method
    //     when(producerConfiguration.getBootstrapServers()).thenReturn("localhost:9092");
    //     String bootstrapServers = producerConfiguration.getBootstrapServers();
    //     assertEquals("localhost:9092", bootstrapServers);
    // }

    // @Test
    // void testRedisConfig() {
    //     // Updated to mock a valid getHost method
    //     when(redisConfig.getHost()).thenReturn("localhost");
    //     String host = redisConfig.getHost();
    //     assertEquals("localhost", host);
    // }
}