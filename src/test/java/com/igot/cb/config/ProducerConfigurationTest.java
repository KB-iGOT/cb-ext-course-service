package com.igot.cb.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ProducerConfigurationTest {

    private ProducerConfiguration config;

    @BeforeEach
    void setUp() throws Exception {
        config = new ProducerConfiguration();

        // Inject mock value into @Value field
        Field field = ProducerConfiguration.class.getDeclaredField("kafkabootstrapAddress");
        field.setAccessible(true);
        field.set(config, "localhost:9092");
    }

    @Test
    void testProducerFactory() {
        ProducerFactory<String, String> factory = config.producerFactory();

        assertNotNull(factory);
        assertTrue(factory instanceof DefaultKafkaProducerFactory);

        // Validate internal config map
        Map<String, Object> configMap = ((DefaultKafkaProducerFactory<String, String>) factory).getConfigurationProperties();

        assertEquals("localhost:9092", configMap.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(StringSerializer.class, configMap.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(StringSerializer.class, configMap.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }

    @Test
    void testKafkaTemplate() {
        KafkaTemplate<String, String> template = config.kafkaTemplate();
        assertNotNull(template);
    }
}