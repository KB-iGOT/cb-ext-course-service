package com.igot.cb.elasticsearch.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.apache.http.HttpHost;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class EsConfigTest {

    @Test
    void testElasticsearchClientCreation() {
        EsConfig esConfig = new EsConfig();
        ReflectionTestUtils.setField(esConfig, "elasticsearchHost", "localhost");
        ReflectionTestUtils.setField(esConfig, "elasticsearchPort", 9200);
        ReflectionTestUtils.setField(esConfig, "elasticsearchUsername", "elastic");
        ReflectionTestUtils.setField(esConfig, "elasticsearchPassword", "password");

        ElasticsearchClient client = esConfig.elasticsearchClient();
        
        assertNotNull(client);
    }

    @Test
    void testParseHostsWithMultipleCommaSeparatedIps() {
        HttpHost[] hosts = EsConfig.parseHosts(" 10.0.0.1, 10.0.0.2:9201 ,,10.0.0.3", 9200);

        assertEquals(3, hosts.length);
        assertEquals("10.0.0.1", hosts[0].getHostName());
        assertEquals(9200, hosts[0].getPort());
        assertEquals("10.0.0.2", hosts[1].getHostName());
        assertEquals(9201, hosts[1].getPort());
        assertEquals("10.0.0.3", hosts[2].getHostName());
        assertEquals(9200, hosts[2].getPort());
    }

    @Test
    void testParseHostsBlankThrows() {
        assertThrows(IllegalArgumentException.class, () -> EsConfig.parseHosts(" , ", 9200));
        assertThrows(IllegalArgumentException.class, () -> EsConfig.parseHosts(null, 9200));
    }

    @Test
    void testClientCreationWithMultipleHosts() {
        EsConfig esConfig = new EsConfig();
        ReflectionTestUtils.setField(esConfig, "orgEligibilityElasticsearchHost", "10.0.0.1,10.0.0.2");
        ReflectionTestUtils.setField(esConfig, "orgEligibilityElasticsearchPort", 9200);
        ReflectionTestUtils.setField(esConfig, "orgEligibilityElasticsearchUsername", "elastic");
        ReflectionTestUtils.setField(esConfig, "orgEligibilityElasticsearchPassword", "password");

        assertNotNull(esConfig.orgEligibilityElasticsearchClient());
    }
}
