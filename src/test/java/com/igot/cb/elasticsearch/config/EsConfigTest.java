package com.igot.cb.elasticsearch.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.apache.http.HttpHost;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Method;

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
    void testOrgEligibilityElasticsearchClientCreation_singleHost() {
        EsConfig esConfig = new EsConfig();
        ReflectionTestUtils.setField(esConfig, "orgEligibilityElasticsearchHost", "localhost");
        ReflectionTestUtils.setField(esConfig, "orgEligibilityElasticsearchPort", 9201);
        ReflectionTestUtils.setField(esConfig, "orgEligibilityElasticsearchUsername", "elastic");
        ReflectionTestUtils.setField(esConfig, "orgEligibilityElasticsearchPassword", "password");

        ElasticsearchClient client = esConfig.orgEligibilityElasticsearchClient();

        assertNotNull(client);
    }

    @Test
    void testOrgEligibilityElasticsearchClientCreation_multipleHosts() {
        EsConfig esConfig = new EsConfig();
        // prod-like config: 3 comma-separated ES nodes configured on one property
        ReflectionTestUtils.setField(esConfig, "orgEligibilityElasticsearchHost",
                "es-node-1.internal,es-node-2.internal,es-node-3.internal");
        ReflectionTestUtils.setField(esConfig, "orgEligibilityElasticsearchPort", 9201);
        ReflectionTestUtils.setField(esConfig, "orgEligibilityElasticsearchUsername", "elastic");
        ReflectionTestUtils.setField(esConfig, "orgEligibilityElasticsearchPassword", "password");

        ElasticsearchClient client = esConfig.orgEligibilityElasticsearchClient();

        assertNotNull(client);
    }

    @Test
    void testBuildHttpHosts_singleHost() throws Exception {
        HttpHost[] hosts = invokeBuildHttpHosts(new EsConfig(), "localhost", 9201);

        assertNotNull(hosts);
        assertEquals(1, hosts.length);
        assertEquals("localhost", hosts[0].getHostName());
        assertEquals(9201, hosts[0].getPort());
        assertEquals("http", hosts[0].getSchemeName());
    }

    @Test
    void testBuildHttpHosts_multipleCommaSeparatedHosts() throws Exception {
        HttpHost[] hosts = invokeBuildHttpHosts(new EsConfig(),
                "es-node-1.internal,es-node-2.internal,es-node-3.internal", 9201);

        assertNotNull(hosts);
        assertEquals(3, hosts.length);
        assertEquals("es-node-1.internal", hosts[0].getHostName());
        assertEquals("es-node-2.internal", hosts[1].getHostName());
        assertEquals("es-node-3.internal", hosts[2].getHostName());
        for (HttpHost host : hosts) {
            assertEquals(9201, host.getPort());
            assertEquals("http", host.getSchemeName());
        }
    }

    @Test
    void testBuildHttpHosts_trimsWhitespaceAndSkipsBlankEntries() throws Exception {
        HttpHost[] hosts = invokeBuildHttpHosts(new EsConfig(),
                " es-node-1.internal , ,es-node-2.internal,", 9201);

        assertNotNull(hosts);
        assertEquals(2, hosts.length);
        assertEquals("es-node-1.internal", hosts[0].getHostName());
        assertEquals("es-node-2.internal", hosts[1].getHostName());
    }

    private HttpHost[] invokeBuildHttpHosts(EsConfig esConfig, String host, int port) throws Exception {
        Method method = EsConfig.class.getDeclaredMethod("buildHttpHosts", String.class, int.class);
        method.setAccessible(true);
        return (HttpHost[]) method.invoke(esConfig, host, port);
    }
}