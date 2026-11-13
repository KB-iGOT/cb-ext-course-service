package com.igot.cb.elasticsearch.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.apache.http.HttpHost;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class EsConfigTest {

    private static final int DEFAULT_PORT = 9200;

    // ---------- client creation ----------

    @Test
    void testElasticsearchClientCreation() {
        EsConfig esConfig = new EsConfig();
        ReflectionTestUtils.setField(esConfig, "elasticsearchHost", "localhost");
        ReflectionTestUtils.setField(esConfig, "elasticsearchPort", DEFAULT_PORT);
        ReflectionTestUtils.setField(esConfig, "elasticsearchUsername", "elastic");
        ReflectionTestUtils.setField(esConfig, "elasticsearchPassword", "password");

        ElasticsearchClient client = esConfig.elasticsearchClient();

        assertNotNull(client);
    }

    @Test
    void testElasticsearchClientCreationWithMultipleHosts() {
        EsConfig esConfig = new EsConfig();
        ReflectionTestUtils.setField(esConfig, "elasticsearchHost", "10.0.0.1,10.0.0.2:9201,10.0.0.3");
        ReflectionTestUtils.setField(esConfig, "elasticsearchPort", DEFAULT_PORT);
        ReflectionTestUtils.setField(esConfig, "elasticsearchUsername", "elastic");
        ReflectionTestUtils.setField(esConfig, "elasticsearchPassword", "password");

        assertNotNull(esConfig.elasticsearchClient());
    }

    @Test
    void testOrgEligibilityClientCreationWithMultipleHosts() {
        EsConfig esConfig = new EsConfig();
        ReflectionTestUtils.setField(esConfig, "orgEligibilityElasticsearchHost", "10.0.0.1,10.0.0.2");
        ReflectionTestUtils.setField(esConfig, "orgEligibilityElasticsearchPort", DEFAULT_PORT);
        ReflectionTestUtils.setField(esConfig, "orgEligibilityElasticsearchUsername", "elastic");
        ReflectionTestUtils.setField(esConfig, "orgEligibilityElasticsearchPassword", "password");

        assertNotNull(esConfig.orgEligibilityElasticsearchClient());
    }

    @Test
    void testClientCreationWithInvalidHostFailsFast() {
        EsConfig esConfig = new EsConfig();
        ReflectionTestUtils.setField(esConfig, "elasticsearchHost", "10.0.0.1,,10.0.0.2");
        ReflectionTestUtils.setField(esConfig, "elasticsearchPort", DEFAULT_PORT);
        ReflectionTestUtils.setField(esConfig, "elasticsearchUsername", "elastic");
        ReflectionTestUtils.setField(esConfig, "elasticsearchPassword", "password");

        assertThrows(IllegalArgumentException.class, esConfig::elasticsearchClient);
    }

    // ---------- parseHosts: valid input ----------

    @Test
    void testParseHostsSingleHostUsesDefaultPort() {
        HttpHost[] hosts = EsConfig.parseHosts("localhost", DEFAULT_PORT);

        assertEquals(1, hosts.length);
        assertHost(hosts[0], "localhost", DEFAULT_PORT);
    }

    @Test
    void testParseHostsSingleHostWithExplicitPort() {
        HttpHost[] hosts = EsConfig.parseHosts("10.0.0.1:9300", DEFAULT_PORT);

        assertEquals(1, hosts.length);
        assertHost(hosts[0], "10.0.0.1", 9300);
    }

    @Test
    void testParseHostsWithMultipleCommaSeparatedIps() {
        HttpHost[] hosts = EsConfig.parseHosts("10.0.0.1,10.0.0.2:9201,10.0.0.3", DEFAULT_PORT);

        assertEquals(3, hosts.length);
        assertHost(hosts[0], "10.0.0.1", DEFAULT_PORT);
        assertHost(hosts[1], "10.0.0.2", 9201);
        assertHost(hosts[2], "10.0.0.3", DEFAULT_PORT);
    }

    @Test
    void testParseHostsTrimsWhitespaceAroundEntriesAndPorts() {
        HttpHost[] hosts = EsConfig.parseHosts("  10.0.0.1 ,\t10.0.0.2 : 9201  ", DEFAULT_PORT);

        assertEquals(2, hosts.length);
        assertHost(hosts[0], "10.0.0.1", DEFAULT_PORT);
        assertHost(hosts[1], "10.0.0.2", 9201);
    }

    @Test
    void testParseHostsSupportsHostnames() {
        HttpHost[] hosts = EsConfig.parseHosts("es-node-1.internal,es-node-2.internal:9201", DEFAULT_PORT);

        assertEquals(2, hosts.length);
        assertHost(hosts[0], "es-node-1.internal", DEFAULT_PORT);
        assertHost(hosts[1], "es-node-2.internal", 9201);
    }

    @Test
    void testParseHostsPreservesOrder() {
        HttpHost[] hosts = EsConfig.parseHosts("c,a,b", DEFAULT_PORT);

        assertEquals("c", hosts[0].getHostName());
        assertEquals("a", hosts[1].getHostName());
        assertEquals("b", hosts[2].getHostName());
    }

    @Test
    void testParseHostsAcceptsPortBoundaries() {
        HttpHost[] hosts = EsConfig.parseHosts("h1:1,h2:65535", DEFAULT_PORT);

        assertHost(hosts[0], "h1", 1);
        assertHost(hosts[1], "h2", 65535);
    }

    // ---------- parseHosts: invalid input ----------

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   ", "\t"})
    void testParseHostsNullOrBlankThrows(String input) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> EsConfig.parseHosts(input, DEFAULT_PORT));
        assertEquals("Elasticsearch host is not configured", ex.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            ",",
            " , ",
            "10.0.0.1,,10.0.0.2",
            "10.0.0.1,",
            ",10.0.0.1",
            "10.0.0.1, ,10.0.0.2"
    })
    void testParseHostsEmptyEntryThrows(String input) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> EsConfig.parseHosts(input, DEFAULT_PORT));
        assertTrue(ex.getMessage().startsWith("Invalid Elasticsearch host entry"));
    }

    @ParameterizedTest
    @ValueSource(strings = {":9200", "10.0.0.1:", " :9200"})
    void testParseHostsMissingHostOrPortAroundColonThrows(String input) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> EsConfig.parseHosts(input, DEFAULT_PORT));
        assertTrue(ex.getMessage().startsWith("Invalid Elasticsearch host entry"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"10.0.0.1:92a0", "10.0.0.1:abc", "10.0.0.1:9200.5", "http://10.0.0.1"})
    void testParseHostsNonNumericPortThrows(String input) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> EsConfig.parseHosts(input, DEFAULT_PORT));
        assertEquals("Invalid port in Elasticsearch host entry: " + input, ex.getMessage());
        assertInstanceOf(NumberFormatException.class, ex.getCause());
    }

    @ParameterizedTest
    @ValueSource(strings = {"10.0.0.1:0", "10.0.0.1:65536", "10.0.0.1:-1"})
    void testParseHostsOutOfRangePortThrows(String input) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> EsConfig.parseHosts(input, DEFAULT_PORT));
        assertTrue(ex.getMessage().startsWith("Invalid Elasticsearch port"));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, 65536})
    void testParseHostsInvalidDefaultPortThrows(int defaultPort) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> EsConfig.parseHosts("10.0.0.1", defaultPort));
        assertEquals("Invalid Elasticsearch port: " + defaultPort, ex.getMessage());
    }

    // ---------- parseHosts: host validation (IPv4 / hostname) ----------

    @ParameterizedTest
    @ValueSource(strings = {
            "0.0.0.0",
            "255.255.255.255",
            "192.168.1.10",
            "localhost",
            "es-node-1",
            "es-node-1.internal",
            "ES.Example.COM",
            "a",
            "10.0.0.1a"
    })
    void testParseHostsAcceptsValidHosts(String host) {
        HttpHost[] hosts = EsConfig.parseHosts(host, DEFAULT_PORT);

        assertEquals(1, hosts.length);
        assertHost(hosts[0], host, DEFAULT_PORT);
    }

    @Test
    void testParseHostsAcceptsMaxLengthLabel() {
        String label = "a".repeat(63);

        HttpHost[] hosts = EsConfig.parseHosts(label + ".com", DEFAULT_PORT);

        assertEquals(label + ".com", hosts[0].getHostName());
    }

    @Test
    void testParseHostsAcceptsMaxLengthHostname() {
        // 4 labels of 61 chars + 3 dots + "a.b" style tail => exactly 253 chars
        String host = String.join(".", "a".repeat(61), "b".repeat(61), "c".repeat(61), "d".repeat(61), "e".repeat(5));
        assertEquals(253, host.length());

        HttpHost[] hosts = EsConfig.parseHosts(host, DEFAULT_PORT);

        assertEquals(host, hosts[0].getHostName());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "256.0.0.1",
            "10.0.0.256",
            "999.999.999.999",
            "10.0.0",
            "10.0.0.1.5",
            "1234.0.0.1",
            "12345",
            "10..0.1",
            "10.0.0.",
            "\u0661\u0660.0.0.1"
    })
    void testParseHostsInvalidIpv4Throws(String host) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> EsConfig.parseHosts(host, DEFAULT_PORT));
        assertEquals("Invalid Elasticsearch host entry: " + host, ex.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "-bad.host",
            "bad-.host",
            "bad..host",
            "host.",
            ".host",
            "under_score.host",
            "bad host",
            "host!name",
            "host~name",
            "h\u00e9.com",
            "::1",
            "[::1]:9200"
    })
    void testParseHostsInvalidHostnameThrows(String entry) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> EsConfig.parseHosts(entry, DEFAULT_PORT));
        assertEquals("Invalid Elasticsearch host entry: " + entry, ex.getMessage());
    }

    @Test
    void testParseHostsUnicodeWhitespaceHostBeforePortThrows() {
        // String.trim() keeps U+2003 (em space) but StringUtils.isBlank treats it as blank
        String entry = "\u2003:9200";

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> EsConfig.parseHosts(entry, DEFAULT_PORT));
        assertEquals("Invalid Elasticsearch host entry: " + entry, ex.getMessage());
    }

    @Test
    void testParseHostsLabelLongerThan63CharsThrows() {
        String host = "a".repeat(64) + ".com";

        assertThrows(IllegalArgumentException.class, () -> EsConfig.parseHosts(host, DEFAULT_PORT));
    }

    @Test
    void testParseHostsHostnameLongerThan253CharsThrows() {
        String host = String.join(".", "a".repeat(61), "b".repeat(61), "c".repeat(61), "d".repeat(61), "e".repeat(6));
        assertEquals(254, host.length());

        assertThrows(IllegalArgumentException.class, () -> EsConfig.parseHosts(host, DEFAULT_PORT));
    }

    @Test
    void testParseHostsInvalidHostWithValidPortThrows() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> EsConfig.parseHosts("300.0.0.1:9200", DEFAULT_PORT));
        assertEquals("Invalid Elasticsearch host entry: 300.0.0.1:9200", ex.getMessage());
    }

    @Test
    void testParseHostsInvalidHostInListFailsWholeList() {
        assertThrows(IllegalArgumentException.class,
                () -> EsConfig.parseHosts("10.0.0.1,bad_host,10.0.0.3", DEFAULT_PORT));
    }

    @Test
    void testParseHostsOneBadEntryFailsWholeList() {
        assertThrows(IllegalArgumentException.class,
                () -> EsConfig.parseHosts("10.0.0.1,10.0.0.2:bad,10.0.0.3", DEFAULT_PORT));
    }

    private static void assertHost(HttpHost host, String expectedName, int expectedPort) {
        assertEquals(expectedName, host.getHostName());
        assertEquals(expectedPort, host.getPort());
        assertEquals("http", host.getSchemeName());
    }
}
