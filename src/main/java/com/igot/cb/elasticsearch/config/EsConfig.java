package com.igot.cb.elasticsearch.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

@Configuration
@Slf4j
public class EsConfig {

    @Value("${elasticsearch.host}")
    private String elasticsearchHost;

    @Value("${elasticsearch.port}")
    private int elasticsearchPort;

    @Value("${elasticsearch.username}")
    private String elasticsearchUsername;

    @Value("${elasticsearch.password}")
    private String elasticsearchPassword;

    @Value("${org.eligibility.elasticsearch.host}")
    private String orgEligibilityElasticsearchHost;

    @Value("${org.eligibility.elasticsearch.port}")
    private int orgEligibilityElasticsearchPort;

    @Value("${org.eligibility.elasticsearch.username}")
    private String orgEligibilityElasticsearchUsername;

    @Value("${org.eligibility.elasticsearch.password}")
    private String orgEligibilityElasticsearchPassword;

    @Bean(name = "elasticsearchClient")
    public ElasticsearchClient elasticsearchClient() {
        return createClient(elasticsearchHost, elasticsearchPort, elasticsearchUsername, elasticsearchPassword);
    }

    /**
     * The org_eligibility_alias index lives on a separate ES cluster from cb_plan_v2 in some
     * environments, so it gets its own client rather than sharing "elasticsearchClient".
     */
    @Bean(name = "orgEligibilityElasticsearchClient")
    public ElasticsearchClient orgEligibilityElasticsearchClient() {
        return createClient(orgEligibilityElasticsearchHost, orgEligibilityElasticsearchPort,
                orgEligibilityElasticsearchUsername, orgEligibilityElasticsearchPassword);
    }

    private ElasticsearchClient createClient(String host, int port, String username, String password) {
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(parseHosts(host, port))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).addInterceptorLast((HttpResponseInterceptor) (response, context) ->
                        response.addHeader("X-Elastic-Product", "Elasticsearch")))
                .setDefaultHeaders(new org.apache.http.Header[]{
                        new org.apache.http.message.BasicHeader("Content-Type", "application/json"),
                        new org.apache.http.message.BasicHeader("X-Elastic-Product", "Elasticsearch")});
        RestClient restClient = builder.build();
        ElasticsearchTransport elasticsearchTransport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(elasticsearchTransport);
    }

    /**
     * Parses a comma separated list of hosts (e.g. "10.0.0.1,10.0.0.2:9201") into HttpHost entries.
     * Entries without an explicit port fall back to the configured default port.
     */
    static HttpHost[] parseHosts(String hosts, int defaultPort) {
        if (StringUtils.isBlank(hosts)) {
            throw new IllegalArgumentException("Elasticsearch host is not configured");
        }

        validatePort(defaultPort);

        HttpHost[] httpHosts = Arrays.stream(hosts.split(",", -1))
                .map(entry -> parseHostEntry(entry, defaultPort))
                .toArray(HttpHost[]::new);

        if (httpHosts.length == 0) {
            throw new IllegalArgumentException("Elasticsearch host is not configured");
        }
        return httpHosts;
    }

    private static HttpHost parseHostEntry(String entry, int defaultPort) {
        entry = entry.trim();

        if (StringUtils.isBlank(entry)) {
            throw new IllegalArgumentException("Invalid Elasticsearch host entry: " + entry);
        }

        int idx = entry.lastIndexOf(':');
        String hostname = entry;
        int port = defaultPort;

        if (idx >= 0) {
            if (idx == 0 || idx == entry.length() - 1) {
                throw new IllegalArgumentException("Invalid Elasticsearch host entry: " + entry);
            }

            hostname = entry.substring(0, idx).trim();
            port = parsePort(entry.substring(idx + 1).trim(), entry);
        }

        if (StringUtils.isBlank(hostname) || !isValidHost(hostname)) {
            throw new IllegalArgumentException("Invalid Elasticsearch host entry: " + entry);
        }

        validatePort(port);

        return new HttpHost(hostname, port, "http");
    }

    private static int parsePort(String portValue, String entry) {
        try {
            return Integer.parseInt(portValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port in Elasticsearch host entry: " + entry, e);
        }
    }

    private static boolean isValidHost(String host) {
        if (isNumericAddress(host)) {
            return isValidIPv4(host);
        }

        if (host.length() > 253) {
            return false;
        }

        String[] labels = host.split("\\.", -1);

        for (String label : labels) {
            if (!isValidHostnameLabel(label)) {
                return false;
            }
        }

        return true;
    }

    private static boolean isNumericAddress(String host) {
        for (int i = 0; i < host.length(); i++) {
            char ch = host.charAt(i);

            if (!Character.isDigit(ch) && ch != '.') {
                return false;
            }
        }

        return true;
    }

    private static boolean isValidHostnameLabel(String label) {
        if (label.isEmpty() || label.length() > 63) {
            return false;
        }

        if (!isAlphaNumeric(label.charAt(0))
                || !isAlphaNumeric(label.charAt(label.length() - 1))) {
            return false;
        }

        for (int i = 0; i < label.length(); i++) {
            char ch = label.charAt(i);

            if (!isAlphaNumeric(ch) && ch != '-') {
                return false;
            }
        }

        return true;
    }

    private static boolean isAlphaNumeric(char ch) {
        return (ch >= 'a' && ch <= 'z')
                || (ch >= 'A' && ch <= 'Z')
                || (ch >= '0' && ch <= '9');
    }

    private static boolean isValidIPv4(String host) {
        String[] parts = host.split("\\.", -1);

        if (parts.length != 4) {
            return false;
        }

        for (String part : parts) {
            if (part.isEmpty() || part.length() > 3) {
                return false;
            }

            int octet = 0;

            for (int i = 0; i < part.length(); i++) {
                char ch = part.charAt(i);

                if (ch < '0' || ch > '9') {
                    return false;
                }

                octet = octet * 10 + (ch - '0');
            }

            if (octet > 255) {
                return false;
            }
        }

        return true;
    }

    private static void validatePort(int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Invalid Elasticsearch port: " + port);
        }
    }
}