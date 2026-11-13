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
        ElasticsearchClient client = new ElasticsearchClient(elasticsearchTransport);
        return client;
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
                .map(String::trim)
                .map(entry -> {
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

                        try {
                            port = Integer.parseInt(entry.substring(idx + 1).trim());
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException("Invalid port in Elasticsearch host entry: " + entry, e);
                        }
                    }

                    if (StringUtils.isBlank(hostname) || !isValidHost(hostname)) {
                        throw new IllegalArgumentException("Invalid Elasticsearch host entry: " + entry);
                    }

                    validatePort(port);

                    return new HttpHost(hostname, port, "http");
                })
                .toArray(HttpHost[]::new);

        if (httpHosts.length == 0) {
            throw new IllegalArgumentException("Elasticsearch host is not configured");
        }
        return httpHosts;
    }

    private static boolean isValidHost(String host) {
        if (host.matches("\\d+(\\.\\d+)*")) {
            return isValidIPv4(host);
        }

        return host.length() <= 253
                && Arrays.stream(host.split("\\.", -1))
                .allMatch(label -> label.matches("[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?"));
    }

    private static boolean isValidIPv4(String host) {
        String[] parts = host.split("\\.", -1);

        if (parts.length != 4) {
            return false;
        }

        for (String part : parts) {
            if (!part.matches("\\d{1,3}")) {
                return false;
            }

            int octet = Integer.parseInt(part);

            if (octet < 0 || octet > 255) {
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