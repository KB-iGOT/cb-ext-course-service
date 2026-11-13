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
        HttpHost[] httpHosts = Arrays.stream(hosts.split(","))
                .map(String::trim)
                .filter(StringUtils::isNotBlank)
                .map(entry -> {
                    int idx = entry.lastIndexOf(':');
                    if (idx > 0 && idx < entry.length() - 1) {
                        return new HttpHost(entry.substring(0, idx).trim(),
                                Integer.parseInt(entry.substring(idx + 1).trim()), "http");
                    }
                    return new HttpHost(entry, defaultPort, "http");
                })
                .toArray(HttpHost[]::new);
        if (httpHosts.length == 0) {
            throw new IllegalArgumentException("Elasticsearch host is not configured");
        }
        return httpHosts;
    }
}
