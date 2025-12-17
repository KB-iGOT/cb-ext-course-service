package com.igot.cb;


import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.util.Timeout;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.client.RestTemplate;


/**
 * @author Mahesh RV
 * @author Ruksana
 */
@ComponentScan(basePackages = "com.igot.cb")
@EntityScan("com.igot.cb")
@SpringBootApplication
@EnableScheduling
public class CbExtCourseServiceApplication {


	public static void main(String[] args) {
		SpringApplication.run(CbExtCourseServiceApplication.class, args);
	}

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate(getClientHttpRequestFactory());
    }

    private ClientHttpRequestFactory getClientHttpRequestFactory() {

      // Get the PropertiesCache instance to fetch the properties

        int timeout = 45000;

      // Configure the RequestConfig with timeouts
      RequestConfig config = RequestConfig.custom()
          .setResponseTimeout(Timeout.ofMilliseconds(timeout))
          .build();
      PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
      cm.setMaxTotal(2000);
      cm.setDefaultMaxPerRoute(500);
      CloseableHttpClient client = HttpClients.custom()
          .setDefaultRequestConfig(config)
          .setConnectionManager(cm)
          .build();
      return new HttpComponentsClientHttpRequestFactory(client);
    }

}
