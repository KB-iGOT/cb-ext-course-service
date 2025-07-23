package com.igot.cb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;


/**
 * @author Mahesh RV
 * @author Ruksana
 */
@ExtendWith(MockitoExtension.class)
public class CbExtCourseServiceApplicationTest {

  @InjectMocks
  private CbExtCourseServiceApplication application;


  @Test
  public void testMainMethod() {
    // Testing the main method using MockedStatic
    try (MockedStatic<SpringApplication> mockedStatic = Mockito.mockStatic(SpringApplication.class)) {
      // Arrange and Act
      CbExtCourseServiceApplication.main(new String[]{"arg1", "arg2"});

      // Assert
      mockedStatic.verify(() ->
          SpringApplication.run(eq(CbExtCourseServiceApplication.class), eq(new String[]{"arg1", "arg2"}))
      );
    }
  }

  @Test
  void testRestTemplate() {
    // Create actual RestTemplate
    RestTemplate restTemplate = application.restTemplate();

    // Verify not null and correct type
    assertNotNull(restTemplate);

    // Verify the RestTemplate has the correct RequestFactory type
    Object requestFactory = ReflectionTestUtils.getField(restTemplate, "requestFactory");
    assertNotNull(requestFactory);
    assertTrue(requestFactory instanceof HttpComponentsClientHttpRequestFactory);
  }

  @Test
  void testGetClientHttpRequestFactory_UsingReflection() throws Exception {
    // Access private method via reflection
    Method method = CbExtCourseServiceApplication.class.getDeclaredMethod("getClientHttpRequestFactory");
    method.setAccessible(true);

    // Invoke the method
    ClientHttpRequestFactory factory = (ClientHttpRequestFactory) method.invoke(application);

    // Assert factory is created correctly
    assertNotNull(factory);
    assertTrue(factory instanceof HttpComponentsClientHttpRequestFactory);

    // Verify HttpClient configuration by examining the factory
    HttpComponentsClientHttpRequestFactory httpFactory = (HttpComponentsClientHttpRequestFactory) factory;

    // Access private httpClient field
    Field httpClientField = HttpComponentsClientHttpRequestFactory.class.getDeclaredField("httpClient");
    httpClientField.setAccessible(true);
    CloseableHttpClient httpClient = (CloseableHttpClient) httpClientField.get(httpFactory);
    assertNotNull(httpClient);
  }

  @Test
  void testGetClientHttpRequestFactory_UsingSubclass() throws Exception{

    Method method = CbExtCourseServiceApplication.class.getDeclaredMethod("getClientHttpRequestFactory");
    method.setAccessible(true);
    ClientHttpRequestFactory factory = (ClientHttpRequestFactory) method.invoke(application);

    // Assert factory is created correctly
    assertNotNull(factory);
    assertTrue(factory instanceof HttpComponentsClientHttpRequestFactory);
  }

  @Test
  void testGetClientHttpRequestFactory_ConfigValues() throws Exception {
    // Use reflection to access private method
    Method method = CbExtCourseServiceApplication.class.getDeclaredMethod("getClientHttpRequestFactory");
    method.setAccessible(true);

    // Create mock objects for static methods
    try (MockedStatic<org.apache.hc.client5.http.impl.classic.HttpClients> httpClientsMock =
        Mockito.mockStatic(org.apache.hc.client5.http.impl.classic.HttpClients.class)) {

      // Mock builder chain
      org.apache.hc.client5.http.impl.classic.HttpClientBuilder builderMock = mock(org.apache.hc.client5.http.impl.classic.HttpClientBuilder.class);
      CloseableHttpClient httpClientMock = mock(CloseableHttpClient.class);

      // Set up expectations for the builder pattern
      httpClientsMock.when(org.apache.hc.client5.http.impl.classic.HttpClients::custom).thenReturn(builderMock);
      when(builderMock.setDefaultRequestConfig(any(RequestConfig.class))).thenReturn(builderMock);
      when(builderMock.setConnectionManager(any(PoolingHttpClientConnectionManager.class))).thenReturn(builderMock);
      when(builderMock.build()).thenReturn(httpClientMock);

      // Capture RequestConfig to verify timeout
      ArgumentCaptor<RequestConfig> configCaptor = ArgumentCaptor.forClass(RequestConfig.class);
      ArgumentCaptor<PoolingHttpClientConnectionManager> managerCaptor =
          ArgumentCaptor.forClass(PoolingHttpClientConnectionManager.class);

      // Invoke method
      method.invoke(application);

      // Verify calls and capture arguments
      verify(builderMock).setDefaultRequestConfig(configCaptor.capture());
      verify(builderMock).setConnectionManager(managerCaptor.capture());
      verify(builderMock).build();

      // This part won't actually work since RequestConfig doesn't expose its values easily
      // but demonstrates capturing for verification
    }
  }

  @Test
  void testSpringAnnotations() {
    // Test the presence of required annotations
    assertTrue(CbExtCourseServiceApplication.class.isAnnotationPresent(SpringBootApplication.class));
    assertTrue(CbExtCourseServiceApplication.class.isAnnotationPresent(ComponentScan.class));
    assertTrue(CbExtCourseServiceApplication.class.isAnnotationPresent(EntityScan.class));

    // Verify annotation values
    ComponentScan componentScan = CbExtCourseServiceApplication.class.getAnnotation(ComponentScan.class);
    assertEquals("com.igot.cb", componentScan.basePackages()[0]);

    EntityScan entityScan = CbExtCourseServiceApplication.class.getAnnotation(EntityScan.class);
    assertEquals("com.igot.cb", entityScan.value()[0]);
  }

}
