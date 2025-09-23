package com.igot.cb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboundRequestHandlerServiceImplTest {

    @Mock
    private RestTemplate restTemplate;

    @InjectMocks
    private OutboundRequestHandlerServiceImpl outboundService;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setup() {
        objectMapper = new ObjectMapper();
    }

    @Test
    void testConstructor() {
        RestTemplate mockRestTemplate = mock(RestTemplate.class);
        OutboundRequestHandlerServiceImpl service = new OutboundRequestHandlerServiceImpl(mockRestTemplate);
        assertNotNull(service);
    }

    @Test
    void testFetchResult_Success() {
        String uri = "http://test.com/api";
        Map<String, Object> mockResponse = Map.of("key", "value");

        when(restTemplate.getForObject(uri, Map.class)).thenReturn(mockResponse);

        Object result = outboundService.fetchResult(uri);
        
        assertNotNull(result);
        assertEquals(mockResponse, result);
    }

    @Test
    void testFetchResult_HttpClientError_ValidJson() throws Exception {
        String uri = "http://test.com/api";
        Map<String, Object> errorMap = Map.of("error", "Bad Request");
        String errorJson = objectMapper.writeValueAsString(errorMap);

        HttpClientErrorException exception = HttpClientErrorException.create(
                HttpStatus.BAD_REQUEST, "Bad Request", new HttpHeaders(),
                errorJson.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);

        when(restTemplate.getForObject(uri, Map.class)).thenThrow(exception);

        Object result = outboundService.fetchResult(uri);

        assertNotNull(result);
        assertEquals("Bad Request", ((Map<?, ?>) result).get("error"));
    }

    @Test
    void testFetchResult_HttpClientError_InvalidJson() {
        String uri = "http://test.com/api";
        String invalidJson = "<html>error</html>";

        HttpClientErrorException exception = HttpClientErrorException.create(
                HttpStatus.INTERNAL_SERVER_ERROR, "Server Error", new HttpHeaders(),
                invalidJson.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);

        when(restTemplate.getForObject(uri, Map.class)).thenThrow(exception);

        Object result = outboundService.fetchResult(uri);

        assertNull(result);
    }

    @Test
    void testFetchResult_GenericException() {
        String uri = "http://test.com/api";

        when(restTemplate.getForObject(uri, Map.class)).thenThrow(new RuntimeException("Connection error"));

        Object result = outboundService.fetchResult(uri);

        assertNull(result);
    }

    @Test
    void testFetchResultUsingExchange_Success() {
        String uri = "http://test.com/api";
        Map<String, Object> mockResponse = Map.of("key", "value");
        ParameterizedTypeReference<Map<String, Object>> typeRef = 
            new ParameterizedTypeReference<Map<String, Object>>() {};
        
        ResponseEntity<Map<String, Object>> responseEntity = 
            new ResponseEntity<>(mockResponse, HttpStatus.OK);
        
        when(restTemplate.exchange(eq(uri), eq(HttpMethod.GET), isNull(), eq(typeRef)))
            .thenReturn(responseEntity);

        Map<String, Object> result = outboundService.fetchResultUsingExchange(uri, typeRef);
        
        assertNotNull(result);
        assertEquals("value", result.get("key"));
    }

    @Test
    void testFetchResultUsingExchange_HttpClientError_ValidJson() throws Exception {
        String uri = "http://test.com/api";
        Map<String, Object> errorMap = Map.of("error", "Bad Request");
        String errorJson = objectMapper.writeValueAsString(errorMap);
        ParameterizedTypeReference<Map<String, Object>> typeRef = 
            new ParameterizedTypeReference<Map<String, Object>>() {};

        HttpClientErrorException exception = HttpClientErrorException.create(
                HttpStatus.BAD_REQUEST, "Bad Request", new HttpHeaders(),
                errorJson.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);

        when(restTemplate.exchange(eq(uri), eq(HttpMethod.GET), isNull(), eq(typeRef)))
            .thenThrow(exception);

        Map<String, Object> result = outboundService.fetchResultUsingExchange(uri, typeRef);

        assertNull(result);
    }

    @Test
    void testFetchResultUsingExchange_HttpClientError_InvalidJson() {
        String uri = "http://test.com/api";
        String invalidJson = "<html>error</html>";
        ParameterizedTypeReference<Map<String, Object>> typeRef = 
            new ParameterizedTypeReference<Map<String, Object>>() {};

        HttpClientErrorException exception = HttpClientErrorException.create(
                HttpStatus.INTERNAL_SERVER_ERROR, "Server Error", new HttpHeaders(),
                invalidJson.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);

        when(restTemplate.exchange(eq(uri), eq(HttpMethod.GET), isNull(), eq(typeRef)))
            .thenThrow(exception);

        Map<String, Object> result = outboundService.fetchResultUsingExchange(uri, typeRef);

        assertNull(result);
    }

    @Test
    void testFetchResultUsingExchange_GenericException() {
        String uri = "http://test.com/api";
        ParameterizedTypeReference<Map<String, Object>> typeRef = 
            new ParameterizedTypeReference<Map<String, Object>>() {};

        when(restTemplate.exchange(eq(uri), eq(HttpMethod.GET), isNull(), eq(typeRef)))
            .thenThrow(new RuntimeException("Connection error"));

        Map<String, Object> result = outboundService.fetchResultUsingExchange(uri, typeRef);

        assertNull(result);
    }

    @Test
    void testFetchResult_GenericException_WithNonNullResponse() {
        String uri = "http://test.com/api";
        
        when(restTemplate.getForObject(uri, Map.class)).thenAnswer(invocation -> {
            throw new RuntimeException("Connection error");
        });

        Object result = outboundService.fetchResult(uri);

        assertNull(result);
    }

    @Test
    void testFetchResultUsingExchange_GenericException_WithNonNullResponse() {
        String uri = "http://test.com/api";
        ParameterizedTypeReference<Map<String, Object>> typeRef = 
            new ParameterizedTypeReference<Map<String, Object>>() {};

        when(restTemplate.exchange(eq(uri), eq(HttpMethod.GET), isNull(), eq(typeRef)))
            .thenAnswer(invocation -> {
                throw new RuntimeException("Connection error");
            });

        Map<String, Object> result = outboundService.fetchResultUsingExchange(uri, typeRef);

        assertNull(result);
    }

    @Test
    void testFetchResult_WithDebugEnabled() {
        String uri = "http://test.com/api";
        Map<String, Object> mockResponse = Map.of("key", "value");

        // Enable debug logging
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) 
            org.slf4j.LoggerFactory.getLogger(OutboundRequestHandlerServiceImpl.class);
        ch.qos.logback.classic.Level originalLevel = logger.getLevel();
        logger.setLevel(ch.qos.logback.classic.Level.DEBUG);

        try {
            when(restTemplate.getForObject(uri, Map.class)).thenReturn(mockResponse);
            Object result = outboundService.fetchResult(uri);
            assertNotNull(result);
        } finally {
            logger.setLevel(originalLevel);
        }
    }

    @Test
    void testFetchResultUsingExchange_WithDebugEnabled() {
        String uri = "http://test.com/api";
        Map<String, Object> mockResponse = Map.of("key", "value");
        ParameterizedTypeReference<Map<String, Object>> typeRef = 
            new ParameterizedTypeReference<Map<String, Object>>() {};
        
        ResponseEntity<Map<String, Object>> responseEntity = 
            new ResponseEntity<>(mockResponse, HttpStatus.OK);

        // Enable debug logging
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) 
            org.slf4j.LoggerFactory.getLogger(OutboundRequestHandlerServiceImpl.class);
        ch.qos.logback.classic.Level originalLevel = logger.getLevel();
        logger.setLevel(ch.qos.logback.classic.Level.DEBUG);

        try {
            when(restTemplate.exchange(eq(uri), eq(HttpMethod.GET), isNull(), eq(typeRef)))
                .thenReturn(responseEntity);
            Map<String, Object> result = outboundService.fetchResultUsingExchange(uri, typeRef);
            assertNotNull(result);
        } finally {
            logger.setLevel(originalLevel);
        }
    }
}