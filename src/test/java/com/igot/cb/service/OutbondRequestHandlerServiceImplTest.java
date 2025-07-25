package com.igot.cb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;

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
    void testFetchResult_success() {
        String uri = "http://mock-service/api/data";
        Map<String, Object> mockResponse = Map.of("key", "value");

        when(restTemplate.getForObject(uri, Map.class)).thenReturn(mockResponse);

        Object result = outboundService.fetchResult(uri);
        assertNotNull(result);
        assertTrue(result instanceof Map);
        assertEquals("value", ((Map<?, ?>) result).get("key"));
    }

    @Test
    void testFetchResult_httpClientError_withValidJsonBody() throws Exception {
        String uri = "http://mock-service/api/fail";
        Map<String, Object> errorMap = Map.of("error", "Bad Request");
        String errorJson = objectMapper.writeValueAsString(errorMap);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpClientErrorException exception = HttpClientErrorException.create(
                HttpStatus.BAD_REQUEST,
                "Bad Request",
                headers,
                errorJson.getBytes(StandardCharsets.UTF_8),
                StandardCharsets.UTF_8);

        when(restTemplate.getForObject(uri, Map.class)).thenThrow(exception);

        Object result = outboundService.fetchResult(uri);

        assertNotNull(result);
        assertTrue(result instanceof Map);
        assertEquals("Bad Request", ((Map<?, ?>) result).get("error"));
    }

    @Test
    void testFetchResult_httpClientError_withInvalidJson() {
        String uri = "http://mock-service/api/fail";
        String invalidJson = "<html>error</html>";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_HTML);

        HttpClientErrorException exception = HttpClientErrorException.create(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "Internal Server Error",
                headers,
                invalidJson.getBytes(StandardCharsets.UTF_8),
                StandardCharsets.UTF_8);

        when(restTemplate.getForObject(uri, Map.class)).thenThrow(exception);

        Object result = outboundService.fetchResult(uri);

        // Parsing failed — should return null
        assertNull(result);
    }

    @Test
    void testFetchResult_genericException() {
        String uri = "http://mock-service/api/exception";

        when(restTemplate.getForObject(uri, Map.class)).thenThrow(new RuntimeException("Unexpected"));

        Object result = outboundService.fetchResult(uri);

        // response object will remain null
        assertNull(result);
    }
}
