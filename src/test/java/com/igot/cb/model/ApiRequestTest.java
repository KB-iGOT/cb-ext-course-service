package com.igot.cb.model;

import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class ApiRequestTest {

    @Test
    void testGettersAndSetters() {
        ApiRequest apiRequest = new ApiRequest();
        
        Map<String, Object> request = new HashMap<>();
        request.put("key", "value");
        
        apiRequest.setRequest(request);
        
        assertEquals(request, apiRequest.getRequest());
    }

    @Test
    void testDefaultValue() {
        ApiRequest apiRequest = new ApiRequest();
        assertNull(apiRequest.getRequest());
    }

    @Test
    void testWithStringRequest() {
        ApiRequest apiRequest = new ApiRequest();
        String request = "test request";
        
        apiRequest.setRequest(request);
        
        assertEquals(request, apiRequest.getRequest());
    }

    @Test
    void testWithNullRequest() {
        ApiRequest apiRequest = new ApiRequest();
        
        apiRequest.setRequest(null);
        
        assertNull(apiRequest.getRequest());
    }
}