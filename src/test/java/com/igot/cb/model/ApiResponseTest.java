package com.igot.cb.model;

import com.igot.cb.util.Constants;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ApiResponseTest {

    @Test
    void testDefaultConstructor() {
        ApiResponse response = new ApiResponse();
        
        assertNotNull(response.getVer());
        assertNotNull(response.getTs());
        assertNotNull(response.getParams());
        assertNotNull(response.getResult());
    }

    @Test
    void testConstructorWithId() {
        String id = "test.api";
        ApiResponse response = new ApiResponse(id);
        
        assertEquals(id, response.getId());
        assertNotNull(response.getVer());
        assertNotNull(response.getTs());
        assertNotNull(response.getParams());
    }

    @Test
    void testSettersAndGetters() {
        ApiResponse response = new ApiResponse();
        
        response.setId("test.id");
        response.setVer("v2");
        response.setTs("2023-01-01");
        response.setResponseCode(HttpStatus.OK);
        
        assertEquals("test.id", response.getId());
        assertEquals("v2", response.getVer());
        assertEquals("2023-01-01", response.getTs());
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void testResultOperations() {
        ApiResponse response = new ApiResponse();
        
        response.put("key1", "value1");
        response.put("key2", 123);
        
        assertEquals("value1", response.get("key1"));
        assertEquals(123, response.get("key2"));
        assertTrue(response.containsKey("key1"));
        assertFalse(response.containsKey("nonexistent"));
    }

    @Test
    void testPutAll() {
        ApiResponse response = new ApiResponse();
        Map<String, Object> data = new HashMap<>();
        data.put("name", "test");
        data.put("count", 5);
        
        response.putAll(data);
        
        assertEquals("test", response.get("name"));
        assertEquals(5, response.get("count"));
    }

    @Test
    void testSetResult() {
        ApiResponse response = new ApiResponse();
        Map<String, Object> result = new HashMap<>();
        result.put("data", "test");
        
        response.setResult(result);
        
        assertEquals(result, response.getResult());
        assertEquals("test", response.get("data"));
    }

    @Test
    void testCreateDefaultResponse() {
        String api = "test.api";
        ApiResponse response = ApiResponse.createDefaultResponse(api);
        
        assertEquals(api, response.getId());
        assertEquals(Constants.API_VERSION_1, response.getVer());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertNotNull(response.getTs());
    }

    @Test
    void testUpdateErrorDetails() {
        ApiResponse response = new ApiResponse();
        String errorMsg = "Test error";
        HttpStatus errorCode = HttpStatus.BAD_REQUEST;
        
        response.updateErrorDetails(errorMsg, errorCode);
        
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(errorMsg, response.getParams().getErrMsg());
        assertEquals(errorCode, response.getResponseCode());
    }
}