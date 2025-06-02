package com.igot.cb.transactional.util;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ApiResponseTest {

    @Test
    void testDefaultConstructor() {
        ApiResponse response = new ApiResponse();
        assertNotNull(response.getVer());
        assertNotNull(response.getTs());
        assertNotNull(response.getParams());
        assertNull(response.getId());
        assertNull(response.getResponseCode());
        assertNotNull(response.getResult());
    }

    @Test
    void testConstructorWithId() {
        ApiResponse response = new ApiResponse("testId");
        assertEquals("testId", response.getId());
        assertNotNull(response.getParams());
    }

    @Test
    void testSettersAndGetters() {
        ApiResponse response = new ApiResponse();
        response.setId("id1");
        response.setVer("v2");
        response.setTs("timestamp");
        ApiRespParam params = new ApiRespParam("paramId");
        response.setParams(params);
        response.setResponseCode(HttpStatus.OK);

        assertEquals("id1", response.getId());
        assertEquals("v2", response.getVer());
        assertEquals("timestamp", response.getTs());
        assertEquals(params, response.getParams());
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void testResultMapOperations() {
        ApiResponse response = new ApiResponse();
        response.put("key1", "value1");
        assertEquals("value1", response.get("key1"));
        assertTrue(response.containsKey("key1"));

        Map<String, Object> map = new HashMap<>();
        map.put("key2", 123);
        response.putAll(map);
        assertEquals(123, response.get("key2"));

        Map<String, Object> newMap = new HashMap<>();
        newMap.put("key3", "val3");
        response.setResult(newMap);
        assertEquals("val3", response.get("key3"));
        assertFalse(response.containsKey("key1")); // old map replaced
    }
}
