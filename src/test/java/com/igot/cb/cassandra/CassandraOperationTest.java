package com.igot.cb.cassandra;

import com.igot.cb.model.ApiResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CassandraOperationTest {

    @Mock
    private CassandraOperation cassandraOperation;

    @Test
    void testInsertRecord() {
        String keyspaceName = "testKeyspace";
        String tableName = "testTable";
        Map<String, Object> request = new HashMap<>();
        request.put("id", "123");

        Object expectedResult = new Object();
        when(cassandraOperation.insertRecord(keyspaceName, tableName, request)).thenReturn(expectedResult);

        Object result = cassandraOperation.insertRecord(keyspaceName, tableName, request);

        assertNotNull(result);
        assertEquals(expectedResult, result);
        verify(cassandraOperation).insertRecord(keyspaceName, tableName, request);
    }

    @Test
    void testGetRecordsByProperties() {
        String keyspaceName = "testKeyspace";
        String tableName = "testTable";
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put("status", "active");
        List<String> fields = Arrays.asList("id", "name");
        Integer limit = 10;

        List<Map<String, Object>> expectedResult = new ArrayList<>();
        when(cassandraOperation.getRecordsByProperties(keyspaceName, tableName, propertyMap, fields, limit))
                .thenReturn(expectedResult);

        List<Map<String, Object>> result = cassandraOperation.getRecordsByProperties(keyspaceName, tableName, propertyMap, fields, limit);

        assertNotNull(result);
        assertEquals(expectedResult, result);
        verify(cassandraOperation).getRecordsByProperties(keyspaceName, tableName, propertyMap, fields, limit);
    }

    @Test
    void testUpdateRecord() {
        String keyspaceName = "testKeyspace";
        String tableName = "testTable";
        Map<String, Object> updateAttributes = new HashMap<>();
        updateAttributes.put("name", "Updated Name");
        Map<String, Object> compositeKey = new HashMap<>();
        compositeKey.put("id", "123");

        Map<String, Object> expectedResult = new HashMap<>();
        when(cassandraOperation.updateRecord(keyspaceName, tableName, updateAttributes, compositeKey))
                .thenReturn(expectedResult);

        Map<String, Object> result = cassandraOperation.updateRecord(keyspaceName, tableName, updateAttributes, compositeKey);

        assertNotNull(result);
        assertEquals(expectedResult, result);
        verify(cassandraOperation).updateRecord(keyspaceName, tableName, updateAttributes, compositeKey);
    }

    @Test
    void testInsertBulkRecord() {
        String keyspaceName = "testKeyspace";
        String tableName = "testTable";
        List<Map<String, Object>> request = new ArrayList<>();
        Map<String, Object> record = new HashMap<>();
        record.put("id", "123");
        request.add(record);

        ApiResponse expectedResult = new ApiResponse();
        when(cassandraOperation.insertBulkRecord(keyspaceName, tableName, request)).thenReturn(expectedResult);

        ApiResponse result = cassandraOperation.insertBulkRecord(keyspaceName, tableName, request);

        assertNotNull(result);
        assertEquals(expectedResult, result);
        verify(cassandraOperation).insertBulkRecord(keyspaceName, tableName, request);
    }

    @Test
    void testDeleteRecord() {
        String keyspaceName = "testKeyspace";
        String tableName = "testTable";
        Map<String, Object> keyMap = new HashMap<>();
        keyMap.put("id", "123");

        doNothing().when(cassandraOperation).deleteRecord(keyspaceName, tableName, keyMap);

        assertDoesNotThrow(() -> cassandraOperation.deleteRecord(keyspaceName, tableName, keyMap));
        verify(cassandraOperation).deleteRecord(keyspaceName, tableName, keyMap);
    }
}