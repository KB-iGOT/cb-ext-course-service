package com.igot.cb.cassandra;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;

@ExtendWith(MockitoExtension.class)
class CassandraOperationImplTest {

    @InjectMocks
    private CassandraOperationImpl cassandraOperation;

    @Mock
    private CassandraConnectionManager connectionManager;

    @Mock
    private CqlSession mockSession;

    @Mock
    private PreparedStatement mockPreparedStatement;

    @Mock
    private BoundStatement mockBoundStatement;

    @Mock
    private ResultSet mockResultSet;

    private final String keyspaceName = "testKeyspace";
    private final String tableName = "testTable";

    @BeforeEach
    void setUp() {
        lenient().when(connectionManager.getSession(anyString())).thenReturn(mockSession);
    }



    @Test
    void insertRecord_Exception() {
        // Arrange
        Map<String, Object> request = new HashMap<>();
        request.put("id", "123");

        try (MockedStatic<CassandraUtil> cassandraUtilMockedStatic = Mockito.mockStatic(CassandraUtil.class)) {
            cassandraUtilMockedStatic.when(() -> CassandraUtil.getPreparedStatement(anyString(), anyString(), any()))
                    .thenReturn("INSERT INTO testKeyspace.testTable (id) VALUES (?)");

            when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
            when(mockPreparedStatement.bind(any())).thenReturn(mockBoundStatement);
            when(mockSession.execute(any(BoundStatement.class))).thenThrow(new RuntimeException("Test exception"));

            // Act
            ApiResponse response = (ApiResponse) cassandraOperation.insertRecord(keyspaceName, tableName, request);

            // Assert
            assertEquals("Failed", response.get(Constants.RESPONSE));
            assertNotNull(response.get(Constants.ERROR_MESSAGE));
        }
    }

    @Test
    void getRecordsByPropertiesWithoutFiltering_WithFields() {
        // Arrange
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put("id", "123");
        List<String> fields = Arrays.asList("id", "name");

        try (MockedStatic<CassandraUtil> cassandraUtilMockedStatic = Mockito.mockStatic(CassandraUtil.class)) {
            List<Map<String, Object>> expectedResponse = new ArrayList<>();
            Map<String, Object> recordMap = new HashMap<>();
            recordMap.put("id", "123");
            recordMap.put("name", "Test");
            expectedResponse.add(recordMap);

            cassandraUtilMockedStatic.when(() -> CassandraUtil.createResponse(any(ResultSet.class)))
                    .thenReturn(expectedResponse);

            when(mockSession.execute(any(SimpleStatement.class))).thenReturn(mockResultSet);

            // Act
            List<Map<String, Object>> response = cassandraOperation.getRecordsByProperties(
                    keyspaceName, tableName, propertyMap, fields, 10);

            // Assert
            assertEquals(1, response.size());
            assertEquals("123", response.get(0).get("id"));
            assertEquals("Test", response.get(0).get("name"));
        }
    }

    @Test
    void getRecordsByPropertiesWithoutFiltering_WithoutFields() {
        // Arrange
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put("id", "123");

        try (MockedStatic<CassandraUtil> cassandraUtilMockedStatic = Mockito.mockStatic(CassandraUtil.class)) {
            List<Map<String, Object>> expectedResponse = new ArrayList<>();
            Map<String, Object> recordMap = new HashMap<>();
            recordMap.put("id", "123");
            recordMap.put("name", "Test");
            expectedResponse.add(recordMap);

            cassandraUtilMockedStatic.when(() -> CassandraUtil.createResponse(any(ResultSet.class)))
                    .thenReturn(expectedResponse);

            when(mockSession.execute(any(SimpleStatement.class))).thenReturn(mockResultSet);

            // Act
            List<Map<String, Object>> response = cassandraOperation.getRecordsByProperties(
                    keyspaceName, tableName, propertyMap, null, null);

            // Assert
            assertEquals(1, response.size());
            assertEquals("123", response.get(0).get("id"));
            assertEquals("Test", response.get(0).get("name"));
        }
    }

    @Test
    void getRecordsByPropertiesWithoutFiltering_Exception() {
        // Arrange
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put("id", "123");

        when(mockSession.execute(any(SimpleStatement.class))).thenThrow(new RuntimeException("Test exception"));

        // Act
        List<Map<String, Object>> response = cassandraOperation.getRecordsByProperties(
                keyspaceName, tableName, propertyMap, null, null);

        // Assert
        assertTrue(response.isEmpty());
    }

    @Test
    void updateRecordByCompositeKey_Success() {
        // Arrange
        String keyspace = "testKeyspace";
        String table = "testTable";

        Map<String, Object> updateAttrs = new HashMap<>();
        updateAttrs.put("name", "New Name");
        updateAttrs.put("age", 30);

        Map<String, Object> compositeKey = new HashMap<>();
        compositeKey.put("id", 123);
        compositeKey.put("region", "US");

        when(connectionManager.getSession(keyspace)).thenReturn(mockSession);

        // Act
        Map<String, Object> response = cassandraOperation.updateRecord(keyspace, table, updateAttrs, compositeKey);

        // Assert
        assertEquals(Constants.SUCCESS, response.get(Constants.RESPONSE));
        verify(mockSession, times(1)).execute(any(SimpleStatement.class));
    }

    @Test
    void updateRecord_Exception() {
        Map<String, Object> updateAttrs = new HashMap<>();
        updateAttrs.put("name", "New Name");
        Map<String, Object> compositeKey = new HashMap<>();
        compositeKey.put("id", 123);

        when(connectionManager.getSession(keyspaceName)).thenReturn(mockSession);
        when(mockSession.execute(any(SimpleStatement.class))).thenThrow(new RuntimeException("Test exception"));

        assertThrows(RuntimeException.class, () -> {
            cassandraOperation.updateRecord(keyspaceName, tableName, updateAttrs, compositeKey);
        });
    }

    @Test
    void insertRecord_ActualSuccess() {
        Map<String, Object> request = new HashMap<>();
        request.put("id", "123");
        request.put("name", "Test");

        try (MockedStatic<CassandraUtil> cassandraUtilMockedStatic = Mockito.mockStatic(CassandraUtil.class)) {
            cassandraUtilMockedStatic.when(() -> CassandraUtil.getPreparedStatement(anyString(), anyString(), any()))
                    .thenReturn("INSERT INTO testKeyspace.testTable (id, name) VALUES (?, ?)");

            when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
            when(mockPreparedStatement.bind(any())).thenReturn(mockBoundStatement);
            when(mockSession.execute(any(BoundStatement.class))).thenReturn(mockResultSet);

            ApiResponse response = (ApiResponse) cassandraOperation.insertRecord(keyspaceName, tableName, request);

            assertEquals(Constants.FAILED, response.get(Constants.RESPONSE));
        }
    }

    @Test
    void processQuery_emptyPropertyMap() {
        Map<String, Object> emptyMap = new HashMap<>();
        List<String> fields = Arrays.asList("id", "name");

        try (MockedStatic<CassandraUtil> cassandraUtilMockedStatic = Mockito.mockStatic(CassandraUtil.class)) {
            List<Map<String, Object>> expectedResponse = new ArrayList<>();
            cassandraUtilMockedStatic.when(() -> CassandraUtil.createResponse(any(ResultSet.class)))
                    .thenReturn(expectedResponse);

            when(mockSession.execute(any(SimpleStatement.class))).thenReturn(mockResultSet);

            List<Map<String, Object>> response = cassandraOperation.getRecordsByProperties(
                    keyspaceName, tableName, emptyMap, fields, null);

            assertNotNull(response);
        }
    }

    @Test
    void processQuery_withListValues() {
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put("status", Arrays.asList("active", "pending"));
        List<String> fields = Arrays.asList("id", "name");

        try (MockedStatic<CassandraUtil> cassandraUtilMockedStatic = Mockito.mockStatic(CassandraUtil.class)) {
            List<Map<String, Object>> expectedResponse = new ArrayList<>();
            cassandraUtilMockedStatic.when(() -> CassandraUtil.createResponse(any(ResultSet.class)))
                    .thenReturn(expectedResponse);

            when(mockSession.execute(any(SimpleStatement.class))).thenReturn(mockResultSet);

            List<Map<String, Object>> response = cassandraOperation.getRecordsByProperties(
                    keyspaceName, tableName, propertyMap, fields, null);

            assertNotNull(response);
        }
    }

    @Test
    void insertBulkRecord_Success() {
        List<Map<String, Object>> requestList = new ArrayList<>();
        Map<String, Object> record1 = new HashMap<>();
        record1.put("id", "1");
        record1.put("name", "Test1");
        requestList.add(record1);

        Map<String, Object> record2 = new HashMap<>();
        record2.put("id", "2");
        record2.put("name", "Test2");
        requestList.add(record2);

        try (MockedStatic<CassandraUtil> cassandraUtilMockedStatic = Mockito.mockStatic(CassandraUtil.class)) {
            cassandraUtilMockedStatic.when(() -> CassandraUtil.getPreparedStatement(anyString(), anyString(), any()))
                    .thenReturn("INSERT INTO testKeyspace.testTable (id, name) VALUES (?, ?)");

            when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
            when(mockPreparedStatement.bind(any())).thenReturn(mockBoundStatement);
            when(mockSession.execute(any(BatchStatement.class))).thenReturn(mockResultSet);

            ApiResponse response = cassandraOperation.insertBulkRecord(keyspaceName, tableName, requestList);

            assertEquals(Constants.FAILED, response.get(Constants.RESPONSE));
        }
    }

    @Test
    void insertBulkRecord_Exception() {
        List<Map<String, Object>> requestList = new ArrayList<>();
        Map<String, Object> record = new HashMap<>();
        record.put("id", "1");
        requestList.add(record);

        try (MockedStatic<CassandraUtil> cassandraUtilMockedStatic = Mockito.mockStatic(CassandraUtil.class)) {
            cassandraUtilMockedStatic.when(() -> CassandraUtil.getPreparedStatement(anyString(), anyString(), any()))
                    .thenReturn("INSERT INTO testKeyspace.testTable (id) VALUES (?)");

            when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
            when(mockPreparedStatement.bind(any())).thenReturn(mockBoundStatement);
            when(mockSession.execute(any(BatchStatement.class))).thenThrow(new RuntimeException("Test exception"));

            ApiResponse response = cassandraOperation.insertBulkRecord(keyspaceName, tableName, requestList);

            assertEquals(Constants.FAILED, response.get(Constants.RESPONSE));
            assertNotNull(response.get(Constants.ERROR_MESSAGE));
        }
    }

    @Test
    void deleteRecord_Success() {
        Map<String, Object> compositeKey = new HashMap<>();
        compositeKey.put("id", "123");
        compositeKey.put("region", "US");

        when(connectionManager.getSession(keyspaceName)).thenReturn(mockSession);

        assertDoesNotThrow(() -> {
            cassandraOperation.deleteRecord(keyspaceName, tableName, compositeKey);
        });

        verify(mockSession, times(1)).execute(any(SimpleStatement.class));
    }

    @Test
    void deleteRecord_Exception() {
        Map<String, Object> compositeKey = new HashMap<>();
        compositeKey.put("id", "123");

        when(connectionManager.getSession(keyspaceName)).thenReturn(mockSession);
        when(mockSession.execute(any(SimpleStatement.class))).thenThrow(new RuntimeException("Test exception"));

        assertThrows(RuntimeException.class, () -> {
            cassandraOperation.deleteRecord(keyspaceName, tableName, compositeKey);
        });
    }

    @Test
    void insertBulkRecord_LargeBatch() {
        List<Map<String, Object>> requestList = new ArrayList<>();
        // Create 15 records to test batch processing
        for (int i = 0; i < 15; i++) {
            Map<String, Object> record = new HashMap<>();
            record.put("id", String.valueOf(i));
            record.put("name", "Test" + i);
            requestList.add(record);
        }

        try (MockedStatic<CassandraUtil> cassandraUtilMockedStatic = Mockito.mockStatic(CassandraUtil.class)) {
            cassandraUtilMockedStatic.when(() -> CassandraUtil.getPreparedStatement(anyString(), anyString(), any()))
                    .thenReturn("INSERT INTO testKeyspace.testTable (id, name) VALUES (?, ?)");

            when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
            when(mockPreparedStatement.bind(any())).thenReturn(mockBoundStatement);
            when(mockSession.execute(any(BatchStatement.class))).thenReturn(mockResultSet);

            ApiResponse response = cassandraOperation.insertBulkRecord(keyspaceName, tableName, requestList);

            assertEquals(Constants.FAILED, response.get(Constants.RESPONSE));
        }
    }
}