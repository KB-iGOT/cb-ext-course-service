package com.igot.cb.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.igot.cb.cassandra.CassandraPropertyReader;
import com.igot.cb.cassandra.CassandraUtil;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class CassandraUtilTest {

    private ResultSet mockResultSet;
    private Row mockRow;
    private ColumnDefinitions mockColumnDefinitions;
    private CassandraPropertyReader mockReader;

    @BeforeEach
    void setUp() {
        mockResultSet = mock(ResultSet.class);
        mockRow = mock(Row.class);
        mockColumnDefinitions = mock(ColumnDefinitions.class);
        mockReader = mock(CassandraPropertyReader.class);
    }

    @Test
    void testGetPreparedStatement() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("id", 1);
        data.put("name", "Mahesh");

        String actual = CassandraUtil.getPreparedStatement("test_keyspace", "test_table", data);
        String expected = "INSERT INTO test_keyspace.test_table(id,name) VALUES (?,?);";
        assertEquals(expected, actual);
    }

    @Test
    void testCreateResponseList() {
        when(mockResultSet.getColumnDefinitions()).thenReturn(mockColumnDefinitions);

        try (MockedStatic<CassandraPropertyReader> staticMock = mockStatic(CassandraPropertyReader.class)) {
            staticMock.when(CassandraPropertyReader::getInstance).thenReturn(mockReader);
            when(mockReader.readProperty("id")).thenReturn("id");

            when(mockResultSet.iterator()).thenReturn(List.of(mockRow).iterator());
            when(mockRow.getObject("id")).thenReturn("123");

            List<Map<String, Object>> result = CassandraUtil.createResponse(mockResultSet);
            assertEquals(1, result.size());
        }
    }

    @Test
    void testCreateResponseMap() {
        when(mockResultSet.getColumnDefinitions()).thenReturn(mockColumnDefinitions);

        try (MockedStatic<CassandraPropertyReader> staticMock = mockStatic(CassandraPropertyReader.class)) {
            staticMock.when(CassandraPropertyReader::getInstance).thenReturn(mockReader);
            when(mockReader.readProperty("id")).thenReturn("id");

            when(mockResultSet.iterator()).thenReturn(List.of(mockRow).iterator());
            when(mockRow.getObject("id")).thenReturn("123");

            Map<String, Object> result = CassandraUtil.createResponse(mockResultSet, "id");
            assertEquals(1, result.size());
        }
    }

    @Test
    void testPrivateConstructor() throws Exception {
        var constructor = CassandraUtil.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        CassandraUtil instance = constructor.newInstance();
        assertNotNull(instance);
    }
}

