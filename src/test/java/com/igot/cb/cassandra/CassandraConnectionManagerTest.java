package com.igot.cb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CassandraConnectionManagerTest {

    @Mock
    private CassandraConnectionManager connectionManager;

    @Mock
    private CqlSession mockSession;

    @Test
    void testGetSession() {
        String keyspaceName = "testKeyspace";
        when(connectionManager.getSession(keyspaceName)).thenReturn(mockSession);

        CqlSession result = connectionManager.getSession(keyspaceName);

        assertNotNull(result);
        assertEquals(mockSession, result);
        verify(connectionManager).getSession(keyspaceName);
    }
}