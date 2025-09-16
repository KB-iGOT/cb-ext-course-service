package com.igot.cb.user.service;

import com.igot.cb.cassandra.CassandraOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class UserUtilityServiceimplTest {

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private DecryptServiceImpl decryptService;

    private UserUtilityServiceimpl userUtilityService;

    @BeforeEach
    void setUp() {
        userUtilityService = new UserUtilityServiceimpl(cassandraOperation);
        userUtilityService.decryptService = decryptService;
    }

    @Test
    void testGetUserDetailsFromDBSuccess() {
        List<String> userIds = Arrays.asList("user1", "user2");
        List<String> fields = Arrays.asList("id", "firstName", "email");
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();

        List<Map<String, Object>> mockUserData = new ArrayList<>();
        Map<String, Object> user1 = new HashMap<>();
        user1.put("userId", "user1");
        user1.put("id", "user1");
        user1.put("firstName", "John");
        user1.put("email", "encrypted_email");
        mockUserData.add(user1);

        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), isNull()))
                .thenReturn(mockUserData);
        when(decryptService.decryptString("encrypted_email")).thenReturn("john@example.com");

        userUtilityService.getUserDetailsFromDB(userIds, fields, userInfoMap);

        assertEquals(1, userInfoMap.size());
        assertTrue(userInfoMap.containsKey("user1"));
        assertEquals("John", userInfoMap.get("user1").get("firstName"));
        assertEquals("john@example.com", userInfoMap.get("user1").get("email"));
    }

    @Test
    void testGetUserDetailsFromDBWithLargeUserList() {
        List<String> userIds = new ArrayList<>();
        for (int i = 1; i <= 25; i++) {
            userIds.add("user" + i);
        }
        List<String> fields = Arrays.asList("id", "firstName");
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();

        List<Map<String, Object>> mockUserData = new ArrayList<>();
        Map<String, Object> user = new HashMap<>();
        user.put("userId", "user1");
        user.put("id", "user1");
        user.put("firstName", "John");
        mockUserData.add(user);

        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), isNull()))
                .thenReturn(mockUserData);

        userUtilityService.getUserDetailsFromDB(userIds, fields, userInfoMap);

        verify(cassandraOperation, times(3)).getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), isNull());
    }

    @Test
    void testGetUserDetailsFromDBException() {
        List<String> userIds = Arrays.asList("user1");
        List<String> fields = Arrays.asList("id", "firstName");
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();

        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), isNull()))
                .thenThrow(new RuntimeException("Database error"));

        userUtilityService.getUserDetailsFromDB(userIds, fields, userInfoMap);

        assertTrue(userInfoMap.isEmpty());
    }

    @Test
    void testGetUserDetailsFromDBWithExistingUser() {
        List<String> userIds = Arrays.asList("user1");
        List<String> fields = Arrays.asList("id", "firstName");
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();
        
        Map<String, String> existingUser = new HashMap<>();
        existingUser.put("firstName", "Existing");
        userInfoMap.put("user1", existingUser);

        List<Map<String, Object>> mockUserData = new ArrayList<>();
        Map<String, Object> user = new HashMap<>();
        user.put("userId", "user1");
        user.put("firstName", "John");
        mockUserData.add(user);

        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), isNull()))
                .thenReturn(mockUserData);

        userUtilityService.getUserDetailsFromDB(userIds, fields, userInfoMap);

        assertEquals("Existing", userInfoMap.get("user1").get("firstName"));
    }
}