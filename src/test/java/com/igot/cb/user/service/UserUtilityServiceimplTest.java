package com.igot.cb.user.service;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

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
    void testGetUserDetailsFromDB_Success() {
        List<String> userIds = Arrays.asList("user1", "user2");
        List<String> fields = Arrays.asList("userId", "firstName", "email");
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();

        Map<String, Object> user1 = new HashMap<>();
        user1.put(Constants.USER_ID, "user1");
        user1.put("firstName", "John");
        user1.put("email", "encrypted_email");

        List<Map<String, Object>> userInfoList = Arrays.asList(user1);

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD), 
                eq(Constants.TABLE_USER), 
                any(Map.class), 
                eq(fields), 
                isNull()
        )).thenReturn(userInfoList);

        when(decryptService.decryptString("encrypted_email")).thenReturn("john@example.com");

        userUtilityService.getUserDetailsFromDB(userIds, fields, userInfoMap);

        verify(cassandraOperation).getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD), 
                eq(Constants.TABLE_USER), 
                any(Map.class), 
                eq(fields), 
                isNull()
        );
    }

    @Test
    void testGetUserDetailsFromDB_WithDecryptedFields() {
        List<String> userIds = Arrays.asList("user1");
        List<String> fields = Arrays.asList("userId", "email", "phone");
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();

        Map<String, Object> user1 = new HashMap<>();
        user1.put(Constants.USER_ID, "user1");
        user1.put("email", "encrypted_email");
        user1.put("phone", "encrypted_phone");

        List<Map<String, Object>> userInfoList = Arrays.asList(user1);

        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(userInfoList);
        when(decryptService.decryptString("encrypted_email")).thenReturn("john@example.com");
        when(decryptService.decryptString("encrypted_phone")).thenReturn("1234567890");

        userUtilityService.getUserDetailsFromDB(userIds, fields, userInfoMap);

        verify(decryptService, times(2)).decryptString(anyString());
    }

    @Test
    void testGetUserDetailsFromDB_WithBlankDecryptedValue() {
        List<String> userIds = Arrays.asList("user1");
        List<String> fields = Arrays.asList("userId", "email");
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();

        Map<String, Object> user1 = new HashMap<>();
        user1.put(Constants.USER_ID, "user1");
        user1.put("email", "encrypted_email");

        List<Map<String, Object>> userInfoList = Arrays.asList(user1);

        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(userInfoList);
        when(decryptService.decryptString("encrypted_email")).thenReturn("");

        userUtilityService.getUserDetailsFromDB(userIds, fields, userInfoMap);

        verify(decryptService).decryptString("encrypted_email");
    }

    @Test
    void testGetUserDetailsFromDB_WithNullDecryptedField() {
        List<String> userIds = Arrays.asList("user1");
        List<String> fields = Arrays.asList("userId", "email");
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();

        Map<String, Object> user1 = new HashMap<>();
        user1.put(Constants.USER_ID, "user1");
        user1.put("email", null);

        List<Map<String, Object>> userInfoList = Arrays.asList(user1);

        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(userInfoList);

        userUtilityService.getUserDetailsFromDB(userIds, fields, userInfoMap);

        verify(decryptService, never()).decryptString(anyString());
    }

    @Test
    void testGetUserDetailsFromDB_UserAlreadyExists() {
        List<String> userIds = Arrays.asList("user1");
        List<String> fields = Arrays.asList("userId", "firstName");
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();
        userInfoMap.put("user1", new HashMap<>());

        Map<String, Object> user1 = new HashMap<>();
        user1.put(Constants.USER_ID, "user1");
        user1.put("firstName", "John");

        List<Map<String, Object>> userInfoList = Arrays.asList(user1);

        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(userInfoList);

        userUtilityService.getUserDetailsFromDB(userIds, fields, userInfoMap);

        verify(cassandraOperation).getRecordsByProperties(any(), any(), any(), any(), any());
    }

    @Test
    void testGetUserDetailsFromDB_LargeUserList() {
        List<String> userIds = new ArrayList<>();
        for (int i = 1; i <= 25; i++) {
            userIds.add("user" + i);
        }
        List<String> fields = Arrays.asList("userId", "firstName");
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();

        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(new ArrayList<>());

        userUtilityService.getUserDetailsFromDB(userIds, fields, userInfoMap);

        verify(cassandraOperation, times(3)).getRecordsByProperties(any(), any(), any(), any(), any());
    }

    @Test
    void testGetUserDetailsFromDB_Exception() {
        List<String> userIds = Arrays.asList("user1");
        List<String> fields = Arrays.asList("userId", "firstName");
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();

        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenThrow(new RuntimeException("Database error"));

        userUtilityService.getUserDetailsFromDB(userIds, fields, userInfoMap);

        verify(cassandraOperation).getRecordsByProperties(any(), any(), any(), any(), any());
    }

    @Test
    void testGetUserDetailsFromDB_MissingField() {
        List<String> userIds = Arrays.asList("user1");
        List<String> fields = Arrays.asList("userId", "firstName", "email");
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();

        Map<String, Object> user1 = new HashMap<>();
        user1.put(Constants.USER_ID, "user1");
        user1.put("firstName", "John");

        List<Map<String, Object>> userInfoList = Arrays.asList(user1);

        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(userInfoList);

        userUtilityService.getUserDetailsFromDB(userIds, fields, userInfoMap);

        verify(cassandraOperation).getRecordsByProperties(any(), any(), any(), any(), any());
    }
}