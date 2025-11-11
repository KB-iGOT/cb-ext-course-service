package com.igot.cb.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.igot.cb.cache.IdMapCacheMgr;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.util.Constants;

@ExtendWith(MockitoExtension.class)
class UserProfileServiceImplTest {

    @Mock
    private RedisCacheMgr redisCacheMgr;

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private IdMapCacheMgr idMapCacheMgr;

    @InjectMocks
    private UserAndOrgServiceImpl userProfileService;

    private final String userId = "user123";

    @Test
    void testGetUserProfile_FromCache_Success() {
        // All keys lowercased to match service expectations
        String cachedJson = """
        {
            "id": "user123",
            "rootOrgId": "org1",
            "profileDetails": {
                "professionalDetails": [{"designation": "teacher", "group": "A"}],
                "profileStatus": "VERIFIED",
                "cadreDetails": {
                    "cadreName": "IAS",
                    "civilServiceName": "Administrative",
                    "cadreBatch": "2010",
                    "isOnCentralDeputation": true
                }
            }
        }
        """;

        when(redisCacheMgr.getFromCache(anyString())).thenReturn(cachedJson);

        final Map<String, Integer> capturedIdMap = new HashMap<>();
        when(idMapCacheMgr.getId(anyList())).thenAnswer(invocation -> {
            List<String> values = invocation.getArgument(0);
            int index = 1;
            for (String val : values) {
                capturedIdMap.put(val, index++);
            }
            return new HashMap<>(capturedIdMap);
        });

        Map<String, Integer> result = userProfileService.getUserProfile(userId);

        // ✅ Assert the expected 8 entries
        assertEquals(9, result.size());
        assertEquals(capturedIdMap.get("user123"), result.get("user"));
        assertEquals(capturedIdMap.get("IAS"), result.get("cadre"));
        assertEquals(capturedIdMap.get("Administrative"), result.get("service"));
        assertEquals(capturedIdMap.get("2010"), result.get("batch"));
        assertEquals(capturedIdMap.get("teacher"), result.get("designation"));
        assertEquals(capturedIdMap.get("A"), result.get("group"));
        assertEquals(capturedIdMap.get("VERIFIED"), result.get("profilestatus"));
        assertEquals(capturedIdMap.get("org1"), result.get("rootorgid"));
        assertEquals(capturedIdMap.get(true), result.get("isOnCentralDeputation"));
    }



    @Test
    void testGetUserProfile_FromCassandra_Success() {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER),
                any(),
                any(),
                isNull()))
                .thenReturn(List.of(Map.of(
                        "id", "user123",
                        "rootOrgId", "org1",
                        "profileDetails", Map.of(
                                "professionalDetails", List.of(Map.of("designation", "teacher", "group", "A")),
                                "profileStatus", "ACTIVE",
                                "designation","teacher",
                                "group", "A",
                                "cadreDetails", Map.of(
                                        "cadreName", "IAS",
                                        "civilServiceName", "Administrative",
                                        "cadreBatch", "2010",
                                        "isOnCentralDeputation", true
                                )
                        )
                )));

        final Map<String, Integer> capturedIdMap = new HashMap<>();
        when(idMapCacheMgr.getId(anyList())).thenAnswer(invocation -> {
            List<String> values = invocation.getArgument(0);
            int index = 1;
            for (String val : values) {
                capturedIdMap.put(val, index++);
            }
            return new HashMap<>(capturedIdMap);
        });

        Map<String, Integer> result = userProfileService.getUserProfile(userId);

        assertEquals(9, result.size());
        assertEquals(capturedIdMap.get("user123"), result.get("user"));
        assertEquals(capturedIdMap.get("IAS"), result.get("cadre"));
        assertEquals(capturedIdMap.get("Administrative"), result.get("service"));
        assertEquals(capturedIdMap.get("2010"), result.get("batch"));
        assertEquals(capturedIdMap.get("teacher"), result.get("designation"));
        assertEquals(capturedIdMap.get("A"), result.get("group"));
        assertEquals(capturedIdMap.get("ACTIVE"), result.get("profilestatus"));
        assertEquals(capturedIdMap.get("org1"), result.get("rootorgid"));
        assertEquals(capturedIdMap.get(true), result.get("isOnCentralDeputation"));
    }


    @Test
    void testGetUserProfile_InvalidCachedJson_ShouldReturnEmpty() {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn("not a json");

        Map<String, Integer> result = userProfileService.getUserProfile(userId);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetUserProfile_IdMapMismatch_ShouldReturnEmpty() {
        // Correct JSON matching service expectations (keys are case-sensitive)
        String cachedJson = """
        {
            "id": "user123",
            "rootOrgId": "org1",
            "profileDetails": {
                "professionalDetails": [{"designation": "teacher", "group": "A"}],
                "profileStatus": "ACTIVE",
                "cadreDetails": {
                    "cadreName": "IAS",
                    "civilServiceName": "Administrative",
                    "cadreBatch": "2010",
                    "isOnCentralDeputation": true
                }
            }
        }
        """;

        // Redis cache stub returns valid JSON
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(cachedJson);

        // Force ID map mismatch
        when(idMapCacheMgr.getId(anyList())).thenReturn(Map.of());

        // Call service
        Map<String, Integer> result = userProfileService.getUserProfile(userId);

        // Verify result is empty because of ID map mismatch
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetUserProfile_EmptyCassandraResponse() {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull())).thenReturn(List.of());

        Map<String, Integer> result = userProfileService.getUserProfile(userId);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetUserProfile_NullCadreDetails() {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull())).thenReturn(List.of(
                Map.of("id", "user123",
                        "rootOrgId", "org1",
                        "profileDetails", Map.of(
                                "professionalDetails", List.of(Map.of("designation", "teacher", "group", "A")),
                                "profileStatus", "ACTIVE"
                        ))));

        final Map<String, Integer> capturedIdMap = new HashMap<>();
        when(idMapCacheMgr.getId(anyList())).thenAnswer(invocation -> {
            List<String> values = invocation.getArgument(0);
            int index = 1;
            for (String val : values) {
                capturedIdMap.put(val, index++);
            }
            return new HashMap<>(capturedIdMap);
        });

        Map<String, Integer> result = userProfileService.getUserProfile(userId);

        assertEquals(5, result.size());
        assertEquals(capturedIdMap.get("user123"), result.get("user"));
        assertEquals(capturedIdMap.get("org1"), result.get("rootorgid"));
        assertEquals(capturedIdMap.get("ACTIVE"), result.get("profilestatus"));
        assertEquals(capturedIdMap.get("teacher"), result.get("designation"));
        assertEquals(capturedIdMap.get("A"), result.get("group"));
    }
    @Test
    void testReadUserProfileFromDB_Success() {
        Map<String, Object> dbUser = Map.of(Constants.ID, "user1", Constants.ROOT_ORG_ID, "org1");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenReturn(List.of(dbUser));

        Map<String, Object> result = userProfileService.readUserProfileFromDB("user1", null);

        assertEquals("user1", result.get(Constants.ID));
        assertEquals("org1", result.get(Constants.ROOT_ORG_ID));
    }

    @Test
    void testReadUserProfileFromDB_EmptyList() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenReturn(List.of());
        Map<String, Object> result = userProfileService.readUserProfileFromDB("noUser", null);
        assertTrue(result.isEmpty());
    }

    @Test
    void testReadUserProfileFromDB_ExceptionHandled() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenThrow(new RuntimeException("DB error"));
        Map<String, Object> result = userProfileService.readUserProfileFromDB("badUser", null);
        assertTrue(result.isEmpty());
    }

    @Test
    void testReadOrgFromDB_Success() {
        Map<String, Object> org = Map.of(Constants.ID, "org1", Constants.ORG_NAME, "OrgName");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenReturn(List.of(org));

        Map<String, Object> result = userProfileService.readOrgFromDB("org1", null);
        assertEquals("OrgName", result.get(Constants.ORG_NAME));
    }

    @Test
    void testReadOrgFromDB_EmptyList() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenReturn(List.of());
        Map<String, Object> result = userProfileService.readOrgFromDB("orgX", null);
        assertTrue(result.isEmpty());
    }

    @Test
    void testReadOrgFromDB_ExceptionHandled() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenThrow(new RuntimeException("fail"));
        Map<String, Object> result = userProfileService.readOrgFromDB("orgErr", null);
        assertTrue(result.isEmpty());
    }

    @Test
    void testReadUserProfile_FromCache_ValidJson() {
        String json = "{\"id\":\"user1\",\"rootOrgId\":\"org1\"}";
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(json);

        Map<String, Object> result = userProfileService.readUserProfile("user1", null);
        assertEquals("user1", result.get("id"));
        assertEquals("org1", result.get("rootOrgId"));
    }

    @Test
    void testSetUserProfile_EmptyProfile() throws Exception {
        Map<String, String> userProfile = new HashMap<>();
        Map<String, Object> userBasicProfile = new HashMap<>();
        var method = UserAndOrgServiceImpl.class.getDeclaredMethod(
                "setUserProfile", Map.class, Map.class);
        method.setAccessible(true);
        method.invoke(userProfileService, userProfile, userBasicProfile);
        assertTrue(userProfile.isEmpty());
    }

    @Test
    void testSetUserProfile_InvalidType_ThrowsException() throws Exception {
        Map<String, String> userProfile = new HashMap<>();
        Map<String, Object> userBasicProfile = Map.of(Constants.PROFILE_DETAILS, 12345);
        var method = UserAndOrgServiceImpl.class.getDeclaredMethod(
                "setUserProfile", Map.class, Map.class);
        method.setAccessible(true);
        assertThrows(Exception.class, () -> method.invoke(userProfileService, userProfile, userBasicProfile));
    }

    @Test
    void testGetUserBitMap_EmptyProfile() throws Exception {
        Map<String, String> profile = new HashMap<>();
        Map<String, Integer> bitmap = new HashMap<>();
        var method = UserAndOrgServiceImpl.class.getDeclaredMethod(
                "getUserBitMap", Map.class, Map.class);
        method.setAccessible(true);
        method.invoke(userProfileService, profile, bitmap);
        assertTrue(bitmap.isEmpty());
    }

    @Test
    void testGetUserBitMap_IdMapEmpty() throws Exception {
        Map<String, String> profile = Map.of(Constants.USER, "u1");
        Map<String, Integer> bitmap = new HashMap<>();
        when(idMapCacheMgr.getId(anyList())).thenReturn(Map.of());
        var method = UserAndOrgServiceImpl.class.getDeclaredMethod(
                "getUserBitMap", Map.class, Map.class);
        method.setAccessible(true);
        method.invoke(userProfileService, profile, bitmap);
        assertTrue(bitmap.isEmpty());
    }
}
