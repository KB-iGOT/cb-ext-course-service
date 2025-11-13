package com.igot.cb.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        String cachedJson = """
    {
        "id": "user123",
        "rootOrgId": "org1",
        "profiledetails": {
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
            Map<String, Integer> map = new HashMap<>();
            int index = 1;
            for (String val : values) {
                map.put(val.toLowerCase(), index++);
            }
            capturedIdMap.putAll(map);
            return map;
        });

        Map<String, Integer> result = userProfileService.getUserProfile(userId);

        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertTrue(result.size() >= 9);

        assertEquals(capturedIdMap.get("user123"), result.get("user"));
        assertEquals(capturedIdMap.get("ias"), result.get("cadre"));
        assertEquals(capturedIdMap.get("administrative"), result.get("service"));
        assertEquals(capturedIdMap.get("2010"), result.get("batch"));
        assertEquals(capturedIdMap.get("teacher"), result.get("designation"));
        assertEquals(capturedIdMap.get("a"), result.get("group"));
        assertEquals(capturedIdMap.get("verified"), result.get("profilestatus"));
        assertEquals(capturedIdMap.get("org1"), result.get("rootorgid"));
        assertEquals(capturedIdMap.get("true"), result.get("isoncentraldeputation"));
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
                        "profiledetails", Map.of(
                                "professionalDetails", List.of(Map.of("designation", "teacher", "group", "A")),
                                "profileStatus", "ACTIVE",
                                "designation", "teacher",
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
            Map<String, Integer> map = new HashMap<>();
            int index = 1;
            for (String val : values) {
                map.put(val.toLowerCase(), index++);
            }
            capturedIdMap.putAll(map);
            return map;
        });

        Map<String, Integer> result = userProfileService.getUserProfile(userId);

        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertTrue(result.size() >= 9);

        assertEquals(capturedIdMap.get("user123"), result.get("user"));
        assertEquals(capturedIdMap.get("ias"), result.get("cadre"));
        assertEquals(capturedIdMap.get("administrative"), result.get("service"));
        assertEquals(capturedIdMap.get("2010"), result.get("batch"));
        assertEquals(capturedIdMap.get("teacher"), result.get("designation"));
        assertEquals(capturedIdMap.get("a"), result.get("group"));
        assertEquals(capturedIdMap.get("active"), result.get("profilestatus"));
        assertEquals(capturedIdMap.get("org1"), result.get("rootorgid"));
        assertEquals(capturedIdMap.get("true"), result.get("isoncentraldeputation"));
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
            "profiledetails": {
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
    void testGetUserProfile_NullCadreDetails() throws Exception {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(List.of(
                        Map.of(
                                "id", "user123",
                                "rootOrgId", "org1",
                                "profiledetails", Map.of(
                                        "professionalDetails",
                                        List.of(Map.of("designation", "teacher", "group", "A")),
                                        "profileStatus", "ACTIVE"
                                )
                        )
                ));
        final Map<String, Integer> capturedIdMap = new HashMap<>();
        when(idMapCacheMgr.getId(anyList())).thenAnswer(invocation -> {
            List<String> values = invocation.getArgument(0);
            Map<String, Integer> result = new HashMap<>();
            int index = 1;
            for (String rawValue : values) {
                String encodedValue;
                try {
                    encodedValue = new URI(null, rawValue, null).toASCIIString();
                } catch (URISyntaxException e) {
                    encodedValue = rawValue;
                }
                result.put(encodedValue.toLowerCase(), index++);
            }
            capturedIdMap.putAll(result);
            return result;
        });
        Map<String, Integer> result = userProfileService.getUserProfile(userId);
        assertNotNull(result);
        assertEquals(5, result.size());
        assertEquals(capturedIdMap.get("user123"), result.get("user"));
        assertEquals(capturedIdMap.get("org1"), result.get("rootorgid"));
        assertEquals(capturedIdMap.get("active"), result.get("profilestatus"));
        assertEquals(capturedIdMap.get("teacher"), result.get("designation"));
        assertEquals(capturedIdMap.get("a"), result.get("group"));
    }


}
