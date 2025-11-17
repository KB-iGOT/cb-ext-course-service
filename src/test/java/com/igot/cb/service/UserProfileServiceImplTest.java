package com.igot.cb.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
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
    void testGetUserProfile_FromCassandra_Success() {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);

        Map<String, Object> cadreDetails = new HashMap<>();
        cadreDetails.put("cadreName", "IAS");
        cadreDetails.put("civilServiceName", "Administrative");
        cadreDetails.put("cadreBatch", "2010");
        cadreDetails.put("isOnCentralDeputation", true);

        Map<String, Object> professional = new HashMap<>();
        professional.put("designation", "teacher");
        professional.put("group", "A");

        Map<String, Object> profileDetails = new HashMap<>();
        profileDetails.put("professionalDetails", List.of(professional));
        profileDetails.put("profileStatus", "ACTIVE");
        profileDetails.put("designation", "teacher");
        profileDetails.put("group", "A");
        profileDetails.put("cadreDetails", cadreDetails);

        Map<String, Object> cassandraRecord = new HashMap<>();
        cassandraRecord.put("id", "user123");
        cassandraRecord.put("rootOrgId", "org1");
        cassandraRecord.put("profiledetails", profileDetails);

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER),
                any(),
                any(),
                isNull()))
                .thenReturn(List.of(cassandraRecord));

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
    void testGetUserProfile_EmptyCassandraResponse() {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull())).thenReturn(List.of());

        Map<String, Integer> result = userProfileService.getUserProfile(userId);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetUserProfile_NullCadreDetails() throws Exception {
        when(redisCacheMgr.getFromCache(anyString())).thenReturn(null);
        Map<String, Object> professionalDetails = new HashMap<>();
        professionalDetails.put("designation", "teacher");
        professionalDetails.put("group", "A");
        Map<String, Object> profileDetails = new HashMap<>();
        profileDetails.put("professionalDetails", List.of(professionalDetails));
        profileDetails.put("profileStatus", "ACTIVE");
        Map<String, Object> cassandraRecord = new HashMap<>();
        cassandraRecord.put("id", "user123");
        cassandraRecord.put("rootOrgId", "org1");
        cassandraRecord.put("profiledetails", profileDetails);
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), isNull()))
                .thenReturn(List.of(cassandraRecord));

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
