package com.igot.cb.service;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CbPlanLearnerServiceImplTest {

    @Mock(lenient = true)
    private AccessTokenValidator accessTokenValidator;

    @Mock(lenient = true)
    private CassandraOperation cassandraOperation;

    @Mock(lenient = true)
    private ContentInfoServiceImpl contentService;



    private CbPlanLearnerServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new CbPlanLearnerServiceImpl(accessTokenValidator, cassandraOperation);
        ReflectionTestUtils.setField(service, "contentService", contentService);
    }

    @Test
    void testGetCBPlanListForUser_Success() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user123");

        Map<String, Object> userData = new HashMap<>();
        userData.put(Constants.ID, "user123");
        userData.put("rootorgid", "org123");
        userData.put("profiledetails", "{\"professionalDetails\":[{\"designation\":\"Test\",\"group\":\"TestGroup\"}],\"profileStatus\":\"VERIFIED\",\"cadreDetails\":{\"cadreName\":\"TestCadre\",\"civilServiceName\":\"TestService\",\"cadreBatch\":2020}}");

        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER), any(), any(), any()))
                .thenReturn(Arrays.asList(userData));

        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ALL_ORG), any(), any(), any()))
                .thenReturn(new ArrayList<>());

        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ORG), any(), any(), any()))
                .thenReturn(new ArrayList<>());

        ApiResponse response = service.getCBPlanListForUser("org123", "token123", false);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testGetCBPlanListForUser_UserNotFound() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user123");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER), any(), any(), any()))
                .thenReturn(new ArrayList<>());

        ApiResponse response = service.getCBPlanListForUser("org123", "token123", false);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testRemoveDuplicateCourses() {
        List<Map<String, Object>> courseList = new ArrayList<>();
        Map<String, Object> course = new HashMap<>();
        course.put(Constants.IDENTIFIER, "course1");
        course.put(Constants.LANGUAGE_MAP_V1, new HashMap<>());
        courseList.add(course);

        List<Map<String, Object>> result = service.removeDuplicateCourses(courseList);

        assertEquals(1, result.size());
    }
}