package com.igot.cb.service;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.user.UserUtilityService;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CbPlanServiceImplTest {

    @Mock(lenient = true)
    private AccessTokenValidator accessTokenValidator;

    @Mock(lenient = true)
    private CassandraOperation cassandraOperation;

    @Mock(lenient = true)
    private EsUtilService esUtilService;

    @Mock(lenient = true)
    private ContentInfoServiceImpl contentService;

    @Mock(lenient = true)
    private CbExtServerProperties serverProperties;

    @Mock(lenient = true)
    private UserUtilityService userUtilityService;



    private CbPlanServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new CbPlanServiceImpl(accessTokenValidator, cassandraOperation);
        ReflectionTestUtils.setField(service, "esUtilService", esUtilService);
        ReflectionTestUtils.setField(service, "contentService", contentService);
        ReflectionTestUtils.setField(service, "serverProperties", serverProperties);
        ReflectionTestUtils.setField(service, "userUtilityService", userUtilityService);
        ReflectionTestUtils.setField(service, "allowedFieldsConfig", "name,contextData,endDate");
        ReflectionTestUtils.setField(service, "cpPlanIndex", "cb_plan_index");
        ReflectionTestUtils.setField(service, "elasticCbPlanJsonPath", "/path/to/json");
    }

    @Test
    void testCreateCbPlan_Success() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user123");

        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("name", "Test Plan");
        requestMap.put("contentType", "Course");
        requestMap.put("contentList", Arrays.asList("course1"));
        requestMap.put("orgScope", "ALL");
        requestMap.put("endDate", "2024-12-31");
        requestMap.put("contextData", new HashMap<>());
        request.setRequest(requestMap);

        ApiResponse mockResponse = new ApiResponse();
        mockResponse.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(mockResponse);
        when(esUtilService.addDocument(anyString(), anyString(), anyString(), any(), anyString())).thenReturn("success");

        ApiResponse response = service.createCbPlan(request, "org123", "token123");

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testUpdateCbPlan_Success() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user123");
        when(serverProperties.getCbPlanUpdatePublishAuthorizedRoles()).thenReturn(Arrays.asList("ADMIN"));

        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, "plan123");
        requestMap.put("name", "Updated Plan");
        request.setRequest(requestMap);

        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put(Constants.CREATED_BY, "user123");
        existingPlan.put(Constants.STATUS, Constants.DRAFT);
        existingPlan.put(Constants.DRAFT_DATA, "{\"name\":\"Test\"}");
        existingPlan.put(Constants.END_DATE_REQUEST, new Date());
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenReturn(Arrays.asList(existingPlan));

        Map<String, Object> updateResponse = new HashMap<>();
        updateResponse.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.updateRecord(anyString(), anyString(), any(), any())).thenReturn(updateResponse);
        when(esUtilService.updateDocument(anyString(), anyString(), anyString(), any(), anyString())).thenReturn("success");

        ApiResponse response = service.updateCbPlan(request, "org123", "token123", Arrays.asList("ADMIN"));

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testPublishCbPlan_Success() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user123");
        when(serverProperties.getCbPlanUpdatePublishAuthorizedRoles()).thenReturn(Arrays.asList("ADMIN"));

        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, "plan123");
        request.setRequest(requestMap);

        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put(Constants.CREATED_BY, "user123");
        existingPlan.put(Constants.STATUS, Constants.DRAFT);
        existingPlan.put(Constants.DRAFT_DATA, "{\"name\":\"Test\",\"contentType\":\"Course\",\"contentList\":[\"course1\"],\"orgScope\":\"ALL\",\"endDate\":\"2024-12-31\"}");
        existingPlan.put(Constants.CREATED_AT_REQ, Instant.now());
        existingPlan.put(Constants.END_DATE, Instant.now());
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenReturn(Arrays.asList(existingPlan));

        Map<String, Object> updateResponse = new HashMap<>();
        updateResponse.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.updateRecord(anyString(), anyString(), any(), any())).thenReturn(updateResponse);
        when(esUtilService.updateDocument(anyString(), anyString(), anyString(), any(), anyString())).thenReturn("success");

        ApiResponse response = service.publishCbPlan(request, "org123", "token123", Arrays.asList("ADMIN"));

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testReadCbPlan_Success() {
        Map<String, Object> cbPlan = new HashMap<>();
        cbPlan.put(Constants.NAME, "Test Plan");
        cbPlan.put(Constants.STATUS, Constants.LIVE);
        cbPlan.put(Constants.CONTENT_LIST, Arrays.asList("course1"));
        cbPlan.put(Constants.CREATED_AT_REQ, java.time.Instant.now());
        cbPlan.put(Constants.DRAFT_DATA, null);
        cbPlan.put(Constants.CREATED_BY, "user123");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenReturn(Arrays.asList(cbPlan));

        Map<String, Object> contentResponse = new HashMap<>();
        contentResponse.put(Constants.STATUS, Constants.LIVE);
        contentResponse.put(Constants.NAME, "Test Course");
        when(contentService.readContent(anyString(), any()))
                .thenReturn(contentResponse);

        // Mock user utility service
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();
        Map<String, String> userInfo = new HashMap<>();
        userInfo.put(Constants.FIRSTNAME, "Test User");
        userInfoMap.put("user123", userInfo);
        doAnswer(invocation -> {
            Map<String, Map<String, String>> map = invocation.getArgument(2);
            map.putAll(userInfoMap);
            return null;
        }).when(userUtilityService).getUserDetailsFromDB(any(), any(), any());

        ApiResponse response = service.readCbPlan("plan123", "org123", "token123");

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testSearchCbPlan_Success() throws Exception {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user123");

        SearchResult searchResult = new SearchResult();
        searchResult.setData(new ArrayList<>());
        when(esUtilService.searchDocuments(anyString(), any(), anyString())).thenReturn(searchResult);

        ApiResponse response = service.searchCbPlan(new SearchCriteria(), "org123", "token123");

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testRetireCbPlan_Success() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user123");
        when(serverProperties.getCbPlanUpdatePublishAuthorizedRoles()).thenReturn(Arrays.asList("ADMIN"));

        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, "plan123");
        request.setRequest(requestMap);

        Map<String, Object> existingPlan = new HashMap<>();
        existingPlan.put(Constants.CREATED_BY, "user123");
        existingPlan.put(Constants.STATUS, Constants.LIVE);
        existingPlan.put(Constants.ORG_SCOPE, "ALL");
        existingPlan.put(Constants.ORG_ID_LIST, Arrays.asList("org1"));
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenReturn(Arrays.asList(existingPlan));

        Map<String, Object> updateResponse = new HashMap<>();
        updateResponse.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.updateRecord(anyString(), anyString(), any(), any())).thenReturn(updateResponse);
        when(esUtilService.addDocument(anyString(), anyString(), anyString(), any(), anyString())).thenReturn("success");

        ApiResponse response = service.retireCbPlan(request, "org123", "token123", Arrays.asList("ADMIN"));

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testParseToDate() {
        Date result = service.parseToDate("2024-12-31");
        assertNotNull(result);

        result = service.parseToDate(Instant.now());
        assertNotNull(result);

        result = service.parseToDate(null);
        assertNull(result);
    }

    @Test
    void testSanitizeForElastic() {
        Map<String, Object> input = new HashMap<>();
        input.put("test", Instant.now());

        Map<String, Object> result = CbPlanServiceImpl.sanitizeForElastic(input);

        assertNotNull(result);
        assertTrue(result.get("test") instanceof String);
    }
}