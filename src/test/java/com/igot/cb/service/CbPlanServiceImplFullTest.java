package com.igot.cb.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;
public class CbPlanServiceImplFullTest {

    @Mock private AccessTokenValidator accessTokenValidator;
    @Mock private CassandraOperation cassandraOperation;
    @Mock private UserAndOrgServiceImpl userAndOrgService;
    @Mock private ContentInfoServiceImpl contentService;
    @Mock private EsUtilService esUtilService;
    @Mock private CbExtServerProperties serverProperties;
    @Mock private RequestValidator requestValidator;

    @InjectMocks
    private CbPlanServiceImpl cbPlanService;

    @BeforeEach
    void init() {
        MockitoAnnotations.openMocks(this);
        // construct instance explicitly (constructor sets final fields)
        cbPlanService = new CbPlanServiceImpl(accessTokenValidator, cassandraOperation, serverProperties,
                userAndOrgService, contentService, esUtilService, requestValidator);

        // set some serverProperties fields used by methods
        ReflectionTestUtils.setField(serverProperties, "cpPlanIndex", "test-index");
        ReflectionTestUtils.setField(serverProperties, "elasticCbPlanJsonPath", "test-path");
        ReflectionTestUtils.setField(serverProperties, "cbPlanUpdateAllowedFields", "name,contextDataRequest,endDate");

        // ensure object is injected correctly
        ReflectionTestUtils.setField(cbPlanService, "userAndOrgService", userAndOrgService);
        ReflectionTestUtils.setField(cbPlanService, "contentService", contentService);
        ReflectionTestUtils.setField(cbPlanService, "esUtilService", esUtilService);
        ReflectionTestUtils.setField(cbPlanService, "serverProperties", serverProperties);
    }

    private Map<String, Object> minimalPlan() {
        Map<String, Object> plan = new HashMap<>();
        plan.put("name", "Plan A");
        plan.put("createdBy", "u1");
        plan.put("contentList", List.of("content1"));
        plan.put("status", "draft");
        plan.put("draftData", "");
        plan.put("createdAtReq", Instant.now());
        return plan;
    }

    @Test
    void createCbPlan_CassandraInsertFails() {
        ApiRequest request = new ApiRequest();
        Map<String,Object> req = new HashMap<>();
        req.put("name", "Plan X");
        req.put("endDateRequest", new Date());
        req.put("orgScope", "single");
        req.put("orgIdList", List.of("org1"));
        req.put("contentType", "Course");
        req.put("contentList", List.of("c1"));
        request.setRequest(req);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");

        ApiResponse cassResp = new ApiResponse();
        cassResp.put(Constants.RESPONSE, Constants.FAILED);
        cassResp.getParams().setErr("DB error");
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(cassResp);

        ApiResponse resp = cbPlanService.createCbPlan(request, "org", "token");
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, resp.getResponseCode());
    }

    @Test
    void createCbPlan_LookupBulkFails() {
        ApiRequest request = new ApiRequest();
        Map<String,Object> req = new HashMap<>();
        req.put("name", "Plan X");
        req.put("endDateRequest", new Date());
        req.put("orgScope", "single");
        req.put("orgIdList", List.of("org1"));
        req.put("contentType", "Course");
        req.put("contentList", List.of("c1"));
        request.setRequest(req);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");

        ApiResponse cassResp = new ApiResponse();
        cassResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(cassResp);

        ApiResponse lookup = new ApiResponse();
        lookup.put(Constants.RESPONSE, Constants.FAILED);
        lookup.getParams().setErr("lookup failed");
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), any())).thenReturn(lookup);

        ApiResponse resp = cbPlanService.createCbPlan(request, "org", "token");
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, resp.getResponseCode());
    }

    @Test
    void createCbPlan_esThrows_setsFailed() {
        ApiRequest request = new ApiRequest();
        Map<String,Object> req = new HashMap<>();
        req.put("name", "Plan X");
        req.put("endDateRequest", new Date());
        req.put("orgScope", "single");
        req.put("orgIdList", List.of("org1"));
        req.put("contentType", "Course");
        req.put("contentList", List.of("c1"));
        request.setRequest(req);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");

        ApiResponse cassResp = new ApiResponse();
        cassResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.insertRecord(anyString(), anyString(), any())).thenReturn(cassResp);

        doThrow(new RuntimeException("ES fail")).when(esUtilService).addDocument(anyString(), anyString(), anyString(), any(), anyString());

        ApiResponse resp = cbPlanService.createCbPlan(request, "org", "token");
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, resp.getResponseCode());
    }

    @Test
    void updateCbPlan_missingId_returnsBadRequest() {
        ApiRequest request = new ApiRequest();
        request.setRequest(Map.of("name", "NoId"));
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");
        ApiResponse resp = cbPlanService.updateCbPlan(request, "org", "token", List.of("role"));
        assertEquals(HttpStatus.BAD_REQUEST, resp.getResponseCode());
    }

    @Test
    void updateCbPlan_planNotFound_error() {
        ApiRequest request = new ApiRequest();
        request.setRequest(Map.of(Constants.ID, "plan1"));
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenReturn(Collections.emptyList());
        ApiResponse resp = cbPlanService.updateCbPlan(request, "org", "token", List.of("admin"));
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, resp.getResponseCode());
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
    }

    @Test
    void publishCbPlan_missingId_returnsBadRequest() {
        ApiRequest request = new ApiRequest();
        request.setRequest(Map.of()); // empty
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");
        ApiResponse resp = cbPlanService.publishCbPlan(request, "org", "token", List.of("role"));
        assertEquals(HttpStatus.BAD_REQUEST, resp.getResponseCode());
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
    }

    @Test
    void publishCbPlan_notAuthorized() {
        ApiRequest request = new ApiRequest();
        request.setRequest(Map.of(Constants.ID, "p1"));
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");
        Map<String,Object> existing = new HashMap<>();
        existing.put(Constants.CREATED_BY, "other");
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenReturn(List.of(existing));
        when(serverProperties.getCbPlanUpdatePublishAuthorizedRoles()).thenReturn(List.of("admin"));
        ApiResponse resp = cbPlanService.publishCbPlan(request, "org", "token", List.of("user"));
        assertEquals(HttpStatus.BAD_REQUEST, resp.getResponseCode());
    }

    @Test
    void publishCbPlan_alreadyPublished_error() {
        ApiRequest request = new ApiRequest();
        request.setRequest(Map.of(Constants.ID, "p1"));
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");
        Map<String,Object> existing = new HashMap<>();
        existing.put(Constants.CREATED_BY, "u1");
        existing.put(Constants.STATUS, "live");
        existing.put("draftData", null);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenReturn(List.of(existing));
        ApiResponse resp = cbPlanService.publishCbPlan(request, "org", "token", List.of("role"));
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, resp.getResponseCode());
    }

    @Test
    void retireCbPlan_missingId_badRequest() {
        ApiRequest request = new ApiRequest();
        request.setRequest(Map.of()); // missing id
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");
        ApiResponse resp = cbPlanService.retireCbPlan(request, "org", "token", List.of("role"));
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
    }

    @Test
    void retireCbPlan_alreadyRetired_error() {
        ApiRequest request = new ApiRequest();
        request.setRequest(Map.of(Constants.ID, "p1"));
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");
        Map<String,Object> existing = new HashMap<>();
        existing.put(Constants.CREATED_BY, "u1");
        existing.put(Constants.STATUS, Constants.CB_RETIRE);  // retired
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any(), any()))
                .thenReturn(List.of(existing));
        ApiResponse resp = cbPlanService.retireCbPlan(request, "org", "token", List.of("role"));
        assertEquals(HttpStatus.BAD_REQUEST, resp.getResponseCode());
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
        assertEquals("CbPlan is already archived for ID: p1", resp.getParams().getErr());
    }

    @Test
    void searchCbPlan_whenEsThrows_raises() throws Exception {
        SearchCriteria crit = new SearchCriteria();
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("u1");
        when(esUtilService.searchDocuments(anyString(), any(), anyString())).thenThrow(new RuntimeException("boom"));
        assertThrows(RuntimeException.class, () -> cbPlanService.searchCbPlan(crit, "org", "token"));
    }

    @Test
    void readCbPlan_emptyId_badRequest() {
        ApiResponse r = cbPlanService.readCbPlan("", "org", "token");
        assertEquals(HttpStatus.BAD_REQUEST, r.getResponseCode());
    }

    @Test
    void readCbPlan_whenDbThrows_internalError() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenThrow(new RuntimeException("db"));
        ApiResponse r = cbPlanService.readCbPlan("id1", "org", "token");
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, r.getResponseCode());
    }

    @Test
    void parseToDate_variousTypes() {
        // null -> null
        assertNull(cbPlanService.parseToDate(null));
        // String date "yyyy-MM-dd"
        assertNotNull(cbPlanService.parseToDate("2024-12-31"));
        // Instant
        assertNotNull(cbPlanService.parseToDate(Instant.now()));
        // sql timestamp
        assertNotNull(cbPlanService.parseToDate(new java.sql.Timestamp(System.currentTimeMillis())));
        // util.Date
        assertNotNull(cbPlanService.parseToDate(new Date()));
    }

    @Test
    void sanitizeForElastic_instantConvertedToString() {
        Map<String,Object> in = new HashMap<>();
        in.put("a","b");
        in.put("instant", Instant.now());
        Map<String,Object> out = CbPlanServiceImpl.sanitizeForElastic(in);
        assertEquals("b", out.get("a"));
        assertTrue(out.get("instant") instanceof String);
    }

    @Test
    void extractUniqueRootOrgIds_paths() throws Exception {
        // empty returns empty
        @SuppressWarnings("unchecked")
        Set<String> s1 = (Set<String>) ReflectionTestUtils.invokeMethod(cbPlanService, "extractUniqueRootOrgIds", new HashMap<>());
        assertTrue(s1.isEmpty());

        // Map path
        Map<String,Object> crit = Map.of(Constants.CRITERIA_KEY, Constants.ROOT_ORG_ID, Constants.CRITERIA_VALUE, List.of("r1"));
        Map<String,Object> ug = Map.of(Constants.USER_GROUP_CRITERIA_LIST, List.of(crit));
        Map<String,Object> ac = Map.of(Constants.USER_GROUPS, List.of(ug));
        Map<String,Object> raw = Map.of(Constants.CONTEXT_DATA_REQUEST, Map.of(Constants.ACCESS_CONTROL, ac));
        @SuppressWarnings("unchecked")
        Set<String> s2 = (Set<String>) ReflectionTestUtils.invokeMethod(cbPlanService, "extractUniqueRootOrgIds", raw);
        assertEquals(Set.of("r1"), s2);

        // JSON string path
        String json = "{\"accessControl\":{\"userGroups\":[{\"userGroupCriteriaList\":[{\"criteriaKey\":\"rootOrgId\",\"criteriaValue\":[\"r2\"]}]}]}}";
        @SuppressWarnings("unchecked")
        Set<String> s3 = (Set<String>) ReflectionTestUtils.invokeMethod(cbPlanService, "extractUniqueRootOrgIds", Map.of(Constants.CONTEXT_DATA_REQUEST, json));
        assertEquals(Set.of("r2"), s3);
    }
}
