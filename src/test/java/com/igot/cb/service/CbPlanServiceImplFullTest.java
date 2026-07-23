package com.igot.cb.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.core.JsonProcessingException;
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
        assertInstanceOf(String.class, out.get("instant"));
    }

    @Test
    void extractUniqueRootOrgIds_paths() throws Exception {
        // empty returns empty
        @SuppressWarnings("unchecked")
        Set<String> s1 = (Set<String>) ReflectionTestUtils.invokeMethod(cbPlanService, "extractUniqueRootOrgIds", new HashMap<>());
        assertNotNull(s1);
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

    @Test
    void testPrepareCbPlanForRePublish() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ReflectionTestUtils.setField(cbPlanService, "mapper", mapper);

        Map<String, Object> draftData = new HashMap<>();
        draftData.put(Constants.IS_APAR, true);
        draftData.put(Constants.ORG_SCOPE, "national");
        draftData.put(Constants.NAME, "Test Name");
        draftData.put(Constants.CONTEXT_DATA_REQUEST, Map.of("key", "value"));
        draftData.put(Constants.END_DATE_REQUEST, "2025-12-12");
        draftData.put(Constants.ROOT_ORG_IDS_IN_CONTEXT_DATA, List.of("org1", "org2"));

        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.DRAFT_DATA, mapper.writeValueAsString(draftData));

        Map<String, Object> incomingRequest = new HashMap<>();
        incomingRequest.put(Constants.COMMENT, "Re-publish comment");

        Map<String, Object> result =
                ReflectionTestUtils.invokeMethod(cbPlanService, "prepareCbPlanForRePublish",
                        existingCbPlan, incomingRequest, "user123");

        assertNotNull(result);
        assertEquals(true, result.get(Constants.IS_APAR));
        assertEquals("national", result.get(Constants.ORG_SCOPE));
        assertEquals("Test Name", result.get(Constants.NAME));
        assertEquals("{\"key\":\"value\"}", result.get(Constants.CONTEXT_DATA_REQUEST));
        assertNotNull(result.get(Constants.END_DATE_REQUEST));
        assertEquals(List.of("org1", "org2"), result.get(Constants.ROOT_ORG_IDS_IN_CONTEXT_DATA));
        assertEquals("Re-publish comment", result.get(Constants.COMMENT));
    }

    @Test
    void testUpsertCustomOrgLookup_success() throws Exception {
        ApiResponse successResponse = new ApiResponse();
        successResponse.getParams().setStatus(Constants.SUCCESS);

        when(cassandraOperation.insertBulkRecord(
                anyString(), anyString(), anyList()))
                .thenReturn(successResponse);

        Set<String> orgIds = Set.of("org1", "org2");

        ApiResponse result = ReflectionTestUtils.invokeMethod(
                cbPlanService, "upsertCustomOrgLookup",
                "plan123", orgIds, Instant.parse("2025-01-01T00:00:00Z"), true);

        assertNotNull(result);
        assertEquals(Constants.SUCCESS, result.getParams().getStatus());
        assertEquals("Lookup entries created successfully for all orgIds",
                result.getResult().get("message"));
    }

    @Test
    void testUpsertCustomOrgLookup_emptyOrgList() throws Exception {
        ApiResponse result = ReflectionTestUtils.invokeMethod(
                cbPlanService, "upsertCustomOrgLookup",
                "plan123", Set.of(), Instant.now(), true);

        assertNotNull(result);
        assertEquals(Constants.FAILED, result.getParams().getStatus());
        assertEquals("orgIdList is empty. Cannot create lookup entries.", result.getParams().getErr());
    }

    @Test
    void testUpsertCustomOrgLookup_failureFromCassandra() throws Exception {
        ApiResponse failResponse = new ApiResponse();
        failResponse.getParams().setStatus(Constants.FAILED);

        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList()))
                .thenReturn(failResponse);

        Set<String> orgIds = Set.of("org1");

        ApiResponse result = ReflectionTestUtils.invokeMethod(
                cbPlanService, "upsertCustomOrgLookup",
                "plan123", orgIds, Instant.now(), true);

        assertNotNull(result);
        assertEquals(Constants.FAILED, result.getParams().getStatus());
    }

    @Test
    void testUpsertCustomOrgLookup_exceptionThrown() throws Exception {
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList()))
                .thenThrow(new RuntimeException("DB error"));

        Set<String> orgIds = Set.of("org1");

        ApiResponse result = ReflectionTestUtils.invokeMethod(
                cbPlanService, "upsertCustomOrgLookup",
                "plan123", orgIds, Instant.now(), true);

        assertNotNull(result);
        assertEquals(Constants.FAILED, result.getParams().getStatus());
        assertTrue(result.getParams().getErr().contains("Exception while creating org lookup entries"));
    }

    @Test
    void testUpsertAllOrgLookup_success() throws Exception {
        ApiResponse successResponse = new ApiResponse();
        successResponse.getParams().setStatus(Constants.SUCCESS);

        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap()))
                .thenReturn(successResponse);

        ApiResponse result = ReflectionTestUtils.invokeMethod(
                cbPlanService, "upsertAllOrgLookup",
                "plan123", Instant.parse("2025-01-01T00:00:00Z"), true);

        assertNotNull(result);
        assertEquals(Constants.SUCCESS, result.getParams().getStatus());
    }

    @Test
    void testUpsertAllOrgLookup_exceptionThrown() throws Exception {
        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap()))
                .thenThrow(new RuntimeException("DB error"));

        ApiResponse result = ReflectionTestUtils.invokeMethod(
                cbPlanService, "upsertAllOrgLookup",
                "plan123", Instant.now(), true);

        assertNotNull(result);
        assertEquals(Constants.FAILED, result.getParams().getStatus());
        assertEquals(Constants.FAILED, result.get(Constants.RESPONSE));
        assertTrue(result.getParams().getErr().contains("Exception while inserting SINGLE org lookup"));
    }

    @Test
    void testPrepareCbPlanForUpdate() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ReflectionTestUtils.setField(cbPlanService, "mapper", mapper);

        Map<String, Object> incomingRequest = new HashMap<>();
        incomingRequest.put(Constants.IS_APAR, true);
        incomingRequest.put(Constants.ORG_ID_LIST, List.of("org1", "org2"));
        incomingRequest.put(Constants.ORG_SCOPE, "national");
        incomingRequest.put(Constants.CONTENT_LIST, List.of("content1"));
        incomingRequest.put(Constants.NAME, "Updated Name");
        incomingRequest.put(Constants.COMMENT, "Update comment");
        incomingRequest.put(Constants.CONTENT_TYPE, "Course");
        incomingRequest.put(Constants.END_DATE_REQUEST, "2025-12-12");
        incomingRequest.put(Constants.CONTEXT_DATA_REQUEST, Map.of("key", "value"));

        Map<String, Object> existingCbPlan = new HashMap<>();

        Map<String, Object> result = ReflectionTestUtils.invokeMethod(
                cbPlanService, "prepareCbPlanForUpdate",
                incomingRequest, existingCbPlan, "user123");

        assertNotNull(result);
        assertEquals("user123", result.get(Constants.UPDATED_BY));
        assertNotNull(result.get(Constants.UPDATED_AT));
        assertEquals(true, result.get(Constants.IS_APAR));
        assertEquals(List.of("org1", "org2"), result.get(Constants.ORG_ID_LIST));
        assertEquals("national", result.get(Constants.ORG_SCOPE));
        assertEquals(List.of("content1"), result.get(Constants.CONTENT_LIST));
        assertEquals("Updated Name", result.get(Constants.NAME));
        assertEquals("Update comment", result.get(Constants.COMMENT));
        assertEquals("Course", result.get(Constants.CONTENT_TYPE));
        assertNotNull(result.get(Constants.END_DATE_REQUEST));
        assertEquals("{\"key\":\"value\"}", result.get(Constants.CONTEXT_DATA_REQUEST));
    }

    @Test
    void testPrepareCbPlanForInsert() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ReflectionTestUtils.setField(cbPlanService, "mapper", mapper);

        Map<String, Object> incomingRequest = new HashMap<>();
        incomingRequest.put(Constants.IS_APAR, true);
        incomingRequest.put(Constants.ORG_ID_LIST, List.of("org1", "org2"));
        incomingRequest.put(Constants.ORG_SCOPE, "national");
        incomingRequest.put(Constants.CONTENT_LIST, List.of("content1"));
        incomingRequest.put(Constants.NAME, "CB Plan");
        incomingRequest.put(Constants.COMMENT, "Inserted new plan");
        incomingRequest.put(Constants.CONTENT_TYPE, "Course");
        incomingRequest.put(Constants.END_DATE_REQUEST, "2025-12-12");
        incomingRequest.put(Constants.CONTEXT_DATA_REQUEST, Map.of("key", "value"));

        Map<String, Object> result =
                ReflectionTestUtils.invokeMethod(
                        cbPlanService,
                        "prepareCbPlanForInsert",
                        incomingRequest,
                        "user123"
                );

        assertNotNull(result);
        assertNotNull(result.get(Constants.PLAN_ID));
        assertNotNull(result.get(Constants.CREATED_AT));
        assertEquals("user123", result.get(Constants.CREATED_BY));
        assertEquals(Constants.DRAFT, result.get(Constants.STATUS));
        assertEquals(true, result.get(Constants.IS_APAR));
        assertEquals(List.of("org1", "org2"), result.get(Constants.ORG_ID_LIST));
        assertEquals("national", result.get(Constants.ORG_SCOPE));
        assertEquals(List.of("content1"), result.get(Constants.CONTENT_LIST));
        assertEquals("CB Plan", result.get(Constants.NAME));
        assertEquals("Inserted new plan", result.get(Constants.COMMENT));
        assertEquals("Course", result.get(Constants.CONTENT_TYPE));
        assertNotNull(result.get(Constants.END_DATE_REQUEST));
        assertEquals("{\"key\":\"value\"}", result.get(Constants.CONTEXT_DATA_REQUEST));
    }

    @Test
    void testSearchCbPlan_success() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        ReflectionTestUtils.setField(cbPlanService, "mapper", mapper);
        ReflectionTestUtils.setField(cbPlanService, "esUtilService", esUtilService);
        ReflectionTestUtils.setField(cbPlanService, "accessTokenValidator", accessTokenValidator);
        ReflectionTestUtils.setField(cbPlanService, "userAndOrgService", userAndOrgService);
        ReflectionTestUtils.setField(cbPlanService, "contentService", contentService);
        ReflectionTestUtils.setField(cbPlanService, "serverProperties", serverProperties);

        SearchCriteria criteria = new SearchCriteria();

        Map<String, Object> esRecord = new HashMap<>();
        esRecord.put(Constants.CREATED_BY, "user123");
        esRecord.put(Constants.CONTENT_LIST, List.of("content1"));

        SearchResult esSearchResult = new SearchResult();
        esSearchResult.setData(List.of(esRecord));

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("userXX");
        when(serverProperties.getCpPlanIndex()).thenReturn("cb_plan_idx");
        when(serverProperties.getElasticCbPlanJsonPath()).thenReturn("json/path");
        when(esUtilService.searchDocuments(anyString(), any(), any())).thenReturn(esSearchResult);
        when(userAndOrgService.readUserProfile(eq("user123"), anyList()))
                .thenReturn(Map.of(Constants.FIRSTNAME, "John", Constants.USER_ID, "user123"));
        when(contentService.enrichContentInfoForCBPlan(List.of("content1")))
                .thenReturn(List.of(Map.of("id", "content1", "name", "Content One")));

        ApiResponse response = cbPlanService.searchCbPlan(criteria, "org1", "token123");

        assertNotNull(response);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());

        SearchResult result = (SearchResult) response.getResult().get(Constants.RESULT);
        assertNotNull(result);
        assertEquals(1, result.getData().size());

        Map<String, Object> enrichedItem = result.getData().get(0);

        assertEquals("John", enrichedItem.get(Constants.CREATED_BY_NAME));
        assertEquals("user123", enrichedItem.get(Constants.CREATED_BY));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> enrichedContent = (List<Map<String, Object>>) enrichedItem.get(Constants.CONTENT_LIST);
        assertEquals(1, enrichedContent.size());
        assertEquals("content1", enrichedContent.get(0).get("id"));
    }

    @Test
    void testCreateCbPlan_success() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        CbPlanServiceImpl spyService = Mockito.spy(cbPlanService);

        ReflectionTestUtils.setField(spyService, "mapper", mapper);
        ReflectionTestUtils.setField(spyService, "cassandraOperation", cassandraOperation);
        ReflectionTestUtils.setField(spyService, "accessTokenValidator", accessTokenValidator);
        ReflectionTestUtils.setField(spyService, "serverProperties", serverProperties);
        ReflectionTestUtils.setField(spyService, "requestValidator", requestValidator);
        ReflectionTestUtils.setField(spyService, "esUtilService", esUtilService);
        ReflectionTestUtils.setField(spyService, "userAndOrgService", userAndOrgService);

        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("comment", "cbPlanId1 is created");
        requestMap.put("contentList", Arrays.asList("do_11438102438907904012", "do_11438323904778240014"));
        requestMap.put("contentType", "Course");

        Map<String, Object> contextData = Map.of(
                "accessControl", Map.of(
                        "userGroups", List.of(
                                Map.of(
                                        "userGroupName", "User Group 1",
                                        "userGroupCriteriaList", List.of(
                                                Map.of("criteriaKey", "group", "criteriaValue", List.of("Group A"))
                                        )
                                )
                        ),
                        "version", 1
                )
        );
        requestMap.put("contextData", contextData);
        requestMap.put("endDate", "2025-09-06");
        requestMap.put("isApar", true);
        requestMap.put("name", "Training Plan for testing endDate");
        requestMap.put("orgScope", "All");

        ApiRequest apiRequest = new ApiRequest();
        apiRequest.setRequest(requestMap);

        // Mock fetch user ID
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any()))
                .thenReturn("user123");

        Map<String, Object> userMap = new HashMap<>();
        userMap.put(Constants.ID, "user123");
        userMap.put(Constants.ROOT_ORG_ID, "root1");
        when(userAndOrgService.readUserProfileFromDB(eq("user123"), anyList())).thenReturn(userMap);

        Map<String, Object> orgMap = new HashMap<>();
        orgMap.put(Constants.IS_CCA, true);
        when(userAndOrgService.readOrgFromDB("root1", null)).thenReturn(orgMap);

        when(requestValidator.validateCbPlanCreateRequest(any(ApiRequest.class), eq(true), anyString(), anyBoolean()))
                .thenReturn(Collections.emptyList());

        ApiResponse insertResponse = new ApiResponse();
        insertResponse.getParams().setStatus(Constants.SUCCESS);
        insertResponse.getResult().put(Constants.RESPONSE, Constants.SUCCESS);
        insertResponse.setResponseCode(HttpStatus.OK);

        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap()))
                .thenReturn(insertResponse);

        when(esUtilService.addDocument(anyString(), anyString(), anyString(), anyMap(), anyString()))
                .thenReturn(null);

        ApiResponse response = spyService.createCbPlan(apiRequest, "org1", "token123");

        assertEquals(Constants.CREATED, response.getResult().get(Constants.STATUS));
        assertNotNull(response.getResult().get(Constants.ID));
        assertEquals(Constants.API_CB_PLAN_CREATE, response.getId());
    }

    @Test
    void testCreateCbPlan_userIdEmpty() throws Exception {
        ApiRequest apiRequest = new ApiRequest();
        apiRequest.setRequest(new HashMap<>());
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any()))
                .thenReturn("");
        ApiResponse response = cbPlanService.createCbPlan(apiRequest, "org1", "token123");
        assertEquals(Constants.API_CB_PLAN_CREATE, response.getId());
        assertNotEquals(Constants.CREATED, response.getResult().get(Constants.STATUS));
    }

    @Test
    void testCreateCbPlan_validationFails() throws JsonProcessingException {
        ApiRequest apiRequest = new ApiRequest();
        apiRequest.setRequest(new HashMap<>());
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any()))
                .thenReturn("user123");
        when(userAndOrgService.readUserProfileFromDB(eq("user123"), anyList()))
                .thenReturn(Map.of("id", "user123", "rootOrgId", "root1"));
        when(userAndOrgService.readOrgFromDB(eq("root1"), any()))
                .thenReturn(Map.of("isCCA", true));
        when(requestValidator.validateCbPlanCreateRequest(any(ApiRequest.class), anyBoolean(), anyString(), anyBoolean()))
                .thenReturn(List.of("Error1"));
        ApiResponse response = cbPlanService.createCbPlan(apiRequest, "org1", "token123");
        assertEquals("Failed", response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testCreateCbPlan_cassandraFails() throws Exception {
        ApiRequest apiRequest = new ApiRequest();
        apiRequest.setRequest(new HashMap<>());
        CbPlanServiceImpl spyService = Mockito.spy(cbPlanService);
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any()))
                .thenReturn("user123");
        when(userAndOrgService.readUserProfileFromDB(eq("user123"), anyList()))
                .thenReturn(Map.of("id", "user123", "rootOrgId", "root1"));
        when(userAndOrgService.readOrgFromDB(eq("root1"), any()))
                .thenReturn(Map.of("isCCA", true));
        when(requestValidator.validateCbPlanCreateRequest(any(ApiRequest.class), anyBoolean(), anyString(), anyBoolean()))
                .thenReturn(Collections.emptyList());
        ApiResponse insertResp = new ApiResponse();
        insertResp.getParams().setStatus(Constants.FAILED);
        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap()))
                .thenReturn(insertResp);
        Method getRootOrgFromUser = CbPlanServiceImpl.class.getDeclaredMethod("getRootOrgFromUser", String.class, ApiResponse.class);
        getRootOrgFromUser.setAccessible(true);
        Method getCCAFromOrg = CbPlanServiceImpl.class.getDeclaredMethod("getCCAFromOrg", String.class, ApiResponse.class);
        getCCAFromOrg.setAccessible(true);
        ReflectionTestUtils.invokeMethod(spyService, "getRootOrgFromUser", "user123", new ApiResponse());
        ReflectionTestUtils.invokeMethod(spyService, "getCCAFromOrg", "root1", new ApiResponse());
        ApiResponse response = spyService.createCbPlan(apiRequest, "org1", "token123");
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testUpdateCbPlan_success() throws Exception {
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ID, "7c0807f0-8642-11f0-88f4-7f16a4fdb692");
        requestMap.put("comment", "planId1 approved.");
        requestMap.put("contextData", Map.of(
                "accessControl", Map.of(
                        "version", 1,
                        "userGroups", List.of(
                                Map.of(
                                        "userGroupName", "User Group 1",
                                        "userGroupCriteriaList", List.of(
                                                Map.of("criteriaKey", "rootOrgId", "criteriaValue", List.of("orgId1","orgId2","orgId11")),
                                                Map.of("criteriaKey", "designation", "criteriaValue", List.of("designation1","designation2")),
                                                Map.of("criteriaKey", "group", "criteriaValue", List.of("Group A","Group B")),
                                                Map.of("criteriaKey", "cadre", "criteriaValue", List.of("AGMUT (Arunachal Pradesh-Goa-Mizoram and Union Territories)","Andhra Pradesh")),
                                                Map.of("criteriaKey", "service", "criteriaValue", List.of("Indian Administrative Service (IAS)")),
                                                Map.of("criteriaKey", "batch", "criteriaValue", List.of(2014, 2015)),
                                                Map.of("criteriaKey", "user", "criteriaValue", List.of("userId1","userId2"))
                                        )
                                )
                        )
                )
        ));

        ApiRequest apiRequest = new ApiRequest();
        apiRequest.setRequest(requestMap);
        CbPlanServiceImpl spyService = Mockito.spy(cbPlanService);
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any()))
                .thenReturn("user123");
        Map<String, Object> existingCbPlan = new HashMap<>();
        existingCbPlan.put(Constants.ID, requestMap.get(Constants.ID));
        existingCbPlan.put(Constants.PLAN_ID, requestMap.get(Constants.ID));
        existingCbPlan.put(Constants.CREATED_BY, "user123");
        existingCbPlan.put(Constants.STATUS, Constants.DRAFT);

        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(List.of(existingCbPlan));

        Map<String, Object> updateResp = new HashMap<>();
        updateResp.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.updateRecord(anyString(), anyString(), anyMap(), anyMap()))
                .thenReturn(updateResp);
        when(esUtilService.addDocument(anyString(), anyString(), anyString(), anyMap(), anyString()))
                .thenReturn(null);
        when(serverProperties.getCbPlanUpdatePublishAuthorizedRoles())
                .thenReturn(List.of("ROLE_ADMIN", "ROLE_MANAGER"));
        Method getRootOrgMethod = CbPlanServiceImpl.class.getDeclaredMethod("getRootOrgFromUser", String.class, ApiResponse.class);
        getRootOrgMethod.setAccessible(true);
        Method getCCAMethod = CbPlanServiceImpl.class.getDeclaredMethod("getCCAFromOrg", String.class, ApiResponse.class);
        getCCAMethod.setAccessible(true);
        when(userAndOrgService.readUserProfileFromDB(eq("user123"), anyList()))
                .thenReturn(Map.of("id", "user123", "rootOrgId", "root1"));
        when(userAndOrgService.readOrgFromDB(eq("root1"), any()))
                .thenReturn(Map.of("isCCA", true));
        ApiResponse response = spyService.updateCbPlan(apiRequest, "org1", "token123", List.of("ROLE_ADMIN"));
        assertEquals(Constants.UPDATED, response.getResult().get(Constants.STATUS));
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

}
