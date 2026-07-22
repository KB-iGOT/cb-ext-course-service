package com.igot.cb.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.http.HttpStatus;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class OrgEligibilityServiceImplTest {

    @Mock
    private EsUtilService esUtilService;

    @Mock
    private CbExtServerProperties serverProperties;

    @Mock
    private ElasticsearchClient orgEligibilityElasticsearchClient;

    @Mock
    private AccessTokenValidator accessTokenValidator;

    private OrgEligibilityServiceImpl orgEligibilityService;

    private static final String AUTH_TOKEN = "test-token";
    private static final String ORG_ID = "0146196505889341440";

    @BeforeEach
    void setUp() {
        orgEligibilityService = new OrgEligibilityServiceImpl(esUtilService, serverProperties,
                orgEligibilityElasticsearchClient, accessTokenValidator);
        when(serverProperties.getOrgEligibilityIndex()).thenReturn("org_eligibility_alias");
        when(serverProperties.getElasticOrgEligibilityJsonPath())
                .thenReturn("/EsRequiredFields/EsRequiredFieldsOrgEligibility.json");
    }

    private Map<String, Object> buildValidRequest() {
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.ORG_ID, ORG_ID);
        request.put(Constants.COURSE_IDS, java.util.List.of("do_1145741849"));
        request.put(Constants.COURSE_COUNT, 1);
        request.put(Constants.VERSION, 1);
        request.put(Constants.UPDATED_AT, "2026-06-25T10:00:00Z");
        return request;
    }

    @Test
    void testUpsertOrgEligibility_UnauthorizedToken() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("");

        ApiResponse response = orgEligibilityService.upsertOrgEligibility(buildValidRequest(), AUTH_TOKEN);

        assertNotNull(response);
        verify(esUtilService, never()).addDocument(any(), anyString(), anyString(), anyString(), any(), anyString());
    }

    @Test
    void testUpsertOrgEligibility_MissingOrgId() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user1");

        ApiResponse response = orgEligibilityService.upsertOrgEligibility(new HashMap<>(), AUTH_TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        verify(esUtilService, never()).addDocument(any(), anyString(), anyString(), anyString(), any(), anyString());
    }

    @Test
    void testUpsertOrgEligibility_Success() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user1");
        when(esUtilService.addDocument(any(ElasticsearchClient.class), anyString(), anyString(), eqOrgId(),
                any(), anyString())).thenReturn("Successfully indexed document with id: Created");

        ApiResponse response = orgEligibilityService.upsertOrgEligibility(buildValidRequest(), AUTH_TOKEN);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(ORG_ID, response.getResult().get(Constants.ORG_ID));
        assertEquals(Constants.SUCCESS, response.getResult().get(Constants.STATUS));
    }

    @Test
    void testUpsertOrgEligibility_EsFailure() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user1");
        when(esUtilService.addDocument(any(ElasticsearchClient.class), anyString(), anyString(), eqOrgId(),
                any(), anyString())).thenReturn(null);

        ApiResponse response = orgEligibilityService.upsertOrgEligibility(buildValidRequest(), AUTH_TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testUpsertOrgEligibility_ExceptionHandling() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user1");
        when(serverProperties.getOrgEligibilityIndex()).thenThrow(new RuntimeException("config error"));

        ApiResponse response = orgEligibilityService.upsertOrgEligibility(buildValidRequest(), AUTH_TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testReadOrgEligibility_UnauthorizedToken() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("");

        ApiResponse response = orgEligibilityService.readOrgEligibility(ORG_ID, AUTH_TOKEN);

        assertNotNull(response);
        verify(esUtilService, never()).getDocumentById(any(), anyString(), anyString());
    }

    @Test
    void testReadOrgEligibility_BlankOrgId() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user1");

        ApiResponse response = orgEligibilityService.readOrgEligibility("", AUTH_TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        verify(esUtilService, never()).getDocumentById(any(), anyString(), anyString());
    }

    @Test
    void testReadOrgEligibility_NotFound() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user1");
        when(esUtilService.getDocumentById(any(ElasticsearchClient.class), anyString(), eqOrgId())).thenReturn(null);

        ApiResponse response = orgEligibilityService.readOrgEligibility(ORG_ID, AUTH_TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testReadOrgEligibility_Success() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user1");
        Map<String, Object> document = buildValidRequest();
        when(esUtilService.getDocumentById(any(ElasticsearchClient.class), anyString(), eqOrgId()))
                .thenReturn(document);

        ApiResponse response = orgEligibilityService.readOrgEligibility(ORG_ID, AUTH_TOKEN);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(ORG_ID, response.getResult().get(Constants.ORG_ID));
        assertEquals(1, response.getResult().get(Constants.COURSE_COUNT));
    }

    @Test
    void testReadOrgEligibility_ExceptionHandling() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString(), any())).thenReturn("user1");
        when(serverProperties.getOrgEligibilityIndex()).thenThrow(new RuntimeException("config error"));

        ApiResponse response = orgEligibilityService.readOrgEligibility(ORG_ID, AUTH_TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    private String eqOrgId() {
        return org.mockito.ArgumentMatchers.eq(ORG_ID);
    }
}
