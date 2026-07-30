package com.igot.cb.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.model.ApiRespParam;
import com.igot.cb.service.UserAndOrgServiceImpl;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for ProgramCoordinatorServiceImpl#upsertCoordinators.
 * Service is constructed directly via its constructor (constructor injection),
 * so config values (lookupTable, requiredRole, allowedTrainerTypes, etc.)
 * are passed in as plain constructor args — no reflection needed.
 *
 * NOTE: adjust the constructor argument list/order below to match the real
 * ProgramCoordinatorServiceImpl constructor signature exactly.
 */
@ExtendWith(MockitoExtension.class)
class ProgramCoordinatorServiceImplTest {

    private static final String PROGRAM_ID = "prog1";
    private static final String USER_ID_1 = "user1";
    private static final String USER_ID_2 = "user2";
    private static final String TOKEN = "dummy-token";
    private static final String REQUIRED_ROLE = "PROGRAM_COORDINATOR";
    private static final String LOOKUP_TABLE = "program_coordinator_lookup";
    private static final String CACHE_KEY_PREFIX = "program_coordinators:";
    private static final String SYNC_TOPIC = "cb.program.coordinator.sync";
    private static final List<String> ALLOWED_TYPES =
            List.of("NATIONAL_LEAD_TRAINER", "STATE_LEAD_TRAINER", "STATE_MASTER_TRAINER");


    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private RedisCacheMgr redisCacheMgr;

    @Mock
    private UserAndOrgServiceImpl userProfileService;

    @Mock
    private KafkaTemplate kafkaTemplate;

    @Mock
    private AccessTokenValidator accessTokenValidator;

    private ProgramCoordinatorServiceImpl service;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        service = new ProgramCoordinatorServiceImpl(
                cassandraOperation,
                redisCacheMgr,
                objectMapper,
                userProfileService,
                kafkaTemplate,
                accessTokenValidator
        );

        ReflectionTestUtils.setField(service, "lookupTable", LOOKUP_TABLE);
        ReflectionTestUtils.setField(service, "cacheKeyPrefix", CACHE_KEY_PREFIX);
        ReflectionTestUtils.setField(service, "coordinatorSyncTopic", SYNC_TOPIC);
        ReflectionTestUtils.setField(service, "requiredRole", REQUIRED_ROLE);
        ReflectionTestUtils.setField(service, "allowedTrainerTypes", ALLOWED_TYPES);
    }


    @Test
    void upsertCoordinators_shouldReturnBadRequest_whenCoordinatorsListIsEmpty() throws Exception {
        ApiResponse response = service.upsertCoordinators(PROGRAM_ID, Collections.emptyList(), TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        verifyNoInteractions(cassandraOperation, kafkaTemplate, accessTokenValidator);
    }

    @Test
    void upsertCoordinators_shouldReturnBadRequest_whenCoordinatorsListIsNull() throws Exception {
        ApiResponse response = service.upsertCoordinators(PROGRAM_ID, null, TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        verifyNoInteractions(cassandraOperation, kafkaTemplate, accessTokenValidator);
    }


    @Test
    void upsertCoordinators_shouldReturnForbidden_whenUserLacksRequiredRole() throws Exception {
        when(accessTokenValidator.fetchUserRolesFromToken(TOKEN)).thenReturn(List.of("PUBLIC"));

        ApiResponse response = service.upsertCoordinators(PROGRAM_ID, sampleCoordinators(), TOKEN);

        assertEquals(HttpStatus.FORBIDDEN, response.getResponseCode());
        verifyNoInteractions(cassandraOperation, kafkaTemplate);
    }

    @Test
    void upsertCoordinators_shouldReturnForbidden_whenTokenHasNoRoles() throws Exception {
        when(accessTokenValidator.fetchUserRolesFromToken(TOKEN)).thenReturn(Collections.emptyList());

        ApiResponse response = service.upsertCoordinators(PROGRAM_ID, sampleCoordinators(), TOKEN);

        assertEquals(HttpStatus.FORBIDDEN, response.getResponseCode());
    }

    @Test
    void upsertCoordinators_shouldProceed_whenUserHasRequiredRole() throws Exception {
        mockValidRole();
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList()))
                .thenReturn(successBulkInsertResponse());

        ApiResponse response = service.upsertCoordinators(PROGRAM_ID, sampleCoordinators(), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }



    @Test
    void upsertCoordinators_shouldReturnBadRequest_whenTrainerTypeIsInvalid() throws Exception {
        mockValidRole();

        List<Map<String, String>> coordinators = List.of(
                Map.of("userId", USER_ID_1, "trainerType", "SOME_INVALID_LABEL")
        );

        ApiResponse response = service.upsertCoordinators(PROGRAM_ID, coordinators, TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        verify(cassandraOperation, never()).insertBulkRecord(anyString(), anyString(), anyList());
    }

    @Test
    void upsertCoordinators_shouldAccept_allThreeValidTrainerTypes() throws Exception {
        mockValidRole();
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList()))
                .thenReturn(successBulkInsertResponse());

        List<Map<String, String>> coordinators = List.of(
                Map.of("userId", "u1", "trainerType", "NATIONAL_LEAD_TRAINER"),
                Map.of("userId", "u2", "trainerType", "STATE_LEAD_TRAINER"),
                Map.of("userId", "u3", "trainerType", "STATE_MASTER_TRAINER")
        );

        ApiResponse response = service.upsertCoordinators(PROGRAM_ID, coordinators, TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }



    @Test
    void upsertCoordinators_firstTime_shouldInsertAllAndDeleteNone() throws Exception {
        mockValidRole();
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList()))
                .thenReturn(successBulkInsertResponse());

        ApiResponse response = service.upsertCoordinators(PROGRAM_ID, sampleCoordinators(), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), anyMap());

        @SuppressWarnings("unchecked")
        Set<String> removed = (Set<String>) response.get("removed");
        assertTrue(removed.isEmpty());
    }

    @Test
    void upsertCoordinators_shouldReturnServerError_whenBulkInsertResponseIsNull() throws Exception {
        mockValidRole();
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList()))
                .thenReturn(null);

        ApiResponse response = service.upsertCoordinators(PROGRAM_ID, sampleCoordinators(), TOKEN);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        verify(kafkaTemplate, never()).send(anyString(), anyString());
        verify(cassandraOperation, never()).deleteRecord(anyString(), anyString(), anyMap());
    }

    @Test
    void upsertCoordinators_shouldReturnServerError_whenBulkInsertResponseIndicatesFailure() throws Exception {
        mockValidRole();
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList()))
                .thenReturn(failedBulkInsertResponse());

        ApiResponse response = service.upsertCoordinators(PROGRAM_ID, sampleCoordinators(), TOKEN);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }


    @Test
    void upsertCoordinators_shouldPublishKafkaEvent_onSuccess() throws Exception {
        mockValidRole();
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList()))
                .thenReturn(successBulkInsertResponse());

        service.upsertCoordinators(PROGRAM_ID, sampleCoordinators(), TOKEN);

        verify(kafkaTemplate, times(1)).send(eq(SYNC_TOPIC), anyString());
    }

    @Test
    void upsertCoordinators_shouldInvalidateReadCache_onSuccess() throws Exception {
        mockValidRole();
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList()))
                .thenReturn(successBulkInsertResponse());

        service.upsertCoordinators(PROGRAM_ID, sampleCoordinators(), TOKEN);

        verify(redisCacheMgr, times(1))
                .putInCache(eq(CACHE_KEY_PREFIX + PROGRAM_ID), eq(""), eq(1));
    }

    @Test
    void upsertCoordinators_responseShouldContainAddedOrUpdatedUserIds() throws Exception {
        mockValidRole();
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());
        when(cassandraOperation.insertBulkRecord(anyString(), anyString(), anyList()))
                .thenReturn(successBulkInsertResponse());

        ApiResponse response = service.upsertCoordinators(PROGRAM_ID, sampleCoordinators(), TOKEN);

        @SuppressWarnings("unchecked")
        Set<String> addedOrUpdated = (Set<String>) response.get("added_or_updated");
        assertEquals(2, addedOrUpdated.size());
        assertTrue(addedOrUpdated.containsAll(Set.of(USER_ID_1, USER_ID_2)));
    }

    @Test
    void upsertCoordinators_shouldReturnServerError_onUnexpectedException() throws Exception {
        mockValidRole();
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenThrow(new RuntimeException("Cassandra unavailable"));

        ApiResponse response = service.upsertCoordinators(PROGRAM_ID, sampleCoordinators(), TOKEN);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }



    private List<Map<String, String>> sampleCoordinators() {
        return List.of(
                Map.of("userId", USER_ID_1, "trainerType", "STATE_MASTER_TRAINER"),
                Map.of("userId", USER_ID_2, "trainerType", "NATIONAL_LEAD_TRAINER")
        );
    }

    private void mockValidRole() {
        when(accessTokenValidator.fetchUserRolesFromToken(TOKEN)).thenReturn(List.of(REQUIRED_ROLE));
    }

    /**
     * bulkInsertResponse success shape, matching:
     * bulkInsertResponse.getResult().get(Constants.RESPONSE) == Constants.SUCCESS
     */
    private ApiResponse successBulkInsertResponse() {
        ApiResponse resp = new ApiResponse();
        resp.setParams(new ApiRespParam(UUID.randomUUID().toString()));
        resp.put(Constants.RESPONSE, Constants.SUCCESS);
        resp.setResponseCode(HttpStatus.OK);
        return resp;
    }

    private ApiResponse failedBulkInsertResponse() {
        ApiResponse resp = new ApiResponse();
        resp.setParams(new ApiRespParam(UUID.randomUUID().toString()));
        resp.put(Constants.RESPONSE, Constants.FAILED);
        resp.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        return resp;
    }
}