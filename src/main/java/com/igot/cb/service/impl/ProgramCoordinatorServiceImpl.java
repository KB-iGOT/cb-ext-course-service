package com.igot.cb.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.ProgramCoordinatorService;
import com.igot.cb.service.UserAndOrgServiceImpl;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static com.igot.cb.util.Constants.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProgramCoordinatorServiceImpl implements ProgramCoordinatorService {


    private final CassandraOperation cassandraOperation;
    private final RedisCacheMgr redisCacheMgr;
    private final ObjectMapper objectMapper;
    private final UserAndOrgServiceImpl userProfileService;
    private final KafkaTemplate kafkaTemplate;
    private final AccessTokenValidator accessTokenValidator;

    @Value("${program.coordinator.lookup.table:program_coordinator_lookup}")
    private String lookupTable;

    @Value("${program.coordinator.cache.key.prefix:program_coordinators:}")
    private String cacheKeyPrefix;

    @Value("${program.coordinator.cache.ttl.seconds:60}")
    private int cacheTtlSeconds;

    @Value("${program.coordinator.sync.topic}")
    private String coordinatorSyncTopic;

    @Value("${program.coordinator.required.role}")
    private String requiredRole;

    @Value("#{'${program.coordinator.allowed.trainer.types}'.split(',')}")
    private List<String> allowedTrainerTypes;

    @Override
    public ApiResponse upsertCoordinators(String programId, List<Map<String, String>> incomingCoordinators, String authUserToken) throws IOException {
        ApiResponse response = ApiResponse.createDefaultResponse("api.program.coordinator.upsert");

        if (CollectionUtils.isEmpty(incomingCoordinators)) {
            response.updateErrorDetails("programId and coordinators are required", HttpStatus.BAD_REQUEST);
            return response;
        }

        List<String> userRoles = accessTokenValidator.fetchUserRolesFromToken(authUserToken);
        if (!userRoles.contains(requiredRole)) {
            response.updateErrorDetails("User does not have the required role: " + requiredRole, HttpStatus.FORBIDDEN);
            return response;
        }

        if (!validateTrainerTypes(incomingCoordinators, response)) {
            return response;
        }

        try {
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(PROGRAM_ID_KEY, programId);
            List<Map<String, Object>> existingRows = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD_COURSES, lookupTable, propertyMap, null, null);

            Set<String> existingUserIds = existingRows.stream()
                    .map(row -> (String) row.get(USER_ID))
                    .collect(Collectors.toSet());
            Set<String> incomingUserIds = incomingCoordinators.stream()
                    .map(c -> c.get(USER_ID))
                    .collect(Collectors.toSet());

            Set<String> toRemove = new HashSet<>(existingUserIds);
            toRemove.removeAll(incomingUserIds);

            List<Map<String, Object>> lookupMaps = new ArrayList<>();
            for (Map<String, String> coordinator : incomingCoordinators) {
                Map<String, Object> lookupMap = new HashMap<>();
                lookupMap.put(PROGRAM_ID_KEY, programId);
                lookupMap.put(USER_ID_KEY, coordinator.get(USER_ID));
                lookupMap.put(TRAINER_TYPE_KEY, coordinator.get(TRAINER_TYPE));
                lookupMap.put(CREATED_AT_FIELD, Instant.now().toString());
                lookupMaps.add(lookupMap);
            }

            ApiResponse bulkInsertResponse = cassandraOperation.insertBulkRecord(
                    Constants.KEYSPACE_SUNBIRD_COURSES, lookupTable, lookupMaps);

            if (bulkInsertResponse == null
                    || !Constants.SUCCESS.equalsIgnoreCase(
                    String.valueOf(bulkInsertResponse.getResult().get(Constants.RESPONSE)))) {
                log.error("Bulk insert failed for programId: {}", programId);
                response.updateErrorDetails("Failed to insert coordinators", HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

            for (String userId : toRemove) {
                Map<String, Object> keyMap = new HashMap<>();
                keyMap.put(PROGRAM_ID_KEY, programId);
                keyMap.put(USER_ID_KEY, userId);
                cassandraOperation.deleteRecord(Constants.KEYSPACE_SUNBIRD_COURSES, lookupTable, keyMap);
            }

            Map<String, Object> event = new HashMap<>();
            event.put(PROGRAM_ID, programId);
            event.put(COORDINATORS, incomingCoordinators);
            event.put(REMOVED, toRemove);
            event.put(EVENT_TYPE, "COORDINATOR_LIST_SYNCED");
            event.put(TIMESTAMP, Instant.now().toString());
            kafkaTemplate.send(coordinatorSyncTopic, objectMapper.writeValueAsString(event));

            redisCacheMgr.putInCache(cacheKeyPrefix + programId, "", 1);

            response.put(PROGRAM_ID, programId);
            response.put(ADDED_OR_UPDATED, incomingUserIds);
            response.put(REMOVED, toRemove);
        } catch (Exception e) {
            log.error("Failed to upsert coordinators for programId: {}", programId, e);
            response.updateErrorDetails("Failed to upsert coordinators: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return response;
    }

    @Override
    public ApiResponse getProgramCoordinators(String programId, String authUserToken) throws IOException {
        ApiResponse response = ApiResponse.createDefaultResponse("api.program.coordinators.read");

        List<String> userRoles = accessTokenValidator.fetchUserRolesFromToken(authUserToken);
        if (!userRoles.contains(requiredRole)) {
            response.updateErrorDetails("User does not have the required role: " + requiredRole, HttpStatus.FORBIDDEN);
            return response;
        }

        try {
            List<Map<String, Object>> coordinators = null;

            String cached = redisCacheMgr.getFromCache(cacheKeyPrefix + programId);
            if (cached != null && !cached.isEmpty()) {
                coordinators = objectMapper.readValue(cached, new TypeReference<List<Map<String, Object>>>() {});
                log.info("Program coordinators cache hit for programId: {}", programId);
            }

            if (coordinators == null) {
                log.info("Program coordinators cache miss for programId: {} — querying Cassandra", programId);
                Map<String, Object> propertyMap = new HashMap<>();
                propertyMap.put("program_id", programId);
                List<Map<String, Object>> rows = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD_COURSES, lookupTable, propertyMap, null, null);

                coordinators = new ArrayList<>();
                for (Map<String, Object> row : rows) {
                    Map<String, Object> entry = new HashMap<>();
                    entry.put(Constants.USER_ID, row.get(Constants.USER_ID));
                    entry.put(TRAINER_TYPE, row.get(TRAINER_TYPE));
                    coordinators.add(entry);
                }

                redisCacheMgr.putInCache(
                        cacheKeyPrefix + programId,
                        objectMapper.writeValueAsString(coordinators),
                        cacheTtlSeconds
                );
            }

            for (Map<String, Object> coordinator : coordinators) {
                String userId = (String) coordinator.get(USER_ID);
                try {
                    Map<String, Object> userProfile = userProfileService.readUserProfile(userId, Arrays.asList(Constants.ID, Constants.PROFILE_DETAILS));

                    Object profileDetailsRaw = userProfile.get(Constants.PROFILE_DETAILS);
                    if (profileDetailsRaw != null) {
                        Map<String, Object> profileDetails = profileDetailsRaw instanceof String
                                ? objectMapper.readValue((String) profileDetailsRaw, new TypeReference<Map<String, Object>>() {})
                                : (Map<String, Object>) profileDetailsRaw;

                        Object personalDetailsObj = profileDetails.get(Constants.PERSONAL_DETAILS);
                        if (personalDetailsObj instanceof Map<?, ?> personalDetails) {
                            Object emailObj = personalDetails.get(Constants.PRIMARY_EMAIL);
                            if (emailObj instanceof String email) {
                                coordinator.put(EMAIL, email);
                            }
                        }

                        Object firstName = profileDetails.get(Constants.USER_FIRST_NAME);
                        if (firstName != null) {
                            coordinator.put("name", firstName.toString());
                        }
                    }
                } catch (Exception e) {
                    log.error("Failed to enrich coordinator profile for userId: {}", userId, e);
                }
            }

            response.put(PROGRAM_ID, programId);
            response.put(COORDINATORS, coordinators);
        } catch (Exception e) {
            response.updateErrorDetails("Failed to fetch coordinators: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return response;
    }

    private boolean validateTrainerTypes(
            List<Map<String, String>> incomingCoordinators,
            ApiResponse response) {

        Set<String> incomingTrainerTypes = incomingCoordinators.stream()
                .map(c -> c.get(TRAINER_TYPE))
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toSet());

        Set<String> invalidTrainerTypes = incomingTrainerTypes.stream()
                .filter(type -> !allowedTrainerTypes.contains(type))
                .collect(Collectors.toSet());

        if (!invalidTrainerTypes.isEmpty()) {
            response.updateErrorDetails(
                    "Invalid trainerType(s): " + invalidTrainerTypes
                            + ". Allowed values are: " + allowedTrainerTypes,
                    HttpStatus.BAD_REQUEST);
            return false;
        }

        return true;
    }

}
