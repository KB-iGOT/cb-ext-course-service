package com.igot.cb.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.PromotionalContentRuleCacheMgr;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.model.CachedAccessSettingRule;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PayloadValidation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.roaringbitmap.RoaringBitmap;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;
import static com.igot.cb.util.ProjectUtil.setFailedResponse;

/**
 * Service for managing promotional content access rules and retrieval.
 * Handles metadata upsert, user-based content filtering, and caching.
 */
@Service
@Slf4j
public class PromotionalContentServiceImpl implements IPromotionalContentService {

    private final AccessTokenValidator accessTokenValidator;
    private final PayloadValidation payloadValidation;
    private final ObjectMapper objectMapper;
    private final CassandraOperation cassandraOperation;
    private final AccessSettingMigrationServiceImpl accessSettingMigrationService;
    private final RedisCacheMgr redisCacheMgr;
    private final UserAndOrgServiceImpl userProfileServiceImpl;
    private final PromotionalContentRuleCacheMgr promotionalContentRuleCacheMgr;
    private final ContentInfoServiceImpl contentService;

    @Value("${promotional.content.read.fields}")
    private String contentReadFields;

    @Value("${promotional.content.user.cache.ttl.seconds}")
    private Integer promotionalContentUserCacheTtlSeconds;

    /**
     * Constructs the service with required dependencies.
     */
    public PromotionalContentServiceImpl(AccessTokenValidator accessTokenValidator, PayloadValidation payloadValidation, ObjectMapper objectMapper,
                                         CassandraOperation cassandraOperation, AccessSettingMigrationServiceImpl accessSettingMigrationService,
                                         RedisCacheMgr redisCacheMgr, UserAndOrgServiceImpl userProfileServiceImpl, PromotionalContentRuleCacheMgr promotionalContentRuleCacheMgr,
                                         ContentInfoServiceImpl contentService) {
        this.accessTokenValidator = accessTokenValidator;
        this.payloadValidation = payloadValidation;
        this.objectMapper = objectMapper;
        this.cassandraOperation = cassandraOperation;
        this.accessSettingMigrationService = accessSettingMigrationService;
        this.redisCacheMgr = redisCacheMgr;
        this.userProfileServiceImpl = userProfileServiceImpl;
        this.promotionalContentRuleCacheMgr = promotionalContentRuleCacheMgr;
        this.contentService = contentService;
    }

    /**
     * Creates or updates promotional content metadata with access control rules.
     * Validates auth token and payload before processing.
     *
     * @param userGroupDetails access control configuration with user groups and criteria
     * @param authToken        authentication token for user validation
     * @return ApiResponse with operation result and access control details
     */
    @Override
    public ApiResponse upsertPromotionalContentMetadata(Map<String, Object> userGroupDetails, String authToken) {
        log.info("PromotionalContentServiceImpl::upsertPromotionalContentMetadata:inside");
        ApiResponse response = ApiResponse.createDefaultResponse(Constants.API_PROMOTIONAL_CONTENT_METADATA_UPSERT);
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
        if (StringUtils.isEmpty(userId)) {
            return response;
        }
        String errMsg = payloadValidation.validateAccessControlPayload(userGroupDetails);
        if (StringUtils.isNotEmpty(errMsg)) {
            setFailedResponse(response, errMsg);
            return response;
        }
        try {
            processAndUpdateUserGroupDetailstoDB(userGroupDetails, response);
        } catch (Exception e) {
            log.error("Error while upserting access settings", e);
            setFailedResponse(response, "Failed to create access settings: " + e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
        return response;
    }

    /**
     * Processes user group details, generates UUIDs, and saves to database.
     * Converts criteria values to id lists via migration service before storage.
     *
     * @throws Exception if processing or database operation fails
     */
    private void processAndUpdateUserGroupDetailstoDB(Map<String, Object> userGroupDetails, ApiResponse response) throws Exception {
        Map<String, Object> createPayloadWithUuid = createUserGroupIds(userGroupDetails);
        Map<String, Object> accessRuleData = new HashMap<>();
        accessRuleData.put(Constants.CONTEXT_ID, userGroupDetails.get(Constants.CONTENT_ID));
        accessRuleData.put(Constants.CONTEXT_DATA, objectMapper.writeValueAsString(createPayloadWithUuid));
        accessRuleData.put(Constants.IS_ARCHIVED, false);
        if (accessSettingMigrationService.processAccessSettingRule(accessRuleData)) {
            cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD_COURSE,
                    Constants.PROMOTIONAL_CONTENT_RULES, accessRuleData);
            response.getResult().put(Constants.MSG, Constants.PROMOTIONAL_CONTENT_CREATED_RULES);
            Map<String, Object> payload = new HashMap<>();
            payload.put(Constants.ACCESS_CONTROL, createPayloadWithUuid.get(Constants.ACCESS_CONTROL));
            response.getResult().putAll(payload);
        } else {
            log.error("Failed to process access setting rule");
            setFailedResponse(response, "Failed to process access setting rule to id-map",
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Generates UUIDs for user groups without IDs.
     * Modifies payload in-place and returns it.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> createUserGroupIds(Map<String, Object> payload) {
        Object accessControlObj = payload.get(Constants.ACCESS_CONTROL);
        if (!(accessControlObj instanceof Map)) {
            return payload;
        }

        Map<String, Object> accessControl = (Map<String, Object>) accessControlObj;
        Object userGroupsObj = accessControl.get(Constants.USER_GROUPS);
        if (!(userGroupsObj instanceof List)) {
            return payload;
        }

        List<Map<String, Object>> userGroups = (List<Map<String, Object>>) userGroupsObj;
        for (Map<String, Object> userGroup : userGroups) {
            String id = (String) userGroup.get(Constants.USER_GROUP_ID);
            if (org.apache.commons.lang.StringUtils.isBlank(id)) {
                userGroup.put(Constants.USER_GROUP_ID, UUID.randomUUID().toString());
            }
        }

        return payload;
    }

    /**
     * Retrieves promotional content assigned to the authenticated user.
     * Checks cache first, then evaluates access rules against user profile.
     *
     * @param authToken authentication token
     * @return ApiResponse with list of accessible promotional content
     */
    @Override
    public ApiResponse getPromotionalContentForUsers(String authToken) {
        log.info("PromotionalContentServiceImpl::getPromotionalContentForUsers:inside");
        ApiResponse response = ApiResponse.createDefaultResponse(Constants.API_PROMOTIONAL_ASSIGNEDTO_USERS);

        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
        if (StringUtils.isEmpty(userId)) {
            return response;
        }
        String cachedCourseForUser = redisCacheMgr.getFromCache(Constants.PROMOTIONAL_CONTENT_KEY + userId, promotionalContentUserCacheTtlSeconds);
        if (StringUtils.isNotEmpty(cachedCourseForUser)) {
            return handleAndProcessCachedPromotionalContent(cachedCourseForUser, response, userId);
        }
        Map<String, Integer> userProfile = userProfileServiceImpl.getUserProfile(userId);
        List<Map<String, Object>> userCourses = new ArrayList<>();
        if (retrieveUserCourses(userProfile, userCourses)) {
            log.info("Promotional Content fetched: UserId: {} courses retrieved: {}", userId, userCourses.size());
            if (!userCourses.isEmpty()) {
                log.info("Promotional content found for userId: {}", userId);
                try {
                    redisCacheMgr.putInCache(Constants.PROMOTIONAL_CONTENT_KEY + userId,
                            objectMapper.writeValueAsString(userCourses),
                            promotionalContentUserCacheTtlSeconds);
                } catch (JsonProcessingException e) {
                    log.error("Error caching promotional content for userId: {}", userId, e);
                    response.updateErrorDetails("Fetching Promotional content failed due to an error", HttpStatus.INTERNAL_SERVER_ERROR);
                }
            } else {
                redisCacheMgr.putInCache(Constants.ACCESS_KEY + userId, Constants.NO_RECORDS_FOUND, promotionalContentUserCacheTtlSeconds);
            }
            response.getResult().put(Constants.CONTENT, userCourses);
        } else {
            response.getResult().put(Constants.CONTENT, new ArrayList<>());
        }
        return response;
    }

    /**
     * Handles cached promotional content retrieval and parsing.
     *
     * @param cachedCourseForUser cached JSON string of courses
     * @param response            ApiResponse to populate
     * @param userId              ID of the user
     * @return populated ApiResponse
     */
    private ApiResponse handleAndProcessCachedPromotionalContent(String cachedCourseForUser, ApiResponse response, String userId) {
        if (cachedCourseForUser.equalsIgnoreCase(Constants.NO_RECORDS_FOUND)) {
            response.getResult().put(Constants.CONTENT, new ArrayList<>());
            return response;
        }
        try {
            response.getResult().put(Constants.CONTENT, objectMapper.readValue(
                    cachedCourseForUser,
                    new TypeReference<List<Map<String, Object>>>() {
                    }
            ));
            log.info("PromotionalContent evalution: UserId:{} ", userId);
            return response;
        } catch (JsonProcessingException e) {
            setFailedResponse(response, "Failed to parse cached promotional content data",
                    HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
    }

    /**
     * Retrieves user courses by evaluating access setting rules against user profile.
     *
     * @param userProfile user profile with criteria values
     * @param userCourses list to populate with accessible courses
     * @return true if retrieval was successful, false otherwise
     */
    private boolean retrieveUserCourses(Map<String, Integer> userProfile, List<Map<String, Object>> userCourses) {
        Collection<CachedAccessSettingRule> cachedAccessSettingRules = promotionalContentRuleCacheMgr
                .getAccessSettingRules();
        if (cachedAccessSettingRules.isEmpty()) {
            log.error("No access setting rules found in cache");
            return false;
        }

        for (CachedAccessSettingRule rule : cachedAccessSettingRules) {
            Map<String, Object> accessSettingIdMap = (Map<String, Object>) rule.getContextData()
                    .get(Constants.ACCESS_CONTROL_ID);
            if (evaluateAccessSettingRule(accessSettingIdMap, userProfile)) {
                List<String> fieldsToFetch = new ArrayList<>(Arrays.asList(contentReadFields.split(",")));
                Map<String, Object> contentDetails = contentService.readContent(rule.getContextId(), fieldsToFetch);
                // Set default values for missing attributes
                if (MapUtils.isNotEmpty(contentDetails)) {
                    Map<String, Object> updateContentDetails = new HashMap<>(contentDetails);
                    updateContentDetails.putIfAbsent(Constants.AVG_RATING, 0.0);
                    updateContentDetails.putIfAbsent(Constants.PROGRAM_DURATION, 0);
                    updateContentDetails.putIfAbsent(Constants.NAME, "");
                    updateContentDetails.putIfAbsent(Constants.CREATOR_LOGO, "");
                    userCourses.add(updateContentDetails);
                }
            }
        }
        return true;
    }

    /**
     * Evaluates if the user profile matches any user group criteria in the access setting.
     *
     * @param accessSettingIdMap access setting data containing user groups and criteria
     * @param userProfile        user profile with criteria values
     * @return true if user has access, false otherwise
     */
    private boolean evaluateAccessSettingRule(Map<String, Object> accessSettingIdMap,
                                              Map<String, Integer> userProfile) {
        if (MapUtils.isEmpty(accessSettingIdMap) || MapUtils.isEmpty(userProfile)) {
            log.error("Access setting ID map or user profile is empty");
            return false;
        }
        List<Map<String, Object>> userGroups = (List<Map<String, Object>>) accessSettingIdMap
                .get(Constants.USER_GROUPS);
        if (CollectionUtils.isEmpty(userGroups)) {
            return false;
        }
        for (Map<String, Object> userGroup : userGroups) {
            String userGroupId = (String) userGroup.get(Constants.USER_GROUP_ID);
            boolean isUserHasAccess = false;
            List<Map<String, Object>> criteriaList = (List<Map<String, Object>>) userGroup
                    .get(Constants.USER_GROUP_CRITERIA_LIST);
            if (CollectionUtils.isEmpty(criteriaList)) {
                return true;
            }
            for (Map<String, Object> criteria : criteriaList) {
                String criteriaKey = criteria.get(Constants.CRITERIA_KEY).toString().toLowerCase();
                RoaringBitmap criteriaValue = (RoaringBitmap) criteria.get(Constants.CRITERIA_VALUE);
                Integer userCriteriaValue = userProfile.get(criteriaKey);
                if (userCriteriaValue == null || !criteriaValue.contains(userCriteriaValue)) {
                    log.info("User profile does not contain criteria key: {} in userGroup: {}", criteriaKey,
                            userGroupId);
                    isUserHasAccess = false;
                    break;
                } else {
                    isUserHasAccess = true;
                }
            }
            if (isUserHasAccess) {
                log.info("User profile does matches all criteria in userGroup: {}", userGroupId);
                return true;
            }
        }
        return false;
    }

    /**
     * Deletes promotional content metadata by marking it as archived.
     *
     * @param contentId ID of the promotional content to delete
     * @param authToken authentication token for user validation
     * @return ApiResponse with operation result
     */
    public ApiResponse delete(String contentId, String authToken) {
        log.info("PromotionalContentServiceImpl::delete:inside");
        ApiResponse response = ApiResponse.createDefaultResponse("api.promotionalcontent.metadata.delete");
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
        if (StringUtils.isEmpty(userId)) {
            return response;
        }
        if (StringUtils.isEmpty(contentId)) {
            log.error("Content ID is null or empty");
            setFailedResponse(response, "Content ID cannot be null or empty");
            return response;
        }
        try {
            Map<String, Object> accessRuleData = new HashMap<>();
            accessRuleData.put(Constants.CONTEXT_ID, contentId);
            accessRuleData.put(Constants.CONTEXT_DATA, "");
            accessRuleData.put(Constants.IS_ARCHIVED, true);
            String contextIdType = contentService.readCourseCategoryForContent(contentId);
            accessRuleData.put(Constants.CONTEXT_ID_TYPE, contextIdType);
            cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD_COURSE,
                    Constants.PROMOTIONAL_CONTENT_RULES, accessRuleData);
            response.setResponseCode(HttpStatus.OK);
            response.getResult().put(Constants.MSG, "Promotional Content Metadata deleted successfully");
            return response;
        } catch (Exception e) {
            log.error("Error while deleting accessRule:", e);
            setFailedResponse(response, "Failed to delete access settings: " + e);
            return response;
        }
    }

    @Override
    public ApiResponse read(String contentId, String authToken) {
        log.info("PromotionalContentServiceImpl::read:inside - contentId: {}", contentId);
        ApiResponse response = ApiResponse.createDefaultResponse(Constants.API_ACCESS_RULE_READ);
        try {
            String contextIdType = contentService.readCourseCategoryForContent(contentId);
            log.debug("Retrieved context ID type: {} for contentId: {}", contextIdType, contentId);
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.CONTEXT_ID, contentId);
            propertyMap.put(Constants.CONTEXT_ID_TYPE, contextIdType);
            List<String> fields = Arrays.asList(Constants.CONTEXT_ID, Constants.CONTEXT_DATA, Constants.IS_ARCHIVED);
            log.debug("Fetching promotional content setting rules from Cassandra for contentId: {}", contentId);
            List<Map<String, Object>> accessSettingRule = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD_COURSE, Constants.PROMOTIONAL_CONTENT_RULES, propertyMap,
                    fields, null);
            if (accessSettingRule.isEmpty()) {
                log.warn("No promotional content rules found for contentId: {}", contentId);
                setFailedResponse(response, Constants.NO_ACCESS_SETTINGS_FOUND, HttpStatus.OK);
                return response;
            }
            log.debug("Found {} promotional content setting rule(s) for contentId: {}", accessSettingRule.size(), contentId);
            Map<String, Object> contextDataMap = processContentSettingRecord(accessSettingRule.get(0), response);
            if (MapUtils.isNotEmpty(contextDataMap)) {
                response.setResult(contextDataMap);
            }
            return response;
        } catch (Exception e) {
            log.error("Error while reading promotional content rule for contentId: {}", contentId, e);
            setFailedResponse(response, "error while processing", HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
    }

    /**
     * Processing promotional content setting record.
     *
     * @param accessRecord the access setting record
     * @param response     the API response
     * @return parsed context data map, or null if error occurred
     */
    private Map<String, Object> processContentSettingRecord(Map<String, Object> accessRecord, ApiResponse response) {
        log.debug("Processing promotional content setting record");
        Boolean status = (Boolean) accessRecord.get(Constants.IS_ARCHIVED_KEY);
        log.debug("Promotional content setting archived status: {}", status);
        if (Boolean.TRUE.equals(status)) {
            log.warn("Promotional Content Access setting is archived, cannot retrieve");
            setFailedResponse(response, Constants.NO_ACCESS_SETTINGS_FOUND, HttpStatus.OK);
            return Collections.emptyMap();
        }
        Object contextDataObj = accessRecord.get(Constants.CONTEXT_DATA_KEY);
        if (!(contextDataObj instanceof String contextDataJson) ||
                StringUtils.isEmpty(contextDataJson)) {
            log.warn("Context data is empty or not a valid string");
            setFailedResponse(response, Constants.NO_ACCESS_SETTINGS_FOUND, HttpStatus.OK);
            return Collections.emptyMap();
        }
        log.debug("Context data found, proceeding to parse JSON");
        return parseAndSetContextData(contextDataJson, response);
    }

    /**
     * Parse context data JSON and return the parsed map.
     *
     * @param contextDataJson JSON string containing context data
     * @param response        the API response
     * @return parsed context data map, or null if error occurred
     */
    private Map<String, Object> parseAndSetContextData(String contextDataJson, ApiResponse response) {
        try {
            log.debug("Parsing context data JSON, length: {}", contextDataJson.length());
            Map<String, Object> contextDataMap = objectMapper.readValue(
                    contextDataJson, new TypeReference<Map<String, Object>>() {
                    });
            log.debug("Successfully parsed context data, entries: {}", contextDataMap.size());
            if (!contextDataMap.isEmpty()) {
                contextDataMap.remove(Constants.ACCESS_CONTROL_ID);
                log.debug("Removed ACCESS_CONTROL_ID from context data");
            }
            log.info("Successfully retrieved and set access settings in response");
            return contextDataMap;
        } catch (Exception e) {
            log.error("Failed to parse CONTEXT_DATA JSON: {}", contextDataJson, e);
            setFailedResponse(response, "error while processing", HttpStatus.INTERNAL_SERVER_ERROR);
            return Collections.emptyMap();
        }
    }
}
