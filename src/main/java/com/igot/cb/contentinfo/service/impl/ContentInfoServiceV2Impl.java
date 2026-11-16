package com.igot.cb.contentinfo.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.contentinfo.service.ContentInfoServiceV2;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ContentInfoUtil;
import com.igot.cb.util.UserProfileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * V2 implementation of {@link ContentInfoServiceV2}.
 */
@Service
public class ContentInfoServiceV2Impl implements ContentInfoServiceV2 {

    private static final Logger log = LoggerFactory.getLogger(ContentInfoServiceV2Impl.class);

    private final AccessTokenValidator accessTokenValidator;
    private final RedisCacheMgr redisCacheMgr;
    private final ObjectMapper objectMapper;
    private final ContentInfoUtil contentInfoUtil;
    private final UserProfileUtil userProfileUtil;

    /**
     * @param accessTokenValidator validates the auth token and extracts userId
     * @param redisCacheMgr        Redis cache for the assembled content-info result
     * @param objectMapper         JSON serialiser / deserialiser
     * @param contentInfoUtil      shared util for CB plan, enrolment, CA program, assessment and moderated content lookups
     * @param userProfileUtil      Cassandra-backed user profile lookup (provides rootOrgId)
     */
    public ContentInfoServiceV2Impl(AccessTokenValidator accessTokenValidator,
                                    RedisCacheMgr redisCacheMgr,
                                    ObjectMapper objectMapper,
                                    ContentInfoUtil contentInfoUtil,
                                    UserProfileUtil userProfileUtil) {
        this.accessTokenValidator = accessTokenValidator;
        this.redisCacheMgr = redisCacheMgr;
        this.objectMapper = objectMapper;
        this.contentInfoUtil = contentInfoUtil;
        this.userProfileUtil = userProfileUtil;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ApiResponse getContentInfo(String authToken) {
        ApiResponse response = ApiResponse.createDefaultResponse(Constants.API_CONTENT_INFO_V2);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (!StringUtils.hasText(userId)) {
                return response;
            }
            Map<String, String> userProfile = userProfileUtil.buildUserProfile(userId, response);
            String rootOrgId = userProfile.get(Constants.USER_ROOT_ORG_ID);
            if (!StringUtils.hasText(rootOrgId)) {
                log.warn("getContentInfo: rootOrgId not found for userId={}", userId);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.ERR_USER_ORG_NOT_FOUND);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            response.setResult(new HashMap<>(getPersonalContentInfoFromCacheOrApi(userId, rootOrgId, authToken)));
        } catch (JsonProcessingException e) {
            handleException(response, e);
        }
        return response;
    }

    private Map<String, Object> getPersonalContentInfoFromCacheOrApi(String userId, String orgId,
                                                                      String authToken) throws JsonProcessingException {
        String redisKey = Constants.PERSONAL_CONTENT_INFO_REDIS_KEY_PREFIX + userId;
        String cached = redisCacheMgr.getFromCache(redisKey);
        if (StringUtils.hasText(cached)) {
            log.info("personalContentInfo cache HIT for userId: {}", userId);
            return objectMapper.readValue(cached, new TypeReference<Map<String, Object>>() {});
        }
        log.info("personalContentInfo cache MISS for userId: {}", userId);
        Map<String, Object> contentInfoMap = buildPersonalContentInfoV3(userId, orgId, authToken);
        redisCacheMgr.putInCache(redisKey, objectMapper.writeValueAsString(contentInfoMap));
        log.info("personalContentInfo cached for userId: {}", userId);
        return contentInfoMap;
    }

    /**
     * Assembles the full personal content-info map for the user.
     *
     * @param userId    user's ID
     * @param orgId     user's organisation ID
     * @param authToken user's auth token
     * @return assembled content info map containing counts and content-id lists per category
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> buildPersonalContentInfoV3(String userId, String orgId, String authToken) {
        Map<String, List<String>> cbPlanIds = contentInfoUtil.getCbPlanV4ContentIds(authToken);
        List<String> aparIds = cbPlanIds.get(Constants.APAR);
        List<String> trainingPlanIds = cbPlanIds.get(Constants.TRAINING_PLAN);
        List<String> aiCbpIds = cbPlanIds.get(Constants.AI_CBP);
        List<String> learningPathwayIds = contentInfoUtil.getAssignedCourseIds(userId, Constants.LEARNING_PATHWAY, authToken);
        Map<String, Map<String, Object>> enrolmentDictionary = contentInfoUtil.callEnrolmentDictionaryApi(authToken);
        List<String> caProgramIds = contentInfoUtil.getFilteredCaProgramIdentifiers(userId, authToken, enrolmentDictionary);
        List<String> standaloneAssessmentIds = contentInfoUtil.getStandaloneAssessmentIdentifiersFromSystem();
        Map<String, Map<String, Object>> enrollmentDetails = contentInfoUtil.callAssessmentEnrollmentDetailsApi(authToken, standaloneAssessmentIds);
        List<String> standaloneIds = contentInfoUtil.filterStandaloneAssessmentIdentifiers(standaloneAssessmentIds, enrollmentDetails);
        Map<String, Object> moderatedContent = fetchModeratedContent(userId, orgId);
        List<String> moderatedContentIds = (List<String>) moderatedContent.get(Constants.IDENTIFIERS);
        Object moderatedContentCount = moderatedContent.get(Constants.COUNT);
        Map<String, Object> contentIds = new HashMap<>();
        contentIds.put(Constants.TRAINING_PLAN, trainingPlanIds);
        contentIds.put(Constants.APAR, aparIds);
        contentIds.put(Constants.AI_CBP, aiCbpIds);
        contentIds.put(Constants.LEARNING_PATHWAY_FIELD, learningPathwayIds);
        contentIds.put(Constants.STANDALONE_ASSESSMENT, standaloneIds);
        contentIds.put(Constants.CA_PROGRAM, caProgramIds);
        contentIds.put(Constants.MODERATED_CONTENT, moderatedContentIds);
        Map<String, Object> map = new HashMap<>();
        map.put(Constants.TRAINING_PLAN, trainingPlanIds.size());
        map.put(Constants.APAR, aparIds.size());
        map.put(Constants.AI_CBP, aiCbpIds.size());
        map.put(Constants.CA_PROGRAM, caProgramIds.size());
        map.put(Constants.LEARNING_PATHWAY_FIELD, learningPathwayIds.size());
        map.put(Constants.STANDALONE_ASSESSMENT, standaloneIds.size());
        map.put(Constants.MODERATED_CONTENT, moderatedContentCount);
        map.put(Constants.CONTENT_IDS, contentIds);
        return map;
    }

    /**
     * Fetches moderated content identifiers; returns an empty result on {@link IOException}
     * so a cache/deserialisation failure never aborts the full content-info assembly.
     */
    private Map<String, Object> fetchModeratedContent(String userId, String orgId) {
        try {
            return contentInfoUtil.getModeratedContentIdentifiers(userId, orgId);
        } catch (IOException e) {
            log.error("fetchModeratedContent: failed for userId={}, orgId={}", userId, orgId, e);
            Map<String, Object> empty = new HashMap<>();
            empty.put(Constants.IDENTIFIERS, new ArrayList<>());
            empty.put(Constants.COUNT, 0);
            return empty;
        }
    }

    private void handleException(ApiResponse response, JsonProcessingException e) {
        log.error("getContentInfo: failed - {}", e.getMessage(), e);
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(e.getMessage());
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
