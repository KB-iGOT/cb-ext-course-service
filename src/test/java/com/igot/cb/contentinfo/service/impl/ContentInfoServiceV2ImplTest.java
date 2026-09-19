package com.igot.cb.contentinfo.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ContentInfoUtil;
import com.igot.cb.util.UserProfileUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ContentInfoServiceV2ImplTest {

    private static final String AUTH_TOKEN = "Bearer test-token";
    private static final String USER_ID = "user-abc";
    private static final String ROOT_ORG_ID = "org-xyz";
    private static final String REDIS_KEY = Constants.PERSONAL_CONTENT_INFO_REDIS_KEY_PREFIX + USER_ID;
    private static final String APAR_CONTENT_ID = "do_apar_1";
    private static final String TP_CONTENT_ID = "do_tp_1";
    private static final String AI_CONTENT_ID = "do_ai_1";
    private static final String LP_CONTENT_ID = "do_lp_1";
    private static final String SA_CONTENT_ID = "do_sa_1";
    private static final String CA_CONTENT_ID = "do_ca_1";
    private static final String MOD_CONTENT_ID = "do_mod_1";

    @Mock
    private AccessTokenValidator accessTokenValidator;
    @Mock
    private RedisCacheMgr redisCacheMgr;
    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();
    @Mock
    private ContentInfoUtil contentInfoUtil;
    @Mock
    private UserProfileUtil userProfileUtil;

    private ContentInfoServiceV2Impl service;

    @BeforeEach
    void setUp() {
        service = new ContentInfoServiceV2Impl(
                accessTokenValidator, redisCacheMgr, objectMapper, contentInfoUtil, userProfileUtil);
    }

    /** Verifies that a null userId from the token validator triggers an immediate return with no downstream calls. */
    @Test
    void getContentInfo_nullUserId_returnsEarlyWithoutCallingDownstream() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(null);

        ApiResponse response = service.getContentInfo(AUTH_TOKEN);

        assertThat(response).isNotNull();
        verifyNoInteractions(userProfileUtil, contentInfoUtil, redisCacheMgr);
    }

    /** Verifies that a blank userId (empty string) from the token validator triggers an immediate return with no downstream calls. */
    @Test
    void getContentInfo_emptyUserId_returnsEarlyWithoutCallingDownstream() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn("");

        ApiResponse response = service.getContentInfo(AUTH_TOKEN);

        assertThat(response).isNotNull();
        verifyNoInteractions(userProfileUtil, contentInfoUtil, redisCacheMgr);
    }

    /** Verifies that a missing rootOrgId in the user profile results in a 400 BAD_REQUEST with ERR_USER_ORG_NOT_FOUND. */
    @Test
    void getContentInfo_rootOrgIdMissing_returnsBadRequest() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(userProfileUtil.buildUserProfile(eq(USER_ID), any(ApiResponse.class)))
                .thenReturn(Collections.emptyMap());

        ApiResponse response = service.getContentInfo(AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getParams().getStatus()).isEqualTo(Constants.FAILED);
        assertThat(response.getParams().getErr()).isEqualTo(Constants.ERR_USER_ORG_NOT_FOUND);
        verifyNoInteractions(redisCacheMgr, contentInfoUtil);
    }

    /** Verifies that a Redis cache hit short-circuits the build path — result is deserialized directly and ContentInfoUtil is never invoked. */
    @Test
    void getContentInfo_cacheHit_returnsDeserializedResultWithoutCallingUtil() throws Exception {
        Map<String, Object> cached = new HashMap<>();
        cached.put(Constants.APAR, 2);
        cached.put(Constants.TRAINING_PLAN, 1);
        String cachedJson = objectMapper.writeValueAsString(cached);

        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(userProfileUtil.buildUserProfile(eq(USER_ID), any(ApiResponse.class)))
                .thenReturn(Map.of(Constants.USER_ROOT_ORG_ID, ROOT_ORG_ID));
        when(redisCacheMgr.getFromCache(REDIS_KEY)).thenReturn(cachedJson);

        ApiResponse response = service.getContentInfo(AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getResult())
                .containsEntry(Constants.APAR, 2)
                .containsEntry(Constants.TRAINING_PLAN, 1);
        verifyNoInteractions(contentInfoUtil);
    }

    /** Verifies that a Redis cache miss triggers the full content-info build and the result is written back to the cache. */
    @Test
    void getContentInfo_cacheMiss_buildsResultAndCachesIt() throws IOException {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(userProfileUtil.buildUserProfile(eq(USER_ID), any(ApiResponse.class)))
                .thenReturn(Map.of(Constants.USER_ROOT_ORG_ID, ROOT_ORG_ID));
        when(redisCacheMgr.getFromCache(REDIS_KEY)).thenReturn(null);
        stubAllUtilMethods();

        ApiResponse response = service.getContentInfo(AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getResult()).containsKey(Constants.APAR);
        verify(redisCacheMgr).putInCache(eq(REDIS_KEY), anyString());
    }

    /** Verifies that malformed JSON in the Redis cache causes a JsonProcessingException which is caught and mapped to a 500 INTERNAL_SERVER_ERROR. */
    @Test
    void getContentInfo_invalidCachedJson_returnsInternalServerError() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(eq(AUTH_TOKEN), any(ApiResponse.class)))
                .thenReturn(USER_ID);
        when(userProfileUtil.buildUserProfile(eq(USER_ID), any(ApiResponse.class)))
                .thenReturn(Map.of(Constants.USER_ROOT_ORG_ID, ROOT_ORG_ID));
        when(redisCacheMgr.getFromCache(REDIS_KEY)).thenReturn("{not-valid-json:::::");

        ApiResponse response = service.getContentInfo(AUTH_TOKEN);

        assertThat(response.getResponseCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getParams().getStatus()).isEqualTo(Constants.FAILED);
        assertThat(response.getParams().getErr()).isNotBlank();
    }

    /** Verifies that the assembled result map contains all eight expected top-level keys (seven category counts + contentIds). */
    @Test
    void buildPersonalContentInfoV3_allCategoriesPresent_returnsCompleteMap() throws IOException {
        stubAllUtilMethods();

        Map<String, Object> result = service.buildPersonalContentInfoV3(USER_ID, ROOT_ORG_ID, AUTH_TOKEN);

        assertThat(result).containsKeys(
                Constants.APAR,
                Constants.TRAINING_PLAN,
                Constants.AI_CBP,
                Constants.CA_PROGRAM,
                Constants.LEARNING_PATHWAY_FIELD,
                Constants.STANDALONE_ASSESSMENT,
                Constants.MODERATED_CONTENT,
                Constants.CONTENT_IDS);
    }

    /** Verifies that each category count in the result equals the size of the corresponding identifier list returned by ContentInfoUtil. */
    @Test
    void buildPersonalContentInfoV3_countsMappedFromListSizes() throws IOException {
        Map<String, List<String>> cbPlanIds = new HashMap<>();
        cbPlanIds.put(Constants.APAR, List.of(APAR_CONTENT_ID, "do_apar_2"));
        cbPlanIds.put(Constants.TRAINING_PLAN, List.of(TP_CONTENT_ID));
        cbPlanIds.put(Constants.AI_CBP, List.of(AI_CONTENT_ID, "do_ai_2", "do_ai_3"));

        when(contentInfoUtil.getCbPlanV4ContentIds(AUTH_TOKEN)).thenReturn(cbPlanIds);
        when(contentInfoUtil.getAssignedCourseIds(USER_ID, Constants.LEARNING_PATHWAY, AUTH_TOKEN))
                .thenReturn(List.of(LP_CONTENT_ID));
        when(contentInfoUtil.callEnrolmentDictionaryApi(AUTH_TOKEN)).thenReturn(Collections.emptyMap());
        when(contentInfoUtil.getFilteredCaProgramIdentifiers(eq(USER_ID), eq(AUTH_TOKEN), any()))
                .thenReturn(List.of(CA_CONTENT_ID, "do_ca_2"));
        when(contentInfoUtil.getStandaloneAssessmentIdentifiersFromSystem()).thenReturn(List.of(SA_CONTENT_ID));
        when(contentInfoUtil.callAssessmentEnrollmentDetailsApi(eq(AUTH_TOKEN), any()))
                .thenReturn(Collections.emptyMap());
        when(contentInfoUtil.filterStandaloneAssessmentIdentifiers(any(), any()))
                .thenReturn(List.of(SA_CONTENT_ID));
        when(contentInfoUtil.getModeratedContentIdentifiers(USER_ID, ROOT_ORG_ID))
                .thenReturn(moderatedResult(List.of(MOD_CONTENT_ID), 1));

        Map<String, Object> result = service.buildPersonalContentInfoV3(USER_ID, ROOT_ORG_ID, AUTH_TOKEN);

        assertThat(result)
                .containsEntry(Constants.APAR, 2)
                .containsEntry(Constants.TRAINING_PLAN, 1)
                .containsEntry(Constants.AI_CBP, 3)
                .containsEntry(Constants.CA_PROGRAM, 2)
                .containsEntry(Constants.LEARNING_PATHWAY_FIELD, 1)
                .containsEntry(Constants.STANDALONE_ASSESSMENT, 1)
                .containsEntry(Constants.MODERATED_CONTENT, 1);
    }

    /** Verifies that the nested contentIds map carries the correct identifier list for every category, including moderated content. */
    @Test
    void buildPersonalContentInfoV3_contentIdsMapPopulatedWithCorrectLists() throws IOException {
        Map<String, List<String>> cbPlanIds = new HashMap<>();
        cbPlanIds.put(Constants.APAR, List.of(APAR_CONTENT_ID));
        cbPlanIds.put(Constants.TRAINING_PLAN, List.of(TP_CONTENT_ID));
        cbPlanIds.put(Constants.AI_CBP, List.of(AI_CONTENT_ID));

        when(contentInfoUtil.getCbPlanV4ContentIds(AUTH_TOKEN)).thenReturn(cbPlanIds);
        when(contentInfoUtil.getAssignedCourseIds(USER_ID, Constants.LEARNING_PATHWAY, AUTH_TOKEN))
                .thenReturn(List.of(LP_CONTENT_ID));
        when(contentInfoUtil.callEnrolmentDictionaryApi(AUTH_TOKEN)).thenReturn(Collections.emptyMap());
        when(contentInfoUtil.getFilteredCaProgramIdentifiers(eq(USER_ID), eq(AUTH_TOKEN), any()))
                .thenReturn(List.of(CA_CONTENT_ID));
        when(contentInfoUtil.getStandaloneAssessmentIdentifiersFromSystem()).thenReturn(List.of(SA_CONTENT_ID));
        when(contentInfoUtil.callAssessmentEnrollmentDetailsApi(eq(AUTH_TOKEN), any()))
                .thenReturn(Collections.emptyMap());
        when(contentInfoUtil.filterStandaloneAssessmentIdentifiers(any(), any()))
                .thenReturn(List.of(SA_CONTENT_ID));
        when(contentInfoUtil.getModeratedContentIdentifiers(USER_ID, ROOT_ORG_ID))
                .thenReturn(moderatedResult(List.of(MOD_CONTENT_ID), 1));
        Map<String, Object> result = service.buildPersonalContentInfoV3(USER_ID, ROOT_ORG_ID, AUTH_TOKEN);
        Map<String, Object> contentIds = (Map<String, Object>) result.get(Constants.CONTENT_IDS);
        assertThat((List<String>) contentIds.get(Constants.APAR)).containsExactly(APAR_CONTENT_ID);
        assertThat((List<String>) contentIds.get(Constants.TRAINING_PLAN)).containsExactly(TP_CONTENT_ID);
        assertThat((List<String>) contentIds.get(Constants.AI_CBP)).containsExactly(AI_CONTENT_ID);
        assertThat((List<String>) contentIds.get(Constants.LEARNING_PATHWAY_FIELD)).containsExactly(LP_CONTENT_ID);
        assertThat((List<String>) contentIds.get(Constants.CA_PROGRAM)).containsExactly(CA_CONTENT_ID);
        assertThat((List<String>) contentIds.get(Constants.STANDALONE_ASSESSMENT)).containsExactly(SA_CONTENT_ID);
        assertThat((List<String>) contentIds.get(Constants.MODERATED_CONTENT)).containsExactly(MOD_CONTENT_ID);
    }

    /** Verifies that an IOException from getModeratedContentIdentifiers is absorbed by fetchModeratedContent, returning an empty list and count 0 without aborting the full assembly. */
    @Test
    @SuppressWarnings("unchecked")
    void buildPersonalContentInfoV3_moderatedContentThrowsIoException_fallsBackToEmptyResult() throws IOException {
        stubUtilMethodsExceptModerated();
        when(contentInfoUtil.getModeratedContentIdentifiers(USER_ID, ROOT_ORG_ID))
                .thenThrow(new IOException("storage error"));

        Map<String, Object> result = service.buildPersonalContentInfoV3(USER_ID, ROOT_ORG_ID, AUTH_TOKEN);

        Map<String, Object> contentIds = (Map<String, Object>) result.get(Constants.CONTENT_IDS);
        assertThat((List<String>) contentIds.get(Constants.MODERATED_CONTENT)).isEmpty();
        assertThat(result).containsEntry(Constants.MODERATED_CONTENT, 0);
    }

    private void stubAllUtilMethods() throws IOException {
        stubUtilMethodsExceptModerated();
        when(contentInfoUtil.getModeratedContentIdentifiers(USER_ID, ROOT_ORG_ID))
                .thenReturn(moderatedResult(Collections.emptyList(), 0));
    }

    private void stubUtilMethodsExceptModerated() {
        Map<String, List<String>> cbPlanIds = new HashMap<>();
        cbPlanIds.put(Constants.APAR, List.of(APAR_CONTENT_ID));
        cbPlanIds.put(Constants.TRAINING_PLAN, List.of(TP_CONTENT_ID));
        cbPlanIds.put(Constants.AI_CBP, Collections.emptyList());
        when(contentInfoUtil.getCbPlanV4ContentIds(AUTH_TOKEN)).thenReturn(cbPlanIds);
        when(contentInfoUtil.getAssignedCourseIds(USER_ID, Constants.LEARNING_PATHWAY, AUTH_TOKEN))
                .thenReturn(Collections.emptyList());
        when(contentInfoUtil.callEnrolmentDictionaryApi(AUTH_TOKEN)).thenReturn(Collections.emptyMap());
        when(contentInfoUtil.getFilteredCaProgramIdentifiers(eq(USER_ID), eq(AUTH_TOKEN), any()))
                .thenReturn(Collections.emptyList());
        when(contentInfoUtil.getStandaloneAssessmentIdentifiersFromSystem()).thenReturn(Collections.emptyList());
        when(contentInfoUtil.callAssessmentEnrollmentDetailsApi(eq(AUTH_TOKEN), any()))
                .thenReturn(Collections.emptyMap());
        when(contentInfoUtil.filterStandaloneAssessmentIdentifiers(any(), any()))
                .thenReturn(Collections.emptyList());
    }

    private static Map<String, Object> moderatedResult(List<String> ids, int count) {
        Map<String, Object> m = new HashMap<>();
        m.put(Constants.IDENTIFIERS, new ArrayList<>(ids));
        m.put(Constants.COUNT, count);
        return m;
    }
}
