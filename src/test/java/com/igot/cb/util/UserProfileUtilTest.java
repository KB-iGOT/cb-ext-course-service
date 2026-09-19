package com.igot.cb.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class UserProfileUtilTest {

    private static final String USER_ID_1 = "user-001";
    private static final String USER_ID_2 = "user-002";
    private static final String USER_ID_3 = "user-003";
    private static final String FIRST_NAME_1 = "Alice";
    private static final String FIRST_NAME_2 = "Bob";
    private static final String CACHE_KEY_1 = "user:basicProfile:" + USER_ID_1;
    private static final String CACHE_KEY_2 = "user:basicProfile:" + USER_ID_2;
    private static final int BATCH_SIZE = 50;

    @Mock
    private CassandraOperation cassandraOperation;
    @Mock
    private RedisCacheMgr redisCacheMgr;
    @Mock
    private CbExtServerProperties serverProperties;

    private UserProfileUtil userProfileUtil;

    @BeforeEach
    void setUp() {
        userProfileUtil = new UserProfileUtil(cassandraOperation, redisCacheMgr,
                serverProperties, new ObjectMapper());
    }

    @Test
    void buildUserProfiles_emptyInput_returnsEmptyMap() {
        Map<String, String> result = userProfileUtil.buildUserProfiles(Collections.emptyList());

        assertThat(result).isEmpty();
        verifyNoInteractions(redisCacheMgr, cassandraOperation);
    }

    @Test
    void buildUserProfiles_nullInput_returnsEmptyMap() {
        Map<String, String> result = userProfileUtil.buildUserProfiles(null);

        assertThat(result).isEmpty();
        verifyNoInteractions(redisCacheMgr, cassandraOperation);
    }

    @Test
    void buildUserProfiles_allCacheHits_returnNamesWithoutCassandra() {
        when(redisCacheMgr.getFromCache(CACHE_KEY_1))
                .thenReturn("{\"firstName\":\"" + FIRST_NAME_1 + "\"}");
        when(redisCacheMgr.getFromCache(CACHE_KEY_2))
                .thenReturn("{\"firstName\":\"" + FIRST_NAME_2 + "\"}");

        Map<String, String> result = userProfileUtil.buildUserProfiles(List.of(USER_ID_1, USER_ID_2));

        assertThat(result).containsEntry(USER_ID_1, FIRST_NAME_1)
                          .containsEntry(USER_ID_2, FIRST_NAME_2);
        verifyNoInteractions(cassandraOperation);
    }

    @Test
    void buildUserProfiles_allCacheMisses_fetchesFromCassandra() {
        when(redisCacheMgr.getFromCache(CACHE_KEY_1)).thenReturn(null);
        when(redisCacheMgr.getFromCache(CACHE_KEY_2)).thenReturn(null);
        when(serverProperties.getCassandraQueryLimitPrimaryKey()).thenReturn(BATCH_SIZE);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), isNull()))
                .thenReturn(List.of(
                        Map.of(Constants.ID, USER_ID_1, Constants.FIRST_NAME, FIRST_NAME_1),
                        Map.of(Constants.ID, USER_ID_2, Constants.FIRST_NAME, FIRST_NAME_2)
                ));

        Map<String, String> result = userProfileUtil.buildUserProfiles(List.of(USER_ID_1, USER_ID_2));

        assertThat(result).containsEntry(USER_ID_1, FIRST_NAME_1)
                          .containsEntry(USER_ID_2, FIRST_NAME_2);
        verify(cassandraOperation, times(1))
                .getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), isNull());
    }

    @Test
    void buildUserProfiles_partialCacheHit_fetchesMissesFromCassandra() {
        when(redisCacheMgr.getFromCache(CACHE_KEY_1))
                .thenReturn("{\"firstName\":\"" + FIRST_NAME_1 + "\"}");
        when(redisCacheMgr.getFromCache(CACHE_KEY_2)).thenReturn(null);
        when(serverProperties.getCassandraQueryLimitPrimaryKey()).thenReturn(BATCH_SIZE);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), isNull()))
                .thenReturn(List.of(Map.of(Constants.ID, USER_ID_2, Constants.FIRST_NAME, FIRST_NAME_2)));

        Map<String, String> result = userProfileUtil.buildUserProfiles(List.of(USER_ID_1, USER_ID_2));

        assertThat(result).containsEntry(USER_ID_1, FIRST_NAME_1)
                          .containsEntry(USER_ID_2, FIRST_NAME_2);
        verify(cassandraOperation, times(1))
                .getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), isNull());
    }

    @Test
    void buildUserProfiles_duplicateUserIds_deduplicatesBeforeQuerying() {
        when(redisCacheMgr.getFromCache(CACHE_KEY_1)).thenReturn(null);
        when(serverProperties.getCassandraQueryLimitPrimaryKey()).thenReturn(BATCH_SIZE);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), isNull()))
                .thenReturn(List.of(Map.of(Constants.ID, USER_ID_1, Constants.FIRST_NAME, FIRST_NAME_1)));

        Map<String, String> result = userProfileUtil.buildUserProfiles(
                List.of(USER_ID_1, USER_ID_1, USER_ID_1));

        assertThat(result).hasSize(1).containsEntry(USER_ID_1, FIRST_NAME_1);
        verify(redisCacheMgr, times(1)).getFromCache(CACHE_KEY_1);
    }

    @Test
    void buildUserProfiles_cassandraReturnsEmptyList_userAbsentFromResult() {
        when(redisCacheMgr.getFromCache(CACHE_KEY_1)).thenReturn(null);
        when(serverProperties.getCassandraQueryLimitPrimaryKey()).thenReturn(BATCH_SIZE);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), isNull()))
                .thenReturn(Collections.emptyList());

        Map<String, String> result = userProfileUtil.buildUserProfiles(List.of(USER_ID_1));

        assertThat(result).doesNotContainKey(USER_ID_1);
    }

    @Test
    void buildUserProfiles_cacheHitMissingFirstName_mapsToEmptyString() {
        when(redisCacheMgr.getFromCache(CACHE_KEY_1))
                .thenReturn("{\"rootOrgId\":\"org-1\"}");

        Map<String, String> result = userProfileUtil.buildUserProfiles(List.of(USER_ID_1));

        assertThat(result).containsEntry(USER_ID_1, "");
        verifyNoInteractions(cassandraOperation);
    }

    @Test
    void buildUserProfiles_corruptCacheJson_fallsBackToCassandra() {
        when(redisCacheMgr.getFromCache(CACHE_KEY_1)).thenReturn("not-valid-json");
        when(serverProperties.getCassandraQueryLimitPrimaryKey()).thenReturn(BATCH_SIZE);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), isNull()))
                .thenReturn(List.of(Map.of(Constants.ID, USER_ID_1, Constants.FIRST_NAME, FIRST_NAME_1)));

        Map<String, String> result = userProfileUtil.buildUserProfiles(List.of(USER_ID_1));

        assertThat(result).containsEntry(USER_ID_1, FIRST_NAME_1);
        verify(cassandraOperation, times(1))
                .getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), isNull());
    }

    @Test
    void buildUserProfiles_largeMissList_batchesCassandraQueries() {
        int batchSize = 2;
        List<String> userIds = List.of(USER_ID_1, USER_ID_2, USER_ID_3);
        userIds.forEach(id -> when(redisCacheMgr.getFromCache("user:basicProfile:" + id)).thenReturn(null));
        when(serverProperties.getCassandraQueryLimitPrimaryKey()).thenReturn(batchSize);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), isNull()))
                .thenReturn(Collections.emptyList());

        userProfileUtil.buildUserProfiles(userIds);

        verify(cassandraOperation, times(2))
                .getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), isNull());
    }

    @Test
    void buildUserProfiles_cassandraThrowsException_returnsPartialResult() {
        when(redisCacheMgr.getFromCache(CACHE_KEY_1))
                .thenReturn("{\"firstName\":\"" + FIRST_NAME_1 + "\"}");
        when(redisCacheMgr.getFromCache(CACHE_KEY_2)).thenReturn(null);
        when(serverProperties.getCassandraQueryLimitPrimaryKey()).thenReturn(BATCH_SIZE);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), isNull()))
                .thenThrow(new RuntimeException("Cassandra unavailable"));

        Map<String, String> result = userProfileUtil.buildUserProfiles(List.of(USER_ID_1, USER_ID_2));

        assertThat(result).containsEntry(USER_ID_1, FIRST_NAME_1)
                          .doesNotContainKey(USER_ID_2);
    }
}
