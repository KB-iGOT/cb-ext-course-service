package com.igot.cb.cache;

import com.igot.cb.cassandra.BatchQueryParams;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Test class for CbPlanUserGroupCacheMgr.
 * Tests Caffeine caching behavior, batch fetching, and Cassandra interaction.
 */
@ExtendWith(MockitoExtension.class)
class CbPlanUserGroupCacheMgrTest {

    private static final String TEST_ORG_ID = "org_001";
    private static final String TEST_USER_GROUP_ID_1 = "ug_123";
    private static final String TEST_USER_GROUP_ID_2 = "ug_456";
    private static final String TEST_USER_GROUP_ID_3 = "ug_789";

    @Mock
    private CassandraOperation cassandraOperation;

    @InjectMocks
    private CbPlanUserGroupCacheMgr cacheMgr;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(cacheMgr, "ttlMinutes", 30);
        ReflectionTestUtils.setField(cacheMgr, "maxCacheSize", 10000);
        ReflectionTestUtils.setField(cacheMgr, "batchSize", 20);
        cacheMgr.initCache();
    }

    @Test
    void getUserGroup_cacheMiss_fetchesFromCassandraAndCaches() {
        Map<String, Object> mockUserGroup = createMockUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);
        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_USER_GROUP_INFO),
                any(Map.class),
                eq(List.of()),
                eq(null)
        )).thenReturn(List.of(mockUserGroup));

        Map<String, Object> result = cacheMgr.getUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);

        assertThat(result).isNotNull();
        assertThat(result).isNotEmpty();
        assertThat(result.get(Constants.COL_USERGROUPID)).isEqualTo(TEST_USER_GROUP_ID_1);
        assertThat(result.get(Constants.COL_ORGID)).isEqualTo(TEST_ORG_ID);
        verify(cassandraOperation, times(1)).getRecordsByProperties(any(), any(), any(), any(), any());

        Map<String, Object> cachedResult = cacheMgr.getUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);

        assertThat(cachedResult).isEqualTo(result);
        verify(cassandraOperation, times(1)).getRecordsByProperties(any(), any(), any(), any(), any());
    }

    @Test
    void getUserGroup_cacheHit_returnsCachedValueWithoutCassandraCall() {
        Map<String, Object> mockUserGroup = createMockUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(List.of(mockUserGroup));

        cacheMgr.getUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);

        Map<String, Object> cachedResult = cacheMgr.getUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);

        assertThat(cachedResult).isNotNull();
        assertThat(cachedResult.get(Constants.COL_USERGROUPID)).isEqualTo(TEST_USER_GROUP_ID_1);
        verify(cassandraOperation, times(1)).getRecordsByProperties(any(), any(), any(), any(), any());
    }

    @Test
    void getUserGroup_userGroupNotFound_returnsEmptyMap() {
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(Collections.emptyList());

        Map<String, Object> result = cacheMgr.getUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);

        assertThat(result).isEmpty();
        verify(cassandraOperation, times(1)).getRecordsByProperties(any(), any(), any(), any(), any());
    }

    @Test
    void getUserGroup_blankUserGroupId_returnsEmptyMap() {
        Map<String, Object> result = cacheMgr.getUserGroup("", TEST_ORG_ID);

        assertThat(result).isEmpty();
        verifyNoInteractions(cassandraOperation);
    }

    @Test
    void getUserGroup_blankOrgId_returnsEmptyMap() {
        Map<String, Object> result = cacheMgr.getUserGroup(TEST_USER_GROUP_ID_1, "");

        assertThat(result).isEmpty();
        verifyNoInteractions(cassandraOperation);
    }

    @Test
    void getUserGroup_cassandraException_returnsEmptyMap() {
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenThrow(new RuntimeException("Cassandra error"));

        Map<String, Object> result = cacheMgr.getUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);

        assertThat(result).isEmpty();
        verify(cassandraOperation, times(1)).getRecordsByProperties(any(), any(), any(), any(), any());
    }

    @Test
    void getUserGroups_allCached_returnsFromCacheWithoutCassandraCall() {
        Map<String, Object> mockGroup1 = createMockUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);
        Map<String, Object> mockGroup2 = createMockUserGroup(TEST_USER_GROUP_ID_2, TEST_ORG_ID);

        when(cassandraOperation.getRecordsByIdsWithGivenPartitionKey(any()))
                .thenReturn(List.of(mockGroup1, mockGroup2));

        cacheMgr.getUserGroups(List.of(TEST_USER_GROUP_ID_1, TEST_USER_GROUP_ID_2), TEST_ORG_ID);

        Map<String, Map<String, Object>> result = cacheMgr.getUserGroups(
                List.of(TEST_USER_GROUP_ID_1, TEST_USER_GROUP_ID_2), TEST_ORG_ID);

        assertThat(result).hasSize(2);
        assertThat(result).containsKey(TEST_USER_GROUP_ID_1);
        assertThat(result).containsKey(TEST_USER_GROUP_ID_2);
        verify(cassandraOperation, times(1)).getRecordsByIdsWithGivenPartitionKey(any());
    }

    @Test
    void getUserGroups_allUncached_fetchesFromCassandraAndPopulatesCache() {
        Map<String, Object> mockGroup1 = createMockUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);
        Map<String, Object> mockGroup2 = createMockUserGroup(TEST_USER_GROUP_ID_2, TEST_ORG_ID);

        when(cassandraOperation.getRecordsByIdsWithGivenPartitionKey(any()))
                .thenReturn(List.of(mockGroup1, mockGroup2));

        Map<String, Map<String, Object>> result = cacheMgr.getUserGroups(
                List.of(TEST_USER_GROUP_ID_1, TEST_USER_GROUP_ID_2), TEST_ORG_ID);

        assertThat(result).hasSize(2);
        assertThat(result.get(TEST_USER_GROUP_ID_1)).isNotNull();
        assertThat(result.get(TEST_USER_GROUP_ID_2)).isNotNull();
        verify(cassandraOperation, times(1)).getRecordsByIdsWithGivenPartitionKey(any());
    }

    @Test
    void getUserGroups_partialCache_fetchesOnlyUncachedGroups() {
        Map<String, Object> mockGroup1 = createMockUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);
        Map<String, Object> mockGroup2 = createMockUserGroup(TEST_USER_GROUP_ID_2, TEST_ORG_ID);
        Map<String, Object> mockGroup3 = createMockUserGroup(TEST_USER_GROUP_ID_3, TEST_ORG_ID);

        when(cassandraOperation.getRecordsByIdsWithGivenPartitionKey(any()))
                .thenReturn(List.of(mockGroup1))
                .thenReturn(List.of(mockGroup2, mockGroup3));

        cacheMgr.getUserGroups(List.of(TEST_USER_GROUP_ID_1), TEST_ORG_ID);

        Map<String, Map<String, Object>> result = cacheMgr.getUserGroups(
                List.of(TEST_USER_GROUP_ID_1, TEST_USER_GROUP_ID_2, TEST_USER_GROUP_ID_3), TEST_ORG_ID);

        assertThat(result).hasSize(3);
        assertThat(result).containsKey(TEST_USER_GROUP_ID_1);
        assertThat(result).containsKey(TEST_USER_GROUP_ID_2);
        assertThat(result).containsKey(TEST_USER_GROUP_ID_3);

        ArgumentCaptor<BatchQueryParams> paramsCaptor = ArgumentCaptor.forClass(BatchQueryParams.class);
        verify(cassandraOperation, times(2)).getRecordsByIdsWithGivenPartitionKey(paramsCaptor.capture());

        List<BatchQueryParams> capturedParams = paramsCaptor.getAllValues();
        assertThat(capturedParams.get(1).getClusteringValues()).containsExactlyInAnyOrder(
                TEST_USER_GROUP_ID_2, TEST_USER_GROUP_ID_3);
    }

    @Test
    void getUserGroups_batchSizeExceeded_chunksIntoBatches() {
        ReflectionTestUtils.setField(cacheMgr, "batchSize", 2);
        cacheMgr.initCache();

        List<String> userGroupIds = List.of(
                "ug_1", "ug_2", "ug_3", "ug_4", "ug_5"
        );

        Map<String, Object> mockGroup1 = createMockUserGroup("ug_1", TEST_ORG_ID);
        Map<String, Object> mockGroup2 = createMockUserGroup("ug_2", TEST_ORG_ID);
        Map<String, Object> mockGroup3 = createMockUserGroup("ug_3", TEST_ORG_ID);
        Map<String, Object> mockGroup4 = createMockUserGroup("ug_4", TEST_ORG_ID);
        Map<String, Object> mockGroup5 = createMockUserGroup("ug_5", TEST_ORG_ID);

        when(cassandraOperation.getRecordsByIdsWithGivenPartitionKey(any()))
                .thenReturn(List.of(mockGroup1, mockGroup2))
                .thenReturn(List.of(mockGroup3, mockGroup4))
                .thenReturn(List.of(mockGroup5));

        Map<String, Map<String, Object>> result = cacheMgr.getUserGroups(userGroupIds, TEST_ORG_ID);

        assertThat(result).hasSize(5);
        verify(cassandraOperation, times(3)).getRecordsByIdsWithGivenPartitionKey(any());
    }

    @Test
    void getUserGroups_emptyUserGroupIdList_returnsEmptyMap() {
        Map<String, Map<String, Object>> result = cacheMgr.getUserGroups(Collections.emptyList(), TEST_ORG_ID);

        assertThat(result).isEmpty();
        verifyNoInteractions(cassandraOperation);
    }

    @Test
    void getUserGroups_nullUserGroupIdList_returnsEmptyMap() {
        Map<String, Map<String, Object>> result = cacheMgr.getUserGroups(null, TEST_ORG_ID);

        assertThat(result).isEmpty();
        verifyNoInteractions(cassandraOperation);
    }

    @Test
    void getUserGroups_blankOrgId_returnsEmptyMap() {
        Map<String, Map<String, Object>> result = cacheMgr.getUserGroups(
                List.of(TEST_USER_GROUP_ID_1), "");

        assertThat(result).isEmpty();
        verifyNoInteractions(cassandraOperation);
    }

    @Test
    void getUserGroups_cassandraReturnsPartialResults_returnsOnlyFoundGroups() {
        Map<String, Object> mockGroup1 = createMockUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);

        when(cassandraOperation.getRecordsByIdsWithGivenPartitionKey(any()))
                .thenReturn(List.of(mockGroup1));

        Map<String, Map<String, Object>> result = cacheMgr.getUserGroups(
                List.of(TEST_USER_GROUP_ID_1, TEST_USER_GROUP_ID_2, TEST_USER_GROUP_ID_3), TEST_ORG_ID);

        assertThat(result).hasSize(1);
        assertThat(result).containsKey(TEST_USER_GROUP_ID_1);
        assertThat(result).doesNotContainKey(TEST_USER_GROUP_ID_2);
        assertThat(result).doesNotContainKey(TEST_USER_GROUP_ID_3);
    }

    @Test
    void getUserGroups_cassandraException_returnsOnlyCachedGroups() {
        Map<String, Object> mockGroup1 = createMockUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);

        when(cassandraOperation.getRecordsByIdsWithGivenPartitionKey(any()))
                .thenReturn(List.of(mockGroup1))
                .thenThrow(new RuntimeException("Cassandra error"));

        cacheMgr.getUserGroups(List.of(TEST_USER_GROUP_ID_1), TEST_ORG_ID);

        Map<String, Map<String, Object>> result = cacheMgr.getUserGroups(
                List.of(TEST_USER_GROUP_ID_1, TEST_USER_GROUP_ID_2), TEST_ORG_ID);

        assertThat(result).hasSize(1);
        assertThat(result).containsKey(TEST_USER_GROUP_ID_1);
        assertThat(result).doesNotContainKey(TEST_USER_GROUP_ID_2);
    }

    @Test
    void getUserGroups_batchQueryParams_buildsCorrectParameters() {
        List<String> userGroupIds = List.of(TEST_USER_GROUP_ID_1, TEST_USER_GROUP_ID_2);
        Map<String, Object> mockGroup1 = createMockUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);
        Map<String, Object> mockGroup2 = createMockUserGroup(TEST_USER_GROUP_ID_2, TEST_ORG_ID);

        when(cassandraOperation.getRecordsByIdsWithGivenPartitionKey(any()))
                .thenReturn(List.of(mockGroup1, mockGroup2));

        cacheMgr.getUserGroups(userGroupIds, TEST_ORG_ID);

        ArgumentCaptor<BatchQueryParams> paramsCaptor = ArgumentCaptor.forClass(BatchQueryParams.class);
        verify(cassandraOperation).getRecordsByIdsWithGivenPartitionKey(paramsCaptor.capture());

        BatchQueryParams capturedParams = paramsCaptor.getValue();
        assertThat(capturedParams.getKeyspaceName()).isEqualTo(Constants.KEYSPACE_SUNBIRD);
        assertThat(capturedParams.getTableName()).isEqualTo(Constants.TABLE_USER_GROUP_INFO);
        assertThat(capturedParams.getPartitionKeyColumn()).isEqualTo(Constants.COL_ORGID);
        assertThat(capturedParams.getPartitionKeyValue()).isEqualTo(TEST_ORG_ID);
        assertThat(capturedParams.getClusteringColumn()).isEqualTo(Constants.COL_USERGROUPID);
        assertThat(capturedParams.getClusteringValues()).containsExactlyInAnyOrder(
                TEST_USER_GROUP_ID_1, TEST_USER_GROUP_ID_2);
    }

    @Test
    void getUserGroups_multipleOrgs_maintainsSeparateCacheKeys() {
        String org1 = "org_001";
        String org2 = "org_002";
        String userGroupId = "ug_123";

        Map<String, Object> mockGroupOrg1 = createMockUserGroup(userGroupId, org1);
        mockGroupOrg1.put("name", "Org1 Group");

        Map<String, Object> mockGroupOrg2 = createMockUserGroup(userGroupId, org2);
        mockGroupOrg2.put("name", "Org2 Group");

        when(cassandraOperation.getRecordsByIdsWithGivenPartitionKey(any()))
                .thenReturn(List.of(mockGroupOrg1))
                .thenReturn(List.of(mockGroupOrg2));

        Map<String, Map<String, Object>> resultOrg1 = cacheMgr.getUserGroups(List.of(userGroupId), org1);
        Map<String, Map<String, Object>> resultOrg2 = cacheMgr.getUserGroups(List.of(userGroupId), org2);

        assertThat(resultOrg1.get(userGroupId).get("name")).isEqualTo("Org1 Group");
        assertThat(resultOrg2.get(userGroupId).get("name")).isEqualTo("Org2 Group");
        verify(cassandraOperation, times(2)).getRecordsByIdsWithGivenPartitionKey(any());
    }

    @Test
    void invalidateUserGroup_removesFromCache() {
        Map<String, Object> mockGroup = createMockUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);
        when(cassandraOperation.getRecordsByProperties(any(), any(), any(), any(), any()))
                .thenReturn(List.of(mockGroup));

        cacheMgr.getUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);
        verify(cassandraOperation, times(1)).getRecordsByProperties(any(), any(), any(), any(), any());

        cacheMgr.invalidateUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);

        cacheMgr.getUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);
        verify(cassandraOperation, times(2)).getRecordsByProperties(any(), any(), any(), any(), any());
    }

    @Test
    void invalidateAll_clearsEntireCache() {
        Map<String, Object> mockGroup1 = createMockUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);
        Map<String, Object> mockGroup2 = createMockUserGroup(TEST_USER_GROUP_ID_2, TEST_ORG_ID);

        when(cassandraOperation.getRecordsByIdsWithGivenPartitionKey(any()))
                .thenReturn(List.of(mockGroup1, mockGroup2));

        cacheMgr.getUserGroups(List.of(TEST_USER_GROUP_ID_1, TEST_USER_GROUP_ID_2), TEST_ORG_ID);
        verify(cassandraOperation, times(1)).getRecordsByIdsWithGivenPartitionKey(any());

        cacheMgr.invalidateAll();

        cacheMgr.getUserGroups(List.of(TEST_USER_GROUP_ID_1, TEST_USER_GROUP_ID_2), TEST_ORG_ID);
        verify(cassandraOperation, times(2)).getRecordsByIdsWithGivenPartitionKey(any());
    }

    @Test
    void getUserGroups_cassandraReturnsNull_handlesGracefully() {
        when(cassandraOperation.getRecordsByIdsWithGivenPartitionKey(any()))
                .thenReturn(null);

        Map<String, Map<String, Object>> result = cacheMgr.getUserGroups(
                List.of(TEST_USER_GROUP_ID_1), TEST_ORG_ID);

        assertThat(result).isEmpty();
        verify(cassandraOperation, times(1)).getRecordsByIdsWithGivenPartitionKey(any());
    }

    @Test
    void getUserGroups_cassandraReturnsEmptyList_handlesGracefully() {
        when(cassandraOperation.getRecordsByIdsWithGivenPartitionKey(any()))
                .thenReturn(Collections.emptyList());

        Map<String, Map<String, Object>> result = cacheMgr.getUserGroups(
                List.of(TEST_USER_GROUP_ID_1, TEST_USER_GROUP_ID_2), TEST_ORG_ID);

        assertThat(result).isEmpty();
        verify(cassandraOperation, times(1)).getRecordsByIdsWithGivenPartitionKey(any());
    }

    @Test
    void getUserGroups_userGroupWithBlankId_skipsEntry() {
        Map<String, Object> mockGroup1 = createMockUserGroup(TEST_USER_GROUP_ID_1, TEST_ORG_ID);
        Map<String, Object> mockGroupBlank = new HashMap<>();
        mockGroupBlank.put(Constants.COL_ORGID, TEST_ORG_ID);
        mockGroupBlank.put(Constants.COL_USERGROUPID, "");

        when(cassandraOperation.getRecordsByIdsWithGivenPartitionKey(any()))
                .thenReturn(List.of(mockGroup1, mockGroupBlank));

        Map<String, Map<String, Object>> result = cacheMgr.getUserGroups(
                List.of(TEST_USER_GROUP_ID_1, "blank_id"), TEST_ORG_ID);

        assertThat(result).hasSize(1);
        assertThat(result).containsKey(TEST_USER_GROUP_ID_1);
    }

    @Test
    void getUserGroups_largeNumberOfGroups_chunksCorrectly() {
        ReflectionTestUtils.setField(cacheMgr, "batchSize", 20);
        cacheMgr.initCache();

        List<String> userGroupIds = new ArrayList<>();
        for (int i = 1; i <= 50; i++) {
            userGroupIds.add("ug_" + i);
        }

        List<Map<String, Object>> mockResults = new ArrayList<>();
        for (String id : userGroupIds) {
            mockResults.add(createMockUserGroup(id, TEST_ORG_ID));
        }

        when(cassandraOperation.getRecordsByIdsWithGivenPartitionKey(any()))
                .thenReturn(mockResults.subList(0, 20))
                .thenReturn(mockResults.subList(20, 40))
                .thenReturn(mockResults.subList(40, 50));

        Map<String, Map<String, Object>> result = cacheMgr.getUserGroups(userGroupIds, TEST_ORG_ID);

        assertThat(result).hasSize(50);
        verify(cassandraOperation, times(3)).getRecordsByIdsWithGivenPartitionKey(any());
    }

    private Map<String, Object> createMockUserGroup(String userGroupId, String orgId) {
        Map<String, Object> userGroup = new HashMap<>();
        userGroup.put(Constants.COL_USERGROUPID, userGroupId);
        userGroup.put(Constants.COL_ORGID, orgId);
        userGroup.put("name", "Test Group " + userGroupId);
        userGroup.put("status", "ACTIVE");
        userGroup.put("criteria", List.of(
                Map.of("department", List.of("HR", "Finance")),
                Map.of("designation", List.of("Manager"))
        ));
        return userGroup;
    }
}
