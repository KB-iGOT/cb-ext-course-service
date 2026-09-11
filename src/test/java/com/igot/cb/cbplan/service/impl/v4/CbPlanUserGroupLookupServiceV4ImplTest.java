package com.igot.cb.cbplan.service.impl.v4;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanUserGroupLookupServiceV4ImplTest {

    private static final String USER_GROUP_ID = "3fa85f64-5717-4562-b3fc-2c963f66afa6";
    private static final String ORG_ID = "org1";

    @Mock
    private CassandraOperation cassandraOperation;

    @InjectMocks
    private CbPlanUserGroupLookupServiceV4Impl lookupService;

    private static Map<String, Object> userGroupEntity(List<Map<String, List<String>>> criteria) {
        Map<String, Object> entity = new HashMap<>();
        entity.put(Constants.COL_CRITERIA, criteria);
        entity.put(Constants.COL_STATUS, Constants.ACTIVE);
        return entity;
    }

    @Test
    void fetchUserGroupById_found_returnsFirstRow() {
        Map<String, Object> row = userGroupEntity(List.of());
        when(cassandraOperation.getRecordsByProperties(anyString(), eq(Constants.TABLE_USER_GROUP_INFO),
                anyMap(), any(), any())).thenReturn(List.of(row));

        Map<String, Object> result = lookupService.fetchUserGroupById(USER_GROUP_ID, ORG_ID);

        assertEquals(row, result);
        verify(cassandraOperation).getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_USER_GROUP_INFO),
                eq(Map.of(Constants.COL_ORGID, ORG_ID, Constants.COL_USERGROUPID, USER_GROUP_ID)),
                eq(List.of()), any());
    }

    @Test
    void fetchUserGroupById_notFound_returnsEmptyMap() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(List.of());

        Map<String, Object> result = lookupService.fetchUserGroupById(USER_GROUP_ID, ORG_ID);

        assertTrue(result.isEmpty());
    }

    @Test
    void fetchUserGroupById_cassandraThrows_returnsEmptyMapInsteadOfPropagating() {
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), any(), any()))
                .thenThrow(new RuntimeException("cassandra down"));

        Map<String, Object> result = lookupService.fetchUserGroupById(USER_GROUP_ID, ORG_ID);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void extractRootOrgIds_criteriaHasRootOrgId_returnsValues() {
        Map<String, Object> entity = userGroupEntity(List.of(Map.of(Constants.ROOT_ORG_ID, List.of("org1", "org2"))));

        Set<String> result = lookupService.extractRootOrgIds(entity);

        assertEquals(Set.of("org1", "org2"), result);
    }

    @Test
    void extractRootOrgIds_criteriaHasOnlyMinistryOrStateId_returnsEmptySet() {
        Map<String, Object> entity = userGroupEntity(List.of(Map.of(Constants.MINISTRY_OR_STATEID, List.of("org1"))));

        Set<String> result = lookupService.extractRootOrgIds(entity);

        assertTrue(result.isEmpty());
    }

    @Test
    void extractRootOrgIds_emptyEntity_returnsEmptySet() {
        Set<String> result = lookupService.extractRootOrgIds(new HashMap<>());

        assertTrue(result.isEmpty());
    }

    @Test
    void extractRootOrgIds_noCriteriaColumn_returnsEmptySet() {
        Map<String, Object> entity = new HashMap<>();
        entity.put(Constants.COL_STATUS, Constants.ACTIVE);

        Set<String> result = lookupService.extractRootOrgIds(entity);

        assertTrue(result.isEmpty());
    }

    @Test
    void extractRootOrgIds_multipleCriteriaEntries_aggregatesAcrossEntries() {
        Map<String, Object> entity = userGroupEntity(List.of(
                Map.of(Constants.ROOT_ORG_ID, List.of("org1")),
                Map.of(Constants.ROOT_ORG_ID, List.of("org2"))));

        Set<String> result = lookupService.extractRootOrgIds(entity);

        assertEquals(Set.of("org1", "org2"), result);
    }

    @Test
    void extractMinistryOrStateIds_criteriaHasMinistryOrStateId_returnsValues() {
        Map<String, Object> entity = userGroupEntity(List.of(Map.of(Constants.MINISTRY_OR_STATEID, List.of("min1"))));

        Set<String> result = lookupService.extractMinistryOrStateIds(entity);

        assertEquals(Set.of("min1"), result);
    }

    @Test
    void extractMinistryOrStateIds_criteriaHasOnlyRootOrgId_returnsEmptySet() {
        Map<String, Object> entity = userGroupEntity(List.of(Map.of(Constants.ROOT_ORG_ID, List.of("org1"))));

        Set<String> result = lookupService.extractMinistryOrStateIds(entity);

        assertTrue(result.isEmpty());
    }

    @Test
    void extractMinistryOrStateIds_emptyEntity_returnsEmptySet() {
        Set<String> result = lookupService.extractMinistryOrStateIds(new HashMap<>());

        assertTrue(result.isEmpty());
    }
}
