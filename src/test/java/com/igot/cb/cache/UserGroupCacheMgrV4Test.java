package com.igot.cb.cache;

import com.igot.cb.cbplan.service.impl.v4.CbPlanUserGroupLookupServiceV4Impl;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserGroupCacheMgrV4Test {

    private static final String ORG_ID = "org_001";
    private static final String UG_ID_1 = "ug_001";
    private static final String UG_ID_2 = "ug_002";

    @Mock
    private CbPlanUserGroupLookupServiceV4Impl userGroupLookupService;

    @InjectMocks
    private UserGroupCacheMgrV4 cacheMgr;

    @BeforeEach
    void setUp() {
        cacheMgr.initCache();
    }

    private static Map<String, Object> buildGroup(String userGroupId) {
        Map<String, Object> group = new HashMap<>();
        group.put(Constants.COL_USERGROUPID, userGroupId);
        group.put(Constants.COL_STATUS, Constants.ACTIVE);
        return group;
    }

    @Test
    void fetchUserGroupById_cacheMiss_fetchesFromLookupServiceAndCaches() {
        Map<String, Object> group = buildGroup(UG_ID_1);
        when(userGroupLookupService.fetchUserGroupById(UG_ID_1, ORG_ID)).thenReturn(group);

        Map<String, Object> result = cacheMgr.fetchUserGroupById(UG_ID_1, ORG_ID);

        assertThat(result).isEqualTo(group);
        verify(userGroupLookupService, times(1)).fetchUserGroupById(UG_ID_1, ORG_ID);
    }

    @Test
    void fetchUserGroupById_cacheHit_doesNotCallLookupService() {
        Map<String, Object> group = buildGroup(UG_ID_1);
        when(userGroupLookupService.fetchUserGroupById(UG_ID_1, ORG_ID)).thenReturn(group);

        cacheMgr.fetchUserGroupById(UG_ID_1, ORG_ID);
        cacheMgr.fetchUserGroupById(UG_ID_1, ORG_ID);

        verify(userGroupLookupService, times(1)).fetchUserGroupById(UG_ID_1, ORG_ID);
    }

    @Test
    void fetchUserGroupById_notFound_doesNotCacheEmptyResult() {
        when(userGroupLookupService.fetchUserGroupById(UG_ID_1, ORG_ID)).thenReturn(Collections.emptyMap());

        cacheMgr.fetchUserGroupById(UG_ID_1, ORG_ID);
        cacheMgr.fetchUserGroupById(UG_ID_1, ORG_ID);

        verify(userGroupLookupService, times(2)).fetchUserGroupById(UG_ID_1, ORG_ID);
    }

    @Test
    void fetchUserGroupsByIds_emptyList_returnsEmptyMap() {
        Map<String, Map<String, Object>> result = cacheMgr.fetchUserGroupsByIds(List.of(), ORG_ID);

        assertThat(result).isEmpty();
        verifyNoInteractions(userGroupLookupService);
    }

    @Test
    void fetchUserGroupsByIds_fullCacheMiss_fetchesFromLookupServiceAndCaches() {
        Map<String, Object> group1 = buildGroup(UG_ID_1);
        Map<String, Object> group2 = buildGroup(UG_ID_2);
        when(userGroupLookupService.fetchUserGroupsByIds(List.of(UG_ID_1, UG_ID_2), ORG_ID))
                .thenReturn(Map.of(UG_ID_1, group1, UG_ID_2, group2));

        Map<String, Map<String, Object>> result = cacheMgr.fetchUserGroupsByIds(List.of(UG_ID_1, UG_ID_2), ORG_ID);

        assertThat(result)
                .containsKey(UG_ID_1)
                .containsKey(UG_ID_2);
        verify(userGroupLookupService, times(1)).fetchUserGroupsByIds(anyList(), anyString());
    }

    @Test
    void fetchUserGroupsByIds_fullCacheHit_skipsCassandra() {
        Map<String, Object> group1 = buildGroup(UG_ID_1);
        Map<String, Object> group2 = buildGroup(UG_ID_2);
        when(userGroupLookupService.fetchUserGroupsByIds(anyList(), eq(ORG_ID)))
                .thenReturn(Map.of(UG_ID_1, group1, UG_ID_2, group2));

        cacheMgr.fetchUserGroupsByIds(List.of(UG_ID_1, UG_ID_2), ORG_ID);
        cacheMgr.fetchUserGroupsByIds(List.of(UG_ID_1, UG_ID_2), ORG_ID);

        verify(userGroupLookupService, times(1)).fetchUserGroupsByIds(anyList(), anyString());
    }

    @Test
    void fetchUserGroupsByIds_partialCacheHit_fetchesOnlyMisses() {
        Map<String, Object> group1 = buildGroup(UG_ID_1);
        Map<String, Object> group2 = buildGroup(UG_ID_2);
        when(userGroupLookupService.fetchUserGroupsByIds(List.of(UG_ID_1), ORG_ID))
                .thenReturn(Map.of(UG_ID_1, group1));
        when(userGroupLookupService.fetchUserGroupsByIds(List.of(UG_ID_2), ORG_ID))
                .thenReturn(Map.of(UG_ID_2, group2));

        cacheMgr.fetchUserGroupsByIds(List.of(UG_ID_1), ORG_ID);
        Map<String, Map<String, Object>> result = cacheMgr.fetchUserGroupsByIds(List.of(UG_ID_1, UG_ID_2), ORG_ID);

        assertThat(result)
                .containsKey(UG_ID_1)
                .containsKey(UG_ID_2);
        verify(userGroupLookupService, times(1)).fetchUserGroupsByIds(List.of(UG_ID_1), ORG_ID);
        verify(userGroupLookupService, times(1)).fetchUserGroupsByIds(List.of(UG_ID_2), ORG_ID);
    }

    @Test
    void invalidateUserGroup_subsequentFetchHitsLookupServiceAgain() {
        Map<String, Object> group = buildGroup(UG_ID_1);
        when(userGroupLookupService.fetchUserGroupById(UG_ID_1, ORG_ID)).thenReturn(group);

        cacheMgr.fetchUserGroupById(UG_ID_1, ORG_ID);
        cacheMgr.invalidateUserGroup(ORG_ID, UG_ID_1);
        cacheMgr.fetchUserGroupById(UG_ID_1, ORG_ID);

        verify(userGroupLookupService, times(2)).fetchUserGroupById(UG_ID_1, ORG_ID);
    }

}
