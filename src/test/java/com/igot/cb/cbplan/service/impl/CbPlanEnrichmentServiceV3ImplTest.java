package com.igot.cb.cbplan.service.impl;

import com.igot.cb.service.ContentInfoServiceImpl;
import com.igot.cb.service.UserAndOrgServiceImpl;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanEnrichmentServiceV3ImplTest {

    @Mock
    private UserAndOrgServiceImpl userAndOrgService;

    @Mock
    private ContentInfoServiceImpl contentService;

    @InjectMocks
    private CbPlanEnrichmentServiceV3Impl enrichmentService;

    @Test
    void testEnrichSearchResultsEnrichesCreatedByAndContentList() {
        Map<String, Object> item = new HashMap<>();
        item.put(Constants.CREATED_BY, "user1");
        item.put(Constants.CONTENT_LIST, List.of("content1"));
        Map<String, Object> userInfoMap = new HashMap<>();
        userInfoMap.put(Constants.FIRSTNAME, "John");
        when(userAndOrgService.readUserProfile(any(), anyList())).thenReturn(userInfoMap);
        List<Map<String, Object>> contentDetails = new ArrayList<>();
        contentDetails.add(Map.of("identifier", "content1"));
        when(contentService.enrichContentInfoForCBPlan(anyList())).thenReturn(contentDetails);
        List<Map<String, Object>> result = enrichmentService.enrichSearchResults(List.of(item));
        assertEquals(1, result.size());
        assertEquals("John", result.get(0).get(Constants.CREATED_BY_NAME));
        assertEquals("user1", result.get(0).get(Constants.CREATED_BY));
        assertEquals(contentDetails, result.get(0).get(Constants.CONTENT_LIST));
    }

    @Test
    void testEnrichSearchResultsDoesNotMutateSourceItems() {
        Map<String, Object> item = new HashMap<>();
        item.put(Constants.CONTENT_LIST, List.of("content1"));
        when(contentService.enrichContentInfoForCBPlan(anyList()))
                .thenReturn(List.of(Map.of("identifier", "content1")));
        enrichmentService.enrichSearchResults(List.of(item));
        assertEquals(List.of("content1"), item.get(Constants.CONTENT_LIST));
    }

    @Test
    void testEnrichSearchResultsWithEmptyInput() {
        assertTrue(enrichmentService.enrichSearchResults(List.of()).isEmpty());
    }

    @Test
    void testEnrichCreatedByInfoSkipsWhenKeyMissing() {
        Map<String, Object> enrichedItem = new HashMap<>();
        enrichmentService.enrichCreatedByInfo(new HashMap<>(), enrichedItem);
        assertTrue(enrichedItem.isEmpty());
        verify(userAndOrgService, never()).readUserProfile(any(), anyList());
    }

    @Test
    void testEnrichCreatedByInfoSkipsWhenValueNull() {
        Map<String, Object> item = new HashMap<>();
        item.put(Constants.CREATED_BY, null);
        Map<String, Object> enrichedItem = new HashMap<>();
        enrichmentService.enrichCreatedByInfo(item, enrichedItem);
        assertTrue(enrichedItem.isEmpty());
        verify(userAndOrgService, never()).readUserProfile(any(), anyList());
    }

    @Test
    void testEnrichCreatedByInfoSkipsWhenValueBlank() {
        Map<String, Object> item = new HashMap<>();
        item.put(Constants.CREATED_BY, "   ");
        Map<String, Object> enrichedItem = new HashMap<>();
        enrichmentService.enrichCreatedByInfo(item, enrichedItem);
        assertTrue(enrichedItem.isEmpty());
        verify(userAndOrgService, never()).readUserProfile(any(), anyList());
    }

    @Test
    void testEnrichCreatedByInfoSkipsWhenUserInfoEmpty() {
        Map<String, Object> item = new HashMap<>();
        item.put(Constants.CREATED_BY, "user1");
        Map<String, Object> enrichedItem = new HashMap<>();
        when(userAndOrgService.readUserProfile(any(), anyList())).thenReturn(new HashMap<>());
        enrichmentService.enrichCreatedByInfo(item, enrichedItem);
        assertFalse(enrichedItem.containsKey(Constants.CREATED_BY_NAME));
    }

    @Test
    void testEnrichContentListInfoSkipsWhenKeyMissing() {
        Map<String, Object> enrichedItem = new HashMap<>();
        enrichmentService.enrichContentListInfo(new HashMap<>(), enrichedItem);
        assertTrue(enrichedItem.isEmpty());
        verify(contentService, never()).enrichContentInfoForCBPlan(anyList());
    }

    @Test
    void testEnrichContentListInfoSkipsWhenValueNull() {
        Map<String, Object> item = new HashMap<>();
        item.put(Constants.CONTENT_LIST, null);
        Map<String, Object> enrichedItem = new HashMap<>();
        enrichmentService.enrichContentListInfo(item, enrichedItem);
        assertTrue(enrichedItem.isEmpty());
        verify(contentService, never()).enrichContentInfoForCBPlan(anyList());
    }

    @Test
    void testEnrichContentListInfoSkipsWhenValueNotAList() {
        Map<String, Object> item = new HashMap<>();
        item.put(Constants.CONTENT_LIST, "not-a-list");
        Map<String, Object> enrichedItem = new HashMap<>();
        enrichmentService.enrichContentListInfo(item, enrichedItem);
        assertTrue(enrichedItem.isEmpty());
        verify(contentService, never()).enrichContentInfoForCBPlan(anyList());
    }

    @Test
    void testConstructor() {
        assertNotNull(new CbPlanEnrichmentServiceV3Impl(userAndOrgService, contentService));
    }

    @Test
    void testExtractMinistryOrStateDetailsExtractsMinistryId() {
        Map<String, String> userProfile = new HashMap<>();
        Map<String, Object> profileDetails = new HashMap<>();
        profileDetails.put(Constants.MINISTRY_OR_STATEID, "ORG_001");
        enrichmentService.extractMinistryOrStateDetails(userProfile, profileDetails);
        assertEquals("ORG_001", userProfile.get(Constants.MINISTRY_OR_STATE_ID_RQST));
    }

    @Test
    void testExtractMinistryOrStateDetailsExtractsMinistryOrgName() {
        Map<String, String> userProfile = new HashMap<>();
        Map<String, Object> profileDetails = new HashMap<>();
        profileDetails.put(Constants.MINISTRY_OR_STATE_ORG_NAME, "Ministry of Example");
        enrichmentService.extractMinistryOrStateDetails(userProfile, profileDetails);
        assertEquals("Ministry of Example", userProfile.get(Constants.MINISTRY_OR_STATE_ORG_NAME.toLowerCase()));
    }

    @Test
    void testExtractMinistryOrStateDetailsExtractsBothFields() {
        Map<String, String> userProfile = new HashMap<>();
        Map<String, Object> profileDetails = new HashMap<>();
        profileDetails.put(Constants.MINISTRY_OR_STATEID, "ORG_001");
        profileDetails.put(Constants.MINISTRY_OR_STATE_ORG_NAME, "Ministry of Example");
        enrichmentService.extractMinistryOrStateDetails(userProfile, profileDetails);
        assertEquals("ORG_001", userProfile.get(Constants.MINISTRY_OR_STATE_ID_RQST));
        assertEquals("Ministry of Example", userProfile.get(Constants.MINISTRY_OR_STATE_ORG_NAME.toLowerCase()));
    }

    @Test
    void testExtractMinistryOrStateDetailsSkipsNullMinistryId() {
        Map<String, String> userProfile = new HashMap<>();
        Map<String, Object> profileDetails = new HashMap<>();
        profileDetails.put(Constants.MINISTRY_OR_STATEID, null);
        enrichmentService.extractMinistryOrStateDetails(userProfile, profileDetails);
        assertFalse(userProfile.containsKey(Constants.MINISTRY_OR_STATE_ID_RQST));
    }

    @Test
    void testExtractMinistryOrStateDetailsSkipsBlankMinistryId() {
        Map<String, String> userProfile = new HashMap<>();
        Map<String, Object> profileDetails = new HashMap<>();
        profileDetails.put(Constants.MINISTRY_OR_STATEID, "   ");
        enrichmentService.extractMinistryOrStateDetails(userProfile, profileDetails);
        assertFalse(userProfile.containsKey(Constants.MINISTRY_OR_STATE_ID_RQST));
    }

    @Test
    void testExtractMinistryOrStateDetailsSkipsNullOrgName() {
        Map<String, String> userProfile = new HashMap<>();
        Map<String, Object> profileDetails = new HashMap<>();
        profileDetails.put(Constants.MINISTRY_OR_STATE_ORG_NAME, null);
        enrichmentService.extractMinistryOrStateDetails(userProfile, profileDetails);
        assertFalse(userProfile.containsKey(Constants.MINISTRY_OR_STATE_ORG_NAME.toLowerCase()));
    }

    @Test
    void testExtractMinistryOrStateDetailsSkipsBlankOrgName() {
        Map<String, String> userProfile = new HashMap<>();
        Map<String, Object> profileDetails = new HashMap<>();
        profileDetails.put(Constants.MINISTRY_OR_STATE_ORG_NAME, "");
        enrichmentService.extractMinistryOrStateDetails(userProfile, profileDetails);
        assertFalse(userProfile.containsKey(Constants.MINISTRY_OR_STATE_ORG_NAME.toLowerCase()));
    }

    @Test
    void testExtractMinistryOrStateDetailsHandlesEmptyProfileDetails() {
        Map<String, String> userProfile = new HashMap<>();
        Map<String, Object> profileDetails = new HashMap<>();
        enrichmentService.extractMinistryOrStateDetails(userProfile, profileDetails);
        assertTrue(userProfile.isEmpty());
    }
}
