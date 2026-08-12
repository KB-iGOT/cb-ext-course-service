package com.igot.cb.cbplan.service.impl;

import java.util.*;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import com.igot.cb.service.ContentInfoServiceImpl;
import com.igot.cb.service.UserAndOrgServiceImpl;
import com.igot.cb.util.Constants;

import lombok.extern.slf4j.Slf4j;

/**
 * Service for enriching CB Plan search results.
 *
 * @version 3.0
 */
@Service
@Slf4j
public class CbPlanEnrichmentServiceV3Impl {
    private final UserAndOrgServiceImpl userAndOrgService;
    private final ContentInfoServiceImpl contentService;

    public CbPlanEnrichmentServiceV3Impl(UserAndOrgServiceImpl userAndOrgService, ContentInfoServiceImpl contentService) {
        this.userAndOrgService = userAndOrgService;
        this.contentService = contentService;
    }

    /**
     * Enriches search results with user and content information.
     *
     * @param dataNode search results data
     * @return enriched search results
     */
    public List<Map<String, Object>> enrichSearchResults(List<Map<String, Object>> dataNode) {
        List<Map<String, Object>> enrichedData = new ArrayList<>();
        for (Map<String, Object> item : dataNode) {
            Map<String, Object> enrichedItem = new HashMap<>(item);
            enrichCreatedByInfo(item, enrichedItem);
            enrichContentListInfo(item, enrichedItem);
            enrichedData.add(enrichedItem);
        }
        return enrichedData;
    }

    /**
     * Enriches createdBy information with user details.
     *
     * @param item         original item
     * @param enrichedItem enriched item to populate
     */
    public void enrichCreatedByInfo(Map<String, Object> item, Map<String, Object> enrichedItem) {
        if (!item.containsKey(Constants.CREATED_BY)) {
            return;
        }
        Object createdByObj = item.get(Constants.CREATED_BY);
        if (Objects.isNull(createdByObj)) {
            return;
        }
        if (createdByObj instanceof String stringValue && StringUtils.isNotBlank(stringValue)) {
            Map<String, Object> userInfoMap = userAndOrgService.readUserProfile(
                    stringValue,
                    Arrays.asList(Constants.FIRSTNAME, Constants.USER_ID));
            if (MapUtils.isNotEmpty(userInfoMap)) {
                enrichedItem.put(Constants.CREATED_BY_NAME, userInfoMap.get(Constants.FIRSTNAME));
                enrichedItem.put(Constants.CREATED_BY, item.get(Constants.CREATED_BY));
            }
        }
    }

    /**
     * Enriches contentList with content metadata.
     *
     * @param item         original item
     * @param enrichedItem enriched item to populate
     */
    public void enrichContentListInfo(Map<String, Object> item, Map<String, Object> enrichedItem) {
        if (!item.containsKey(Constants.CONTENT_LIST)) {
            return;
        }
        Object contentListObj = item.get(Constants.CONTENT_LIST);
        if (Objects.isNull(contentListObj)) {
            return;
        }
        if (contentListObj instanceof List) {
            enrichedItem.put(Constants.CONTENT_LIST,
                    contentService.enrichContentInfoForCBPlan((List<String>) contentListObj));
        }
    }
}
