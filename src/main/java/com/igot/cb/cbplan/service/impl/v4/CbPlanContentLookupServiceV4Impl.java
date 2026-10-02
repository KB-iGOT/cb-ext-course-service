package com.igot.cb.cbplan.service.impl.v4;

import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PropertiesCache;

import lombok.extern.slf4j.Slf4j;

/**
 * V4 content lookup service targeting the table configured via
 * {@code cbplan.v4.content.lookup.table} in application.properties.
 *
 * @version 4.0
 */
@Service
@Slf4j
public class CbPlanContentLookupServiceV4Impl {

    private final CassandraOperation cassandraOperation;
    private final RedisCacheMgr redisCacheMgr;
    private final OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    private final CbExtServerProperties serverProperties;
    private final PropertiesCache propertiesCache;
    private final ObjectMapper mapper;

    public CbPlanContentLookupServiceV4Impl(CassandraOperation cassandraOperation,
                                            RedisCacheMgr redisCacheMgr,
                                            OutboundRequestHandlerServiceImpl outboundRequestHandlerService,
                                            CbExtServerProperties serverProperties) {
        this.cassandraOperation = cassandraOperation;
        this.redisCacheMgr = redisCacheMgr;
        this.outboundRequestHandlerService = outboundRequestHandlerService;
        this.serverProperties = serverProperties;
        this.propertiesCache = PropertiesCache.getInstance();
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Updates content lookup table for a CB Plan.
     *
     * @param planId   CB Plan ID
     * @param planData CB Plan data containing content list
     */
    public void updateContentLookup(String planId, Map<String, Object> planData) {
        log.debug("updateContentLookup: Entry - planId={}", planId);
        List<String> contentIds = (List<String>) planData.get(Constants.CONTENT_LIST);
        if (CollectionUtils.isEmpty(contentIds)) {
            log.debug("updateContentLookup: No content to update - planId={}", planId);
            return;
        }
        log.info("updateContentLookup: Updating lookup for planId={}, contentCount={}", planId, contentIds.size());
        upsertCbPlanContentLookup(planId, contentIds);
    }

    /**
     * Upserts content lookup entries for a CB Plan.
     *
     * @param planId     CB Plan ID
     * @param contentIds list of content IDs
     */
    public void upsertCbPlanContentLookup(String planId, List<String> contentIds) {
        log.debug("upsertCbPlanContentLookup: Processing - planId={}, contentCount={}", planId, contentIds.size());
        int updated = 0;
        int skipped = 0;
        for (String contentId : contentIds) {
            Map<String, Object> where = Map.of(Constants.CONTENT_ID_COLUMN, contentId);
            Set<String> planIds = fetchExistingPlanIds(where);
            if (!planIds.contains(planId)) {
                planIds.add(planId);
                updateContentLookupRecord(where, planIds);
                updated++;
            } else {
                skipped++;
            }
        }
        log.info("upsertCbPlanContentLookup: Completed - planId={}, updated={}, skipped={}",
                planId, updated, skipped);
    }

    /**
     * Updates content lookup for a modified CB Plan.
     * Handles content additions and deletions.
     *
     * @param cbPlanId       CB Plan ID
     * @param updatedRequest updated request data
     * @param existingCbPlan existing CB Plan data
     */
    public void updateContentLookupForModifiedPlan(String cbPlanId, Map<String, Object> updatedRequest,
                                                   Map<String, Object> existingCbPlan) {
        log.debug("updateContentLookupForModifiedPlan: Entry - planId={}", cbPlanId);
        List<String> updatedContentIds = (List<String>) updatedRequest.get(Constants.CONTENT_LIST);
        if (CollectionUtils.isEmpty(updatedContentIds)) {
            log.debug("updateContentLookupForModifiedPlan: No updated content - planId={}", cbPlanId);
            return;
        }
        List<String> existingContentIds = (List<String>) existingCbPlan.get(Constants.CONTENT_LIST);
        List<String> addedContent = getAddedContent(existingContentIds, updatedContentIds);
        List<String> deletedContent = getDeletedContent(existingContentIds, updatedContentIds);
        log.info("updateContentLookupForModifiedPlan: planId={}, added={}, deleted={}",
                cbPlanId, addedContent.size(), deletedContent.size());
        if (CollectionUtils.isNotEmpty(addedContent)) {
            upsertCbPlanContentLookup(cbPlanId, addedContent);
        }
        if (CollectionUtils.isNotEmpty(deletedContent)) {
            removeCbPlanInfoForUpdateOrDeleteCbPlan(cbPlanId, deletedContent);
        }
    }

    /**
     * Removes CB Plan from all its content lookup entries.
     *
     * @param cbPlanId       CB Plan ID
     * @param existingCbPlan existing CB Plan data
     */
    public void removeFromContentLookup(String cbPlanId, Map<String, Object> existingCbPlan) {
        log.debug("removeFromContentLookup: Entry - planId={}", cbPlanId);
        List<String> contentIds = (List<String>) existingCbPlan.get(Constants.CONTENT_LIST);
        if (CollectionUtils.isEmpty(contentIds)) {
            log.debug("removeFromContentLookup: No content to remove - planId={}", cbPlanId);
            return;
        }
        log.info("removeFromContentLookup: Removing planId={} from {} content entries", cbPlanId, contentIds.size());
        removeCbPlanInfoForUpdateOrDeleteCbPlan(cbPlanId, contentIds);
    }

    /**
     * Removes CB Plan info from content lookup entries.
     * Deletes row if it's the last plan, otherwise removes plan from the set.
     *
     * @param cbPlanId   CB Plan ID
     * @param contentIds list of content IDs
     */
    public void removeCbPlanInfoForUpdateOrDeleteCbPlan(String cbPlanId, List<String> contentIds) {
        log.debug("removeCbPlanInfoForUpdateOrDeleteCbPlan: Entry - planId={}, contentCount={}",
                cbPlanId, contentIds.size());
        int deleted = 0;
        int updated = 0;
        int skipped = 0;
        for (String contentId : contentIds) {
            Map<String, Object> where = Map.of(Constants.CONTENT_ID_COLUMN, contentId);
            List<Map<String, Object>> rows = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, serverProperties.getCbPlanV4ContentLookupTable(),
                    where, List.of(Constants.PLAN_ID_COLUMN), 1);
            if (CollectionUtils.isEmpty(rows)) {
                skipped++;
                continue;
            }
            String result = processContentLookupRemoval(cbPlanId, contentId, rows, where);
            if (Constants.RESULT_DELETED.equals(result)) {
                deleted++;
            } else if (Constants.RESULT_UPDATED.equals(result)) {
                updated++;
            } else {
                skipped++;
            }
        }
        log.info("removeCbPlanInfoForUpdateOrDeleteCbPlan: Completed - planId={}, deleted={}, updated={}, skipped={}",
                cbPlanId, deleted, updated, skipped);
    }

    /**
     * Returns added content by comparing existing and updated content lists.
     *
     * @param existingContent existing content list
     * @param updatedContent  updated content list
     * @return list of added content IDs
     */
    public List<String> getAddedContent(List<String> existingContent, List<String> updatedContent) {
        Set<String> existingSet = new HashSet<>(Objects.nonNull(existingContent) ? existingContent : List.of());
        Set<String> updatedSet = new HashSet<>(Objects.nonNull(updatedContent) ? updatedContent : List.of());
        updatedSet.removeAll(existingSet);
        return new ArrayList<>(updatedSet);
    }

    /**
     * Returns deleted content by comparing existing and updated content lists.
     *
     * @param existingContent existing content list
     * @param updatedContent  updated content list
     * @return list of deleted content IDs
     */
    public List<String> getDeletedContent(List<String> existingContent, List<String> updatedContent) {
        Set<String> existingSet = new HashSet<>(Objects.nonNull(existingContent) ? existingContent : List.of());
        Set<String> updatedSet = new HashSet<>(Objects.nonNull(updatedContent) ? updatedContent : List.of());
        existingSet.removeAll(updatedSet);
        return new ArrayList<>(existingSet);
    }

    /**
     * Gets content metadata from Redis cache first, falls back to extended content read API.
     *
     * @param contentId content ID
     * @return content metadata map, or empty map if not found
     */
    public Map<String, Object> getContentMetadata(String contentId) throws com.fasterxml.jackson.core.JsonProcessingException {
        String cacheKey = Constants.EXTENDED_READ_CONTENT_CACHE_KEY_PREFIX + contentId;
        String cachedContent = redisCacheMgr.getFromCache(cacheKey);
        if (StringUtils.isNotBlank(cachedContent)) {
            log.info("getContentMetadata: Redis cache hit - contentId={}", contentId);
            Map<String, Object> cachedData = mapper.readValue(cachedContent, new TypeReference<Map<String, Object>>() {
            });
            return extractContentFromCachedResponse(cachedData);
        }
        log.debug("getContentMetadata: Redis cache miss - contentId={}", contentId);
        Map<String, Object> contentDetails = readExtendedContent(contentId);
        return Objects.nonNull(contentDetails) ? contentDetails : new HashMap<>();
    }

    /**
     * Enriches content list using extended content read API with Redis caching.
     * Only returns LIVE status content with allowed fields filtered.
     *
     * @param contentIdList list of content IDs to enrich
     * @param allowedFields list of fields to include in enriched response
     * @return list of enriched content maps with filtered fields
     */
    public List<Map<String, Object>> enrichContentListForRead(List<String> contentIdList, List<String> allowedFields) {
        if (CollectionUtils.isEmpty(contentIdList)) {
            log.debug("enrichContentListForRead: Empty content list");
            return Collections.emptyList();
        }
        log.debug("enrichContentListForRead: Enriching {} content item(s)", contentIdList.size());
        List<Map<String, Object>> enrichedList = new ArrayList<>();
        for (String contentId : contentIdList) {
            Map<String, Object> enrichedContent = enrichSingleContent(contentId, allowedFields);
            if (MapUtils.isNotEmpty(enrichedContent)) {
                enrichedList.add(enrichedContent);
            }
        }
        log.info("enrichContentListForRead: Enriched {} out of {} item(s)", enrichedList.size(), contentIdList.size());
        return enrichedList;
    }

    private Set<String> fetchExistingPlanIds(Map<String, Object> where) {
        Set<String> planIds = new HashSet<>();
        List<Map<String, Object>> rows = cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD,
                serverProperties.getCbPlanV4ContentLookupTable(),
                where,
                List.of(Constants.PLAN_ID_COLUMN),
                1);
        if (CollectionUtils.isNotEmpty(rows)) {
            Object existing = rows.get(0).get(Constants.PLAN_ID);
            if (existing instanceof Set) {
                planIds.addAll((Set<String>) existing);
            }
        }
        return planIds;
    }

    private void updateContentLookupRecord(Map<String, Object> where, Set<String> planIds) {
        Map<String, Object> update = Map.of(Constants.PLAN_ID_COLUMN, planIds);
        cassandraOperation.updateRecord(
                Constants.KEYSPACE_SUNBIRD,
                serverProperties.getCbPlanV4ContentLookupTable(),
                update,
                where);
    }

    private String processContentLookupRemoval(String cbPlanId, String contentId,
                                               List<Map<String, Object>> rows, Map<String, Object> where) {
        Object existing = rows.get(0).get(Constants.PLAN_ID);
        if (!(existing instanceof Set)) {
            log.warn("processContentLookupRemoval: Invalid planid data type - contentId={}", contentId);
            return Constants.RESULT_SKIPPED;
        }
        Set<String> planIds = new HashSet<>((Set<String>) existing);
        if (planIds.size() == 1 && planIds.contains(cbPlanId)) {
            cassandraOperation.deleteRecord(Constants.KEYSPACE_SUNBIRD,
                    serverProperties.getCbPlanV4ContentLookupTable(), where);
            log.info("processContentLookupRemoval: Deleted row - contentId={}, lastPlanId={}", contentId, cbPlanId);
            return Constants.RESULT_DELETED;
        } else if (planIds.size() > 1 && planIds.contains(cbPlanId)) {
            planIds.remove(cbPlanId);
            Map<String, Object> update = Map.of(Constants.PLAN_ID_COLUMN, planIds);
            cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD,
                    serverProperties.getCbPlanV4ContentLookupTable(), update, where);
            log.info("processContentLookupRemoval: Removed plan - contentId={}, removedPlanId={}, remainingCount={}",
                    contentId, cbPlanId, planIds.size());
            return Constants.RESULT_UPDATED;
        }
        return Constants.RESULT_SKIPPED;
    }

    private Map<String, Object> enrichSingleContent(String contentId, List<String> allowedFields) {
        try {
            Map<String, Object> contentDetails = getContentMetadata(contentId);
            if (MapUtils.isEmpty(contentDetails)) {
                log.warn("enrichSingleContent: Content metadata not found - contentId={}", contentId);
                return Collections.emptyMap();
            }
            String status = (String) contentDetails.get(Constants.STATUS);
            if (!Constants.LIVE.equalsIgnoreCase(status)) {
                log.warn("enrichSingleContent: Content not LIVE - contentId={}, status={}", contentId, status);
                return Collections.emptyMap();
            }
            Map<String, Object> filteredDetails = filterContentFields(contentDetails, allowedFields);
            return MapUtils.isNotEmpty(filteredDetails) ? filteredDetails : Collections.emptyMap();
        } catch (Exception e) {
            log.error("enrichSingleContent: Failed to enrich content - contentId={}", contentId, e);
            return Collections.emptyMap();
        }
    }

    private Map<String, Object> filterContentFields(Map<String, Object> contentDetails, List<String> allowedFields) {
        if (MapUtils.isEmpty(contentDetails) || CollectionUtils.isEmpty(allowedFields)) {
            return contentDetails;
        }
        Map<String, Object> filteredMap = new LinkedHashMap<>();
        for (String field : allowedFields) {
            if (contentDetails.containsKey(field)) {
                filteredMap.put(field, contentDetails.get(field));
            }
        }
        return filteredMap;
    }

    private Map<String, Object> readExtendedContent(String contentId) {
        if (StringUtils.isBlank(contentId)) {
            log.error("readExtendedContent: contentId is blank");
            return Collections.emptyMap();
        }
        try {
            if (contentId.startsWith(Constants.EXTERNAL_CONTENT_PREFIX)) {
                return readExternalContent(contentId);
            }
            return callExtendedContentReadAPI(contentId);
        } catch (Exception e) {
            log.error("readExtendedContent: Failed for contentId={}", contentId, e);
            return Collections.emptyMap();
        }
    }

    private Map<String, Object> callExtendedContentReadAPI(String contentId) {
        String url = propertiesCache.getProperty(Constants.CONTENT_SERVICE_HOST)
                + propertiesCache.getProperty(Constants.EXTENDED_CONTENT_READ_END_POINT)
                + "/" + contentId;
        Map<String, Object> response = (Map<String, Object>) outboundRequestHandlerService.fetchResult(url);
        if (Objects.nonNull(response)
                && Constants.OK.equalsIgnoreCase((String) response.get(Constants.RESPONSE_CODE))) {
            Map<String, Object> result = (Map<String, Object>) response.get(Constants.RESULT);
            return (Map<String, Object>) result.get(Constants.CONTENT);
        }
        log.warn("callExtendedContentReadAPI: Non-OK response for contentId={}", contentId);
        return Collections.emptyMap();
    }

    private Map<String, Object> readExternalContent(String contentId) {
        try {
            String url = propertiesCache.getProperty(Constants.CB_PORES_SERVICE_HOST)
                    + propertiesCache.getProperty(Constants.EXTERNAL_CONTENT_READ_END_POINT)
                    + "/" + contentId;
            Map<String, Object> response = (Map<String, Object>) outboundRequestHandlerService.fetchResult(url);
            if (Objects.nonNull(response) && response.containsKey(Constants.RESULT)) {
                Map<String, Object> result = (Map<String, Object>) response.get(Constants.RESULT);
                Map<String, Object> event = (Map<String, Object>) result.get(Constants.EVENT);
                if (Objects.nonNull(event) && !event.isEmpty()) {
                    return event;
                }
            }
        } catch (Exception e) {
            log.error("readExternalContent: Failed for contentId={}", contentId, e);
        }
        return Collections.emptyMap();
    }

    private Map<String, Object> extractContentFromCachedResponse(Map<String, Object> cachedData) {
        if (cachedData.containsKey(Constants.RESULT)) {
            Map<String, Object> result = (Map<String, Object>) cachedData.get(Constants.RESULT);
            if (Objects.nonNull(result) && result.containsKey(Constants.CONTENT)) {
                return (Map<String, Object>) result.get(Constants.CONTENT);
            }
        }
        return cachedData;
    }
}
