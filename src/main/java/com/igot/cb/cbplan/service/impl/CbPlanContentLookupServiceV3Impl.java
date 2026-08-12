package com.igot.cb.cbplan.service.impl;

import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PropertiesCache;

import lombok.extern.slf4j.Slf4j;

/**
 * Service for managing CB Plan content lookup table operations.
 *
 * @version 3.0
 */
@Service
@Slf4j
public class CbPlanContentLookupServiceV3Impl {
    private final CassandraOperation cassandraOperation;
    private final RedisCacheMgr redisCacheMgr;
    private final OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    private final PropertiesCache propertiesCache;
    private final ObjectMapper mapper;

    public CbPlanContentLookupServiceV3Impl(CassandraOperation cassandraOperation,
                                            RedisCacheMgr redisCacheMgr,
                                            OutboundRequestHandlerServiceImpl outboundRequestHandlerService) {
        this.cassandraOperation = cassandraOperation;
        this.redisCacheMgr = redisCacheMgr;
        this.outboundRequestHandlerService = outboundRequestHandlerService;
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
     * Adds the plan ID to each content's plan set in the lookup table.
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
     * Updates content lookup for modified CB Plan.
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
            log.debug("updateContentLookupForModifiedPlan: Upserting added content - planId={}, count={}",
                    cbPlanId, addedContent.size());
            upsertCbPlanContentLookup(cbPlanId, addedContent);
        }
        if (CollectionUtils.isNotEmpty(deletedContent)) {
            log.debug("updateContentLookupForModifiedPlan: Removing deleted content - planId={}, count={}",
                    cbPlanId, deletedContent.size());
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
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_CB_PLAN_V3_CONTENT_LOOKUP,
                    where, List.of(Constants.PLAN_ID_COLUMN), 1);
            if (CollectionUtils.isEmpty(rows)) {
                log.debug("removeCbPlanInfoForUpdateOrDeleteCbPlan: No row found - contentId={}", contentId);
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
     * Gets added content by comparing existing and updated content lists.
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
     * Gets deleted content by comparing existing and updated content lists.
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
     * Fetches existing plan IDs from content lookup table.
     *
     * @param where query criteria (content_id)
     * @return set of plan IDs, or empty set if not found
     */
    private Set<String> fetchExistingPlanIds(Map<String, Object> where) {
        Set<String> planIds = new HashSet<>();
        List<Map<String, Object>> rows = cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_CB_PLAN_V3_CONTENT_LOOKUP,
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

    /**
     * Updates content lookup record with new plan IDs set.
     *
     * @param where   query criteria (content_id)
     * @param planIds updated set of plan IDs
     */
    private void updateContentLookupRecord(Map<String, Object> where, Set<String> planIds) {
        Map<String, Object> update = Map.of(Constants.PLAN_ID_COLUMN, planIds);
        cassandraOperation.updateRecord(
                Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_CB_PLAN_V3_CONTENT_LOOKUP,
                update,
                where);
    }

    /**
     * Processes content lookup removal.
     * Deletes row if it's the last plan, otherwise updates the plan set.
     *
     * @param cbPlanId  CB Plan ID to remove
     * @param contentId content ID
     * @param rows      existing rows from query
     * @param where     query criteria
     * @return Constants.RESULT_DELETED, RESULT_UPDATED, or RESULT_SKIPPED
     */
    private String processContentLookupRemoval(String cbPlanId, String contentId,
                                               List<Map<String, Object>> rows, Map<String, Object> where) {
        Object existing = rows.get(0).get(Constants.PLAN_ID);
        if (!(existing instanceof Set)) {
            log.warn("processContentLookupRemoval: Invalid planid data type - contentId={}", contentId);
            return Constants.RESULT_SKIPPED;
        }
        Set<String> planIds = new HashSet<>((Set<String>) existing);
        if (planIds.size() == 1 && planIds.contains(cbPlanId)) {
            deleteContentLookupRow(contentId, cbPlanId, where);
            return Constants.RESULT_DELETED;
        } else if (planIds.size() > 1 && planIds.contains(cbPlanId)) {
            updateContentLookupRow(contentId, cbPlanId, planIds, where);
            return Constants.RESULT_UPDATED;
        }
        return Constants.RESULT_SKIPPED;
    }

    /**
     * Deletes content lookup row.
     *
     * @param contentId content ID
     * @param cbPlanId  CB Plan ID
     * @param where     query criteria
     */
    private void deleteContentLookupRow(String contentId, String cbPlanId, Map<String, Object> where) {
        cassandraOperation.deleteRecord(Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_CB_PLAN_V3_CONTENT_LOOKUP, where);
        log.info("deleteContentLookupRow: Deleted row - contentId={}, lastPlanId={}",
                contentId, cbPlanId);
    }

    /**
     * Updates content lookup row by removing a plan from the set.
     *
     * @param contentId content ID
     * @param cbPlanId  CB Plan ID to remove
     * @param planIds   current set of plan IDs
     * @param where     query criteria
     */
    private void updateContentLookupRow(String contentId, String cbPlanId, Set<String> planIds,
                                        Map<String, Object> where) {
        planIds.remove(cbPlanId);
        Map<String, Object> update = Map.of(Constants.PLAN_ID_COLUMN, planIds);
        cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_CB_PLAN_V3_CONTENT_LOOKUP, update, where);
        log.info("updateContentLookupRow: Removed plan - contentId={}, removedPlanId={}, remainingCount={}",
                contentId, cbPlanId, planIds.size());
    }

    /**
     * Gets content metadata from Redis cache first, falls back to extended content read API if not cached.
     * Uses the same Redis key format as the extended content read API in knowledge-platform.
     * Redis key: extended_read_content_{contentId}
     * API endpoint: GET /content/v1/extended/read/{contentId}
     *
     * @param contentId content ID
     * @return content metadata map, or empty map if not found
     * @throws JsonProcessingException if deserialization fails
     */
    public Map<String, Object> getContentMetadata(String contentId) throws JsonProcessingException {
        String cacheKey = Constants.EXTENDED_READ_CONTENT_CACHE_KEY_PREFIX + contentId;
        String cachedContent = redisCacheMgr.getFromCache(cacheKey);
        if (StringUtils.isNotBlank(cachedContent)) {
            log.debug("getContentMetadata: Redis cache hit - contentId={}, cacheKey={}", contentId, cacheKey);
            return mapper.readValue(cachedContent, new TypeReference<Map<String, Object>>() {
            });
        }
        log.debug("getContentMetadata: Redis cache miss, calling extended content read API - contentId={}, cacheKey={}",
                contentId, cacheKey);
        Map<String, Object> contentDetails = readExtendedContent(contentId);
        return Objects.nonNull(contentDetails) ? contentDetails : new HashMap<>();
    }

    /**
     * Reads content metadata from extended content read API in knowledge-platform.
     * Endpoint: GET /content/v1/extended/read/{contentId}
     * Special handling: External content (contentId starts with "ext_") uses readExternalContent.
     *
     * @param contentId content identifier
     * @return content metadata map, or empty map if not found
     */
    private Map<String, Object> readExtendedContent(String contentId) {
        if (StringUtils.isBlank(contentId)) {
            log.error("Content ID is null or empty for extended read");
            return Collections.emptyMap();
        }
        try {
            log.info("Reading extended content for contentId: {}", contentId);
            if (contentId.startsWith(Constants.EXTERNAL_CONTENT_PREFIX)) {
                return readExternalContent(contentId);
            }
            return callExtendedContentReadAPI(contentId);
        } catch (Exception e) {
            log.error("Failed to read extended content for contentId: {}", contentId, e);
            return Collections.emptyMap();
        }
    }

    /**
     * Calls extended content read API endpoint.
     *
     * @param contentId content identifier
     * @return content metadata map, or empty map if not found
     */
    private Map<String, Object> callExtendedContentReadAPI(String contentId) {
        String url = buildExtendedContentURL(contentId);
        Map<String, Object> response = (Map<String, Object>) outboundRequestHandlerService.fetchResult(url);
        if (Objects.nonNull(response) && Constants.OK.equalsIgnoreCase((String) response.get(Constants.RESPONSE_CODE))) {
            Map<String, Object> result = (Map<String, Object>) response.get(Constants.RESULT);
            return (Map<String, Object>) result.get(Constants.CONTENT);
        }
        log.warn("Extended content read returned non-OK response for contentId: {}", contentId);
        return Collections.emptyMap();
    }

    /**
     * Builds URL for extended content read API.
     *
     * @param contentId content identifier
     * @return API URL
     */
    private String buildExtendedContentURL(String contentId) {
        return propertiesCache.getProperty(Constants.CONTENT_SERVICE_HOST) +
                propertiesCache.getProperty(Constants.EXTENDED_CONTENT_READ_END_POINT) +
                "/" + contentId;
    }

    /**
     * Reads external content information from the external content service.
     *
     * @param contentId external content ID (starts with "ext_")
     * @return content metadata map, or empty map if not found
     */
    private Map<String, Object> readExternalContent(String contentId) {
        try {
            log.info("Reading external content for contentId: {}", contentId);
            String url = buildExternalContentURL(contentId);
            return fetchExternalContentFromAPI(url);
        } catch (Exception e) {
            log.error("Failed to read external content for contentId: {}", contentId, e);
            return Collections.emptyMap();
        }
    }

    /**
     * Fetches external content from API and extracts event data.
     *
     * @param url API URL
     * @return event data map, or empty map if not found
     */
    private Map<String, Object> fetchExternalContentFromAPI(String url) {
        Map<String, Object> response = (Map<String, Object>) outboundRequestHandlerService.fetchResult(url);
        if (Objects.nonNull(response) && response.containsKey(Constants.RESULT)) {
            Map<String, Object> result = (Map<String, Object>) response.get(Constants.RESULT);
            Map<String, Object> event = (Map<String, Object>) result.get(Constants.EVENT);
            if (Objects.nonNull(event) && !event.isEmpty()) {
                return event;
            }
        }
        return Collections.emptyMap();
    }

    /**
     * Builds URL for external content read API.
     *
     * @param contentId content identifier
     * @return API URL
     */
    private String buildExternalContentURL(String contentId) {
        return propertiesCache.getProperty(Constants.CB_PORES_SERVICE_HOST) +
                propertiesCache.getProperty(Constants.EXTERNAL_CONTENT_READ_END_POINT) +
                "/" + contentId;
    }
}
