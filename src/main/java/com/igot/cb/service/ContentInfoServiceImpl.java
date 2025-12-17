package com.igot.cb.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PropertiesCache;

import lombok.extern.slf4j.Slf4j;

/**
 * Service implementation for reading content information.
 * It fetches content data from cache or service based on the content ID and requested fields.
 */
@Service
@Slf4j
public class ContentInfoServiceImpl {

    private final OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    private final RedisCacheMgr redisCacheMgr;
    private final PropertiesCache propertiesCache;
    private final ObjectMapper mapper;

    /**
     * Constructor for ContentInfoServiceImpl.
     *
     * @param outboundRequestHandlerService Service to handle outbound requests.
     * @param redisCacheMgr                 Cache manager for Redis.
     */
    public ContentInfoServiceImpl(OutboundRequestHandlerServiceImpl outboundRequestHandlerService,
            RedisCacheMgr redisCacheMgr) {
        this.outboundRequestHandlerService = outboundRequestHandlerService;
        this.redisCacheMgr = redisCacheMgr;
        this.propertiesCache = PropertiesCache.getInstance();
        this.mapper = new ObjectMapper();
    }

    /**
     * Reads content information based on the content ID and requested fields.
     *
     * @param contentId The ID of the content to read.
     * @param fields    The list of fields to retrieve from the content.
     * @return A map containing the requested fields and their values, or an empty map if not found.
     */
    public Map<String, Object> readContent(String contentId, List<String> fields) {
        if (!StringUtils.hasText(contentId)) {
            log.error("Content ID is null or empty");
            return Collections.emptyMap();
        }
        try {
            log.info("Reading content with ID: {}", contentId);

            Map<String, Object> responseData = readContentFromCache(contentId, fields);
            if (MapUtils.isEmpty(responseData)) {
                log.info("Content not found in cache, fetching from service for contentId: {}", contentId);
                if(contentId.startsWith("ext_")){
                    return readExternalContent(contentId);
                }
                return readContentFromService(contentId, fields);
            } else {
                return responseData;
            }
        } catch (Exception e) {
            log.error("Failed to parse content info from redis. Exception: " + e.getMessage(), e);
        }
        return Collections.emptyMap();
    }

    /**
     * Reads content information from the cache.
     *
     * @param contentId The ID of the content to read.
     * @param fields    The list of fields to retrieve from the content.
     * @return A map containing the requested fields and their values, or an empty map if not found.
     * @throws Exception If there is an error reading from the cache.
     */
    public Map<String, Object> readContentFromCache(String contentId, List<String> fields) throws Exception {
        log.info("Reading content with ID from redis: {}", contentId);
        Map<String, Object> responseData = new HashMap<>();
        // Logic to read content from redis or content service
        String contentString = redisCacheMgr.getFromCache(contentId);
        if (StringUtils.hasText(contentString)) {
            Map<String, Object> contentData = mapper.readValue(contentString,
                    new TypeReference<Map<String, Object>>() {
                    });
            if (MapUtils.isNotEmpty(contentData)) {
                if (CollectionUtils.isEmpty(fields)) {
                    // If no specific fields are requested, return the entire content data
                    return contentData;
                }
                for (String field : fields) {
                    if (contentData.containsKey(field)) {
                        responseData.put(field, contentData.get(field));
                    }
                }
            }
        }
        return responseData;
    }

    /**
     * Reads content information from the service.
     *
     * @param contentId The ID of the content to read.
     * @param fields    The list of fields to retrieve from the content.
     * @return A map containing the requested fields and their values, or an empty map if not found.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> readContentFromService(String contentId, List<String> fields) {
        StringBuilder url = new StringBuilder();

        url.append(propertiesCache.getProperty(Constants.CONTENT_SERVICE_HOST))
                .append(propertiesCache.getProperty(Constants.CONTENT_READ_END_POINT)).append("/" + contentId);

        if (CollectionUtils.isNotEmpty(fields)) {
            StringBuffer stringBuffer = new StringBuffer(String.join(",", fields));
            url.append(Constants.QUE_MARK).append(Constants.FIELDS).append(Constants.EQUAL_TO).append(stringBuffer);
        }
        Map<String, Object> response = (Map<String, Object>) outboundRequestHandlerService
                .fetchResult(url.toString());
        if (null != response && Constants.OK.equalsIgnoreCase((String) response.get(Constants.RESPONSE_CODE))) {
            Map<String, Object> contentResult = (Map<String, Object>) response.get(Constants.RESULT);
            return (Map<String, Object>) contentResult.get(Constants.CONTENT);
        }

        return Collections.emptyMap();
    }

    /**
     * Reads the course category for a given content ID.
     *
     * @param contentId The ID of the content to read.
     * @return The course category associated with the content ID, or an empty string if not found.
     */
    public String readCourseCategoryForContent(String contentId) {
        List<String> fields = List.of(Constants.CONTENT_ID, Constants.COURSE_CATEGORY);
        return readContent(contentId, fields).getOrDefault(Constants.COURSE_CATEGORY, "").toString();
    }

    public List<Map<String, Object>> enrichContentInfoForCBPlan(List<String> contentIdList) {
        List<Map<String, Object>> enrichContentInfoMap = new ArrayList<>();
        for (Object contentIdObj : contentIdList) {
            String contentId = String.valueOf(contentIdObj);
            Map<String, Object> contentResponse = readContent(contentId, null);

            if (MapUtils.isNotEmpty(contentResponse) &&
                    Constants.LIVE.equalsIgnoreCase((String) contentResponse.get(Constants.STATUS))) {

                Map<String, Object> enrichContentMap = new HashMap<>();
                enrichContentMap.put(Constants.NAME, contentResponse.getOrDefault(Constants.NAME, ""));
                enrichContentMap.put(Constants.COMPETENCIES_V5,
                        contentResponse.getOrDefault(Constants.COMPETENCIES_V5, Collections.emptyList()));
                enrichContentMap.put(Constants.AVG_RATING, contentResponse.getOrDefault(Constants.AVG_RATING, 0.0));
                enrichContentMap.put(Constants.IDENTIFIER, contentResponse.getOrDefault(Constants.IDENTIFIER, ""));
                enrichContentMap.put(Constants.DESCRIPTION, contentResponse.getOrDefault(Constants.DESCRIPTION, ""));
                enrichContentMap.put(Constants.ADDITIONAL_TAGS,
                        contentResponse.getOrDefault(Constants.ADDITIONAL_TAGS, Collections.emptyList()));
                enrichContentMap.put(Constants.CONTENT_TYPE_KEY,
                        contentResponse.getOrDefault(Constants.CONTENT_TYPE_KEY, ""));
                enrichContentMap.put(Constants.PRIMARY_CATEGORY,
                        contentResponse.getOrDefault(Constants.PRIMARY_CATEGORY, ""));
                enrichContentMap.put(Constants.DURATION, contentResponse.getOrDefault(Constants.DURATION, 0));
                enrichContentMap.put(Constants.COURSE_APP_ICON,
                        contentResponse.getOrDefault(Constants.COURSE_APP_ICON, ""));
                enrichContentMap.put(Constants.POSTER_IMAGE, contentResponse.getOrDefault(Constants.POSTER_IMAGE, ""));
                enrichContentMap.put(Constants.ORGANISATION, contentResponse.getOrDefault(Constants.ORGANISATION, ""));
                enrichContentMap.put(Constants.CREATOR_LOGO, contentResponse.getOrDefault(Constants.CREATOR_LOGO, ""));
                enrichContentMap.put(Constants.LANGUAGE_MAP_V1,
                        contentResponse.getOrDefault(Constants.LANGUAGE_MAP_V1, Collections.emptyMap()));

                enrichContentInfoMap.add(enrichContentMap);
            } else {
                log.error("Content not found or not live for contentId: {}", contentId);
            }
        }
        return enrichContentInfoMap;
    }

    public Map<String, Object> readExternalContent(String contentId) {
        log.info("Reading external content with ID: {}", contentId);
        try {
            StringBuilder url = new StringBuilder();
            url.append(propertiesCache.getProperty(Constants.CB_PORES_SERVICE_HOST))
                    .append(propertiesCache.getProperty(Constants.EXTERNAL_CONTENT_READ_END_POINT)).append("/" + contentId);
            Map<String, Object> response = (Map<String, Object>) outboundRequestHandlerService
                    .fetchResult(url.toString());
            if (MapUtils.isNotEmpty(response) && response.containsKey(Constants.CONTENT)) {
                return response;
            }
            return Collections.emptyMap();
        } catch (Exception e) {
            log.error("Failed to parse external content info. Exception: " + e.getMessage(), e);
        }
        return Collections.emptyMap();
    }

    public Map<String, Object> retireContent(String contentId) {
        StringBuilder url = new StringBuilder();

        url.append(propertiesCache.getProperty(Constants.CONTENT_SERVICE_HOST))
                .append(propertiesCache.getProperty(Constants.CONTENT_RETIRE_END_POINT)).append("/" + contentId);

        Map<String, Object> response = (Map<String, Object>) outboundRequestHandlerService
                .fetchResultUsingDelete(url.toString(), new HashMap<>(), new HashMap<>());
        if (null != response && Constants.OK.equalsIgnoreCase((String) response.get(Constants.RESPONSE_CODE))) {
            Map<String, Object> contentResult = (Map<String, Object>) response.get(Constants.RESULT);
            return contentResult;
        }
        return Collections.emptyMap();
    }
}
