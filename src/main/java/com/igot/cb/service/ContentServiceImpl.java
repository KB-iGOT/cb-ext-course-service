package com.igot.cb.service;

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

@Service
@Slf4j
public class ContentServiceImpl {

    private final OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    private final RedisCacheMgr redisCacheMgr;
    private final PropertiesCache propertiesCache;
    private final ObjectMapper mapper;

    public ContentServiceImpl(OutboundRequestHandlerServiceImpl outboundRequestHandlerService,
            RedisCacheMgr redisCacheMgr) {
        this.outboundRequestHandlerService = outboundRequestHandlerService;
        this.redisCacheMgr = redisCacheMgr;
        this.propertiesCache = PropertiesCache.getInstance();
        this.mapper = new ObjectMapper();
    }

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
                return readContentFromService(contentId, fields);
            } else {
                return responseData;
            }
        } catch (Exception e) {
            log.error("Failed to parse content info from redis. Exception: " + e.getMessage(), e);
        }
        return Collections.emptyMap();
    }

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

    public String readCourseCategoryForContent(String contentId) {
        List<String> fields = List.of(Constants.CONTENT_ID, Constants.COURSE_CATEGORY);
        return readContent(contentId, fields).getOrDefault(Constants.COURSE_CATEGORY, "").toString();
    }
}
