package com.igot.cb.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Service class for handling operations related to Content Dictionary.
 */
@Service
@Slf4j
public class ContentDictionaryService {

    private final RedisCacheMgr redisCacheMgr;
    private final ObjectMapper objectMapper;

    public ContentDictionaryService(RedisCacheMgr redisCacheMgr, ObjectMapper objectMapper) {
        this.redisCacheMgr = redisCacheMgr;
        this.objectMapper = objectMapper;
    }

    /**
     * Reads the content dictionary from Redis HASH "content_dictionary_v2"
     * and constructs the API Response.
     * <p>
     * Response structure is maintained for backward compatibility with UI.
     *
     * @return ApiResponse containing the content dictionary as result map or error status.
     */
    public ApiResponse getContentDictionary() {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CONTENT_DICTIONARY_READ);
        try {
            log.debug("ContentDictionaryService::getContentDictionary - Fetching from Redis HASH: {}", Constants.REDIS_CONTENT_DICTIONARY_KEY);
            Map<String, String> hashData = redisCacheMgr.getAllCachedAccessRules(Constants.REDIS_CONTENT_DICTIONARY_KEY);
            if (MapUtils.isEmpty(hashData)) {
                log.warn("ContentDictionaryService::getContentDictionary - Content dictionary HASH '{}' not found or empty in Redis", Constants.REDIS_CONTENT_DICTIONARY_KEY);
                ProjectUtil.setFailedResponse(response, "Content dictionary not found in cache", HttpStatus.NOT_FOUND);
                return response;
            }
            Map<String, Object> dictionary = new HashMap<>();
            hashData.forEach((identifier, jsonValue) ->
                parseHashItem(identifier, jsonValue)
                    .ifPresent(itemData -> dictionary.put(identifier, itemData))
            );
            if (dictionary.isEmpty()) {
                log.error("ContentDictionaryService::getContentDictionary - No valid items found after parsing");
                ProjectUtil.setFailedResponse(response, "Content dictionary is empty or corrupted", HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
            response.setResult(dictionary);
            response.setResponseCode(HttpStatus.OK);
            log.info("ContentDictionaryService::getContentDictionary - Successfully retrieved {} items from content dictionary",
                    dictionary.size());
        } catch (Exception e) {
            log.error("ContentDictionaryService::getContentDictionary - Error retrieving content dictionary from Redis", e);
            ProjectUtil.setFailedResponse(response, "Failed to read content dictionary: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }


    /**
     * Parse a single item from Redis HASH field.
     *
     * @param identifier The item identifier
     * @param jsonValue  The JSON string value
     * @return Optional containing parsed Map, empty if parsing fails
     */
    private Optional<Map<String, Object>> parseHashItem(String identifier, String jsonValue) {
        try {
            Map<String, Object> parsedData = objectMapper.readValue(jsonValue, new TypeReference<>() {
            });
            return Optional.of(parsedData);
        } catch (Exception parseException) {
            log.warn("ContentDictionaryService::parseHashItem - Failed to parse JSON for identifier: {}, error: {}",
                    identifier, parseException.getMessage());
            return Optional.empty();
        }
    }
}
