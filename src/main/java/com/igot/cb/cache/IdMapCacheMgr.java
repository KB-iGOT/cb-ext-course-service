package com.igot.cb.cache;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.web.util.UriComponentsBuilder;

import com.igot.cb.model.CachedIdMap;
import com.igot.cb.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PropertiesCache;

import lombok.extern.slf4j.Slf4j;

/**
 * Cache manager for ID mappings, which retrieves and caches ID mappings from an external service.
 * It checks the cache for existing mappings and fetches missing ones from the ID Map service.
 */
@Component
@Slf4j
public class IdMapCacheMgr {
    private final OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    private final PropertiesCache propertiesCache = PropertiesCache.getInstance();
    private Map<String, CachedIdMap> cacheMap = new ConcurrentHashMap<>();
    private long defaultExpiryTime = 3600000; // 1 hour in milliseconds

    /**
     * Constructor for IdMapCacheMgr.
     *
     * @param outboundRequestHandlerService Service to handle outbound requests.
     */
    public IdMapCacheMgr(OutboundRequestHandlerServiceImpl outboundRequestHandlerService) {
        this.outboundRequestHandlerService = outboundRequestHandlerService;
    }

    /**
     * Retrieves the ID mappings for a list of keys.
     * It checks the cache first and fetches missing mappings from the ID Map service.
     *
     * @param keys List of keys to retrieve ID mappings for.
     * @return A map containing the keys and their corresponding ID mappings.
     */
    public Map<String, Integer> getId(List<String> keys) {
        StringBuilder missingKeys = new StringBuilder();
        Map<String, Integer> result = new HashMap<>();
        for (String key : keys) {
            String keyTrimmed = key.trim().toLowerCase();
            if (cacheMap.containsKey(keyTrimmed)) {
                CachedIdMap cachedIdMap = cacheMap.get(keyTrimmed);
                if (cachedIdMap.isExpired(defaultExpiryTime)) {
                    missingKeys.append(key).append(Constants.HASH);
                } else {
                    result.put(key, cachedIdMap.getValue());
                }
            } else {
                missingKeys.append(key).append(Constants.HASH);
            }
        }
        if (missingKeys.isEmpty()) {
            return result;
        } else {
            missingKeys.setLength(missingKeys.length() - 1);
            List<String> missingKeysList = Arrays.asList(missingKeys.toString().split(Constants.HASH));
            int batchSize = 50;
            List<List<String>> batches = createBatches(missingKeysList, batchSize);
            for (List<String> batch : batches) {
                URI uri = UriComponentsBuilder
                        .fromHttpUrl(propertiesCache.getProperty(Constants.ID_MAP_SERVICE_URL)
                                + propertiesCache.getProperty(Constants.ID_MAP_SERVICE_READ_ENDPOINT))
                        .queryParam(Constants.ID_MAP_SERVICE_PARAM_LIST, String.join(Constants.HASH, batch))
                        .queryParam(Constants.ID_MAP_SERVICE_PARAM_SEPARATOR, Constants.HASH)
                        .build().encode().toUri();

                ParameterizedTypeReference<List<Map<String, Integer>>> responseType = new ParameterizedTypeReference<>() {};
                List<Map<String, Integer>> response = outboundRequestHandlerService
                        .fetchResultUsingExchange(uri.toString(), responseType);

                if (CollectionUtils.isEmpty(response)) {
                    log.error("IdMapCacheMgr::getId: No response from ID Map service for keys: {}", batch);
                    break;
                } else {
                    response.stream()
                        .flatMap(responseObject -> responseObject.entrySet().stream())
                        .forEach(entry -> {
                            String decodedKey = decodeKey(entry.getKey());
                            cacheMap.put(decodedKey, new CachedIdMap(entry.getValue(), defaultExpiryTime));
                            result.put(decodedKey, entry.getValue());
                        });
                }
                log.info("IdMapCacheMgr::getId: request url : {}, response: {}", uri.toString(), result);
            }
        }
        return result;
    }

    private List<List<String>> createBatches(List<String> missingKeys, int batchSize) {
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < missingKeys.size(); i += batchSize) {
            batches.add(missingKeys.subList(i, Math.min(i + batchSize, missingKeys.size())));
        }
        return batches;
    }

    /**
     * Decodes a URL-encoded key and normalizes it to lowercase.
     *
     * @param encodedKey The URL-encoded key from ID-Mapping service
     * @return Decoded and normalized key
     */
    private String decodeKey(String encodedKey) {
        return URLDecoder.decode(encodedKey, StandardCharsets.UTF_8).trim().toLowerCase();
    }
}
