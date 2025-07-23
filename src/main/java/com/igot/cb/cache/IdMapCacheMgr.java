package com.igot.cb.cache;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections4.MapUtils;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.web.util.UriComponentsBuilder;

import com.igot.cb.model.CachedIdMap;
import com.igot.cb.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PropertiesCache;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class IdMapCacheMgr {
    private final OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    private final PropertiesCache propertiesCache = PropertiesCache.getInstance();
    private Map<String, CachedIdMap> cacheMap = new ConcurrentHashMap<>();
    private long defaultExpiryTime = 3600000; // 1 hour in milliseconds

    public IdMapCacheMgr(OutboundRequestHandlerServiceImpl outboundRequestHandlerService) {
        this.outboundRequestHandlerService = outboundRequestHandlerService;
    }

    public Map<String, Long> getId(List<String> keys) {
        StringBuilder missingKeys = new StringBuilder();
        Map<String, Long> result = new HashMap<>();
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
            
            URI uri = UriComponentsBuilder
                    .fromHttpUrl(propertiesCache.getProperty(Constants.ID_MAP_SERVICE_URL)
                            + propertiesCache.getProperty(Constants.ID_MAP_SERVICE_READ_ENDPOINT))
                    .queryParam(Constants.ID_MAP_SERVICE_PARAM_LIST, missingKeys.toString())
                    .queryParam(Constants.ID_MAP_SERVICE_PARAM_SEPARATOR, Constants.HASH)
                    .build().encode().toUri();

            ParameterizedTypeReference<List<Map<String, Long>>> responseType = new ParameterizedTypeReference<>() {
            };
            List<Map<String, Long>> response = outboundRequestHandlerService
                    .fetchResultUsingExchange(uri.toString(), responseType);

            if (CollectionUtils.isEmpty(response)) {
                log.error("IdMapCacheMgr::getId: No response from ID Map service for keys: {}", missingKeys);
                return result;
            } else {
                for (Map<String, Long> responseObject : response) {
                    while (responseObject.keySet().iterator().hasNext()) {
                        String key = responseObject.keySet().iterator().next();
                        cacheMap.put(key.trim().toLowerCase(), new CachedIdMap(responseObject.get(key), defaultExpiryTime));
                        result.put(key, responseObject.get(key));
                        responseObject.remove(key);
                    }
                }
            }
            log.info("IdMapCacheMgr::getId: url executed: {}", uri.toString());
            log.info("IdMapCacheMgr::getId: Fetched keys with value: {}", result);
        }
        return result;
    }
}
