package com.igot.cb.service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.IdMapCacheMgr;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class UserProfileServiceImpl {
    private final RedisCacheMgr redisCacheMgr;
    private final CassandraOperation cassandraOperation;
    private final IdMapCacheMgr idMapCacheMgr;

    private final ObjectMapper mapper = new ObjectMapper();

    public UserProfileServiceImpl(RedisCacheMgr redisCacheMgr, CassandraOperation cassandraOperation,
            IdMapCacheMgr idMapCacheMgr) {
        this.redisCacheMgr = redisCacheMgr;
        this.cassandraOperation = cassandraOperation;
        this.idMapCacheMgr = idMapCacheMgr;
    }

    public Map<String, Integer> getUserProfile(String userId) {
        Map<String, Integer> userProfileBitMap = new HashMap<>();
        Map<String, String> userProfile = new HashMap<>();

        String cacheKey = Constants.USER + ":basicProfile:" + userId;
        String cachedProfile = redisCacheMgr.getFromCache(cacheKey);
        try {
            if (StringUtils.hasText(cachedProfile)) {
                setUserProfile(userProfile, mapper.readValue(cachedProfile,
                        new TypeReference<Map<String, Object>>() {
                        }));
            } else {
                Map<String, Object> queryParams = Map.of(Constants.ID, userId);
                List<String> fields = Arrays.asList(Constants.ID, Constants.ROOT_ORG_ID, Constants.PROFILE_DETAILS);
                List<Map<String, Object>> userList = cassandraOperation.getRecordsByProperties(
                        Constants.KEYSPACE_SUNBIRD, Constants.USER, queryParams, fields, null);

                if (CollectionUtils.isEmpty(userList)) {
                    log.error("Failed to read the user profile for userId: {}", userId);
                    return Map.of();
                }
                setUserProfile(userProfile, userList.get(0));
            }
            getUserBitMap(userProfile, userProfileBitMap);
            log.info("User profile fetched for userId: {}, converted bit map profile: {}", userId,
                    mapper.writeValueAsString(userProfileBitMap));
        } catch (Exception e) {
            log.error("Failed to parse user profile from cache for userId: {}. Exception: {}", userId, e.getMessage(),
                    e);
        }
        return userProfileBitMap;
    }

    @SuppressWarnings("unchecked")
    private void setUserProfile(Map<String, String> userProfile, Map<String, Object> userBasicProfile) {
        if (MapUtils.isEmpty(userBasicProfile)) {
            log.warn("User basic profile is empty for userId: {}", userProfile.get(Constants.ID));
            return;
        }
        userProfile.put(Constants.USER, (String) userBasicProfile.get(Constants.ID));
        userProfile.put(Constants.ROOT_ORG_ID.toLowerCase(), (String) userBasicProfile.get(Constants.ROOT_ORG_ID));
        Map<String, Object> profileDetails = (Map<String, Object>) userBasicProfile.get(Constants.PROFILE_DETAILS_KEY);
        if (!MapUtils.isEmpty(profileDetails)) {
            List<Map<String, Object>> professionalDetailList = (List<Map<String, Object>>) profileDetails
                    .get(Constants.PROFESSIONAL_DETAILS);
            if (CollectionUtils.isNotEmpty(professionalDetailList)) {
                Map<String, Object> professionalDetails = professionalDetailList.get(0);
                userProfile.put(Constants.DESIGNATION, (String) professionalDetails.get(Constants.DESIGNATION));
                userProfile.put(Constants.GROUP, (String) professionalDetails.get(Constants.GROUP));
            }
            userProfile.put(Constants.PROFILE_STATUS_KEY.toLowerCase(),
                    (String) profileDetails.get(Constants.PROFILE_STATUS_KEY));
            Map<String, Object> cadreDetails = (Map<String, Object>) profileDetails.get(Constants.CADRE_DETAILS);

            if (MapUtils.isNotEmpty(cadreDetails)) {
                userProfile.put(Constants.CADRE, (String) cadreDetails.get(Constants.CADRE_NAME));
                userProfile.put(Constants.SERVICE, (String) cadreDetails.get(Constants.CIVIL_SERVICE_NAME));
                if (cadreDetails.containsKey(Constants.CADRE_BATCH)) {
                    userProfile.put(Constants.BATCH, String.valueOf(cadreDetails.get(Constants.CADRE_BATCH)));
                }
            }
        }
    }

    private void getUserBitMap(Map<String, String> userProfile, Map<String, Integer> userProfileBitMap) {
        if (MapUtils.isEmpty(userProfile)) {
            log.warn("User profile is empty, cannot generate bitmap");
            return;
        }

        Map<String, Integer> idResultMap = idMapCacheMgr.getId(userProfile.values().stream().toList());
        if (MapUtils.isEmpty(idResultMap)) {
            log.error("Failed to fetch ID-Map for User: {}", userProfile.get(Constants.USER));
            return;
        }
        if (userProfile.values().size() != idResultMap.size()) {
            log.error("ID-Map values size mismatch for User Profile: {}", userProfile.get(Constants.USER));
            return;
        }
        for (Map.Entry<String, String> entry : userProfile.entrySet()) {
            if (idResultMap.containsKey(entry.getValue())) {
                userProfileBitMap.put(entry.getKey().toLowerCase(), idResultMap.get(entry.getValue()));
            } else {
                log.warn("ID-Map does not contain value for User: {}, Key: {}, Value: {}",
                        userProfile.get(Constants.USER), entry.getKey(), entry.getValue());
                userProfileBitMap.clear();
                return;
            }
        }
    }
}
