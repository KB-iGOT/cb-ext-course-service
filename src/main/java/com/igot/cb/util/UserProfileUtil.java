package com.igot.cb.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Utility component for fetching and caching user profile data.
 * Provides centralized user profile retrieval with Redis caching.
 */
@Component
public final class UserProfileUtil {

    private static final Logger log = LoggerFactory.getLogger(UserProfileUtil.class);

    private final CassandraOperation cassandraOperation;
    private final RedisCacheMgr redisCacheMgr;
    private final CbExtServerProperties serverProperties;
    private final ObjectMapper objectMapper;

    public UserProfileUtil(CassandraOperation cassandraOperation,
                           RedisCacheMgr redisCacheMgr,
                           CbExtServerProperties serverProperties,
                           ObjectMapper objectMapper) {
        this.cassandraOperation = cassandraOperation;
        this.redisCacheMgr = redisCacheMgr;
        this.serverProperties = serverProperties;
        this.objectMapper = objectMapper;
    }

    /**
     * Fetches user basic profile with Redis cache read-only.
     * Checks Redis cache first; on cache miss, queries Cassandra directly.
     * Does NOT write to cache - caching is the responsibility of User service.
     *
     * @param userId   user ID to fetch profile for
     * @param response ApiResponse to populate with error details if fetch fails
     * @return Map containing user profile data (including USER_ROOT_ORG_ID), empty map on failure
     */
    public Map<String, String> buildUserProfile(String userId, ApiResponse response) {
        log.debug("buildUserProfile: Fetching profile for userId={}", userId);
        try {
            String cacheKey = Constants.USER + ":basicProfile:" + userId;
            String cachedData = redisCacheMgr.getFromCache(cacheKey);
            Map<String, Object> userBasicProfile;

            if (StringUtils.isNotBlank(cachedData)) {
                log.debug("buildUserProfile: Redis cache HIT for userId={}", userId);
                userBasicProfile = objectMapper.readValue(cachedData, new TypeReference<Map<String, Object>>() {
                });
            } else {
                log.debug("buildUserProfile: Redis cache MISS, querying Cassandra (read-only) - userId={}", userId);
                Map<String, Object> propertiesMap = Map.of(Constants.ID, userId);
                List<String> userFields = Arrays.asList(Constants.ID, Constants.ROOT_ORG_ID, Constants.PROFILE_DETAILS);
                List<Map<String, Object>> userList = cassandraOperation.getRecordsByProperties(
                        Constants.KEYSPACE_SUNBIRD, Constants.USER, propertiesMap, userFields,
                        serverProperties.getCassandraQueryLimitPrimaryKey());

                if (CollectionUtils.isEmpty(userList)) {
                    log.warn("buildUserProfile: User not found in Cassandra - userId={}", userId);
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErr("User not found");
                    response.setResponseCode(HttpStatus.NOT_FOUND);
                    return new HashMap<>();
                }

                userBasicProfile = userList.get(0);
                log.debug("buildUserProfile: Fetched from Cassandra (NOT cached) - userId={}", userId);
            }

            Map<String, String> result = new HashMap<>();
            if (MapUtils.isNotEmpty(userBasicProfile)) {
                String userRootOrgId = userBasicProfile.get(Constants.ROOT_ORG_ID) != null
                        ? userBasicProfile.get(Constants.ROOT_ORG_ID).toString()
                        : null;
                result.put(Constants.USER_ROOT_ORG_ID, userRootOrgId);

                List<String> roles = getUserRoles(userId, userRootOrgId);
                result.put(Constants.ROLES, String.join(",", roles));
                log.debug("buildUserProfile: Enriched roles for userId={}, roles={}", userId, roles);
            }
            return result;
        } catch (Exception e) {
            log.error("buildUserProfile: Failed to fetch user profile - userId={}", userId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Failed to fetch user profile");
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return new HashMap<>();
        }
    }

    /**
     * Fetches user roles filtered by rootOrgId scope.
     * Queries user_roles table and returns only roles matching the user's organization.
     *
     * @param userId     user ID
     * @param rootOrgId  root organization ID to filter roles by scope
     * @return List of role names for the user in the specified organization
     */
    private List<String> getUserRoles(String userId, String rootOrgId) {
        if (StringUtils.isBlank(userId) || StringUtils.isBlank(rootOrgId)) {
            log.warn("getUserRoles: userId or rootOrgId is blank - userId={}, rootOrgId={}", userId, rootOrgId);
            return List.of();
        }

        try {
            Map<String, Object> queryParams = Map.of(Constants.USER_ID_LOWER_CASE, userId);
            List<String> fields = Arrays.asList(Constants.ROLE, Constants.SCOPE);
            List<Map<String, Object>> userRoleList = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER_ROLES, queryParams, fields, null);

            if (CollectionUtils.isEmpty(userRoleList)) {
                log.debug("getUserRoles: No roles found for userId={}", userId);
                return List.of();
            }

            return userRoleList.stream()
                    .map(userRoleObj -> {
                        Object userRoleScope = userRoleObj.get(Constants.SCOPE);
                        List<Map<String, Object>> scopes = new ArrayList<>();

                        if (userRoleScope instanceof List) {
                            scopes = (List<Map<String, Object>>) userRoleScope;
                        } else if (userRoleScope instanceof String scopeStr && StringUtils.isNotBlank(scopeStr)) {
                            try {
                                scopes = objectMapper.readValue(scopeStr, new TypeReference<List<Map<String, Object>>>() {});
                            } catch (Exception e) {
                                log.warn("getUserRoles: Failed to parse scope JSON for userId={}", userId, e);
                                return null;
                            }
                        }

                        if (CollectionUtils.isNotEmpty(scopes) &&
                                scopes.stream().allMatch(scope -> rootOrgId.equals(scope.get(Constants.ORGANISATION_ID)))) {
                            return (String) userRoleObj.get(Constants.ROLE);
                        }
                        return null;
                    })
                    .filter(Objects::nonNull)
                    .distinct()
                    .toList();
        } catch (Exception e) {
            log.error("getUserRoles: Failed to fetch roles for userId={}", userId, e);
            return List.of();
        }
    }
}
