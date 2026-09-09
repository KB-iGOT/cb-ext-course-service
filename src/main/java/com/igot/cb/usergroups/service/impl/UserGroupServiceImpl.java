package com.igot.cb.usergroups.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.usergroups.model.CriteriaItem;
import com.igot.cb.usergroups.model.UserGroupEntity;
import com.igot.cb.usergroups.model.UserGroupRequest;
import com.igot.cb.usergroups.service.UserGroupService;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import com.igot.cb.util.UserProfileUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Main User Group service implementation.
 * Orchestrates validation, transformation, Cassandra, and Elasticsearch operations.
 */
@Service
public class UserGroupServiceImpl implements UserGroupService {

    private static final Logger log = LoggerFactory.getLogger(UserGroupServiceImpl.class);

    private final CassandraOperation cassandraOperation;
    private final UserGroupValidationServiceImpl validationService;
    private final UserGroupDataTransformServiceImpl dataTransformService;
    private final UserGroupElasticSearchServiceImpl esService;
    private final AccessTokenValidator accessTokenValidator;
    private final UserProfileUtil userProfileUtil;
    private final ObjectMapper objectMapper;

    public UserGroupServiceImpl(CassandraOperation cassandraOperation,
                                UserGroupValidationServiceImpl validationService,
                                UserGroupDataTransformServiceImpl dataTransformService,
                                UserGroupElasticSearchServiceImpl esService,
                                AccessTokenValidator accessTokenValidator,
                                UserProfileUtil userProfileUtil,
                                ObjectMapper objectMapper) {
        this.cassandraOperation = cassandraOperation;
        this.validationService = validationService;
        this.dataTransformService = dataTransformService;
        this.esService = esService;
        this.accessTokenValidator = accessTokenValidator;
        this.userProfileUtil = userProfileUtil;
        this.objectMapper = objectMapper;
    }


    @Override
    public ApiResponse createUserGroup(ApiRequest request, String authToken) {
        log.info("createUserGroup: starting");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_CREATE);

        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }

            Map<String, String> userProfile = userProfileUtil.buildUserProfile(userId, response);
            String userRootOrgId = userProfile.get(Constants.USER_ROOT_ORG_ID);
            if (StringUtils.isBlank(userRootOrgId)) {
                log.warn("createUserGroup: Failed to fetch userRootOrgId for userId={}", userId);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.ERR_USER_ORG_NOT_FOUND);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            log.info("createUserGroup: userId={}, userRootOrgId={}", userId, userRootOrgId);

            UserGroupRequest userGroupRequest = parseRequest(request, response);
            if (userGroupRequest == null || Constants.FAILED.equals(response.getParams().getStatus())) {
                return response;
            }

            String userGroupName = userGroupRequest.userGroupName();
            List<CriteriaItem> criteria = userGroupRequest.criteria();

            if (!validationService.validateCreateRequest(userGroupName, criteria, response)) {
                return response;
            }

            String userGroupId = UUID.randomUUID().toString();
            UserGroupEntity entity = dataTransformService.buildEntityForCreate(userGroupId, userGroupName, criteria, userRootOrgId, userId);

            if (!insertUserGroupInCassandra(entity, response)) {
                return response;
            }

            esService.indexUserGroup(entity);

            log.info("User group created successfully: usergroupid={}", userGroupId);
            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.CREATED);
            response.put(Constants.ID, userGroupId);
        } catch (Exception e) {
            handleException(response, e);
        }
        return response;
    }

    @Override
    public ApiResponse readUserGroup(String userGroupId, String authToken) {
        log.info("readUserGroup: userGroupId={}", userGroupId);
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_READ);

        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }

            Map<String, String> userProfile = userProfileUtil.buildUserProfile(userId, response);
            String userRootOrgId = userProfile.get(Constants.USER_ROOT_ORG_ID);
            if (StringUtils.isBlank(userRootOrgId)) {
                log.warn("readUserGroup: Failed to fetch userRootOrgId for userId={}", userId);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.ERR_USER_ORG_NOT_FOUND);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            if (!validationService.validateUserGroupId(userGroupId, response)) {
                return response;
            }

            UserGroupEntity entity = fetchUserGroupById(userGroupId, userRootOrgId, response);
            if (entity == null) {
                return response;
            }
            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
            response.putAll( dataTransformService.entityToResponseMap(entity));
        } catch (Exception e) {
            handleException(response, e);
        }
        return response;
    }

    @Override
    public ApiResponse updateUserGroup(String userGroupId, ApiRequest request, String authToken) {
        log.info("updateUserGroup: userGroupId={}", userGroupId);
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_UPDATE);

        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }

            Map<String, String> userProfile = userProfileUtil.buildUserProfile(userId, response);
            String userRootOrgId = userProfile.get(Constants.USER_ROOT_ORG_ID);
            String userRoles = userProfile.get(Constants.ROLES);
            if (StringUtils.isBlank(userRootOrgId)) {
                log.warn("updateUserGroup: Failed to fetch userRootOrgId for userId={}", userId);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.ERR_USER_ORG_NOT_FOUND);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            UserGroupRequest userGroupRequest = parseRequest(request, response);
            if (userGroupRequest == null || Constants.FAILED.equals(response.getParams().getStatus())) {
                return response;
            }

            String userGroupName = userGroupRequest.userGroupName();
            List<CriteriaItem> criteria = userGroupRequest.criteria();

            if (!validationService.validateUpdateRequest(userGroupId, userGroupName, criteria, response)) {
                return response;
            }

            UserGroupEntity existingEntity = fetchUserGroupById(userGroupId, userRootOrgId, response);
            if (existingEntity == null) {
                return response;
            }

            if (!validationService.validateUpdateAuthorization(
                    userId, userRootOrgId, userRoles,
                    existingEntity.getCreatedBy(), existingEntity.getOrgId(), response)) {
                return response;
            }

            Map<String, Object> updateProps = dataTransformService.buildUpdateProperties(userGroupName, criteria, userId);
            if (!updateUserGroupInCassandra(userGroupId, userRootOrgId, updateProps, response)) {
                return response;
            }

            esService.updateUserGroup(userGroupId, updateProps);

            log.info("User group updated successfully: usergroupid={}", userGroupId);
            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
            response.put(Constants.ID, userGroupId);
        } catch (Exception e) {
            handleException(response, e);
        }
        return response;
    }

    @Override
    public ApiResponse deleteUserGroup(String userGroupId, String authToken) {
        log.info("deleteUserGroup: userGroupId={}", userGroupId);
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_DELETE);

        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }

            Map<String, String> userProfile = userProfileUtil.buildUserProfile(userId, response);
            String userRootOrgId = userProfile.get(Constants.USER_ROOT_ORG_ID);
            if (StringUtils.isBlank(userRootOrgId)) {
                log.warn("deleteUserGroup: Failed to fetch userRootOrgId for userId={}", userId);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.ERR_USER_ORG_NOT_FOUND);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            if (!validationService.validateUserGroupId(userGroupId, response)) {
                return response;
            }

            UserGroupEntity entity = fetchUserGroupById(userGroupId, userRootOrgId, response);
            if (entity == null) {
                return response;
            }

            if (!updateUserGroupInCassandra(userGroupId, userRootOrgId,
                    Map.of(Constants.COL_STATUS, Constants.ARCHIVED), response)) {
                return response;
            }

            esService.updateUserGroup(userGroupId, Map.of(Constants.COL_STATUS, Constants.ARCHIVED));

            log.info("User group archived successfully: usergroupid={}", userGroupId);
            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
            response.put(Constants.RESPONSE, Constants.MSG_USER_GROUP_ARCHIVED);
        } catch (Exception e) {
            handleException(response, e);
        }
        return response;
    }

    @Override
    public ApiResponse searchUserGroups(ApiRequest request, String authToken) {
        log.info("searchUserGroups: starting");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_GROUP_SEARCH);

        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }

            Map<String, String> userProfile = userProfileUtil.buildUserProfile(userId, response);
            String userRootOrgId = userProfile.get(Constants.USER_ROOT_ORG_ID);
            if (StringUtils.isBlank(userRootOrgId)) {
                log.warn("searchUserGroups: Failed to fetch userRootOrgId for userId={}", userId);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.ERR_USER_ORG_NOT_FOUND);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            Map<String, Object> filters = extractSearchFilters(request, userRootOrgId);
            int pageSize = extractPageSize(request);
            int pageNumber = extractPageNumber(request);
            String sortBy = extractSortField(request);
            String sortOrder = extractSortOrder(request);

            Map<String, Object> searchResult = esService.searchUserGroups(filters, pageSize, pageNumber, sortBy, sortOrder);

            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
            response.putAll(searchResult);
        } catch (Exception e) {
            handleException(response, e);
        }
        return response;
    }

    /**
     * Parses API request to UserGroupRequest object.
     *
     * @param request API request
     * @return parsed UserGroupRequest
     */
    private UserGroupRequest parseRequest(ApiRequest request, ApiResponse response) {
        try {
            return objectMapper.convertValue(request.getRequest(), UserGroupRequest.class);
        } catch (Exception e) {
            log.error("Failed to parse request: {}", e.getMessage());
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.MSG_INVALID_REQUEST_FORMAT);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return null;
        }
    }

    /**
     * Inserts user group entity into Cassandra.
     *
     * @param entity user group entity to insert
     */
    private boolean insertUserGroupInCassandra(UserGroupEntity entity, ApiResponse response) {
        try {
            Map<String, Object> insertMap = new HashMap<>();
            insertMap.put(Constants.COL_ORGID, entity.getOrgId());
            insertMap.put(Constants.COL_USERGROUPID, entity.getUserGroupId());
            insertMap.put(Constants.COL_USERGROUPNAME, entity.getUserGroupName());
            insertMap.put(Constants.COL_CREATEDBY, entity.getCreatedBy());
            insertMap.put(Constants.COL_CREATEDDATE, entity.getCreatedDate());
            insertMap.put(Constants.COL_UPDATEDBY, entity.getUpdatedBy());
            insertMap.put(Constants.COL_UPDATEDDATE, entity.getUpdatedDate());
            insertMap.put(Constants.COL_CRITERIA, entity.getCriteria());
            insertMap.put(Constants.COL_STATUS, entity.getStatus());

            cassandraOperation.insertRecord(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_USER_GROUP_INFO,
                    insertMap
            );
            log.debug("Inserted user group in Cassandra: usergroupid={}", entity.getUserGroupId());
            return true;
        } catch (Exception e) {
            log.error("Failed to insert user group in Cassandra: usergroupid={}", entity.getUserGroupId(), e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.MSG_FAILED_CREATE_USER_GROUP);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return false;
        }
    }

    /**
     * Fetches user group by ID from Cassandra.
     *
     * @param userGroupId user group ID
     * @param userOrgId   organization ID
     * @return user group entity
     */
    private UserGroupEntity fetchUserGroupById(String userGroupId, String userOrgId, ApiResponse response) {
        try {
            List<Map<String, Object>> results = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_USER_GROUP_INFO,
                    Map.of(Constants.COL_ORGID, userOrgId, Constants.COL_USERGROUPID, userGroupId),
                    List.of(),
                    null
            );

            if (CollectionUtils.isEmpty(results)) {
                log.warn("User group not found: usergroupid={}, orgid={}", userGroupId, userOrgId);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.MSG_USER_GROUP_NOT_FOUND);
                response.setResponseCode(HttpStatus.NOT_FOUND);
                return null;
            }

            return mapToEntity(results.get(0));
        } catch (Exception e) {
            log.error("Failed to fetch user group from Cassandra: usergroupid={}", userGroupId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.MSG_FAILED_FETCH_USER_GROUP);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return null;
        }
    }

    /**
     * Updates user group in Cassandra with provided properties.
     *
     * @param userGroupId user group ID
     * @param userOrgId   organization ID
     * @param updateProps properties to update
     */
    private boolean updateUserGroupInCassandra(String userGroupId, String userOrgId, Map<String, Object> updateProps, ApiResponse response) {
        try {
            cassandraOperation.updateRecord(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_USER_GROUP_INFO,
                    updateProps,
                    Map.of(Constants.COL_ORGID, userOrgId, Constants.COL_USERGROUPID, userGroupId)
            );
            log.debug("Updated user group in Cassandra: usergroupid={}", userGroupId);
            return true;
        } catch (Exception e) {
            log.error("Failed to update user group in Cassandra: usergroupid={}", userGroupId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(Constants.MSG_FAILED_UPDATE_USER_GROUP);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return false;
        }
    }

    /**
     * Maps Cassandra row to UserGroupEntity.
     *
     * @param row Cassandra row data
     * @return user group entity
     */
    private UserGroupEntity mapToEntity(Map<String, Object> row) {
        return UserGroupEntity.builder()
                .orgId((String) row.get(Constants.COL_ORGID))
                .userGroupId((String) row.get(Constants.COL_USERGROUPID))
                .userGroupName((String) row.get(Constants.COL_USERGROUPNAME))
                .createdBy((String) row.get(Constants.COL_CREATEDBY))
                .createdDate((String) row.get(Constants.COL_CREATEDDATE))
                .updatedBy((String) row.get(Constants.COL_UPDATEDBY))
                .updatedDate((String) row.get(Constants.COL_UPDATEDDATE))
                .criteria((List<Map<String, List<String>>>) row.get(Constants.COL_CRITERIA))
                .status((String) row.get(Constants.COL_STATUS))
                .build();
    }

    /**
     * Extracts search filters from request and adds organization filter.
     *
     * @param request   API request
     * @param userOrgId user organization ID
     * @return search filters map
     */
    private Map<String, Object> extractSearchFilters(ApiRequest request, String userOrgId) {
        Map<String, Object> filters = new HashMap<>();
        filters.put(Constants.COL_ORGID, userOrgId);

        Map<String, Object> requestMap = (Map<String, Object>) request.getRequest();
        if (MapUtils.isNotEmpty(requestMap) && requestMap.containsKey(Constants.FILTERS)) {
            Map<String, Object> requestFilters = (Map<String, Object>) requestMap.get(Constants.FILTERS);
            if (MapUtils.isNotEmpty(requestFilters)) {
                filters.putAll(requestFilters);
            }
        }

        filters.putIfAbsent(Constants.COL_STATUS, List.of(Constants.ACTIVE, Constants.INACTIVE));

        return filters;
    }

    /**
     * Extracts page size from request with default value.
     *
     * @param request API request
     * @return page size (default 10)
     */
    private int extractPageSize(ApiRequest request) {
        Map<String, Object> requestMap = (Map<String, Object>) request.getRequest();
        if (MapUtils.isNotEmpty(requestMap) && requestMap.containsKey(Constants.PAGE_SIZE)) {
            return Math.min((Integer) requestMap.get(Constants.PAGE_SIZE), 50);
        }
        return 20;
    }

    /**
     * Extracts page number from request with default value.
     *
     * @param request API request
     * @return page number (default 0)
     */
    private int extractPageNumber(ApiRequest request) {
        Map<String, Object> requestMap = (Map<String, Object>) request.getRequest();
        if (MapUtils.isNotEmpty(requestMap) && requestMap.containsKey(Constants.PAGE_NUMBER)) {
            return (Integer) requestMap.get(Constants.PAGE_NUMBER);
        }
        return 0;
    }

    /**
     * Extracts sort field from request with default value.
     *
     * @param request API request
     * @return sort field (default "createddate")
     */
    private String extractSortField(ApiRequest request) {
        Map<String, Object> requestMap = (Map<String, Object>) request.getRequest();
        if (MapUtils.isNotEmpty(requestMap) && requestMap.containsKey("sortBy")) {
            return (String) requestMap.get("sortBy");
        }
        return Constants.COL_UPDATEDDATE;
    }

    /**
     * Extracts sort order from request with default value.
     *
     * @param request API request
     * @return sort order (default "desc")
     */
    private String extractSortOrder(ApiRequest request) {
        Map<String, Object> requestMap = (Map<String, Object>) request.getRequest();
        if (MapUtils.isNotEmpty(requestMap) && requestMap.containsKey("sortOrder")) {
            return (String) requestMap.get("sortOrder");
        }
        return Constants.DESC;
    }

    private void handleException(ApiResponse response, Exception e) {
        log.error("UserGroupService: Exception occurred", e);
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(e.getMessage());
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
