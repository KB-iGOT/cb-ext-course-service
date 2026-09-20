package com.igot.cb.usergroups.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.usergroups.model.UserGroupEntity;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Elasticsearch service for User Group operations.
 * Handles ES indexing, search, and synchronization.
 */
@Service
public class UserGroupElasticSearchServiceImpl {

    private static final Logger log = LoggerFactory.getLogger(UserGroupElasticSearchServiceImpl.class);

    private final EsUtilService esUtilService;
    private final UserGroupDataTransformServiceImpl dataTransformService;
    private final CbExtServerProperties serverProperties;
    private final ObjectMapper objectMapper;

    public UserGroupElasticSearchServiceImpl(EsUtilService esUtilService,
                                             UserGroupDataTransformServiceImpl dataTransformService,
                                             CbExtServerProperties serverProperties,
                                             ObjectMapper objectMapper) {
        this.esUtilService = esUtilService;
        this.dataTransformService = dataTransformService;
        this.serverProperties = serverProperties;
        this.objectMapper = objectMapper;
    }

    /**
     * Indexes a user group in Elasticsearch.
     *
     * @param entity user group entity
     */
    public void indexUserGroup(UserGroupEntity entity) {
        try {
            Map<String, Object> document = dataTransformService.entityToResponseMap(entity);
            esUtilService.addDocument(
                    serverProperties.getUserGroupIndex(),
                    Constants.INDEX_TYPE,
                    entity.getUserGroupId(),
                    document,
                    serverProperties.getElasticUserGroupJsonPath()
            );
            log.info("Indexed user group in ES: usergroupid={}", entity.getUserGroupId());
        } catch (Exception e) {
            log.error("Failed to index user group in ES: usergroupid={}", entity.getUserGroupId(), e);
        }
    }

    /**
     * Updates a user group in Elasticsearch.
     *
     * @param userGroupId user group ID
     * @param updateProps update properties
     */
    public void updateUserGroup(String userGroupId, Map<String, Object> updateProps) {
        try {
            esUtilService.updateDocument(
                    serverProperties.getUserGroupIndex(),
                    Constants.INDEX_TYPE,
                    userGroupId,
                    updateProps,
                    serverProperties.getElasticUserGroupJsonPath()
            );
            log.info("Updated user group in ES: usergroupid={}", userGroupId);
        } catch (Exception e) {
            log.error("Failed to update user group in ES: usergroupid={}", userGroupId, e);
        }
    }

    /**
     * Searches user groups in Elasticsearch.
     *
     * @param filters    search filters
     * @param pageSize   page size
     * @param pageNumber page number
     * @param sortBy     sort field
     * @param sortOrder  sort order
     * @return search results with count
     */
    public Map<String, Object> searchUserGroups(Map<String, Object> filters,
                                                int pageSize,
                                                int pageNumber,
                                                String sortBy,
                                                String sortOrder) {
        try {
            SearchCriteria searchCriteria = buildSearchCriteria(filters, pageSize, pageNumber, sortBy, sortOrder);
            SearchResult searchResult = esUtilService.searchDocuments(
                    serverProperties.getUserGroupIndex(),
                    searchCriteria,
                    serverProperties.getElasticUserGroupJsonPath()
            );

            List<Map<String, Object>> content = objectMapper.convertValue(
                    searchResult.getData(),
                    new TypeReference<List<Map<String, Object>>>() {
                    }
            );

            return Map.of(
                    Constants.CONTENT, content,
                    Constants.COUNT, searchResult.getTotalCount()
            );
        } catch (Exception e) {
            log.error("Failed to search user groups in ES: filters={}", filters, e);
            return Map.of(Constants.CONTENT, List.of(), Constants.COUNT, 0);
        }
    }

    /**
     * Builds SearchCriteria from filters.
     *
     * @param filters    search filters
     * @param pageSize   page size
     * @param pageNumber page number
     * @param sortBy     sort field
     * @param sortOrder  sort order
     * @return SearchCriteria object
     */
    private SearchCriteria buildSearchCriteria(Map<String, Object> filters,
                                               int pageSize,
                                               int pageNumber,
                                               String sortBy,
                                               String sortOrder) {
        SearchCriteria searchCriteria = new SearchCriteria();
        searchCriteria.setFilter(MapUtils.isNotEmpty(filters) ? new HashMap<>(filters) : new HashMap<>());
        searchCriteria.setPageSize(pageSize);
        searchCriteria.setPageNumber(pageNumber);

        if (StringUtils.isNotBlank(sortBy)) {
            searchCriteria.setOrderBy(sortBy);
            searchCriteria.setOrderDirection(sortOrder);
        }

        return searchCriteria;
    }

    /**
     * Attempts to update a user group in Elasticsearch, returning whether the update succeeded.
     * Used as a {@code preCommitValidator} in the transactional {@code updateRecord} overload.
     *
     * @param userGroupId user group ID
     * @param updateProps fields to update
     * @return true if ES responded with a non-null result
     */
    public boolean tryUpdateDocument(String userGroupId, Map<String, Object> updateProps) {
        return Objects.nonNull(esUtilService.updateDocument(
                serverProperties.getUserGroupIndex(),
                Constants.INDEX_TYPE,
                userGroupId,
                updateProps,
                serverProperties.getElasticUserGroupJsonPath()
        ));
    }

    /**
     * Best-effort ES rollback: restores the user group document to its pre-update state.
     * Called as {@code onCommitFailureRollback} when Cassandra fails after ES already succeeded.
     *
     * @param userGroupId   user group ID
     * @param previousState full document state before the attempted update
     */
    public void rollbackUpdate(String userGroupId, Map<String, Object> previousState) {
        if (!tryUpdateDocument(userGroupId, previousState)) {
            log.error("ES_CASSANDRA_DIVERGENCE: ES rollback failed for usergroupid={} — manual reconciliation required", userGroupId);
        }
    }
}
