package com.igot.cb.usergroups.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.usergroups.model.UserGroupEntity;
import com.igot.cb.util.Constants;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Elasticsearch service for User Group operations.
 * Handles ES indexing, search, and synchronization.
 */
@Service
public class UserGroupElasticSearchServiceImpl {

    private static final Logger log = LoggerFactory.getLogger(UserGroupElasticSearchServiceImpl.class);

    private final EsUtilService esUtilService;
    private final UserGroupDataTransformServiceImpl dataTransformService;
    private final ObjectMapper objectMapper;

    public UserGroupElasticSearchServiceImpl(EsUtilService esUtilService,
                                             UserGroupDataTransformServiceImpl dataTransformService,
                                             ObjectMapper objectMapper) {
        this.esUtilService = esUtilService;
        this.dataTransformService = dataTransformService;
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
                    Constants.ES_INDEX_USER_GROUP_INFO,
                    Constants.INDEX_TYPE,
                    entity.getUserGroupId(),
                    document,
                    Constants.ES_USERGROUP_FIELDS_JSON_PATH
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
                    Constants.ES_INDEX_USER_GROUP_INFO,
                    Constants.INDEX_TYPE,
                    userGroupId,
                    updateProps,
                    Constants.ES_USERGROUP_FIELDS_JSON_PATH
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
                    Constants.ES_INDEX_USER_GROUP_INFO,
                    searchCriteria,
                    Constants.ES_USERGROUP_FIELDS_JSON_PATH
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
}
