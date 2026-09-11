package com.igot.cb.cbplan.service.impl.v4;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import com.igot.cb.cassandra.BatchQueryParams;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;

import lombok.extern.slf4j.Slf4j;

/**
 * Shared lookup for CB Plan V4 user group references.
 * Centralises fetching {@code user_group_info} rows and extracting the
 * rootOrgId criteria used for CB Plan org-scope resolution.
 *
 * @version 4.0
 */
@Service
@Slf4j
public class CbPlanUserGroupLookupServiceV4Impl {
    private final CassandraOperation cassandraOperation;
    private final CbExtServerProperties serverProperties;

    public CbPlanUserGroupLookupServiceV4Impl(CassandraOperation cassandraOperation,
                                               CbExtServerProperties serverProperties) {
        this.cassandraOperation = cassandraOperation;
        this.serverProperties = serverProperties;
    }

    /**
     * Fetches a user group by its composite key (orgId, userGroupId).
     *
     * @param userGroupId user group ID
     * @param orgId       organization ID the group must belong to
     * @return the user group row, empty map when not found
     */
    public Map<String, Object> fetchUserGroupById(String userGroupId, String orgId) {
        try {
            Map<String, Object> compositeKey = Map.of(
                    Constants.COL_ORGID, orgId,
                    Constants.COL_USERGROUPID, userGroupId
            );
            List<Map<String, Object>> results = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_USER_GROUP_INFO,
                    compositeKey,
                    List.of(),
                    null
            );
            if (CollectionUtils.isEmpty(results)) {
                log.debug("CbPlanUserGroupLookupServiceV4: User group not found: usergroupid={}, orgid={}", userGroupId, orgId);
                return Collections.emptyMap();
            }
            return results.get(0);
        } catch (Exception e) {
            log.error("CbPlanUserGroupLookupServiceV4: Failed to fetch user group from Cassandra: usergroupid={}", userGroupId, e);
            return Collections.emptyMap();
        }
    }

    /**
     * Batch fetches multiple user groups in a single or multiple Cassandra queries.
     * Automatically chunks large requests based on the configured batch size to avoid
     * overwhelming Cassandra with massive IN clauses.
     *
     * @param userGroupIds list of user group IDs to fetch
     * @param orgId        organization ID the groups must belong to
     * @return map of userGroupId → user group entity; missing entries for groups not found
     */
    public Map<String, Map<String, Object>> fetchUserGroupsByIds(List<String> userGroupIds, String orgId) {
        if (CollectionUtils.isEmpty(userGroupIds)) {
            return Collections.emptyMap();
        }
        Map<String, Map<String, Object>> resultMap = new HashMap<>();
        int batchSize = serverProperties.getCbPlanV4UserGroupBatchSize();
        if (batchSize <= 0) {
            log.error("CbPlanUserGroupLookupServiceV4.fetchUserGroupsByIds: Invalid batch size {} configured, using default 20",
                    batchSize);
            batchSize = 20;
        }
        log.debug("CbPlanUserGroupLookupServiceV4.fetchUserGroupsByIds: Fetching {} user groups for orgId={}, batchSize={}",
                userGroupIds.size(), orgId, batchSize);
        List<List<String>> chunks = chunkList(userGroupIds, batchSize);
        for (List<String> chunk : chunks) {
            Map<String, Map<String, Object>> chunkResults = fetchUserGroupChunk(chunk, orgId);
            resultMap.putAll(chunkResults);
        }
        log.debug("CbPlanUserGroupLookupServiceV4.fetchUserGroupsByIds: Fetched {} out of {} requested user groups",
                resultMap.size(), userGroupIds.size());
        return resultMap;
    }

    /**
     * Fetches a single chunk of user groups using Cassandra IN query.
     * The IN query targets the composite key (orgid, usergroupid) with a fixed orgid
     * and multiple usergroupid values.
     *
     * @param userGroupIds chunk of user group IDs (size ≤ configured batch size)
     * @param orgId        organization ID the groups must belong to
     * @return map of userGroupId → user group entity for this chunk
     */
    private Map<String, Map<String, Object>> fetchUserGroupChunk(List<String> userGroupIds, String orgId) {
        Map<String, Map<String, Object>> chunkMap = new HashMap<>();
        try {
            BatchQueryParams params = BatchQueryParams.builder()
                    .keyspaceName(Constants.KEYSPACE_SUNBIRD)
                    .tableName(Constants.TABLE_USER_GROUP_INFO)
                    .partitionKeyColumn(Constants.COL_ORGID)
                    .partitionKeyValue(orgId)
                    .clusteringColumn(Constants.COL_USERGROUPID)
                    .clusteringValues(userGroupIds)
                    .build();
            List<Map<String, Object>> results = cassandraOperation.getRecordsByIdsWithGivenPartitionKey(params);
            if (CollectionUtils.isNotEmpty(results)) {
                for (Map<String, Object> group : results) {
                    String userGroupId = (String) group.get(Constants.COL_USERGROUPID);
                    if (StringUtils.isNotBlank(userGroupId)) {
                        chunkMap.put(userGroupId, group);
                    }
                }
            }
            log.debug("CbPlanUserGroupLookupServiceV4.fetchUserGroupChunk: Fetched {} groups from chunk of {} for orgId={}",
                    chunkMap.size(), userGroupIds.size(), orgId);
        } catch (Exception e) {
            log.error("CbPlanUserGroupLookupServiceV4.fetchUserGroupChunk: Failed to batch fetch user groups for orgId={}, chunk size={}",
                    orgId, userGroupIds.size(), e);
        }
        return chunkMap;
    }

    /**
     * Splits a list into fixed-size chunks to respect Cassandra batch size limits.
     *
     * @param list      list to chunk
     * @param chunkSize maximum chunk size
     * @return list of chunks
     */
    private <T> List<List<T>> chunkList(List<T> list, int chunkSize) {
        if (CollectionUtils.isEmpty(list) || chunkSize <= 0) {
            return Collections.emptyList();
        }
        List<List<T>> chunks = new ArrayList<>();
        for (int i = 0; i < list.size(); i += chunkSize) {
            int end = Math.min(i + chunkSize, list.size());
            chunks.add(list.subList(i, end));
        }
        return chunks;
    }

    /**
     * Extracts the rootOrgId criteria values stored against a user group.
     *
     * @param userGroupEntity user group row fetched from Cassandra
     * @return rootOrgId values declared in the group's criteria, empty when none are present
     */
    public Set<String> extractRootOrgIds(Map<String, Object> userGroupEntity) {
        Set<String> rootOrgIds = new HashSet<>();
        if (MapUtils.isEmpty(userGroupEntity)) {
            return rootOrgIds;
        }
        List<Map<String, List<String>>> criteria =
                (List<Map<String, List<String>>>) userGroupEntity.get(Constants.COL_CRITERIA);
        if (CollectionUtils.isEmpty(criteria)) {
            return rootOrgIds;
        }
        for (Map<String, List<String>> criteriaEntry : criteria) {
            if (criteriaEntry.containsKey(Constants.ROOT_ORG_ID)) {
                List<String> values = criteriaEntry.get(Constants.ROOT_ORG_ID);
                if (CollectionUtils.isNotEmpty(values)) {
                    rootOrgIds.addAll(values);
                }
            }
        }
        return rootOrgIds;
    }

    /**
     * Extracts the ministryOrStateId criteria values stored against a user group.
     *
     * @param userGroupEntity user group row fetched from Cassandra
     * @return ministryOrStateId values declared in the group's criteria, empty when none are present
     */
    public Set<String> extractMinistryOrStateIds(Map<String, Object> userGroupEntity) {
        Set<String> ministryOrStateIds = new HashSet<>();
        if (MapUtils.isEmpty(userGroupEntity)) {
            return ministryOrStateIds;
        }
        List<Map<String, List<String>>> criteria =
                (List<Map<String, List<String>>>) userGroupEntity.get(Constants.COL_CRITERIA);
        if (CollectionUtils.isEmpty(criteria)) {
            return ministryOrStateIds;
        }
        for (Map<String, List<String>> criteriaEntry : criteria) {
            if (criteriaEntry.containsKey(Constants.MINISTRY_OR_STATEID)) {
                List<String> values = criteriaEntry.get(Constants.MINISTRY_OR_STATEID);
                if (CollectionUtils.isNotEmpty(values)) {
                    ministryOrStateIds.addAll(values);
                }
            }
        }
        return ministryOrStateIds;
    }
}
