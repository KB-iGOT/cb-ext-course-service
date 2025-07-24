package com.igot.cb.service;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.IdMapCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class AccessSettingMigrationServiceImpl {
    private final CassandraOperation cassandraOperation;
    private final ContentInfoServiceImpl contentService;
    private final IdMapCacheMgr idMapCacheMgr;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public AccessSettingMigrationServiceImpl(CassandraOperation cassandraOperation, ContentInfoServiceImpl contentService,
            IdMapCacheMgr idMapCacheMgr) {
        this.cassandraOperation = cassandraOperation;
        this.contentService = contentService;
        this.idMapCacheMgr = idMapCacheMgr;
    }

    public ApiResponse migrateAccessSettingRules() {
        ApiResponse response = ApiResponse.createDefaultResponse("migrateAccessSettingRules");

        try {
            List<Map<String, Object>> accessSettingRuleMapList = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD_COURSE, Constants.ACCESS_SETTINGS_RULES_TABLE, null,
                    null, null);
            for (Map<String, Object> accessSettingMap : accessSettingRuleMapList) {
                String contextId = (String) accessSettingMap.get(Constants.CONTEXT_ID);
                if (processAccessSettingRule(accessSettingMap)) {
                    cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD_COURSE,
                            Constants.ACCESS_SETTINGS_RULES_TABLE_V2, accessSettingMap);
                    response.getResult().put(contextId, Constants.SUCCESS);
                } else {
                    response.getResult().put(contextId, Constants.FAILED);
                }
            }
        } catch (Exception e) {
            log.error("Error occurred while migrating access setting rules: {}", e.getMessage(), e);
            response.updateErrorDetails("Migration failed due to an error", HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    @SuppressWarnings("unchecked")
    public boolean processAccessSettingRule(Map<String, Object> accessSettingMap) throws Exception {
        String contextId = (String) accessSettingMap.get(Constants.CONTEXT_ID);
        String contextData = (String) accessSettingMap.get(Constants.CONTEXT_DATA);

        if (!StringUtils.hasText(contextData)) {
            log.warn("Context Data is missing for access setting map: {}", contextId);
            return false;
        }

        String contextIdType = contentService.readCourseCategoryForContent(contextId);
        accessSettingMap.put(Constants.CONTEXT_ID_TYPE, contextIdType);

        Map<String, Object> contextDataMap = objectMapper.readValue(contextData,
                new TypeReference<Map<String, Object>>() {
                });
        if (MapUtils.isEmpty(contextDataMap)) {
            log.error("Failed to parse context data for contextId: {}", contextId);
            return false;
        }

        Map<String, Object> accessControl = (Map<String, Object>) contextDataMap
                .get(Constants.ACCESS_CONTROL);
        if (MapUtils.isEmpty(accessControl)) {
            log.error("Access control data is missing for contextId: {}", contextId);
            return false;
        }

        // We do have accessControlMap, let's create accessControlIdMap
        Map<String, Object> accessControlIdMap = new HashMap<>();
        boolean isSuccess = updateContextDataWithIdMap(contextId, accessControl, accessControlIdMap);

        if (!isSuccess) {
            log.error("Failed to update context data with ID map for contextId: {}", contextId);
            return false;
        }
        if (((List<Map<String, Object>>) accessControl
                .get(Constants.USER_GROUPS))
                .size() != ((List<Map<String, Object>>) accessControlIdMap.get(Constants.USER_GROUPS)).size()) {
            log.error("User groups are missing in access control id map for contextId: {}", contextId);
            return false;
        }
        contextDataMap.put(Constants.ACCESS_CONTROL_ID, accessControlIdMap);
        accessSettingMap.put(Constants.CONTEXT_DATA, objectMapper.writeValueAsString(contextDataMap));
        return true;
    }

    @SuppressWarnings("unchecked")
    protected boolean updateContextDataWithIdMap(String contextId, Map<String, Object> accessControl,
            Map<String, Object> accessControlIdMap) {
        List<Map<String, Object>> userGroupsList = (List<Map<String, Object>>) accessControl
                .get(Constants.USER_GROUPS);
        if (CollectionUtils.isEmpty(userGroupsList)) {
            log.error("User groups are missing in access control for contextId: {}", contextId);
            return false;
        }
        List<Map<String, Object>> userGroupIdMapList = new ArrayList<>();
        accessControlIdMap.put(Constants.USER_GROUPS, userGroupIdMapList);
        accessControlIdMap.put(Constants.VERSION, 1);

        for (Map<String, Object> userGroup : userGroupsList) {
            String userGroupId = (String) userGroup.get(Constants.USER_GROUP_ID);
            Map<String, Object> userGroupIdMap = new HashMap<>();
            userGroupIdMap.put(Constants.USER_GROUP_ID, userGroupId);
            userGroupIdMap.put(Constants.USER_GROUP_NAME, userGroup.get(Constants.USER_GROUP_NAME));
            List<Map<String, Object>> criteriaIdMapList = new ArrayList<>();
            List<Map<String, Object>> criteriaList = (List<Map<String, Object>>) userGroup
                    .get(Constants.USER_GROUP_CRTIRIA_LIST);

            for (Map<String, Object> criteria : criteriaList) {
                String criteriaKey = (String) criteria.get(Constants.CRITERIA_KEY);
                List<String> criteriaValues = (List<String>) criteria.get(Constants.CRITERIA_VALUE);

                if (CollectionUtils.isEmpty(criteriaValues)) {
                    log.error("Criteria values are missing for criteriaKey: {} in userGroupId: {}", criteriaKey,
                            userGroupId);
                    return false;
                }

                Map<String, Integer> idResultMap = idMapCacheMgr.getId(criteriaValues);
                if (MapUtils.isEmpty(idResultMap)) {
                    log.error("Failed to fetch criteria ID for criteriaKey: {} in userGroupId: {}", criteriaKey,
                            userGroupId);
                    return false;
                }
                if (criteriaValues.size() != idResultMap.size()) {
                    log.error("Criteria values size mismatch for criteriaKey: {} in userGroupId: {}", criteriaKey,
                            userGroupId);
                    return false;
                }
                Map<String, Object> criteriaIdMap = new HashMap<>();
                criteriaIdMap.put(Constants.CRITERIA_KEY, criteriaKey);
                criteriaIdMap.put(Constants.CRITERIA_VALUE, createBitSetForAttribute(idResultMap.values()));

                criteriaIdMapList.add(criteriaIdMap);
            }
            userGroupIdMap.put(Constants.USER_GROUP_CRTIRIA_LIST, criteriaIdMapList);
            userGroupIdMapList.add(userGroupIdMap);
        }
        return true;
    }

    /**
     * Creates a BitSet for the given attribute values.
     * 
     * @param attributeValues Collection of Integer values representing the attribute.
     * @return BitSet representing the attribute values.
     */
    BitSet createBitSetForAttribute(Collection<Integer> attributeValues) {
        BitSet bitSet = new BitSet();
        for (Integer part : attributeValues) {
            try {
                bitSet.set(part);
            } catch (Exception ex) {
                log.error("Failed to set the bit map positing for value: {}", part, ex);
                throw ex;
            }
        }
        return bitSet;
    }
}
