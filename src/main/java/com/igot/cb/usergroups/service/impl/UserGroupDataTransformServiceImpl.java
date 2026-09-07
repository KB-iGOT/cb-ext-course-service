package com.igot.cb.usergroups.service.impl;

import com.igot.cb.usergroups.model.CriteriaItem;
import com.igot.cb.usergroups.model.UserGroupEntity;
import com.igot.cb.util.Constants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;

/**
 * Data transformation service for User Group operations.
 * Handles conversions between API layer and database layer representations.
 */
@Service
public class UserGroupDataTransformServiceImpl {

    /**
     * Converts API criteria list to Cassandra frozen list format.
     * API: List<CriteriaItem>
     * DB:  List<Map<String, List<String>>>
     *
     * @param criteriaItems API criteria list
     * @return database criteria list
     */
    public List<Map<String, List<String>>> criteriaItemsToDbFormat(List<CriteriaItem> criteriaItems) {
        if (CollectionUtils.isEmpty(criteriaItems)) {
            return List.of();
        }

        return criteriaItems.stream()
                .map(item -> Map.of(item.criteriaKey(), List.copyOf(item.criteriaValue())))
                .toList();
    }

    /**
     * Converts Cassandra frozen list format to API criteria list.
     * DB:  List<Map<String, List<String>>>
     * API: List<CriteriaItem>
     *
     * @param dbCriteria database criteria list
     * @return API criteria list
     */
    public List<CriteriaItem> dbFormatToCriteriaItems(List<Map<String, List<String>>> dbCriteria) {
        if (CollectionUtils.isEmpty(dbCriteria)) {
            return List.of();
        }

        return dbCriteria.stream()
                .map(map -> {
                    Map.Entry<String, List<String>> entry = map.entrySet().iterator().next();
                    return new CriteriaItem(entry.getKey(), entry.getValue());
                })
                .toList();
    }

    /**
     * Converts UserGroupEntity to API response map.
     *
     * @param entity user group entity
     * @return response map
     */
    public Map<String, Object> entityToResponseMap(UserGroupEntity entity) {
        List<CriteriaItem> criteria = dbFormatToCriteriaItems(entity.getCriteria());

        Map<String, Object> response = new LinkedHashMap<>();
        response.put(Constants.COL_ORGID, entity.getOrgid());
        response.put(Constants.COL_USERGROUPID, entity.getUsergroupid());
        response.put(Constants.COL_USERGROUPNAME, entity.getUsergroupname());
        response.put(Constants.COL_CREATEDBY, entity.getCreatedby());
        response.put(Constants.COL_CREATEDDATE, entity.getCreateddate());
        response.put(Constants.COL_UPDATEDBY, entity.getUpdatedby());
        response.put(Constants.COL_UPDATEDDATE, entity.getUpdateddate());
        response.put(Constants.COL_STATUS, entity.getStatus());
        response.put(Constants.COL_CRITERIA, criteria);

        return response;
    }

    /**
     * Builds UserGroupEntity for create operation.
     *
     * @param userGroupId   generated user group ID
     * @param usergroupname user group name
     * @param criteriaItems API criteria list
     * @param userOrgId     organization ID
     * @param userId        user ID
     * @return user group entity
     */
    public UserGroupEntity buildEntityForCreate(String userGroupId, String usergroupname,
                                                List<CriteriaItem> criteriaItems,
                                                String userOrgId, String userId) {
        String now = Instant.now().toString();
        List<Map<String, List<String>>> dbCriteria = criteriaItemsToDbFormat(criteriaItems);

        return UserGroupEntity.builder()
                .orgid(userOrgId)
                .usergroupid(userGroupId)
                .usergroupname(usergroupname)
                .createdby(userId)
                .createddate(now)
                .updatedby(userId)
                .updateddate(now)
                .criteria(dbCriteria)
                .status(Constants.ACTIVE)
                .build();
    }

    /**
     * Builds update properties map from request data.
     *
     * @param usergroupname user group name (optional)
     * @param criteriaItems criteria list (optional)
     * @param userId        user ID
     * @return update properties map
     */
    public Map<String, Object> buildUpdateProperties(String usergroupname,
                                                     List<CriteriaItem> criteriaItems,
                                                     String userId) {
        String now = Instant.now().toString();
        Map<String, Object> updateProps = new HashMap<>();

        updateProps.put(Constants.COL_UPDATEDBY, userId);
        updateProps.put(Constants.COL_UPDATEDDATE, now);

        if (StringUtils.isNotBlank(usergroupname)) {
            updateProps.put(Constants.COL_USERGROUPNAME, usergroupname);
        }

        if (CollectionUtils.isNotEmpty(criteriaItems)) {
            List<Map<String, List<String>>> dbCriteria = criteriaItemsToDbFormat(criteriaItems);
            updateProps.put(Constants.COL_CRITERIA, dbCriteria);
        }

        return updateProps;
    }
}
