package com.igot.cb.usergroups.model;

import org.apache.commons.collections.CollectionUtils;

import java.util.List;

/**
 * Immutable request record for User Group create/update operations.
 *
 * @param userGroupName user group name (required for create)
 * @param criteria      list of criteria items (required, min 1 entry)
 */
public record UserGroupRequest(String userGroupName, List<CriteriaItem> criteria) {

    public UserGroupRequest {
        // Defensive copy for immutability
        if (CollectionUtils.isNotEmpty(criteria)) {
            criteria = List.copyOf(criteria);
        }
    }
}
