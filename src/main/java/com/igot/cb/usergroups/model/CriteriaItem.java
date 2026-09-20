package com.igot.cb.usergroups.model;

import com.igot.cb.util.Constants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Immutable criteria item record for API layer.
 * Represents a single criteria entry with key-value pair.
 *
 * @param criteriaKey   the criteria key (e.g., "department", "role")
 * @param criteriaValue list of values for this criteria key
 */
public record CriteriaItem(String criteriaKey, List<String> criteriaValue) {

    public CriteriaItem {
        if (StringUtils.isBlank(criteriaKey)) {
            throw new IllegalArgumentException(Constants.MSG_CRITERIA_KEY_NULL);
        }
        if (CollectionUtils.isEmpty(criteriaValue)) {
            throw new IllegalArgumentException(Constants.MSG_CRITERIA_VALUE_NULL);
        }
        criteriaValue = List.copyOf(criteriaValue);
    }
}
