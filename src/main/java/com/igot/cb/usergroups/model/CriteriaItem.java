package com.igot.cb.usergroups.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.igot.cb.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;

/**
 * Immutable criteria item record for API layer.
 * Represents a single criteria entry with key-value pair.
 *
 * @param criteriaKey   the criteria key (e.g., "department", "role")
 * @param criteriaValue list of values for this criteria key; scalars (boolean, number, string)
 *                      are coerced to a single-element list by {@link CriteriaValueDeserializer}
 */
public record CriteriaItem(String criteriaKey,
                            @JsonDeserialize(using = CriteriaValueDeserializer.class)
                            List<String> criteriaValue) {

    public CriteriaItem {
        if (StringUtils.isBlank(criteriaKey)) {
            throw new IllegalArgumentException(Constants.MSG_CRITERIA_KEY_NULL);
        }
        criteriaValue = List.copyOf(Objects.requireNonNull(criteriaValue, Constants.MSG_CRITERIA_VALUE_NULL));
    }
}
