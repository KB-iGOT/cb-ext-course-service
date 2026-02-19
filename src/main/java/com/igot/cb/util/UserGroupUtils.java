package com.igot.cb.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for user group operations.
 */
public class UserGroupUtils {

    /**
     * Validates that no criteria key or value is empty in the userGroups list.
     * If criteriaValue is a list, ensures no element is null, empty, or blank.
     * Returns null if valid, or an error message if invalid.
     * Handles String and Boolean types for value. Boolean must be true or false (not null).
     */
    @SuppressWarnings("unchecked")
    public static String validateUserGroupsNoEmptyCriteria(List<Map<String, Object>> userGroups) {
        for (Map<String, Object> userGroup : userGroups) {
            Object criteriaListObj = userGroup.get(Constants.USER_GROUP_CRITERIA_LIST);
            if (criteriaListObj instanceof List) {
                List<Map<String, Object>> criteriaList = (List<Map<String, Object>>) criteriaListObj;
                for (Map<String, Object> criteria : criteriaList) {
                    Object key = criteria.get(Constants.CRITERIA_KEY);
                    Object value = criteria.get(Constants.CRITERIA_VALUE);
                    if (key == null || key.toString().trim().isEmpty() || value == null) {
                        return "Criteria key and value must not be empty";
                    }
                    if (value instanceof String) {
                        if (((String) value).trim().isEmpty()) {
                            return "Criteria key and value must not be empty";
                        }
                    } else if (value instanceof List) {
                        List<?> valueList = (List<?>) value;
                        if (valueList.isEmpty()) {
                            return "Criteria value list must not be empty";
                        }
                        for (Object eachValue : valueList) {
                            if (eachValue == null) {
                                return "Criteria value list must not contain null values";
                            }
                            if (eachValue instanceof String && ((String) eachValue).trim().isEmpty()) {
                                return "Criteria value list must not contain empty or blank values";
                            }
                            // Boolean true/false are both valid, only null is invalid (already checked)
                        }
                    } else if (!(value instanceof Boolean)) {
                        // Only String, List, or Boolean are allowed
                        return "Criteria value must be a String, Boolean, or List";
                    }
                }
            }
        }
        return null;
    }
}
