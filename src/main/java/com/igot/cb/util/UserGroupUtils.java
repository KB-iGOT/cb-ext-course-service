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
                    // Use trim() to ensure whitespace-only strings are also caught
                    if (key == null || key.toString().trim().isEmpty() || value == null || (value instanceof String && ((String)value).trim().isEmpty())) {
                        return "Criteria key and value must not be empty";
                    }
                    if (value instanceof List) {
                        List<?> valueList = (List<?>) value;
                        if (valueList.isEmpty()) {
                            return "Criteria value list must not be empty";
                        }
                        for (Object v : valueList) {
                            if (v == null || (v instanceof String && ((String) v).trim().isEmpty())) {
                                return "Criteria value list must not contain empty or blank values";
                            }
                        }
                    }
                }
            }
        }
        return null;
    }
}
