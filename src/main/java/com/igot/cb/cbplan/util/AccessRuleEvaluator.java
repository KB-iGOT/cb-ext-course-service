package com.igot.cb.cbplan.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.igot.cb.util.Constants;

import lombok.extern.slf4j.Slf4j;

/**
 * Evaluates access control rules (accessControl.userGroups criteria) against a
 * user profile. Rules are compiled once per distinct contextData string - the
 * JSON is parsed and each criteria's expected values are lowercased into a Set -
 * and cached, so a request evaluates each plan with O(1) lookups instead of
 * re-parsing the JSON and scanning value lists per criteria.
 */
@Slf4j
public class AccessRuleEvaluator {

    private static final CompiledRule GRANT_ALL = new CompiledRule(true, List.of());
    private static final CompiledRule DENY_ALL = new CompiledRule(false, List.of());

    private final ObjectMapper mapper = new ObjectMapper();
    private final Cache<String, CompiledRule> compiledRules = Caffeine.newBuilder()
            .maximumSize(10_000)
            .build();

    /**
     * Evaluates whether the user has access as per the rule in contextData.
     * Match-all criteria in at least one group grants access.
     *
     * @param contextDataObj contextData as a JSON string or an already parsed Map
     * @param userProfile    user profile with lowercased keys
     * @return true if access is granted
     */
    public boolean hasAccess(Object contextDataObj, Map<String, String> userProfile) {
        return hasAccess(contextDataObj, userProfile, null);
    }

    /**
     * Same as {@link #hasAccess(Object, Map)}; refId (e.g. the planId) is used only
     * in log statements so a bad record can be located and fixed in the DB.
     */
    public boolean hasAccess(Object contextDataObj, Map<String, String> userProfile, Object refId) {
        if (Objects.isNull(contextDataObj)) {
            return true;
        }
        if (contextDataObj instanceof String str && str.trim().startsWith("\"")) {
            log.warn("contextData is double-encoded (JSON string literal) for planId={} - record should be fixed in DB", refId);
        }
        if (userProfile == null || userProfile.isEmpty()) {
            return false;
        }
        CompiledRule rule = compile(contextDataObj);
        if (rule.grantAll()) {
            return true;
        }
        for (List<CompiledCriteria> group : rule.groups()) {
            if (matchesGroup(group, userProfile)) {
                return true;
            }
        }
        return false;
    }

    private CompiledRule compile(Object contextDataObj) {
        if (contextDataObj instanceof String str) {
            if (str.isBlank()) {
                return GRANT_ALL;
            }
            return compiledRules.get(str, this::compileFromJson);
        } else if (contextDataObj instanceof Map<?, ?> map) {
            return compileFromMap((Map<String, Object>) map);
        }
        return GRANT_ALL;
    }

    private CompiledRule compileFromJson(String contextDataJson) {
        try {
            String json = contextDataJson;
            // Tolerate double-encoded rows: a JSON string literal ("{\"...\"}") is unwrapped first
            if (json.trim().startsWith("\"")) {
                json = mapper.readValue(json, String.class);
            }
            Map<String, Object> contextDataMap = mapper.readValue(json,
                    new TypeReference<Map<String, Object>>() {
                    });
            return compileFromMap(contextDataMap);
        } catch (Exception e) {
            log.error("AccessRuleEvaluator: Failed to parse contextData - denying access for this rule", e);
            return DENY_ALL;
        }
    }

    @SuppressWarnings("unchecked")
    private CompiledRule compileFromMap(Map<String, Object> contextDataMap) {
        try {
            if (contextDataMap == null || contextDataMap.isEmpty()) {
                return GRANT_ALL;
            }
            Map<String, Object> accessControl = (Map<String, Object>) contextDataMap.get(Constants.ACCESS_CONTROL);
            if (accessControl == null || accessControl.isEmpty()) {
                return DENY_ALL;
            }
            List<Map<String, Object>> userGroups = (List<Map<String, Object>>) accessControl
                    .get(Constants.USER_GROUPS);
            if (userGroups == null || userGroups.isEmpty()) {
                return DENY_ALL;
            }
            List<List<CompiledCriteria>> groups = new ArrayList<>();
            for (Map<String, Object> userGroup : userGroups) {
                List<Map<String, Object>> criteriaList = (List<Map<String, Object>>) userGroup
                        .get(Constants.USER_GROUP_CRITERIA_LIST);
                if (criteriaList == null || criteriaList.isEmpty()) {
                    continue; // a group without criteria never grants access
                }
                List<CompiledCriteria> group = new ArrayList<>(criteriaList.size());
                for (Map<String, Object> criteria : criteriaList) {
                    String criteriaKey = ((String) criteria.get(Constants.CRITERIA_KEY)).toLowerCase().trim();
                    Object rawCriteriaValue = criteria.get(Constants.CRITERIA_VALUE);
                    if (Constants.CENTRAL_DEPUTATION.equalsIgnoreCase(criteriaKey)) {
                        boolean expected = Boolean.parseBoolean(String.valueOf(rawCriteriaValue));
                        group.add(new CompiledCriteria(criteriaKey, true, expected, Set.of()));
                    } else {
                        group.add(new CompiledCriteria(criteriaKey, false, false,
                                toLowercasedSet(rawCriteriaValue)));
                    }
                }
                groups.add(group);
            }
            return new CompiledRule(false, List.copyOf(groups));
        } catch (Exception e) {
            log.error("AccessRuleEvaluator: Failed to compile access rule - denying access for this rule", e);
            return DENY_ALL;
        }
    }

    private Set<String> toLowercasedSet(Object rawCriteriaValue) {
        Set<String> values = new HashSet<>();
        if (rawCriteriaValue instanceof List<?> list) {
            for (Object value : list) {
                if (Objects.nonNull(value)) {
                    values.add(value.toString().toLowerCase());
                }
            }
        } else if (Objects.nonNull(rawCriteriaValue)) {
            values.add(rawCriteriaValue.toString().toLowerCase());
        }
        return Set.copyOf(values);
    }

    private boolean matchesGroup(List<CompiledCriteria> group, Map<String, String> userProfile) {
        for (CompiledCriteria criteria : group) {
            if (!matchesCriteria(criteria, userProfile)) {
                return false;
            }
        }
        return true;
    }

    private boolean matchesCriteria(CompiledCriteria criteria, Map<String, String> userProfile) {
        if (criteria.isCentralDeputation()) {
            boolean actual = Boolean.parseBoolean(
                    userProfile.getOrDefault(Constants.CENTRAL_DEPUTATION_LOWER_KEY, "false"));
            return criteria.expectedBoolean() == actual;
        }
        String actualValue = userProfile.get(criteria.key());
        if (Objects.isNull(actualValue)) {
            return false;
        }
        return criteria.expectedValues().contains(actualValue.toLowerCase());
    }

    private record CompiledCriteria(String key, boolean isCentralDeputation, boolean expectedBoolean,
            Set<String> expectedValues) {
    }

    private record CompiledRule(boolean grantAll, List<List<CompiledCriteria>> groups) {
    }
}
