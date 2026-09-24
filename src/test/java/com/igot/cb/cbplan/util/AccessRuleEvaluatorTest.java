package com.igot.cb.cbplan.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AccessRuleEvaluatorTest {

    private AccessRuleEvaluator evaluator;

    @BeforeEach
    void setUp() {
        evaluator = new AccessRuleEvaluator();
    }

    private String rule(String criteriaKey, String... values) {
        String vals = String.join("\",\"", values);
        return "{\"accessControl\":{\"userGroups\":[{\"userGroupId\":\"g1\",\"userGroupCriteriaList\":"
                + "[{\"criteriaKey\":\"" + criteriaKey + "\",\"criteriaValue\":[\"" + vals + "\"]}]}]}}";
    }

    @Test
    void grantsWhenProfileValueInCriteria_caseInsensitive() {
        String contextData = rule("designation", "Director", "Secretary");
        assertTrue(evaluator.hasAccess(contextData, Map.of("designation", "SECRETARY")));
    }

    @Test
    void deniesWhenProfileValueNotInCriteria() {
        String contextData = rule("designation", "director");
        assertFalse(evaluator.hasAccess(contextData, Map.of("designation", "clerk")));
    }

    @Test
    void deniesWhenProfileMissingCriteriaKey() {
        String contextData = rule("designation", "director");
        assertFalse(evaluator.hasAccess(contextData, Map.of("group", "group a")));
    }

    @Test
    void grantsWhenAnyOneGroupMatchesFully() {
        String contextData = "{\"accessControl\":{\"userGroups\":["
                + "{\"userGroupCriteriaList\":[{\"criteriaKey\":\"designation\",\"criteriaValue\":[\"director\"]},"
                + "{\"criteriaKey\":\"group\",\"criteriaValue\":[\"group a\"]}]},"
                + "{\"userGroupCriteriaList\":[{\"criteriaKey\":\"designation\",\"criteriaValue\":[\"clerk\"]}]}"
                + "]}}";
        // fails group 1 (wrong group attr), matches group 2
        assertTrue(evaluator.hasAccess(contextData, Map.of("designation", "clerk", "group", "group b")));
    }

    @Test
    void deniesWhenAllCriteriaInGroupNotMet() {
        String contextData = "{\"accessControl\":{\"userGroups\":[{\"userGroupCriteriaList\":"
                + "[{\"criteriaKey\":\"designation\",\"criteriaValue\":[\"director\"]},"
                + "{\"criteriaKey\":\"group\",\"criteriaValue\":[\"group a\"]}]}]}}";
        assertFalse(evaluator.hasAccess(contextData, Map.of("designation", "director", "group", "group b")));
    }

    @Test
    void centralDeputationBooleanCriteria() {
        String contextData = "{\"accessControl\":{\"userGroups\":[{\"userGroupCriteriaList\":"
                + "[{\"criteriaKey\":\"isOnCentralDeputation\",\"criteriaValue\":\"true\"}]}]}}";
        assertTrue(evaluator.hasAccess(contextData, Map.of("isoncentraldeputation", "true")));
        assertFalse(evaluator.hasAccess(contextData, Map.of("isoncentraldeputation", "false")));
        // absent profile attribute defaults to false
        assertFalse(evaluator.hasAccess(contextData, Map.of("designation", "director")));
    }

    @Test
    void nullContextDataGrantsAccess() {
        assertTrue(evaluator.hasAccess(null, Map.of("designation", "director")));
    }

    @Test
    void blankOrEmptyContextDataGrantsAccess() {
        assertTrue(evaluator.hasAccess("  ", Map.of("designation", "director")));
        assertTrue(evaluator.hasAccess("{}", Map.of("designation", "director")));
    }

    @Test
    void emptyProfileDeniesAccess() {
        assertFalse(evaluator.hasAccess(rule("designation", "director"), Map.of()));
        assertFalse(evaluator.hasAccess(rule("designation", "director"), null));
    }

    @Test
    void doubleEncodedContextDataIsUnwrapped() throws Exception {
        // rows written by the draft->publish flow contain a JSON string literal instead of an object
        String raw = rule("group", "Group B", "Group C");
        String doubleEncoded = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(raw);
        assertTrue(evaluator.hasAccess(doubleEncoded, Map.of("group", "group b")));
        assertFalse(evaluator.hasAccess(doubleEncoded, Map.of("group", "group a")));
    }

    @Test
    void malformedJsonDeniesAccess() {
        assertFalse(evaluator.hasAccess("{not-json", Map.of("designation", "director")));
    }

    @Test
    void missingAccessControlOrUserGroupsDeniesAccess() {
        assertFalse(evaluator.hasAccess("{\"accessControl\":{}}", Map.of("designation", "director")));
        assertFalse(evaluator.hasAccess("{\"accessControl\":{\"userGroups\":[]}}", Map.of("designation", "director")));
    }

    @Test
    void emptyCriteriaValuesDenyAccess() {
        String contextData = "{\"accessControl\":{\"userGroups\":[{\"userGroupCriteriaList\":"
                + "[{\"criteriaKey\":\"designation\",\"criteriaValue\":[]}]}]}}";
        assertFalse(evaluator.hasAccess(contextData, Map.of("designation", "director")));
    }

    @Test
    void mapContextDataIsSupported() {
        Map<String, Object> contextData = Map.of("accessControl", Map.of("userGroups", List.of(
                Map.of("userGroupCriteriaList", List.of(
                        Map.of("criteriaKey", "designation", "criteriaValue", List.of("director")))))));
        assertTrue(evaluator.hasAccess(contextData, Map.of("designation", "Director")));
    }

    @Test
    void repeatedEvaluationUsesCompiledRuleConsistently() {
        String contextData = rule("designation", "director");
        for (int i = 0; i < 3; i++) {
            assertTrue(evaluator.hasAccess(contextData, Map.of("designation", "director")));
            assertFalse(evaluator.hasAccess(contextData, Map.of("designation", "clerk")));
        }
    }
}
