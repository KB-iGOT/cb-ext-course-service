package com.igot.cb.cbplan.service.impl.v4;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.cbplan.model.CriteriaFlags;
import com.igot.cb.cbplan.model.OrgScopeContext;
import com.igot.cb.cbplan.service.impl.CbPlanRequestValidatorImpl;
import com.igot.cb.service.UserAndOrgServiceImpl;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;

import lombok.extern.slf4j.Slf4j;

/**
 * Resolves CB Plan V4 org scope (orgScope/orgIdList) from userGroupId references.
 * V4 groups carry no inline criteria; the rootOrgId and ministryOrStateId criteria
 * are read from the referenced {@code user_group_info} row instead, mirroring
 * V3's inline-criteria rules: ministryOrStateId values must belong to an L0 org,
 * an L0 org cannot mix rootOrgId and ministryOrStateId criteria across its
 * referenced groups, and any ministryOrStateId usage forces orgScope to ALL.
 *
 * @version 4.0
 */
@Service
@Slf4j
public class CbPlanOrgScopeServiceV4Impl {
    private final CbPlanUserGroupLookupServiceV4Impl userGroupLookupService;
    private final UserAndOrgServiceImpl userAndOrgService;
    private final CassandraOperation cassandraOperation;
    private final CbExtServerProperties serverProperties;
    private final CbPlanRequestValidatorImpl cbPlanRequestValidator;
    private final ObjectMapper mapper;

    public CbPlanOrgScopeServiceV4Impl(CbPlanUserGroupLookupServiceV4Impl userGroupLookupService,
                                       UserAndOrgServiceImpl userAndOrgService,
                                       CassandraOperation cassandraOperation,
                                       CbExtServerProperties serverProperties,
                                       CbPlanRequestValidatorImpl cbPlanRequestValidator) {
        this.userGroupLookupService = userGroupLookupService;
        this.userAndOrgService = userAndOrgService;
        this.cassandraOperation = cassandraOperation;
        this.serverProperties = serverProperties;
        this.cbPlanRequestValidator = cbPlanRequestValidator;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Validates contextData and resolves orgScope/orgIdList. Accepts both V3 format
     * (inline userGroupCriteriaList + userGroupName) and V4 format (userGroupId reference),
     * but not both in the same request. On success the request map is updated in place
     * with ORG_SCOPE and ORG_ID_LIST.
     *
     * @param request              map holding the contextData to validate
     * @param isCCA                whether the logged in org is CCA
     * @param userRootOrgId        logged in user's organization ID
     * @param rootOrgIdsOut        set populated with the rootOrgId values resolved from the referenced groups
     * @param ministryOrStateIdsOut set populated with the ministryOrStateId values resolved from the referenced groups
     * @return validation errors, empty when the contextData is valid
     */
    public List<String> resolveOrgScope(Map<String, Object> request, boolean isCCA, String userRootOrgId,
                                        Set<String> rootOrgIdsOut, Set<String> ministryOrStateIdsOut) {
        List<String> errors = new ArrayList<>();
        Map<String, Object> contextData = readContextData(request, errors);
        if (CollectionUtils.isNotEmpty(errors)) {
            return errors;
        }
        List<Map<String, Object>> userGroups = readUserGroups(contextData, errors);
        if (CollectionUtils.isNotEmpty(errors)) {
            return errors;
        }
        if (isV3Format(userGroups, errors)) {
            log.debug("CbPlanOrgScopeServiceV4: Detected V3 format (inline criteria), delegating to V3 validator");
            Set<String> rootOrgIds = Objects.nonNull(rootOrgIdsOut) ? rootOrgIdsOut : new HashSet<>();
            return cbPlanRequestValidator.validateContextData(request, isCCA, userRootOrgId,
                    rootOrgIds, false);
        }
        if (CollectionUtils.isNotEmpty(errors)) {
            return errors;
        }
        Set<String> criteriaOrgIds = Objects.nonNull(rootOrgIdsOut) ? rootOrgIdsOut : new HashSet<>();
        Set<String> ministryOrStateIds = Objects.nonNull(ministryOrStateIdsOut) ? ministryOrStateIdsOut : new HashSet<>();
        CriteriaFlags criteriaFlags = new CriteriaFlags();
        boolean rootOrgMissingInSomeGroup = collectCriteria(userGroups, userRootOrgId, isCCA,
                criteriaOrgIds, ministryOrStateIds, criteriaFlags, errors);
        if (CollectionUtils.isNotEmpty(errors)) {
            return errors;
        }
        applyResolvedScope(request, isCCA, userRootOrgId, criteriaOrgIds, rootOrgMissingInSomeGroup,
                criteriaFlags, errors);
        return errors;
    }

    /**
     * Resolves the final orgScope from the collected criteria and, on success,
     * stamps orgIdList to the caller's own org (ownership, not targeting).
     *
     * @param request                 map to populate with ORG_SCOPE/ORG_ID_LIST
     * @param isCCA                   whether the logged in org is CCA
     * @param userRootOrgId           logged in user's organization ID
     * @param criteriaOrgIds          rootOrgId values collected from the referenced groups
     * @param rootOrgMissingInSomeGroup true when at least one group declared no rootOrgId criteria
     * @param criteriaFlags           criteria type flags (ministryOrStateId used, rootOrgId used)
     * @param errors                  collector for validation errors
     */
    private void applyResolvedScope(Map<String, Object> request, boolean isCCA, String userRootOrgId,
                                    Set<String> criteriaOrgIds, boolean rootOrgMissingInSomeGroup,
                                    CriteriaFlags criteriaFlags, List<String> errors) {
        boolean userIsL0 = checkUserOrgIsL0(userRootOrgId);
        if (criteriaFlags.isMinistryOrStateIdUsed() && criteriaFlags.isRootOrgIdUsed() && userIsL0) {
            errors.add(Constants.ERR_BOTH_ROOT_ORG_AND_MINISTRY_USED);
            return;
        }
        OrgScopeContext context = new OrgScopeContext(isCCA, userIsL0, userRootOrgId, false,
                criteriaOrgIds, rootOrgMissingInSomeGroup, criteriaFlags.isMinistryOrStateIdUsed());
        applyOrgScope(request, context, errors);
        if (CollectionUtils.isEmpty(errors)) {
            request.put(Constants.ORG_ID_LIST, Collections.singletonList(userRootOrgId));
            log.info("CbPlanOrgScopeServiceV4: Resolved orgScope {} for orgId {}",
                    request.get(Constants.ORG_SCOPE), userRootOrgId);
        }
    }

    /**
     * Reads and parses the request's contextData field, accepting either a
     * pre-parsed Map or a JSON string.
     *
     * @param request map holding the contextData to read
     * @param errors  collector for validation errors
     * @return parsed contextData, empty map when missing/invalid
     */
    private Map<String, Object> readContextData(Map<String, Object> request, List<String> errors) {
        if (!request.containsKey(Constants.CONTEXT_DATA_REQUEST)) {
            errors.add(Constants.ERR_CONTEXT_DATA_MISSING);
            return Collections.emptyMap();
        }
        Object contextDataObj = request.get(Constants.CONTEXT_DATA_REQUEST);
        if (contextDataObj instanceof String contextDataJson) {
            return parseContextDataJson(contextDataJson, errors);
        }
        if (contextDataObj instanceof Map) {
            return (Map<String, Object>) contextDataObj;
        }
        errors.add(Constants.ERR_CONTEXT_DATA_INVALID_TYPE);
        return Collections.emptyMap();
    }

    /**
     * Deserializes a contextData JSON string to a Map.
     *
     * @param contextDataJson contextData as a JSON string
     * @param errors          collector for validation errors
     * @return parsed contextData, empty map when unparseable
     */
    private Map<String, Object> parseContextDataJson(String contextDataJson, List<String> errors) {
        try {
            return mapper.readValue(contextDataJson, new TypeReference<Map<String, Object>>() {
            });
        } catch (JsonProcessingException e) {
            log.warn("CbPlanOrgScopeServiceV4: Failed to parse contextData", e);
            errors.add(Constants.ERR_CONTEXT_DATA_UNPARSEABLE);
            return Collections.emptyMap();
        }
    }

    /**
     * Reads the userGroups list declared under contextData.accessControl.
     *
     * @param contextData parsed contextData
     * @param errors      collector for validation errors
     * @return userGroups entries, empty when accessControl/userGroups are missing
     */
    private List<Map<String, Object>> readUserGroups(Map<String, Object> contextData, List<String> errors) {
        if (MapUtils.isEmpty(contextData) || !contextData.containsKey(Constants.ACCESS_CONTROL)) {
            errors.add(Constants.ERR_ACCESS_CONTROL_MISSING);
            return Collections.emptyList();
        }
        Map<String, Object> accessControl = (Map<String, Object>) contextData.get(Constants.ACCESS_CONTROL);
        List<Map<String, Object>> userGroups =
                MapUtils.isEmpty(accessControl) ? null : (List<Map<String, Object>>) accessControl.get(Constants.USER_GROUPS);
        if (CollectionUtils.isEmpty(userGroups)) {
            errors.add(Constants.ERR_USER_GROUPS_MISSING);
            return Collections.emptyList();
        }
        return userGroups;
    }

    /**
     * Detects whether the userGroups use V3 format (inline criteria) or V4 format (references).
     * V3 format has userGroupCriteriaList and/or userGroupName; V4 has userGroupId.
     * Mixing V3 and V4 formats in the same request is not allowed.
     *
     * @param userGroups userGroups entries to inspect
     * @param errors     collector for validation errors
     * @return true if V3 format detected, false if V4 format
     */
    private boolean isV3Format(List<Map<String, Object>> userGroups, List<String> errors) {
        boolean hasV3Fields = false;
        boolean hasV4Fields = false;
        for (Map<String, Object> userGroup : userGroups) {
            if (userGroup.containsKey(Constants.USER_GROUP_CRITERIA_LIST)
                    || userGroup.containsKey(Constants.USER_GROUP_NAME)) {
                hasV3Fields = true;
            }
            if (userGroup.containsKey(Constants.USER_GROUP_ID)) {
                hasV4Fields = true;
            }
        }
        if (hasV3Fields && hasV4Fields) {
            errors.add(Constants.ERR_USER_GROUP_MIXED_FORMAT);
            return false;
        }
        if (!hasV3Fields && !hasV4Fields) {
            errors.add(Constants.ERR_USER_GROUP_NO_FORMAT);
            return false;
        }
        return hasV3Fields;
    }

    /**
     * Walks every referenced userGroup, resolves its stored rootOrgId/ministryOrStateId
     * criteria via a per-group Cassandra fetch, and aggregates the results.
     *
     * @param userGroups     userGroups entries to resolve
     * @param userOrgId      caller's organization ID
     * @param isCCA          whether the logged in org is CCA
     * @param criteriaOrgIds set populated with the rootOrgId values found across all groups
     * @param ministryOrStateIds set populated with the validated ministryOrStateId values
     * @param criteriaFlags  holder for criteria type flags, set on return
     * @param errors         collector for validation errors
     * @return true when at least one group declared no rootOrgId criteria, which only CCA orgs allow
     */
    private boolean collectCriteria(List<Map<String, Object>> userGroups, String userOrgId, boolean isCCA,
                                    Set<String> criteriaOrgIds, Set<String> ministryOrStateIds,
                                    CriteriaFlags criteriaFlags, List<String> errors) {
        log.debug("CbPlanOrgScopeServiceV4.collectCriteria: Resolving {} userGroup(s) for orgId={}",
                userGroups.size(), userOrgId);
        boolean rootOrgMissingInSomeGroup = false;
        Set<String> rawMinistryOrStateIds = new HashSet<>();
        Set<String> validUserGroupIds = new HashSet<>();
        List<String> userGroupIds = extractUserGroupIds(userGroups, validUserGroupIds, errors);
        if (CollectionUtils.isEmpty(userGroupIds)) {
            return rootOrgMissingInSomeGroup;
        }
        Map<String, Map<String, Object>> groupEntities = userGroupLookupService.fetchUserGroupsByIds(userGroupIds, userOrgId);
        for (Map<String, Object> userGroup : userGroups) {
            List<String> groupErrors = new ArrayList<>();
            Set<String> groupRootOrgIds = new HashSet<>();
            Set<String> groupMinistryOrStateIds = new HashSet<>();
            processSingleGroupWithFetchedData(userGroup, groupEntities, validUserGroupIds, groupRootOrgIds,
                    groupMinistryOrStateIds, groupErrors);
            if (CollectionUtils.isNotEmpty(groupErrors)) {
                errors.addAll(groupErrors);
                continue;
            }
            if (CollectionUtils.isEmpty(groupRootOrgIds) && CollectionUtils.isEmpty(groupMinistryOrStateIds)) {
                if (isCCA) {
                    rootOrgMissingInSomeGroup = true;
                } else {
                    errors.add(Constants.ERR_ROOT_ORG_CRITERIA_MISSING);
                }
            } else {
                criteriaOrgIds.addAll(groupRootOrgIds);
                rawMinistryOrStateIds.addAll(groupMinistryOrStateIds);
            }
        }
        if (CollectionUtils.isNotEmpty(rawMinistryOrStateIds)) {
            validateAndCollectMinistryOrStateIds(rawMinistryOrStateIds, ministryOrStateIds, errors);
        }
        criteriaFlags.setRootOrgIdUsed(CollectionUtils.isNotEmpty(criteriaOrgIds));
        criteriaFlags.setMinistryOrStateIdUsed(CollectionUtils.isNotEmpty(ministryOrStateIds));
        return rootOrgMissingInSomeGroup;
    }

    /**
     * Extracts and validates userGroupId values from the userGroups list.
     * Performs early validation: presence check, UUID format check, and
     * disallowed-field check (userGroupCriteriaList, userGroupName).
     *
     * @param userGroups       user groups from contextData
     * @param validIdsOut      set populated with valid userGroupId strings (for duplicate error prevention)
     * @param errors           collector for validation errors
     * @return list of valid userGroupId strings to fetch
     */
    private List<String> extractUserGroupIds(List<Map<String, Object>> userGroups, Set<String> validIdsOut,
                                             List<String> errors) {
        List<String> userGroupIds = new ArrayList<>();
        for (Map<String, Object> userGroup : userGroups) {
            String userGroupId = extractUserGroupId(userGroup);
            if (StringUtils.isNotBlank(userGroupId)) {
                if (isValidUUID(userGroupId)) {
                    userGroupIds.add(userGroupId);
                    validIdsOut.add(userGroupId);
                } else {
                    errors.add(Constants.ERR_USER_GROUP_ID_INVALID_FORMAT);
                }
            } else {
                errors.add(Constants.ERR_USER_GROUP_ID_REQUIRED);
            }
        }
        return userGroupIds;
    }

    private String extractUserGroupId(Map<String, Object> userGroup) {
        if (!userGroup.containsKey(Constants.USER_GROUP_ID)) {
            return null;
        }
        String userGroupId = (String) userGroup.get(Constants.USER_GROUP_ID);
        return StringUtils.isNotBlank(userGroupId) ? userGroupId : null;
    }

    /**
     * Processes a single user group using pre-fetched data from the batch lookup.
     * Validates existence, status, and extracts criteria without hitting Cassandra again.
     * Only reports "not found" for groups that passed initial validation (to avoid duplicate errors).
     *
     * @param userGroup              single userGroup entry from contextData
     * @param groupEntities          pre-fetched user group entities (userGroupId → entity map)
     * @param validUserGroupIds      set of IDs that passed format validation (were attempted to be fetched)
     * @param rootOrgIdsOut          populated with rootOrgId criteria from the group
     * @param ministryOrStateIdsOut  populated with ministryOrStateId criteria from the group
     * @param errors                 collector for validation errors
     */
    private void processSingleGroupWithFetchedData(Map<String, Object> userGroup,
                                                    Map<String, Map<String, Object>> groupEntities,
                                                    Set<String> validUserGroupIds,
                                                    Set<String> rootOrgIdsOut,
                                                    Set<String> ministryOrStateIdsOut,
                                                    List<String> errors) {
        String userGroupId = (String) userGroup.get(Constants.USER_GROUP_ID);
        if (StringUtils.isBlank(userGroupId) || !validUserGroupIds.contains(userGroupId)) {
            return;
        }
        Map<String, Object> userGroupEntity = groupEntities.get(userGroupId);
        if (MapUtils.isEmpty(userGroupEntity)) {
            errors.add(Constants.ERR_USER_GROUP_NOT_FOUND + userGroupId);
            return;
        }
        String status = (String) userGroupEntity.get(Constants.COL_STATUS);
        if (!Constants.ACTIVE.equalsIgnoreCase(status)) {
            errors.add(Constants.ERR_USER_GROUP_NOT_ACTIVE + userGroupId);
            return;
        }
        rootOrgIdsOut.addAll(userGroupLookupService.extractRootOrgIds(userGroupEntity));
        ministryOrStateIdsOut.addAll(userGroupLookupService.extractMinistryOrStateIds(userGroupEntity));
    }


    /**
     * Validates that every ministryOrStateId references an L0 organization, matching V3's rule
     * that only L0 orgs may use ministryOrStateId criteria. Invalid IDs are reported as errors
     * and excluded from the validated output.
     *
     * @param rawIds  ministryOrStateId values collected from the referenced groups
     * @param validatedOut populated with the subset of rawIds confirmed to be L0 orgs
     * @param errors  collector for validation errors
     */
    private void validateAndCollectMinistryOrStateIds(Set<String> rawIds, Set<String> validatedOut,
                                                       List<String> errors) {
        Map<String, String> orgMinistryTypeMap = batchFetchOrgMinistryOrStateTypes(rawIds);
        for (String orgId : rawIds) {
            if (orgMinistryTypeMap.containsKey(orgId) && Constants.SPV.equalsIgnoreCase(orgMinistryTypeMap.get(orgId))) {
                validatedOut.add(orgId);
            } else if (!orgMinistryTypeMap.containsKey(orgId)) {
                errors.add(String.format(Constants.ERR_ORG_NOT_FOUND_FOR_L0_VALIDATION, orgId));
            } else {
                errors.add(String.format(Constants.ERR_ORG_NOT_L0, orgId));
            }
        }
    }

    /**
     * Batch-resolves each org's ministryOrStateType (used to confirm L0 status).
     * Uses a single Cassandra IN query instead of N individual SELECTs.
     *
     * @param orgIds org IDs to resolve (small, bounded set from a single plan)
     * @return map of orgId to ministryOrStateType, missing entries for unresolvable orgs
     */
    private Map<String, String> batchFetchOrgMinistryOrStateTypes(Set<String> orgIds) {
        Map<String, String> orgMinistryTypeMap = new HashMap<>();
        if (CollectionUtils.isEmpty(orgIds)) {
            return orgMinistryTypeMap;
        }
        try {
            Map<String, Object> queryMap = new HashMap<>();
            queryMap.put(Constants.ID, new ArrayList<>(orgIds));
            List<Map<String, Object>> orgList = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.ORG_TABLE,
                    queryMap,
                    List.of(Constants.ID, Constants.MINISTRY_OR_STATETYPE),
                    null
            );
            if (CollectionUtils.isNotEmpty(orgList)) {
                for (Map<String, Object> orgMap : orgList) {
                    String orgId = (String) orgMap.get(Constants.ID);
                    String ministryType = (String) orgMap.get(Constants.MINISTRY_OR_STATETYPE_DB);
                    if (StringUtils.isNotBlank(orgId) && StringUtils.isNotBlank(ministryType)) {
                        orgMinistryTypeMap.put(orgId, ministryType);
                    }
                }
            }
            log.debug("batchFetchOrgMinistryOrStateTypes: Fetched {} out of {} org ministry types",
                    orgMinistryTypeMap.size(), orgIds.size());
        } catch (Exception e) {
            log.error("batchFetchOrgMinistryOrStateTypes: Failed to batch fetch org ministry types for {} orgs",
                    orgIds.size(), e);
        }
        return orgMinistryTypeMap;
    }

    /**
     * Checks whether a string is a well-formed UUID.
     *
     * @param uuid candidate userGroupId value
     * @return true when it parses as a UUID
     */
    private boolean isValidUUID(String uuid) {
        if (StringUtils.isBlank(uuid)) {
            return false;
        }
        try {
            UUID.fromString(uuid);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Dispatches to the correct orgScope rule based on ministryOrStateId usage,
     * CCA status, and whether the caller's own org is L0.
     *
     * @param request map to populate with ORG_SCOPE
     * @param context resolved criteria context for this request
     * @param errors  collector for validation errors
     */
    private void applyOrgScope(Map<String, Object> request, OrgScopeContext context, List<String> errors) {
        if (context.isMinistryOrStateIdUsed()) {
            request.put(Constants.ORG_SCOPE, Constants.ALL);
            log.info("CbPlanOrgScopeServiceV4: Set orgScope to ALL for org using ministryOrStateId criteria");
            return;
        }
        if (context.isCCA()) {
            applyCcaOrgScope(request, context, errors);
            return;
        }
        if (context.isUserIsL0()) {
            applyL0OrgScope(request, context.getCriteriaOrgIds(), errors);
            return;
        }
        applyNonCcaOrgScope(request, context, errors);
    }

    /**
     * Applies orgScope rules for a CCA caller: ALL when no rootOrgId criteria was
     * declared, SINGLE/CUSTOM otherwise, unless some group omitted rootOrgId
     * criteria while others declared it (a mix CCA orgs are not allowed).
     *
     * @param request map to populate with ORG_SCOPE
     * @param context resolved criteria context for this request
     * @param errors  collector for validation errors
     */
    private void applyCcaOrgScope(Map<String, Object> request, OrgScopeContext context, List<String> errors) {
        if (context.getCriteriaOrgIds().isEmpty()) {
            request.put(Constants.ORG_SCOPE, Constants.ALL);
            return;
        }
        if (context.isRootOrgMissingInSomeGroup()) {
            errors.add(serverProperties.getMsgOnUserGroupRestrictionForAllOrg());
            return;
        }
        request.put(Constants.ORG_SCOPE,
                context.getCriteriaOrgIds().size() == 1 ? Constants.SINGLE : Constants.CUSTOM);
    }

    /**
     * Applies orgScope rules for a non-CCA, non-L0 caller: exactly one rootOrgId
     * criteria is required and it must match the caller's own org.
     *
     * @param request map to populate with ORG_SCOPE
     * @param context resolved criteria context for this request
     * @param errors  collector for validation errors
     */
    private void applyNonCcaOrgScope(Map<String, Object> request, OrgScopeContext context, List<String> errors) {
        if (context.getCriteriaOrgIds().size() > 1) {
            errors.add(Constants.ERR_MULTIPLE_ROOT_ORG_IDS);
            return;
        }
        if (context.getCriteriaOrgIds().isEmpty()) {
            errors.add(Constants.ERR_NO_ROOT_ORG_ID);
            return;
        }
        String rootOrgId = context.getCriteriaOrgIds().iterator().next();
        if (!StringUtils.equalsIgnoreCase(rootOrgId, context.getUserOrgId())) {
            errors.add(Constants.ERR_ROOT_ORG_ID_MISMATCH);
            return;
        }
        request.put(Constants.ORG_SCOPE, Constants.SINGLE);
    }

    /**
     * Applies orgScope rules for an L0-org caller: SINGLE for one rootOrgId
     * criteria value, CUSTOM for more than one; at least one is required.
     *
     * @param request        map to populate with ORG_SCOPE
     * @param criteriaOrgIds rootOrgId values collected from the referenced groups
     * @param errors         collector for validation errors
     */
    private void applyL0OrgScope(Map<String, Object> request, Set<String> criteriaOrgIds, List<String> errors) {
        if (criteriaOrgIds.isEmpty()) {
            errors.add(Constants.ERR_NO_ROOT_ORG_ID);
            return;
        }
        request.put(Constants.ORG_SCOPE, criteriaOrgIds.size() == 1 ? Constants.SINGLE : Constants.CUSTOM);
    }

    /**
     * Checks whether the given org is a Level 0 (ministryOrStateType = SPV) organization.
     *
     * @param userOrgId org ID to check
     * @return true when the org is L0, false when not L0 or not found
     */
    private boolean checkUserOrgIsL0(String userOrgId) {
        Map<String, Object> orgMap = userAndOrgService.readOrgFromDB(userOrgId,
                List.of(Constants.ID, Constants.MINISTRY_OR_STATETYPE));
        if (MapUtils.isEmpty(orgMap)) {
            log.warn("CbPlanOrgScopeServiceV4: Organization not found for userOrgId: {}", userOrgId);
            return false;
        }
        String ministryOrStateType = (String) orgMap.get(Constants.MINISTRY_OR_STATETYPE_DB);
        return Constants.SPV.equalsIgnoreCase(ministryOrStateType);
    }
}
