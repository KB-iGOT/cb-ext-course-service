package com.igot.cb.cbplan.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.igot.cb.cbplan.model.CriteriaFlags;
import com.igot.cb.cbplan.model.OrgScopeContext;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.CbPlanDto;
import com.igot.cb.service.UserAndOrgServiceImpl;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;

import lombok.extern.slf4j.Slf4j;

/**
 * Service for CB Plan validation operations.
 *
 * @version 3.0
 */
@Service
@Slf4j
public class CbPlanRequestValidatorImpl {
    private final UserAndOrgServiceImpl userAndOrgService;
    private final CbExtServerProperties serverProperties;
    private final ObjectMapper mapper;
    private static final Validator VALIDATOR = buildValidator();

    public CbPlanRequestValidatorImpl(
            UserAndOrgServiceImpl userAndOrgService,
            CbExtServerProperties serverProperties) {
        this.userAndOrgService = userAndOrgService;
        this.serverProperties = serverProperties;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Builds the bean validator used to check the mandatory CB Plan fields.
     * The factory is closed once the validator has been obtained; holding it open for the lifetime
     * of the bean would leak it, and the validator does not need it.
     *
     * @return validator for CB Plan payloads
     */
    private static Validator buildValidator() {
        try (ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory()) {
            return validatorFactory.getValidator();
        }
    }


    /**
     * Validates a CB Plan create/update payload: mandatory fields first, then access control.
     * Owned by V3 so the rules can evolve without affecting the V1/V2 flows.
     *
     * @param request       API request holding the CB Plan payload
     * @param isCCA         whether the logged in org is CCA
     * @param userRootOrgId logged in user's organization ID
     * @param isAdmin       whether the caller is acting through an admin flow
     * @return validation errors, empty when the payload is valid
     */
    public List<String> validateCbPlanCreateRequest(ApiRequest request, boolean isCCA, String userRootOrgId,
                                                    boolean isAdmin) {
        log.debug("CbPlanValidationService.validateCbPlanCreateRequest: orgId={}, isCCA={}", userRootOrgId, isCCA);
        Map<String, Object> rawRequest = (Map<String, Object>) request.getRequest();
        List<String> errors = validateMandatoryFields(rawRequest);
        if (CollectionUtils.isNotEmpty(errors)) {
            log.debug("CbPlanValidationService.validateCbPlanCreateRequest: {} mandatory field(s) missing",
                    errors.size());
            return errors;
        }
        return validateContextData(rawRequest, isCCA, userRootOrgId, null, isAdmin);
    }

    /**
     * Validates the mandatory CB Plan fields declared on {@link CbPlanDto}.
     * Each violation names the offending field and the list is sorted so the same payload always
     * yields the same error order.
     *
     * @param request raw request map
     * @return sorted validation errors, empty when all mandatory fields are present
     */
    public List<String> validateMandatoryFields(Map<String, Object> request) {
        CbPlanDto cbPlanDto = mapper.convertValue(request, CbPlanDto.class);
        if (Objects.isNull(cbPlanDto.getIsApar())) {
            cbPlanDto.setIsApar(false);
        }
        request.put(Constants.IS_APAR, cbPlanDto.getIsApar());
        List<String> errors = new ArrayList<>();
        for (ConstraintViolation<CbPlanDto> violation : VALIDATOR.validate(cbPlanDto)) {
            errors.add(Constants.ERR_PREFIX + violation.getPropertyPath() + " " + violation.getMessage());
        }
        Collections.sort(errors);
        return errors;
    }

    /**
     * Validates the access control rules held in a plan's contextData and derives its org scope.
     * On success the request map is updated in place with the resolved ORG_SCOPE and ORG_ID_LIST,
     * and every criteria value is normalised to a list.
     *
     * @param request              map holding the contextData to validate
     * @param isCCA                whether the logged in org is CCA
     * @param userRootOrgId        logged in user's organization ID
     * @param rootOrgIdsInCriteria set populated with the org IDs found in the criteria, may be null
     * @param isAdmin              when true, skips the check that the criteria org matches userOrgId
     * @return validation errors, empty when the contextData is valid
     */
    public List<String> validateContextData(Map<String, Object> request, boolean isCCA, String userRootOrgId,
                                            Set<String> rootOrgIdsInCriteria, boolean isAdmin) {
        List<String> errors = new ArrayList<>();
        Map<String, Object> contextData = readContextData(request, errors);
        if (CollectionUtils.isNotEmpty(errors)) {
            return errors;
        }
        List<Map<String, Object>> userGroups = readUserGroups(contextData, errors);
        if (CollectionUtils.isNotEmpty(errors)) {
            return errors;
        }
        boolean userIsL0 = checkUserOrgIsL0(userRootOrgId);
        Set<String> criteriaOrgIds = Objects.nonNull(rootOrgIdsInCriteria) ? rootOrgIdsInCriteria : new HashSet<>();
        CriteriaFlags criteriaFlags = new CriteriaFlags();
        boolean rootOrgMissingInSomeGroup = collectRootOrgIds(userGroups, isCCA, criteriaOrgIds, criteriaFlags, errors);
        if (CollectionUtils.isNotEmpty(errors)) {
            return errors;
        }
        if (criteriaFlags.isMinistryOrStateIdUsed() && criteriaFlags.isRootOrgIdUsed() && userIsL0) {
            errors.add(Constants.ERR_BOTH_ROOT_ORG_AND_MINISTRY_USED);
            log.warn("CbPlanValidationService.validateContextData: L0 org cannot use both rootOrgId and ministryOrStateId criteria");
            return errors;
        }
        if (log.isDebugEnabled()) {
            log.debug("CbPlanValidationService.validateContextData: scanned {} user group(s), collected {} root org id(s), ministryOrStateId={}, rootOrgId={}, userIsL0={}",
                    userGroups.size(), criteriaOrgIds.size(), criteriaFlags.isMinistryOrStateIdUsed(), criteriaFlags.isRootOrgIdUsed(), userIsL0);
        }
        OrgScopeContext context = new OrgScopeContext(isCCA, userIsL0, userRootOrgId, isAdmin,
                criteriaOrgIds, rootOrgMissingInSomeGroup, criteriaFlags.isMinistryOrStateIdUsed());
        applyOrgScope(request, context, errors);
        if (CollectionUtils.isEmpty(errors)) {
            request.put(Constants.ORG_ID_LIST, Collections.singletonList(userRootOrgId));
            log.info("CbPlanValidationService.validateContextData: Resolved orgScope {} for orgId {}",
                    request.get(Constants.ORG_SCOPE), userRootOrgId);
        }
        return errors;
    }

    /**
     * Reads contextData off the request, accepting either a JSON string or an already parsed map.
     *
     * @param request request map to read from
     * @param errors  collector for validation errors
     * @return parsed contextData, empty when it is missing or unusable
     */
    private Map<String, Object> readContextData(Map<String, Object> request, List<String> errors) {
        if (!request.containsKey(Constants.CONTEXT_DATA_REQUEST)) {
            errors.add(Constants.ERR_CONTEXT_DATA_MISSING);
            return Collections.emptyMap();
        }
        Object contextDataObj = request.get(Constants.CONTEXT_DATA_REQUEST);
        if (contextDataObj instanceof String contextDataJson) {
            try {
                return mapper.readValue(contextDataJson, new TypeReference<Map<String, Object>>() {
                });
            } catch (JsonProcessingException e) {
                log.warn("CbPlanValidationService.readContextData: Failed to parse contextData", e);
                errors.add(Constants.ERR_CONTEXT_DATA_UNPARSEABLE);
                return Collections.emptyMap();
            }
        }
        if (contextDataObj instanceof Map) {
            return (Map<String, Object>) contextDataObj;
        }
        errors.add(Constants.ERR_CONTEXT_DATA_INVALID_TYPE);
        return Collections.emptyMap();
    }

    /**
     * Reads the user groups declared under contextData.accessControl.
     *
     * @param contextData parsed contextData
     * @param errors      collector for validation errors
     * @return user groups, empty when accessControl or the groups are missing
     */
    private List<Map<String, Object>> readUserGroups(Map<String, Object> contextData, List<String> errors) {
        if (MapUtils.isEmpty(contextData) || !contextData.containsKey(Constants.ACCESS_CONTROL)) {
            errors.add(Constants.ERR_ACCESS_CONTROL_MISSING);
            return Collections.emptyList();
        }
        Map<String, Object> accessControl = (Map<String, Object>) contextData.get(Constants.ACCESS_CONTROL);
        if (MapUtils.isEmpty(accessControl)) {
            errors.add(Constants.ERR_ACCESS_CONTROL_MISSING);
            return Collections.emptyList();
        }
        List<Map<String, Object>> userGroups =
                (List<Map<String, Object>>) accessControl.get(Constants.USER_GROUPS);
        if (CollectionUtils.isEmpty(userGroups)) {
            errors.add(Constants.ERR_USER_GROUPS_MISSING);
            return Collections.emptyList();
        }
        return userGroups;
    }

    /**
     * Walks every user group's criteria, normalising each criteria value to a list and collecting
     * the root organization IDs it targets.
     *
     * @param userGroups     user groups to inspect
     * @param isCCA          whether the logged in org is CCA
     * @param criteriaOrgIds set populated with the org IDs found in the criteria
     * @param criteriaFlags  holder for criteria type flags (ministryOrStateId used, rootOrgId used)
     * @param errors         collector for validation errors
     * @return true when at least one group declared no root org criteria, which only CCA orgs allow
     */
    private boolean collectRootOrgIds(List<Map<String, Object>> userGroups, boolean isCCA,
                                      Set<String> criteriaOrgIds, CriteriaFlags criteriaFlags, List<String> errors) {
        boolean rootOrgMissingInSomeGroup = false;
        for (Map<String, Object> userGroup : userGroups) {
            List<Map<String, Object>> criteriaList = readCriteriaList(userGroup, errors);
            if (CollectionUtils.isNotEmpty(errors)) {
                return false;
            }
            boolean rootOrgFoundInGroup = collectGroupRootOrgIds(criteriaList, criteriaOrgIds, criteriaFlags, errors);
            if (CollectionUtils.isNotEmpty(errors)) {
                return false;
            }
            if (!rootOrgFoundInGroup) {
                if (!isCCA) {
                    errors.add(Constants.ERR_ROOT_ORG_CRITERIA_MISSING);
                    return false;
                }
                rootOrgMissingInSomeGroup = true;
            }
        }
        return rootOrgMissingInSomeGroup;
    }

    /**
     * Reads the criteria list declared on a user group.
     *
     * @param userGroup user group to inspect
     * @param errors    collector for validation errors
     * @return criteria list, empty when it is missing or empty
     */
    private List<Map<String, Object>> readCriteriaList(Map<String, Object> userGroup, List<String> errors) {
        if (!userGroup.containsKey(Constants.USER_GROUP_CRITERIA_LIST)) {
            errors.add(Constants.ERR_CRITERIA_LIST_MISSING);
            return Collections.emptyList();
        }
        List<Map<String, Object>> criteriaList =
                (List<Map<String, Object>>) userGroup.get(Constants.USER_GROUP_CRITERIA_LIST);
        if (CollectionUtils.isEmpty(criteriaList)) {
            errors.add(Constants.ERR_CRITERIA_LIST_EMPTY);
            return Collections.emptyList();
        }
        return criteriaList;
    }

    /**
     * Normalises each criteria value to a list and collects any root organization IDs.
     * When ministryOrStateId is used, validates that the organizations are Level 0 and sets the flag.
     *
     * @param criteriaList   criteria belonging to a single user group
     * @param criteriaOrgIds set populated with the org IDs found in the criteria
     * @param criteriaFlags  holder for criteria type flags (ministryOrStateId used, rootOrgId used)
     * @param errors         collector for validation errors
     * @return true when this group declared a root org criteria
     */
    private boolean collectGroupRootOrgIds(List<Map<String, Object>> criteriaList, Set<String> criteriaOrgIds,
                                           CriteriaFlags criteriaFlags, List<String> errors) {
        boolean rootOrgFoundInGroup = false;
        for (Map<String, Object> criteria : criteriaList) {
            String criteriaKey = (String) criteria.get(Constants.CRITERIA_KEY);
            if (StringUtils.isEmpty(criteriaKey)) {
                errors.add(Constants.ERR_CRITERIA_KEY_MISSING);
                return false;
            }
            if (!criteria.containsKey(Constants.CRITERIA_VALUE)) {
                errors.add(Constants.ERR_CRITERIA_VALUE_MISSING + criteriaKey);
                return false;
            }
            List<String> criteriaValues = normaliseCriteriaValues(criteria.get(Constants.CRITERIA_VALUE),
                    criteriaKey, errors);
            if (CollectionUtils.isNotEmpty(errors)) {
                return false;
            }
            criteria.put(Constants.CRITERIA_VALUE, criteriaValues);
            if (Constants.ROOT_ORG_ID.equalsIgnoreCase(criteriaKey)
                    || Constants.TARGETED_ORGANISATION.equalsIgnoreCase(criteriaKey)) {
                rootOrgFoundInGroup = true;
                criteriaOrgIds.addAll(criteriaValues);
                criteriaFlags.setRootOrgIdUsed(true);
            } else if (Constants.MINISTRY_OR_STATEID.equalsIgnoreCase(criteriaKey)) {
                if (!validateAndCollectMinistryOrStateIds(criteriaValues, criteriaOrgIds, errors)) {
                    return false;
                }
                criteriaFlags.setMinistryOrStateIdUsed(true);
                rootOrgFoundInGroup = true;
            }
        }
        return rootOrgFoundInGroup;
    }

    /**
     * Validates and collects ministry or state organization IDs.
     * Each organization must be a Level 0 (L0) organization (ministryOrStateType = "SPV").
     * Uses batch fetching to minimize DB calls.
     *
     * @param criteriaValues list of organization IDs to validate
     * @param criteriaOrgIds set populated with validated org IDs
     * @param errors         collector for validation errors
     * @return true when all organizations are valid L0 orgs, false otherwise
     */
    private boolean validateAndCollectMinistryOrStateIds(List<String> criteriaValues, Set<String> criteriaOrgIds,
                                                         List<String> errors) {
        if (CollectionUtils.isEmpty(criteriaValues)) {
            return true;
        }
        Set<String> orgIdsToValidate = new HashSet<>(criteriaValues);
        Map<String, String> orgMinistryTypeMap = batchFetchOrgMinistryOrStateTypes(orgIdsToValidate);
        for (String orgId : criteriaValues) {
            if (!orgMinistryTypeMap.containsKey(orgId)) {
                String errorMsg = String.format(Constants.ERR_ORG_NOT_FOUND_FOR_L0_VALIDATION, orgId);
                errors.add(errorMsg);
                log.error("CbPlanValidationService.validateAndCollectMinistryOrStateIds: {}", errorMsg);
                return false;
            }
            String ministryOrStateType = orgMinistryTypeMap.get(orgId);
            if (!Constants.SPV.equalsIgnoreCase(ministryOrStateType)) {
                String errorMsg = String.format(Constants.ERR_ORG_NOT_L0, orgId);
                errors.add(errorMsg);
                log.warn("CbPlanValidationService.validateAndCollectMinistryOrStateIds: Org {} has ministryOrStateType={}, expected {}",
                        orgId, ministryOrStateType, Constants.SPV);
                return false;
            }
            criteriaOrgIds.add(orgId);
        }
        log.info("CbPlanValidationService.validateAndCollectMinistryOrStateIds: Validated {} L0 org(s) in batch",
                criteriaValues.size());
        return true;
    }

    /**
     * Coerces a criteria value into a list of strings, accepting a list, a boolean or a string.
     *
     * @param criteriaValueObj raw criteria value
     * @param criteriaKey      key the value belongs to, used for error reporting
     * @param errors           collector for validation errors
     * @return normalised values, empty when the type is unsupported
     */
    private List<String> normaliseCriteriaValues(Object criteriaValueObj, String criteriaKey, List<String> errors) {
        if (criteriaValueObj instanceof List) {
            return (List<String>) criteriaValueObj;
        }
        if (criteriaValueObj instanceof Boolean || criteriaValueObj instanceof String) {
            return Collections.singletonList(String.valueOf(criteriaValueObj));
        }
        errors.add(Constants.ERR_CRITERIA_VALUE_UNSUPPORTED + criteriaKey + ", type="
                + (Objects.isNull(criteriaValueObj) ? null : criteriaValueObj.getClass().getSimpleName()));
        return Collections.emptyList();
    }


    private void applyOrgScope(Map<String, Object> request, OrgScopeContext context, List<String> errors) {
        if (context.isMinistryOrStateIdUsed()) {
            request.put(Constants.ORG_SCOPE, Constants.ALL);
            log.info("CbPlanValidationService.applyOrgScope: Set orgScope to ALL for L0 org using ministryOrStateId");
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
     * Resolves the org scope for a CCA organization.
     *
     * @param request request map updated in place with the resolved ORG_SCOPE
     * @param context org scope context holding criteria details
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
     * Resolves the org scope for a non CCA organization, which may only target its own org.
     *
     * @param request request map updated in place with the resolved ORG_SCOPE
     * @param context org scope context holding user and criteria details
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
        if (!context.isAdmin() && !StringUtils.equalsIgnoreCase(rootOrgId, context.getUserOrgId())) {
            errors.add(Constants.ERR_ROOT_ORG_ID_MISMATCH);
            return;
        }
        request.put(Constants.ORG_SCOPE, Constants.SINGLE);
    }

    /**
     * Checks if the user's organization is a Level 0 (L0) organization.
     * Called once at the start of validation to avoid repeated DB calls.
     * An organization is considered L0 if its ministryOrStateType is "SPV".
     *
     * @param userOrgId user's organization ID
     * @return true when the organization is L0 (ministryOrStateType = SPV), false otherwise
     */
    private boolean checkUserOrgIsL0(String userOrgId) {
        log.debug("CbPlanValidationService.checkUserOrgIsL0: Checking L0 status for userOrgId: {}", userOrgId);
        Map<String, Object> orgMap = userAndOrgService.readOrgFromDB(userOrgId,
                Arrays.asList(Constants.ID, Constants.MINISTRY_OR_STATETYPE));
        if (MapUtils.isEmpty(orgMap)) {
            log.warn("CbPlanValidationService.checkUserOrgIsL0: Organization not found for userOrgId: {}", userOrgId);
            return false;
        }
        String ministryOrStateType = (String) orgMap.get(Constants.MINISTRY_OR_STATETYPE_DB);
        boolean isL0 = Constants.SPV.equalsIgnoreCase(ministryOrStateType);
        log.debug("CbPlanValidationService.checkUserOrgIsL0: userOrgId={}, ministryOrStateType={}, isL0={}",
                userOrgId, ministryOrStateType, isL0);
        return isL0;
    }

    /**
     * Batch fetches ministryOrStateType values for multiple organization IDs.
     * Returns a map of orgId to ministryOrStateType for efficient L0 validation.
     *
     * @param orgIds set of organization IDs to fetch
     * @return map of orgId to ministryOrStateType, empty map if fetch fails
     */
    private Map<String, String> batchFetchOrgMinistryOrStateTypes(Set<String> orgIds) {
        if (CollectionUtils.isEmpty(orgIds)) {
            return Collections.emptyMap();
        }
        log.debug("CbPlanValidationService.batchFetchOrgMinistryOrStateTypes: Fetching ministryOrStateType for {} org(s)", orgIds.size());
        try {
            Map<String, String> orgMinistryTypeMap = new HashMap<>();
            for (String orgId : orgIds) {
                Map<String, Object> orgMap = userAndOrgService.readOrgFromDB(orgId,
                        Arrays.asList(Constants.ID, Constants.MINISTRY_OR_STATETYPE));
                if (MapUtils.isNotEmpty(orgMap)) {
                    String ministryOrStateType = (String) orgMap.get(Constants.MINISTRY_OR_STATETYPE_DB);
                    orgMinistryTypeMap.put(orgId, ministryOrStateType);
                }
            }
            log.debug("CbPlanValidationService.batchFetchOrgMinistryOrStateTypes: Fetched {} ministryOrStateType value(s)",
                    orgMinistryTypeMap.size());
            return orgMinistryTypeMap;
        } catch (Exception e) {
            log.error("CbPlanValidationService.batchFetchOrgMinistryOrStateTypes: Failed to fetch org ministryOrStateType values", e);
            return Collections.emptyMap();
        }
    }

    /**
     * Applies org scope for L0 organizations with rootOrgId criteria.
     * L0 orgs have special privilege to target multiple organizations.
     *
     * @param request        request map updated in place with the resolved ORG_SCOPE
     * @param criteriaOrgIds org IDs collected from the criteria
     * @param errors         collector for validation errors
     */
    private void applyL0OrgScope(Map<String, Object> request, Set<String> criteriaOrgIds, List<String> errors) {
        if (criteriaOrgIds.isEmpty()) {
            errors.add(Constants.ERR_NO_ROOT_ORG_ID);
            log.warn("CbPlanValidationService.applyL0OrgScope: No rootOrgId found in criteria for L0 organization");
            return;
        }
        if (criteriaOrgIds.size() == 1) {
            request.put(Constants.ORG_SCOPE, Constants.SINGLE);
            log.info("CbPlanValidationService.applyL0OrgScope: Set orgScope to SINGLE for L0 org with 1 rootOrgId");
        } else {
            request.put(Constants.ORG_SCOPE, Constants.CUSTOM);
            log.info("CbPlanValidationService.applyL0OrgScope: Set orgScope to CUSTOM for L0 org with {} rootOrgIds",
                    criteriaOrgIds.size());
        }
    }

}
