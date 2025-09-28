package com.igot.cb.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.CbPlanDto;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;

@Component
@SuppressWarnings("unchecked")
public class RequestValidator {
    private final ObjectMapper mapper = new ObjectMapper();
    private final CbExtServerProperties cbExtServerProperties;

    public RequestValidator(CbExtServerProperties cbExtServerProperties) {
        this.cbExtServerProperties = cbExtServerProperties;
    }

    public List<String> validateCbPlanCreateRequest(ApiRequest request, boolean isCCA, String loggedInOrgId) {
        List<String> errors = new ArrayList<>();
        Map<String, Object> rawRequest = (Map<String, Object>) request.getRequest();
        errors = validateCbPlanRequest(rawRequest);
        if (CollectionUtils.isNotEmpty(errors)) {
            return errors;
        }
        return validateContextData(rawRequest, isCCA, loggedInOrgId);
    }

    public List<String> validateCbPlanRequest(Map<String, Object> request) {
        CbPlanDto cbPlanDto = mapper.convertValue(request, CbPlanDto.class);
        if (cbPlanDto.getIsApar() == null) {
            cbPlanDto.setIsApar(false);
        }
        request.put(Constants.IS_APAR, cbPlanDto.getIsApar() != null ? cbPlanDto.getIsApar() : false);
        List<String> validationErrors = new ArrayList<>();

        ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory();
        Validator validator = validatorFactory.getValidator();

        Set<ConstraintViolation<CbPlanDto>> violations = validator.validate(cbPlanDto);

        // Check for violations
        if (!violations.isEmpty()) {
            for (ConstraintViolation<CbPlanDto> violation : violations) {
                String errorMessage = "Validation Error: " + violation.getMessage();
                validationErrors.add(errorMessage);
            }
        }
        return validationErrors;
    }

    public List<String> validateContextData(Map<String, Object> request, boolean isCCA, String userOrgId) {
        return validateContextData(request, isCCA, userOrgId, null);
    }

    public List<String> validateContextData(Map<String, Object> request, boolean isCCA, String userOrgId,
            Set<String> rootOrgIdsInCriteria) {
        List<String> errors = new ArrayList<>();

        if (!request.containsKey(Constants.CONTEXT_DATA_REQUEST)) {
            errors.add("Validation Error: contextData is missing in request");
            return errors; // no contextData = no extra validation
        }

        Map<String, Object> contextData = null;
        Object contextDataObj = request.get(Constants.CONTEXT_DATA_REQUEST);
        if (contextDataObj instanceof String) {
            try {
                contextData = mapper.readValue((String) contextDataObj, new TypeReference<Map<String, Object>>() {
                });
            } catch (Exception e) {
                errors.add("Validation Error: Failed to parse contextData");
                return errors;
            }
        } else if (contextDataObj instanceof Map) {
            contextData = (Map<String, Object>) contextDataObj;
        } else {
            errors.add("Validation Error: contextData is of invalid type");
            return errors;
        }

        if (MapUtils.isEmpty(contextData) || !contextData.containsKey(Constants.ACCESS_CONTROL)) {
            errors.add("Validation Error: accessControl is missing in contextData");
            return errors; // no accessControl = no extra validation
        }

        Map<String, Object> accessControl = (Map<String, Object>) contextData.get(Constants.ACCESS_CONTROL);
        List<Map<String, Object>> userGroups = (List<Map<String, Object>>) accessControl.get(Constants.USER_GROUPS);

        if (CollectionUtils.isEmpty(userGroups)) {
            errors.add("Validation Error: User groups are missing in accessControl");
            return errors;
        }

        if (rootOrgIdsInCriteria == null) {
            rootOrgIdsInCriteria = new HashSet<>();
        }
        boolean rootOrgCriteriaNotFoundInUserGroup = false;
        for (Map<String, Object> userGroup : userGroups) {
            if (!userGroup.containsKey(Constants.USER_GROUP_CRITERIA_LIST)) {
                errors.add("Validation Error: criteriaList is missing in userGroup");
                return errors; // no criteriaList = no extra validation
            }

            List<Map<String, Object>> criteriaList = (List<Map<String, Object>>) userGroup
                    .get(Constants.USER_GROUP_CRITERIA_LIST);
            if (CollectionUtils.isEmpty(criteriaList)) {
                errors.add("Validation Error: criteriaList is empty in userGroup");
                return errors; // empty criteriaList = no extra validation
            }

            boolean rootOrgCriteriaFound = false;
            for (Map<String, Object> criteria : criteriaList) {
                String criteriaKey = (String) criteria.get(Constants.CRITERIA_KEY);
                if (StringUtils.isEmpty(criteriaKey)) {
                    errors.add("Validation Error: criteriaKey is missing in userGroup");
                    return errors; // no criteriaKey = no extra validation
                }
                if (!criteria.containsKey(Constants.CRITERIA_VALUE)) {
                    errors.add("Validation Error: criteriaValue is missing for criteriaKey: " + criteriaKey);
                    return errors; // no criteriaValue = no extra validation
                }
                List<String> criteriaValues = (List<String>) criteria.get(Constants.CRITERIA_VALUE);
                if (CollectionUtils.isEmpty(criteriaValues)) {
                    errors.add("Validation Error: criteriaValue is empty for criteriaKey: " + criteriaKey);
                    return errors; // empty criteriaValue = no extra validation
                }
                if (Constants.ROOT_ORG_ID.equalsIgnoreCase(criteriaKey)) {
                    rootOrgCriteriaFound = true;
                    rootOrgIdsInCriteria.addAll(criteriaValues);
                }
            }

            if (!rootOrgCriteriaFound) {
                if (!isCCA) {
                    errors.add(
                            "Validation Error: ROOT_ORG_ID criteria is missing in userGroup and organization is not CCA");
                    return errors; // rootOrgId criteria is mandatory if not CCA
                } else {
                    rootOrgCriteriaNotFoundInUserGroup = true;
                }
            }
        }

        if (isCCA) {
            if (rootOrgIdsInCriteria.size() == 0) {
                request.put(Constants.ORG_SCOPE, Constants.ALL);
            } else if (rootOrgCriteriaNotFoundInUserGroup) {
                errors.add(
                        "Validation Error: ROOT_ORG_ID criteria is added in one or more userGroups but missing in other.");
                return errors;
            } else {
                if (rootOrgIdsInCriteria.size() == 1) {
                    request.put(Constants.ORG_SCOPE, Constants.SINGLE);
                } else {
                    request.put(Constants.ORG_SCOPE, Constants.CUSTOM);
                }
            }
        } else {
            if (rootOrgIdsInCriteria.size() > 1) {
                errors.add("Validation Error: Multiple ROOT_ORG_IDs found in criteria but organization is not CCA");
                return errors; // only one rootOrgId allowed if not CCA
            } else if (rootOrgIdsInCriteria.size() == 1) {
                String rootOrgId = rootOrgIdsInCriteria.iterator().next();
                if (!StringUtils.equalsIgnoreCase(rootOrgId, userOrgId)) {
                    errors.add("Validation Error: ROOT_ORG_ID in criteria does not match logged-in user's orgId");
                    return errors; // rootOrgId must match logged-in user's orgId
                }
            } else {
                errors.add("Validation Error: No ROOT_ORG_ID found in criteria but organization is not CCA");
                return errors; // rootOrgId is mandatory if not CCA
            }
            request.put(Constants.ORG_SCOPE, Constants.SINGLE);
        }

        if (CollectionUtils.isEmpty(errors)) {
            request.put(Constants.ORG_ID_LIST, Arrays.asList(userOrgId));
        }

        return errors;
    }
}
