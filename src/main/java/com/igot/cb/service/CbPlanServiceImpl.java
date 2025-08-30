package com.igot.cb.service;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.model.CbPlanDto;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

@Service
@Slf4j
public class CbPlanServiceImpl {

    private final AccessTokenValidator accessTokenValidator;

    ObjectMapper mapper = new ObjectMapper();

    private Logger logger = LoggerFactory.getLogger(getClass().getName());

    private final CassandraOperation cassandraOperation;

    public CbPlanServiceImpl(AccessTokenValidator accessTokenValidator, CassandraOperation cassandraOperation) {
        this.accessTokenValidator = accessTokenValidator;
        this.cassandraOperation = cassandraOperation;
    }

    public ApiResponse createCbPlan(ApiRequest request, String userOrgId, String authUserToken) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_CREATE);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authUserToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }

            Map<String, Object> requestMap = new HashMap<>();
            requestMap.put(Constants.CREATED_BY, userId);
            requestMap.put(Constants.CREATED_AT, Instant.now());
            requestMap.put(Constants.UPDATED_AT, Instant.now());
            UUID cbPlanId = Uuids.timeBased();
            requestMap.put(Constants.PLAN_ID, String.valueOf(cbPlanId));
            CbPlanDto cbPlanDto = mapper.convertValue(request.getRequest(), CbPlanDto.class);
            if (cbPlanDto.getIsApar() == null) {
                cbPlanDto.setIsApar(false);
            }
            requestMap.put(Constants.IS_APAR, cbPlanDto.getIsApar() != null ? cbPlanDto.getIsApar() : false);
            List<String> validations = validateCbPlanRequest(cbPlanDto);
            if (CollectionUtils.isNotEmpty(validations)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(mapper.writeValueAsString(validations));
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            validations = validateContextData(cbPlanDto, request);
            if (CollectionUtils.isNotEmpty(validations)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(mapper.writeValueAsString(validations));
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            try {
                requestMap.put(Constants.DRAFT_DATA, mapper.writeValueAsString(cbPlanDto));
                requestMap.put(Constants.STATUS, Constants.DRAFT);
                Map<String, Object> requestMapFromApiRequest = (Map<String, Object>) request.getRequest();

                List<String> orgIdList = (List<String>) requestMapFromApiRequest.get(Constants.ORGIDLIST);
                requestMap.put(Constants.ORGIDLIST, orgIdList);
                requestMap.put(Constants.ORG_SCOPE, requestMapFromApiRequest.get(Constants.ORG_SCOPE));
                requestMap.put(Constants.CONTENT_LIST, requestMapFromApiRequest.get(Constants.CONTENT_LIST));
                requestMap.put(Constants.NAME, requestMapFromApiRequest.get(Constants.NAME));
                requestMap.put(Constants.COMMENT, requestMapFromApiRequest.get(Constants.COMMENT));
                requestMap.put(Constants.CONTENT_TYPE, requestMapFromApiRequest.get(Constants.CONTENT_TYPE));
                requestMap.put(Constants.END_DATE, cbPlanDto.getEndDate().toInstant());
                requestMap.put(Constants.CONTEXT_DATA_REQUEST, mapper.writeValueAsString(requestMapFromApiRequest.get(Constants.CONTEXT_DATA_REQUEST)));
                ApiResponse resp = (ApiResponse) cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_CB_PLAN_V2, requestMap);
                if (Constants.SUCCESS.equals(resp.get(Constants.RESPONSE))) {

                    response.getResult().put(Constants.ID, String.valueOf(cbPlanId));
                    if (Constants.CUSTOM.equalsIgnoreCase(cbPlanDto.getOrgScope())) {
                        ApiResponse lookupResp = insertCustomOrgLookup(String.valueOf(cbPlanId), orgIdList, cbPlanDto.getEndDate());
                        if (!Constants.SUCCESS.equals(lookupResp.getParams().getStatus())) {
                            response.getParams().setStatus(Constants.FAILED);
                            response.getParams().setErr(lookupResp.getParams().getErr());
                            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                            return response;
                        }
                    }

                    if (Constants.ALL.equalsIgnoreCase(cbPlanDto.getOrgScope())) {
                        ApiResponse singleResp = insertAllOrgLookup(String.valueOf(cbPlanId), cbPlanDto.getEndDate());
                        if (!Constants.SUCCESS.equals(singleResp.get(Constants.RESPONSE))) {
                            response.getParams().setStatus(Constants.FAILED);
                            response.getParams().setErr(singleResp.getParams().getErr());
                            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                            return response;
                        }
                    }

                    response.getResult().put(Constants.STATUS, Constants.CREATED);
                } else {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErr("Failed to Create CB Plan for OrgId: " + userOrgId + " message: " + resp.getParams().getErr());
                    response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                }
            } catch (JsonProcessingException e) {
                logger.error("Failed to Create CB Plan for OrgId: " + userOrgId, e);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(e.getMessage());
                response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            }
        } catch (Exception e) {
            logger.error("Failed to Create CB Plan for OrgId: " + userOrgId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private List<String> validateCbPlanRequest(CbPlanDto cbPlanDto) {
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



    @SuppressWarnings("unchecked")
    private List<String> validateContextData(CbPlanDto cbPlanDto, ApiRequest request) {
        List<String> errors = new ArrayList<>();
        Map<String, Object> rawRequest = (Map<String, Object>) request.getRequest();

        if (!rawRequest.containsKey(Constants.CONTEXT_DATA_REQUEST)) {
            return errors; // no contextData = no extra validation
        }

        Map<String, Object> contextData = (Map<String, Object>) rawRequest.get(Constants.CONTEXT_DATA_REQUEST);

        if (!contextData.containsKey(Constants.ACCESS_CONTROL)) {
            return errors; // no accessControl = no extra validation
        }

        Map<String, Object> accessControl = (Map<String, Object>) contextData.get(Constants.ACCESS_CONTROL);
        List<Map<String, Object>> userGroups = (List<Map<String, Object>>) accessControl.get(Constants.USER_GROUPS);

        if (CollectionUtils.isEmpty(userGroups)) {
            errors.add("Validation Error: User groups are missing in accessControl");
            return errors;
        }

        // If orgScope = Single/Custom → rootOrgId is mandatory
        if (Constants.SINGLE.equalsIgnoreCase(cbPlanDto.getOrgScope()) ||
                Constants.CUSTOM.equalsIgnoreCase(cbPlanDto.getOrgScope())) {

            boolean rootOrgFound = false;
            for (Map<String, Object> userGroup : userGroups) {
                List<Map<String, Object>> criteriaList =
                        (List<Map<String, Object>>) userGroup.get(Constants.USER_GROUP_CRTIRIA_LIST);
                if (criteriaList != null) {
                    for (Map<String, Object> criteria : criteriaList) {
                        String criteriaKey = (String) criteria.get(Constants.CRITERIA_KEY);
                        if (Constants.ROOT_ORG_ID.equalsIgnoreCase(criteriaKey)) {
                            rootOrgFound = true;
                            List<String> orgIdList = (List<String>) criteria.get(Constants.CRITERIA_VALUE);
                            if (CollectionUtils.isEmpty(orgIdList)) {
                                errors.add("Validation Error: orgId list cannot be empty for rootOrgId");
                                return errors;
                            }
                            rawRequest.put(Constants.ORGIDLIST, orgIdList);
                            request.setRequest(rawRequest);
                            break;
                        }
                    }
                }
                if (rootOrgFound) break;
            }

            if (!rootOrgFound) {
                errors.add("Validation Error: rootOrgId criteria is required in userGroupCriteriaList");
            }
        }
        return errors;
    }

    private ApiResponse insertCustomOrgLookup(String cbPlanId,
                                              List<String> orgIdList, Date endDate) {
        ApiResponse response = new ApiResponse();
        try {
            if (CollectionUtils.isEmpty(orgIdList)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("orgIdList is empty. Cannot create lookup entries.");
                return response;
            }

            // Prepare all lookup maps
            List<Map<String, Object>> lookupMaps = new ArrayList<>();
            for (String orgId : orgIdList) {
                Map<String, Object> lookupMap = new HashMap<>();
                lookupMap.put("planid", cbPlanId);
                lookupMap.put("orgid", orgId);
                lookupMap.put("enddate", endDate.toInstant());
                lookupMap.put("isactive", true);
                lookupMaps.add(lookupMap);
            }

            // Call bulk insertion (batching handled inside insertBulkRecord)
            response = cassandraOperation.insertBulkRecord(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ORG,
                    lookupMaps
            );

            if (!Constants.SUCCESS.equals(response.getParams().getStatus())) {
                return response; // return on first failure
            }

            response.getParams().setStatus(Constants.SUCCESS);
            response.getResult().put("message", "Lookup entries created successfully for all orgIds");

        } catch (Exception e) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Exception while creating org lookup entries: " + e.getMessage());
            log.error("Error inserting org lookup entries for CB Plan: " + cbPlanId, e);
        }

        return response;
    }

    private ApiResponse insertAllOrgLookup(String cbPlanId,
                                              Date endDate) {
        ApiResponse response = new ApiResponse();
        try {
            Map<String, Object> allOrgMap = new HashMap<>();
            int currentYear = Calendar.getInstance().get(Calendar.YEAR);
            allOrgMap.put("planyear", "ALL#" + currentYear);
            allOrgMap.put(Constants.PLAN_ID, cbPlanId);
            allOrgMap.put(Constants.END_DATE, endDate.toInstant()); // java.util.Date or Timestamp
            allOrgMap.put("isactive", true);

            response = (ApiResponse) cassandraOperation.insertRecord(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ALL_ORG,
                    allOrgMap
            );

        } catch (Exception e) {
            response.getParams().setStatus(Constants.FAILED);
            response.put(Constants.RESPONSE, Constants.FAILED);
            response.getParams().setErr("Exception while inserting SINGLE org lookup: " + e.getMessage());
            log.error("Error inserting SINGLE org lookup for CB Plan: " + cbPlanId, e);
        }

        return response;
    }



}
