package com.igot.cb.service;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.cassandra.exceptions.CustomException;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiRespParam;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.model.CbPlanDto;
import com.igot.cb.user.UserUtilityService;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
@Slf4j
public class CbPlanServiceImpl {

    private final AccessTokenValidator accessTokenValidator;

    ObjectMapper mapper = new ObjectMapper();

    private Logger logger = LoggerFactory.getLogger(getClass().getName());

    private final CassandraOperation cassandraOperation;

    @Value("${cbplan.allowed.fields.update}")
    private String allowedFieldsConfig;

    @Autowired
    CbExtServerProperties serverProperties;


    @Autowired
    UserUtilityService userUtilityService;

    @Autowired
    ContentInfoServiceImpl contentService;

    @Autowired
    private EsUtilService esUtilService;

    @Value("${cb.plan.v2.index}")
    private String cpPlanIndex;

    @Value("${elastic.required.field.cb.plan.json.path}")
    private String elasticCbPlanJsonPath;

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

                List<String> orgIdList = (List<String>) requestMapFromApiRequest.get(Constants.ORG_ID_LIST);
                requestMap.put(Constants.ORG_ID_LIST, orgIdList);
                requestMap.put(Constants.ORG_SCOPE, requestMapFromApiRequest.get(Constants.ORG_SCOPE));
                requestMap.put(Constants.CONTENT_LIST, requestMapFromApiRequest.get(Constants.CONTENT_LIST));
                requestMap.put(Constants.NAME, requestMapFromApiRequest.get(Constants.NAME));
                requestMap.put(Constants.COMMENT, requestMapFromApiRequest.get(Constants.COMMENT));
                requestMap.put(Constants.CONTENT_TYPE, requestMapFromApiRequest.get(Constants.CONTENT_TYPE));
                requestMap.put(Constants.END_DATE, cbPlanDto.getEndDate().toInstant());
                requestMap.put(Constants.CONTEXT_DATA_REQUEST, mapper.writeValueAsString(requestMapFromApiRequest.get(Constants.CONTEXT_DATA_REQUEST)));
                ApiResponse resp = (ApiResponse) cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_CB_PLAN_V2, requestMap);
                if (Constants.SUCCESS.equals(resp.get(Constants.RESPONSE))) {
                    requestMap.put(Constants.ID, String.valueOf(cbPlanId));
                    requestMap.put(Constants.END_DATE_REQUEST, cbPlanDto.getEndDate().toInstant());
                    Map<String, Object> sanitizedMap = sanitizeForElastic(requestMap);
                    esUtilService.addDocument(cpPlanIndex, Constants.INDEX_TYPE, String.valueOf(cbPlanId), sanitizedMap, elasticCbPlanJsonPath);
                    response.getResult().put(Constants.ID, String.valueOf(cbPlanId));
                    if (Constants.SINGLE.equalsIgnoreCase(cbPlanDto.getOrgScope()) || Constants.CUSTOM.equalsIgnoreCase(cbPlanDto.getOrgScope())) {
                        ApiResponse lookupResp = insertCustomOrgLookup(String.valueOf(cbPlanId), orgIdList, cbPlanDto.getEndDate());
                        if (!Constants.SUCCESS.equals(lookupResp.get(Constants.RESPONSE))) {
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
                        (List<Map<String, Object>>) userGroup.get(Constants.USER_GROUP_CRITERIA_LIST);
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
                            rawRequest.put(Constants.ORG_ID_LIST, orgIdList);
                            request.setRequest(rawRequest);
                            cbPlanDto.setOrgIdList(orgIdList);
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

    public static Map<String, Object> sanitizeForElastic(Map<String, Object> input) {
        Map<String, Object> sanitized = new HashMap<>();
        for (Map.Entry<String, Object> entry : input.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Instant) {
                // Convert Instant → ISO String (e.g., 2025-09-02T09:30:56.446Z)
                sanitized.put(entry.getKey(), DateTimeFormatter.ISO_INSTANT.format((Instant) value));
            } else {
                sanitized.put(entry.getKey(), value);
            }
        }
        return sanitized;
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


    public ApiResponse updateCbPlan(ApiRequest request, String userOrgId, String token, List<String> userRoles) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_UPDATE);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(token, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }
            Map<String, Object> updatedCbPlan = (Map<String, Object>) request.getRequest();
            if (updatedCbPlan.get(Constants.ID) != null) {
                String cbPlanId = (String) updatedCbPlan.get(Constants.ID);
                Map<String, Object> cbPlanInfo = new HashMap<>();
                cbPlanInfo.put(Constants.PLAN_ID, cbPlanId);
                updatedCbPlan.remove(Constants.ID);
                List<Map<String, Object>> cbPlanMapInfo = cassandraOperation.getRecordsByProperties(
                        Constants.KEYSPACE_SUNBIRD, Constants.TABLE_CB_PLAN_V2, cbPlanInfo, null, null);
                if (CollectionUtils.isNotEmpty(cbPlanMapInfo)) {
                    Map<String, Object> cbPlanInfoMap = cbPlanMapInfo.get(0);
                    if (!(userId.equals(cbPlanInfoMap.get(Constants.CREATED_BY)) ||
                            serverProperties.getCbPlanUpdatePublishAuthorizedRoles().stream().anyMatch(roles -> CollectionUtils.isNotEmpty(userRoles) && userRoles.contains(roles)))) {
                        response.getParams().setStatus(Constants.FAILED);
                        response.getParams().setErr("Not Authorized to update cbp Plan");
                        response.setResponseCode(HttpStatus.BAD_REQUEST);
                        return response;
                    }
                    Map<String, Object> draftData = new HashMap<>(cbPlanInfoMap);
                    draftData.putAll(updatedCbPlan);
                    draftData.put(Constants.PLAN_ID, cbPlanInfoMap.get(Constants.PLAN_ID));
                    String draftInfo = null;
                    try {
                        draftInfo = mapper.writeValueAsString(updatedCbPlan);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    if (Constants.LIVE.equalsIgnoreCase((String) cbPlanInfoMap.get(Constants.STATUS))
                            && cbPlanInfoMap.get(Constants.CB_PUBLISHED_BY) != null) {
                        // check when the cbPlan is published, need to check only few field need to be
                        // modified.
                        List<String> allowedFieldForUpdate = Arrays.asList(allowedFieldsConfig.split(","));
                        long keyNotAllowedCount = updatedCbPlan.keySet().stream()
                                .filter(key -> !allowedFieldForUpdate.contains(key)).count();
                        if (keyNotAllowedCount > 0) {
                            response.getParams().setStatus(Constants.FAILED);
                            response.getParams().setErr("Allowed Field for update cbPlan are: " + Constants.NAME
                                    + ", " + Constants.CONTEXT_DATA_REQUEST + ", " + Constants.END_DATE);
                            response.setResponseCode(HttpStatus.BAD_REQUEST);
                            return response;
                        }
                    } else {
                        try {
                            draftInfo = updateDraftInfo(updatedCbPlan, cbPlanMapInfo.get(0));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    Map<String, Object> updatedCbPlanData = new HashMap<>();

                    updatedCbPlanData.put(Constants.UPDATED_BY, userId);
                    updatedCbPlanData.put(Constants.UPDATED_AT, Instant.now());
                    updatedCbPlanData.putAll(updatedCbPlan);
                    if (updatedCbPlan.containsKey(Constants.IS_APAR)) {
                        Object isAparVal = updatedCbPlan.get(Constants.IS_APAR);
                        if (isAparVal != null) {
                            updatedCbPlanData.put(Constants.IS_APAR, isAparVal);
                        }
                    }
                    if (updatedCbPlan.containsKey(Constants.CONTEXT_DATA_REQUEST)) {
                        Object contextData = updatedCbPlan.get(Constants.CONTEXT_DATA_REQUEST);
                        if (contextData != null) {
                            try {
                                updatedCbPlanData.put(Constants.CONTEXT_DATA_REQUEST, mapper.writeValueAsString(contextData));
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                    List<String> deletedOrgIds = new ArrayList<>();
                    List<String> addedOrgIds = new ArrayList<>();
                    if (updatedCbPlan.containsKey(Constants.CONTEXT_DATA_REQUEST)) {
                        Object contextDataObj = updatedCbPlan.get(Constants.CONTEXT_DATA_REQUEST);
                        List<String> newOrgIds = extractRootOrgIds(contextDataObj);
                        if (!newOrgIds.isEmpty()) {
                            updatedCbPlanData.put(Constants.ORG_ID_LIST, newOrgIds);
                            log.info("Extracted orgIds from contextData: {}", newOrgIds);
                            List<String> oldOrgIds = (List<String>) cbPlanInfoMap.getOrDefault(Constants.ORG_ID_LIST, new ArrayList<>());

                            // Compare lists
                            if (CollectionUtils.isNotEmpty(oldOrgIds)) {
                                deletedOrgIds = oldOrgIds.stream()
                                        .filter(id -> !newOrgIds.contains(id))
                                        .collect(Collectors.toList());

                                addedOrgIds = newOrgIds.stream()
                                        .filter(id -> !oldOrgIds.contains(id))
                                        .collect(Collectors.toList());
                            }

                        }
                    }
                    Date endDate = null;
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

                    Object endDateObj = updatedCbPlan.containsKey(Constants.END_DATE_REQUEST)
                            ? updatedCbPlan.get(Constants.END_DATE_REQUEST)
                            : cbPlanInfoMap.get(Constants.END_DATE_REQUEST);

                    endDate = parseToDate(endDateObj);
                    updatedCbPlan.put(Constants.END_DATE, endDate.toInstant());
                    updatedCbPlanData.put(Constants.DRAFT_DATA, draftInfo);
                    updatedCbPlanData.put(Constants.STATUS, Constants.DRAFT);
                    Map<String, Object> resp = cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD,
                            Constants.TABLE_CB_PLAN_V2, updatedCbPlanData, cbPlanInfo);
                    if (resp.get(Constants.RESPONSE).equals(Constants.SUCCESS)) {
                        for (Map.Entry<String, Object> entry : cbPlanInfoMap.entrySet()) {
                            updatedCbPlanData.putIfAbsent(entry.getKey(), entry.getValue());
                        }
                        updatedCbPlanData.put(Constants.ID, cbPlanId);
                        updatedCbPlanData.put(Constants.UPDATED_BY, userId);
                        updatedCbPlanData.put(Constants.CREATED_AT, cbPlanInfoMap.get(Constants.CREATED_AT_REQ));
                        updatedCbPlanData.put(Constants.PUBLISHED_ON, cbPlanInfoMap.get(Constants.CREATED_AT_REQ));
                        Map<String, Object> sanitizedMap = sanitizeForElastic(updatedCbPlanData);
                        esUtilService.updateDocument(cpPlanIndex, Constants.INDEX_TYPE, cbPlanId, sanitizedMap, elasticCbPlanJsonPath);
                        response.getResult().put(Constants.STATUS, Constants.UPDATED);
                        if (!addedOrgIds.isEmpty()) {
                            ApiResponse lookupResp = insertCustomOrgLookup(String.valueOf(cbPlanId), addedOrgIds, endDate);
                            if (!Constants.SUCCESS.equals(lookupResp.get(Constants.RESPONSE))) {
                                response.getParams().setStatus(Constants.FAILED);
                                response.getParams().setErr("Failed to insert orgId lookup: " + lookupResp.getParams().getErr());
                                response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                                return response;
                            }
                            response.getResult().put(Constants.MESSAGE, "updated cbPlan for cbPlanId: " + cbPlanId);
                        }
                        if (!deletedOrgIds.isEmpty()) {
                            for (String orgId : deletedOrgIds) {
                                Map<String, Object> deleteLookupMap = new HashMap<>();
                                deleteLookupMap.put("planid", String.valueOf(cbPlanId));
                                deleteLookupMap.put("orgid", orgId);
                                cassandraOperation.deleteRecord(Constants.KEYSPACE_SUNBIRD,
                                        Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ORG, deleteLookupMap);
                            }
                            response.getResult().put(Constants.MESSAGE, "updated cbPlan and deleted orgId lookup for cbPlanId: " + cbPlanId);
                        }

                    } else {
                        response.getParams().setStatus(Constants.FAILED);
                        response.getParams().setErr("cbPlan is not found for id: " + cbPlanId);
                        response.setResponseCode(HttpStatus.BAD_REQUEST);
                    }
                } else {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErr("Required Param id is missing");
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                }
                return response;
            } else {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("cbPlan is not found for id: " + updatedCbPlan.get(Constants.ID));
                response.setResponseCode(HttpStatus.BAD_REQUEST);
            }

        }catch(RuntimeException e){
            logger.error("Failed to Update CB Plan for OrgId: " + userOrgId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return response;
    }

    private String updateDraftInfo(Map<String, Object> updatedCbPlan, Map<String, Object> cbPlan) throws IOException {
        Map<String, Object> draftInfo = new HashMap<>();
        if (StringUtils.isBlank((String) cbPlan.get(Constants.DRAFT_DATA))) {
            draftInfo.put(Constants.NAME, updatedCbPlan.getOrDefault(Constants.NAME, cbPlan.get(Constants.NAME)));
            draftInfo.put(Constants.CONTENT_TYPE,
                    updatedCbPlan.getOrDefault(Constants.CONTENT_TYPE, cbPlan.get(Constants.CONTENT_TYPE)));
            draftInfo.put(Constants.CONTENT_LIST,
                    updatedCbPlan.getOrDefault(Constants.CONTENT_LIST, cbPlan.get(Constants.CONTENT_LIST)));
            draftInfo.put(Constants.END_DATE,
                    updatedCbPlan.getOrDefault(Constants.END_DATE, cbPlan.get(Constants.END_DATE)));
            draftInfo.put(Constants.CONTEXT_DATA_REQUEST,
                    updatedCbPlan.getOrDefault(Constants.CONTEXT_DATA_REQUEST, cbPlan.get(Constants.CONTEXT_DATA_REQUEST)));
            draftInfo.put(Constants.ORG_SCOPE,
                    updatedCbPlan.getOrDefault(Constants.ORG_SCOPE, cbPlan.get(Constants.ORG_SCOPE)));
            draftInfo.put(Constants.ORG_ID_LIST,
                    updatedCbPlan.getOrDefault(Constants.ORG_ID_LIST, cbPlan.get(Constants.ORG_ID_LIST)));
            draftInfo.put(Constants.IS_APAR,
                    updatedCbPlan.getOrDefault(Constants.IS_APAR,
                            cbPlan.getOrDefault(Constants.IS_APAR, false)));
        } else {
            CbPlanDto cbPlanDto = mapper.readValue((String) cbPlan.get(Constants.DRAFT_DATA), CbPlanDto.class);
            draftInfo.put(Constants.NAME, updatedCbPlan.getOrDefault(Constants.NAME, cbPlanDto.getName()));
            draftInfo.put(Constants.CONTENT_TYPE,
                    updatedCbPlan.getOrDefault(Constants.CONTENT_TYPE, cbPlanDto.getContentType()));
            draftInfo.put(Constants.CONTENT_LIST,
                    updatedCbPlan.getOrDefault(Constants.CONTENT_LIST, cbPlanDto.getContentList()));
            draftInfo.put(Constants.CONTEXT_DATA_REQUEST,
                    updatedCbPlan.getOrDefault(Constants.CONTEXT_DATA_REQUEST, cbPlanDto.getContextData()));
            draftInfo.put(Constants.ORG_SCOPE,
                    updatedCbPlan.getOrDefault(Constants.ORG_SCOPE, cbPlanDto.getOrgScope()));
            draftInfo.put(Constants.ORG_ID_LIST,
                    updatedCbPlan.getOrDefault(Constants.ORG_ID_LIST, cbPlanDto.getOrgIdList()));
            if (updatedCbPlan.containsKey(Constants.END_DATE)) {
                draftInfo.put(Constants.END_DATE, updatedCbPlan.get(Constants.END_DATE));
            } else if (cbPlanDto.getEndDate() != null) {
                // cbPlanDto.getEndDate() is usually a Timestamp -> convert to Date
                draftInfo.put(Constants.END_DATE, new Date(cbPlanDto.getEndDate().getTime()));
            }


            draftInfo.put(Constants.IS_APAR,
                    updatedCbPlan.getOrDefault(Constants.IS_APAR,
                            cbPlan.getOrDefault(Constants.IS_APAR, false)));
        }
        return mapper.writeValueAsString(draftInfo);
    }



    @SuppressWarnings("unchecked")
    private List<String> extractRootOrgIds(Object contextDataObj) {
        List<String> orgIdList = new ArrayList<>();

        try {
            if (!(contextDataObj instanceof Map)) {
                return orgIdList;
            }

            Map<String, Object> contextData = (Map<String, Object>) contextDataObj;

            if (!contextData.containsKey(Constants.ACCESS_CONTROL)) {
                return orgIdList;
            }

            Map<String, Object> accessControl = (Map<String, Object>) contextData.get(Constants.ACCESS_CONTROL);
            List<Map<String, Object>> userGroups = (List<Map<String, Object>>) accessControl.get(Constants.USER_GROUPS);

            if (CollectionUtils.isEmpty(userGroups)) {
                return orgIdList;
            }

            boolean rootOrgFound = false;

            for (Map<String, Object> userGroup : userGroups) {
                List<Map<String, Object>> criteriaList =
                        (List<Map<String, Object>>) userGroup.get(Constants.USER_GROUP_CRITERIA_LIST);

                if (CollectionUtils.isNotEmpty(criteriaList)) {
                    for (Map<String, Object> criteria : criteriaList) {
                        String criteriaKey = (String) criteria.get(Constants.CRITERIA_KEY);
                        if (Constants.ROOT_ORG_ID.equalsIgnoreCase(criteriaKey)) {
                            List<String> values = (List<String>) criteria.get(Constants.CRITERIA_VALUE);
                            if (CollectionUtils.isNotEmpty(values)) {
                                orgIdList.addAll(values);
                            }
                            rootOrgFound = true;
                            break; // ✅ exit criteria loop once rootOrgId found
                        }
                    }
                }
                if (rootOrgFound) {
                    break; // ✅ exit userGroups loop as well
                }
            }
        } catch (Exception e) {
            log.error("Error extracting rootOrgIds from contextData: {}", e.getMessage(), e);
        }

        return orgIdList;
    }



    public ApiResponse publishCbPlan(ApiRequest request, String userOrgId, String authUserToken, List<String> userRoles) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_PUBLISH);
        Map<String, Object> requestData = (Map<String, Object>) request.getRequest();
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authUserToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }
            String cbPlanId = (String) requestData.get(Constants.ID);
            String comment = (String) requestData.get(Constants.COMMENT);
            if (cbPlanId == null) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("CbPlanId is missing.");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            Map<String, Object> cbPlanInfo = new HashMap<>();
            cbPlanInfo.put(Constants.PLAN_ID, cbPlanId);
            List<Map<String, Object>> cbPlanMap = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_CB_PLAN_V2, cbPlanInfo, null, null);

            if (CollectionUtils.isNotEmpty(cbPlanMap)) {
                Map<String, Object> cbPlan = cbPlanMap.get(0);
                if (!(userId.equals(cbPlan.get(Constants.CREATED_BY)) ||
                        serverProperties.getCbPlanUpdatePublishAuthorizedRoles().stream().anyMatch(roles -> CollectionUtils.isNotEmpty(userRoles) && userRoles.contains(roles)))) {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErr("Not Authorized to publish cbp Plan");
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return response;
                }
                Map<String, Object> publishCbPlan = new HashMap<>();
                publishCbPlan.putAll(cbPlan);
                if ((Constants.LIVE.equalsIgnoreCase((String) cbPlan.get(Constants.STATUS))
                        && cbPlan.get(Constants.DRAFT_DATA) == null)
                        || Constants.CB_RETIRE.equalsIgnoreCase((String) cbPlan.get(Constants.STATUS))) {
                    response.getParams().setStatus(Constants.FAILED);
                    String errMsg = "CbPlan is already published for ID: " + cbPlanId;
                    if (Constants.CB_RETIRE.equalsIgnoreCase((String) cbPlan.get(Constants.STATUS))) {
                        errMsg = "CbPlan is already retired for ID: " + cbPlanId;
                    }
                    response.getParams().setErr(errMsg);
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return response;
                }
                if (Constants.DRAFT.equalsIgnoreCase((String) cbPlan.get(Constants.STATUS))) {
                    CbPlanDto cbPlanDto = mapper.readValue((String) cbPlan.get(Constants.DRAFT_DATA), CbPlanDto.class);
                    updateCbPlanData(cbPlan, cbPlanDto);
                } else {
                    Map<String, Object> cbPlanDtoMap = mapper.readValue((String) cbPlan.get(Constants.DRAFT_DATA),
                            new TypeReference<Map<String, Object>>() {
                            });
                    cbPlan.put(Constants.NAME,
                            cbPlanDtoMap.getOrDefault(Constants.NAME, publishCbPlan.get(Constants.NAME)));
                    if (cbPlanDtoMap.containsKey(Constants.IS_APAR)) {
                        Object isAparVal = cbPlanDtoMap.get(Constants.IS_APAR);
                        cbPlan.put(Constants.IS_APAR, isAparVal != null ? isAparVal : false);
                    } else if (publishCbPlan.containsKey(Constants.IS_APAR) && publishCbPlan.get(Constants.IS_APAR) != null) {
                        cbPlan.put(Constants.IS_APAR, publishCbPlan.get(Constants.IS_APAR));
                    } else {
                        cbPlan.put(Constants.IS_APAR, false);
                    }
                    cbPlan.put(Constants.DRAFT_DATA, null);
                }
                cbPlan.put(Constants.CB_PUBLISHED_BY, userId);
                if (StringUtils.isNoneBlank(comment)) {
                    cbPlan.put(Constants.COMMENT, comment);
                }
                cbPlan.remove(Constants.ID);
                cbPlan.remove(Constants.PLAN_ID);
                cbPlan.put(Constants.CB_PUBLISHED_AT, Instant.now());
                cbPlan.put(Constants.UPDATED_AT, Instant.now());
                cbPlan.put(Constants.CONTEXT_DATA_REQUEST, mapper.writeValueAsString(cbPlan.get(Constants.CONTEXT_DATA_REQUEST)));
                cbPlan.remove(Constants.END_DATE_REQUEST);
                Map<String, Object> resp = cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD,
                        Constants.TABLE_CB_PLAN_V2, cbPlan, cbPlanInfo);
                if (resp.get(Constants.RESPONSE).equals(Constants.SUCCESS)) {

                    cbPlan.put(Constants.ID, cbPlanId);
                    cbPlan.put(Constants.PUBLISHED_AT,Instant.now());
                    cbPlan.put(Constants.PUBLISHED_BY,userId);
                    cbPlan.put(Constants.UPDATED_AT, Instant.now());
                    cbPlan.put(Constants.CREATED_AT, cbPlan.get(Constants.CREATED_AT_REQ));
                    cbPlan.put(Constants.END_DATE_REQUEST, toInstant(cbPlan.get(Constants.END_DATE)));
                    Map<String, Object> sanitizedMap = sanitizeForElastic(cbPlan);
                    esUtilService.updateDocument(cpPlanIndex, Constants.INDEX_TYPE, cbPlanId, sanitizedMap, elasticCbPlanJsonPath);
                    response.getResult().put(Constants.STATUS, Constants.UPDATED);
                    response.getResult().put(Constants.MESSAGE, "Published cbPlan for cbPlanId: " + cbPlanId);
                } else {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams()
                            .setErr((String) resp.get(Constants.ERROR_MESSAGE) + "for cbPlanId: " + cbPlanId);
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                }
            } else {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("CbPlan is not exist for ID: " + cbPlanId);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
            }
        } catch (Exception e) {
            logger.error("Failed to Publish CB Plan for OrgId: " + userOrgId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private void updateCbPlanData(Map<String, Object> cbPlan, CbPlanDto planDto) {
        cbPlan.put(Constants.NAME, planDto.getName());
        try {
            cbPlan.put(Constants.CONTEXT_DATA_REQUEST, mapper.writeValueAsString(planDto.getContextData()));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        cbPlan.put(Constants.ORG_SCOPE, planDto.getOrgScope());
        cbPlan.put(Constants.ORG_ID_LIST, planDto.getOrgIdList());
        cbPlan.put(Constants.DRAFT_DATA, null);
        cbPlan.put(Constants.CONTENT_TYPE, planDto.getContentType());
        cbPlan.put(Constants.CONTENT_LIST, planDto.getContentList());
        cbPlan.put(Constants.END_DATE, planDto.getEndDate() != null ? planDto.getEndDate().toInstant() : null);
        cbPlan.put(Constants.STATUS, Constants.LIVE);
        cbPlan.put(Constants.IS_APAR, planDto.getIsApar() != null ? planDto.getIsApar() : false);
    }



    /**
     * Safely converts an object to java.util.Date.
     * Supports String (ISO 8601), Instant, Timestamp, and Date types.
     *
     * @param endDateObj the object to convert
     * @return Date object or null if conversion fails
     */
    public Date parseToDate(Object endDateObj) {
        if (endDateObj == null) return null;

        try {
            if (endDateObj instanceof String) {
                // ISO 8601 string, e.g., "2023-12-14T00:00:00Z"
                String str = (String) endDateObj;
                try {
                    // Try full ISO-8601 datetime first
                    return Date.from(Instant.parse(str));
                } catch (DateTimeParseException e) {
                    // Fallback for date-only strings "yyyy-MM-dd"
                    LocalDate localDate = LocalDate.parse(str, DateTimeFormatter.ISO_LOCAL_DATE);
                    return Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
                }
            } else if (endDateObj instanceof Instant) {
                return Date.from((Instant) endDateObj);
            } else if (endDateObj instanceof java.sql.Timestamp) {
                return new Date(((java.sql.Timestamp) endDateObj).getTime());
            } else if (endDateObj instanceof java.util.Date) {
                return new Date(((java.util.Date) endDateObj).getTime());
            }
        } catch (Exception e) {
            log.error("Error parsing endDate: {}", endDateObj, e);
        }

        return null;
    }


    // Convert an object to Instant safely
    private Instant toInstant(Object obj) {
        if (obj == null) return null;

        try {
            if (obj instanceof Instant) {
                return (Instant) obj;
            } else if (obj instanceof Date) {
                return ((Date) obj).toInstant();
            } else if (obj instanceof String) {
                String str = (String) obj;
                // Try ISO format first
                try {
                    return Instant.parse(str);
                } catch (DateTimeParseException e1) {
                    // Try simple date format yyyy-MM-dd
                    try {
                        LocalDate ld = LocalDate.parse(str, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                        return ld.atStartOfDay(ZoneId.systemDefault()).toInstant();
                    } catch (DateTimeParseException e2) {
                        logger.error("Unable to parse date string: {}", str, e2);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error converting to Instant: {}", obj, e);
        }

        return null;
    }


    public ApiResponse readCbPlan(String cbPlanId, String userOrgId, String authUserToken) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_READ_BY_ID);
        try {
            if (cbPlanId == null) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("CbPlanId is missing.");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            Map<String, Object> cbPlanInfo = new HashMap<>();
            cbPlanInfo.put(Constants.PLAN_ID, cbPlanId);
            List<Map<String, Object>> cbPlanMap = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_CB_PLAN_V2, cbPlanInfo, null, null);

            if (CollectionUtils.isNotEmpty(cbPlanMap)) {
                Map<String, Object> cbPlan = cbPlanMap.get(0);
                Map<String, Object> enrichData = populateReadData(cbPlan);
                enrichData.put(Constants.ID,cbPlanId);
                response.getResult().put(Constants.CONTENT, enrichData);
            } else {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("CbPlan is not exist for ID: " + cbPlanId);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
            }
        } catch (Exception e) {
            logger.error("Failed to Read CB Plan for OrgId: " + userOrgId + "for CB PlanId: " + cbPlanId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;

    }



    private Map<String, Object> populateReadData(Map<String, Object> cbPlan) throws Exception {
        Map<String, Object> enrichData = new HashMap<>();
        List<String> contentTypeInfo = new ArrayList<>();
        List<String> userDraftAssignmentTypeInfoForLive = new ArrayList<>();
        if (StringUtils.isBlank((String) cbPlan.get(Constants.DRAFT_DATA)) ||
                (StringUtils.isNotBlank((String) cbPlan.get(Constants.DRAFT_DATA))
                        && Constants.LIVE.equalsIgnoreCase((String) cbPlan.get(Constants.STATUS)))) {
            enrichData.put(Constants.NAME, cbPlan.get(Constants.NAME));
            enrichData.put(Constants.CONTENT_TYPE, cbPlan.get(Constants.CONTENT_TYPE));
            contentTypeInfo = (List<String>) cbPlan.get(Constants.CONTENT_LIST);
            enrichData.put(Constants.END_DATE_REQUEST, cbPlan.get(Constants.END_DATE_REQUEST));
            enrichData.put(Constants.CREATED_AT, cbPlan.get(Constants.CREATED_AT_REQ));
            enrichData.put(Constants.IS_APAR, cbPlan.getOrDefault(Constants.IS_APAR, false));
            if (StringUtils.isNotBlank((String) cbPlan.get(Constants.DRAFT_DATA))) {
                Map<String, Object> cbPlanDtoMap = mapper.readValue((String) cbPlan.get(Constants.DRAFT_DATA),
                        new TypeReference<Map<String, Object>>() {
                        });
                cbPlanDtoMap.remove(Constants.ID);
                enrichData.put(Constants.DRAFT_DATA, cbPlanDtoMap);
            }
        } else if (StringUtils.isNotBlank((String) cbPlan.get(Constants.DRAFT_DATA))
                && Constants.DRAFT.equalsIgnoreCase((String) cbPlan.get(Constants.STATUS))) {
            CbPlanDto cbPlanDto = mapper.readValue((String) cbPlan.get(Constants.DRAFT_DATA), CbPlanDto.class);
            enrichData.put(Constants.NAME, cbPlanDto.getName());
            enrichData.put(Constants.CONTENT_TYPE, cbPlanDto.getContentType());
            contentTypeInfo = cbPlanDto.getContentList();
            enrichData.put(Constants.END_DATE_REQUEST, cbPlanDto.getEndDate());
            enrichData.put(Constants.IS_APAR, cbPlanDto.getIsApar() != null ? cbPlanDto.getIsApar() : false);
        }
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();
        enrichData.put(Constants.CREATED_AT, cbPlan.get(Constants.CREATED_AT_REQ));
        enrichData.put(Constants.CB_PUBLISHED_AT, cbPlan.get(Constants.CB_PUBLISHED_AT));
        enrichData.put(Constants.STATUS, cbPlan.get(Constants.STATUS));
        Object contextData = cbPlan.get(Constants.CONTEXT_DATA_REQUEST);
        if (contextData != null) {
            try {
                JsonNode contextDataNode = mapper.readTree(contextData.toString());
                enrichData.put(Constants.CONTEXT_DATA_REQUEST, contextDataNode);
            } catch (Exception ex) {
                log.error("Failed to parse contextDataRequest: {}", contextData, ex);
            }
        } else {
            enrichData.put(Constants.CONTEXT_DATA_REQUEST, null); // or skip putting if you prefer
        }

        Object createdByObj = cbPlan.get(Constants.CREATED_BY);
        if (createdByObj != null && createdByObj instanceof String && !((String) createdByObj).trim().isEmpty()) {
            userUtilityService.getUserDetailsFromDB(
                    Arrays.asList((String) createdByObj),
                    Arrays.asList(Constants.FIRSTNAME, Constants.USER_ID),
                    userInfoMap
            );
        }

        enrichUserInfo(userInfoMap);

        enrichData.put(Constants.CREATED_BY_NAME,
                userInfoMap.get((String) cbPlan.get(Constants.CREATED_BY)).get(Constants.FIRSTNAME));
        enrichData.put(Constants.CREATED_BY, cbPlan.get(Constants.CREATED_BY));
        List<Map<String, Object>> enrichContentInfoMap = new ArrayList<>();
        for (String contentId : contentTypeInfo) {
            Map<String, Object> contentResponse = contentService.readContent(contentId, null);
            if (MapUtils.isNotEmpty(contentResponse)) {
                if (Constants.LIVE.equalsIgnoreCase((String) contentResponse.get(Constants.STATUS))) {
                    Map<String, Object> enrichContentMap = new HashMap<>();

                    enrichContentMap.put(Constants.NAME, contentResponse.getOrDefault(Constants.NAME, ""));
                    enrichContentMap.put(Constants.COMPETENCIES_V5, contentResponse.getOrDefault(Constants.COMPETENCIES_V5, Collections.emptyList()));
                    enrichContentMap.put(Constants.AVG_RATING, contentResponse.getOrDefault(Constants.AVG_RATING, 0.0));
                    enrichContentMap.put(Constants.IDENTIFIER, contentResponse.getOrDefault(Constants.IDENTIFIER, ""));
                    enrichContentMap.put(Constants.DESCRIPTION, contentResponse.getOrDefault(Constants.DESCRIPTION, ""));
                    enrichContentMap.put(Constants.ADDITIONAL_TAGS, contentResponse.getOrDefault(Constants.ADDITIONAL_TAGS, Collections.emptyList()));
                    enrichContentMap.put(Constants.CONTENT_TYPE_KEY, contentResponse.getOrDefault(Constants.CONTENT_TYPE_KEY, ""));
                    enrichContentMap.put(Constants.PRIMARY_CATEGORY, contentResponse.getOrDefault(Constants.PRIMARY_CATEGORY, ""));
                    enrichContentMap.put(Constants.DURATION, contentResponse.getOrDefault(Constants.DURATION, 0));
                    enrichContentMap.put(Constants.COURSE_APP_ICON, contentResponse.getOrDefault(Constants.COURSE_APP_ICON, ""));
                    enrichContentMap.put(Constants.POSTER_IMAGE, contentResponse.getOrDefault(Constants.POSTER_IMAGE, ""));
                    enrichContentMap.put(Constants.ORGANISATION, contentResponse.getOrDefault(Constants.ORGANISATION, ""));
                    enrichContentMap.put(Constants.CREATOR_LOGO, contentResponse.getOrDefault(Constants.CREATOR_LOGO, ""));
                    enrichContentMap.put(Constants.LANGUAGE_MAP_V1, contentResponse.getOrDefault(Constants.LANGUAGE_MAP_V1, Collections.emptyMap()));

                    enrichContentInfoMap.add(enrichContentMap);
                }

            }

            enrichData.put(Constants.CONTENT_LIST, enrichContentInfoMap);
        }
        return enrichData;
    }

    private String getDesignationForUser(String profileDetails, String userId) {
        String userDesignation = "";
        try {
            Map<String, Object> profileDetailsMap = null;
            List<Map<String, Object>> professionalDetails = null;
            if (StringUtils.isNotEmpty(profileDetails)) {
                profileDetailsMap = mapper.readValue(profileDetails, new TypeReference<HashMap<String, Object>>() {
                });
            }
            if (MapUtils.isNotEmpty(profileDetailsMap)) {
                professionalDetails = (List<Map<String, Object>>) profileDetailsMap.get(Constants.PROFESSIONAL_DETAILS);
            }
            if (CollectionUtils.isNotEmpty(professionalDetails)) {
                userDesignation = (String) professionalDetails.get(0).get(Constants.DESIGNATION);
            }
        } catch (Exception e) {
            logger.error("Not able to read the profile Details for userId: " + userId, e);
        }
        return userDesignation;
    }

    private void enrichUserInfo(Map<String, Map<String, String>> userInfoMap) {
        for (Map.Entry userEntry : userInfoMap.entrySet()) {
            Map<String, String> userInfo = (Map<String, String>) userEntry.getValue();
            String profileDetails = userInfo.get(Constants.PROFILE_DETAILS_KEY);
            String userDesignation = userInfo.get(Constants.DESIGNATION) != null ? userInfo.get(Constants.DESIGNATION) :
                    getDesignationForUser(profileDetails, (String) userEntry.getKey());
            userInfo.put(Constants.DESIGNATION, userDesignation);
            userInfo.remove(Constants.PROFILE_DETAILS_KEY);
        }
    }

    public ApiResponse searchCbPlan(SearchCriteria searchCriteria, String userOrgId, String token) {
        log.info("CbPlanService:searchCbPlan::inside method");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMMUNITY_SEARCH);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(token, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }
            SearchResult searchResult = esUtilService.searchDocuments(cpPlanIndex,
                    searchCriteria, elasticCbPlanJsonPath);
            List<Map<String, Object>> cbPlans = mapper.convertValue(
                    searchResult.getData(),
                    new TypeReference<List<Map<String, Object>>>() {
                    }
            );
            if (!searchResult.getData().isEmpty()) {
                List<Map<String, Object>> dataNode = searchResult.getData();

                if (dataNode != null) {
                    List<Map<String, Object>> enrichedData = new ArrayList<>();

                    for (Map<String, Object> item : dataNode) {
                        // Create a copy of item so we don’t mutate original
                        Map<String, Object> enrichedItem = new HashMap<>(item);
                        if (item.containsKey(Constants.CREATED_BY) && item.get(Constants.CREATED_BY) != null) {
                            Object createdByObj = item.get(Constants.CREATED_BY);
                            Map<String, Map<String, String>> userInfoMap = new HashMap<>();
                            if (createdByObj instanceof String && !((String) createdByObj).trim().isEmpty()) {
                                // fetch user details from DB
                                userUtilityService.getUserDetailsFromDB(
                                        Arrays.asList((String) createdByObj),
                                        Arrays.asList(Constants.FIRSTNAME, Constants.USER_ID),
                                        userInfoMap
                                );
                                // enrich user info map
                                enrichUserInfo(userInfoMap);
                                // add createdBy and createdByName to enrichedItem
                                Map<String, String> userDetails = userInfoMap.get((String) createdByObj);
                                if (userDetails != null) {

                                    enrichedItem.put(Constants.CREATED_BY_NAME,
                                            userInfoMap.get((String) item.get(Constants.CREATED_BY)).get(Constants.FIRSTNAME));
                                    enrichedItem.put(Constants.CREATED_BY, item.get(Constants.CREATED_BY));
                                }
                            }
                        }
                        if (item.containsKey(Constants.CONTENT_LIST) && item.get(Constants.CONTENT_LIST) != null) {
                            Object contentListObj = item.get(Constants.CONTENT_LIST);

                            if (contentListObj instanceof List) {
                                List<?> contentList = (List<?>) contentListObj;
                                List<Map<String, Object>> enrichContentInfoMap = new ArrayList<>();

                                for (Object contentIdObj : contentList) {
                                    String contentId = String.valueOf(contentIdObj);
                                    Map<String, Object> contentResponse = contentService.readContent(contentId, null);

                                    if (MapUtils.isNotEmpty(contentResponse) &&
                                            Constants.LIVE.equalsIgnoreCase((String) contentResponse.get(Constants.STATUS))) {

                                        Map<String, Object> enrichContentMap = new HashMap<>();
                                        enrichContentMap.put(Constants.NAME, contentResponse.getOrDefault(Constants.NAME, ""));
                                        enrichContentMap.put(Constants.COMPETENCIES_V5, contentResponse.getOrDefault(Constants.COMPETENCIES_V5, Collections.emptyList()));
                                        enrichContentMap.put(Constants.AVG_RATING, contentResponse.getOrDefault(Constants.AVG_RATING, 0.0));
                                        enrichContentMap.put(Constants.IDENTIFIER, contentResponse.getOrDefault(Constants.IDENTIFIER, ""));
                                        enrichContentMap.put(Constants.DESCRIPTION, contentResponse.getOrDefault(Constants.DESCRIPTION, ""));
                                        enrichContentMap.put(Constants.ADDITIONAL_TAGS, contentResponse.getOrDefault(Constants.ADDITIONAL_TAGS, Collections.emptyList()));
                                        enrichContentMap.put(Constants.CONTENT_TYPE_KEY, contentResponse.getOrDefault(Constants.CONTENT_TYPE_KEY, ""));
                                        enrichContentMap.put(Constants.PRIMARY_CATEGORY, contentResponse.getOrDefault(Constants.PRIMARY_CATEGORY, ""));
                                        enrichContentMap.put(Constants.DURATION, contentResponse.getOrDefault(Constants.DURATION, 0));
                                        enrichContentMap.put(Constants.COURSE_APP_ICON, contentResponse.getOrDefault(Constants.COURSE_APP_ICON, ""));
                                        enrichContentMap.put(Constants.POSTER_IMAGE, contentResponse.getOrDefault(Constants.POSTER_IMAGE, ""));
                                        enrichContentMap.put(Constants.ORGANISATION, contentResponse.getOrDefault(Constants.ORGANISATION, ""));
                                        enrichContentMap.put(Constants.CREATOR_LOGO, contentResponse.getOrDefault(Constants.CREATOR_LOGO, ""));
                                        enrichContentMap.put(Constants.LANGUAGE_MAP_V1, contentResponse.getOrDefault(Constants.LANGUAGE_MAP_V1, Collections.emptyMap()));

                                        enrichContentInfoMap.add(enrichContentMap);
                                    }
                                }

                                // 🔑 Replace CONTENT_LIST with enriched maps
                                enrichedItem.put(Constants.CONTENT_LIST, enrichContentInfoMap);
                            }
                        }

                        enrichedData.add(enrichedItem);
                    }

                    // 🔑 Replace original data with enrichedData
                    searchResult.setData(enrichedData);

                    response.getResult().put(Constants.RESULT, searchResult);
                    createSuccessResponse(response);
                    return response;
                }
            }



        } catch (Exception e) {
            logger.error("Error occured while searching:", e);
            throw new CustomException(Constants.ERROR, "error while processing",
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private void createSuccessResponse(ApiResponse response) {
        response.setParams(new ApiRespParam());
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
    }


    public ApiResponse retireCbPlan(ApiRequest request, String userOrgId, String token, List<String> userRoles) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CB_PLAN_RETIRE);
        Map<String, Object> requestData = (Map<String, Object>) request.getRequest();
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(token, response);
            if (StringUtils.isBlank(userId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            String cbPlanId = requestData.get(Constants.ID).toString();
            String comment = (String) requestData.get(Constants.COMMENT);
            if (StringUtils.isBlank(cbPlanId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("CbPlanId is missing.");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            Map<String, Object> cbPlanInfo = new HashMap<>();
            cbPlanInfo.put(Constants.PLAN_ID, cbPlanId);
            List<Map<String, Object>> cbPlanMap = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_CB_PLAN_V2, cbPlanInfo, null, null);

            if (CollectionUtils.isNotEmpty(cbPlanMap)) {
                Map<String, Object> cbPlan = cbPlanMap.get(0);
                if (!(userId.equals(cbPlan.get(Constants.CREATED_BY)) ||
                        serverProperties.getCbPlanUpdatePublishAuthorizedRoles().stream().anyMatch(roles -> CollectionUtils.isNotEmpty(userRoles) && userRoles.contains(roles)))) {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErr("Not Authorized to delete cbp Plan");
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return response;
                }
                if (Constants.CB_RETIRE.equalsIgnoreCase((String) cbPlan.get(Constants.STATUS))) {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErr("CbPlan is already archived for ID: " + cbPlanId);
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return response;
                }

                cbPlan.put(Constants.UPDATED_AT, Instant.now());
                cbPlan.put(Constants.UPDATED_BY, userId);
                cbPlan.put(Constants.STATUS, Constants.CB_RETIRE);
                if (StringUtils.isNoneBlank(comment)) {
                    cbPlan.put(Constants.COMMENT, comment);
                }
                cbPlan.remove(Constants.PLAN_ID);
                cbPlan.put(Constants.CB_PUBLISHED_AT, Instant.now());
                Map<String, Object> resp = cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD,
                        Constants.TABLE_CB_PLAN_V2, cbPlan, cbPlanInfo);
                if (resp.get(Constants.RESPONSE).equals(Constants.SUCCESS)) {
                    cbPlan.put(Constants.ID, cbPlanId);
                    cbPlan.put(Constants.STATUS, Constants.CB_RETIRE);
                    Map<String, Object> sanitizedMap = sanitizeForElastic(cbPlan);
                    esUtilService.addDocument(cpPlanIndex, Constants.INDEX_TYPE, cbPlanId, sanitizedMap, elasticCbPlanJsonPath);
                    CbPlanDto cbPlanDto = mapper.convertValue(sanitizedMap, CbPlanDto.class);
                    List<String> orgIdList= cbPlanDto.getOrgIdList();
                    if (Constants.SINGLE.equalsIgnoreCase(cbPlanDto.getOrgScope()) || Constants.CUSTOM.equalsIgnoreCase(cbPlanDto.getOrgScope())) {
                        ApiResponse lookupResp = archiveCustomOrgLookup(cbPlanId, orgIdList);
                        if (!Constants.SUCCESS.equals(lookupResp.get(Constants.RESPONSE))) {
                            response.getParams().setStatus(Constants.FAILED);
                            response.getParams().setErr(lookupResp.getParams().getErr());
                            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                            return response;
                        }
                    }
                    int currentYear = Calendar.getInstance().get(Calendar.YEAR);
                    if (Constants.ALL.equalsIgnoreCase(cbPlanDto.getOrgScope())) {
                        Map<String, Object> compositeKeyMap = Map.of(
                                Constants.PLAN_ID_RQST, cbPlanId,
                                Constants.PLAN_YEAR, "ALL#" + currentYear
                        );

                        Map<String, Object> singleResp =  cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD,
                                Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ALL_ORG,
                                Collections.singletonMap(Constants.IS_ACTIVE, false),
                                compositeKeyMap);
                        if (!Constants.SUCCESS.equals(singleResp.get(Constants.RESPONSE))) {
                            response.getParams().setStatus(Constants.FAILED);
                            response.getParams().setErr((String) singleResp.get(Constants.ERROR_MESSAGE));
                            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                            return response;
                        }
                    }
                    response.getResult().put(Constants.STATUS, Constants.UPDATED);
                    response.getResult().put(Constants.MESSAGE, "Archived cbPlan for cbPlanId: " + cbPlanId);
                } else {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams()
                            .setErr((String) resp.get(Constants.ERROR_MESSAGE) + "for cbPlanId: " + cbPlanId);
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                }
            } else {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("CbPlan is not exist for ID: " + cbPlanId);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
            }
        } catch (Exception e) {
            logger.error("Failed to Retire CB Plan for OrgId: " + userOrgId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private ApiResponse archiveCustomOrgLookup(String cbPlanId, List<String> orgIdList) {
        ApiResponse response = new ApiResponse();
        try {
            if (CollectionUtils.isEmpty(orgIdList)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("orgIdList is empty. Cannot archive lookup entries.");
                return response;
            }

            for (String orgId : orgIdList) {
                // attributes to update
                Map<String, Object> updateAttributes = new HashMap<>();
                updateAttributes.put(Constants.IS_ACTIVE, false);
                // primary/composite key for lookup
                Map<String, Object> compositeKey = new HashMap<>();
                compositeKey.put(Constants.PLAN_ID_RQST, cbPlanId);
                compositeKey.put(Constants.ORG_ID_RQST, orgId);

                Map<String, Object> updateResp = cassandraOperation.updateRecord(
                        Constants.KEYSPACE_SUNBIRD,
                        Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ORG,
                        updateAttributes,
                        compositeKey
                );

                if (!Constants.SUCCESS.equals(updateResp.get(Constants.RESPONSE))) {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErr("Failed to archive record for orgId: " + orgId);
                    return response;
                }
            }
            response.put(Constants.RESPONSE, Constants.SUCCESS);
            response.getParams().setStatus(Constants.SUCCESS);
            response.getResult().put("message", "Lookup entries archived successfully for all orgIds");

        } catch (Exception e) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Exception while archiving org lookup entries: " + e.getMessage());
            log.error("Error archiving org lookup entries for CB Plan: " + cbPlanId, e);
        }

        return response;
    }


    public ApiResponse getCBPlanListForUser(String userOrgId, String authTokenOrUserId, boolean isPrivate) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.CBP_PLAN_USER_LIST_API);
        try {
            String userId = "";
            if (isPrivate)
                userId = authTokenOrUserId;
            else
                userId = accessTokenValidator.fetchUserIdFromAccessToken(authTokenOrUserId, response);
            if (StringUtils.isBlank(userId)) {
                return response;
            }
            logger.info("UserId of the User : " + userId + ", User org ID : " + userOrgId);

            Map<String, Object> propertiesMap = new HashMap<>();

            Map<String, String> userProfile = new HashMap<>();
            Map<String, Object> queryParams = Map.of(Constants.ID, userId);
            List<String> userFields = Arrays.asList(Constants.ID, Constants.ROOT_ORG_ID, Constants.PROFILE_DETAILS);
            List<Map<String, Object>> userList = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.USER, queryParams, userFields, null);
            if (CollectionUtils.isEmpty(userList)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("User Does not Exist");
                response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
            setUserProfile(userProfile, userList.get(0));
            propertiesMap.clear();
            int currentYear = Calendar.getInstance().get(Calendar.YEAR);
            propertiesMap.put(Constants.PLAN_YEAR, "ALL#" + currentYear);
            List<Map<String, Object>> cbplanResult = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ALL_ORG, propertiesMap, new ArrayList<>(), null);
            propertiesMap.clear();
            propertiesMap.put(Constants.ORG_ID, userOrgId);
            List<Map<String, Object>> cbplanOrgResult = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ORG,
                    propertiesMap,
                    new ArrayList<>(),
                    null
            );

// 3️⃣ Merge results into one list/map
            if (CollectionUtils.isNotEmpty(cbplanOrgResult)) {
                cbplanResult.addAll(cbplanOrgResult);
            }

            if (CollectionUtils.isEmpty(cbplanResult)) {
                response.getParams().setStatus(Constants.SUCCESS);
                response.getParams().setErr("CB Plan does not exist for the user");
                response.setResponseCode(HttpStatus.OK);
                return response;
            }

            List<Map<String, Object>> resultMap = new ArrayList<>();
            Map<String, Object> courseDetailsMap = new HashMap<>();
            cbplanResult = cbplanResult.stream()
                    .filter(plan -> Boolean.TRUE.equals(plan.get(Constants.IS_ACTIVE)))
                    .sorted(Comparator.comparing(m -> (Instant) ((Map<String, Object>) m).get(Constants.END_DATE_REQUEST), Comparator.reverseOrder()))
                    .collect(Collectors.toList());

            List<String> planIds = cbplanResult.stream()
                    .map(plan -> (String) plan.get(Constants.PLAN_ID))
                    .collect(Collectors.toList());
            if (CollectionUtils.isEmpty(planIds)) {
                response.getParams().setStatus(Constants.SUCCESS);
                response.getParams().setErr("No active CB Plans found for  user");
                response.setResponseCode(HttpStatus.OK);
                return response;
            }
            propertiesMap.clear();
            propertiesMap.put(Constants.PLAN_ID, planIds);
            List<Map<String, Object>> activeCbPlans = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_CB_PLAN_V2,
                    propertiesMap,
                    new ArrayList<>(),
                    null
            );
            activeCbPlans= activeCbPlans.stream()
                    .filter(plan -> Constants.LIVE.equalsIgnoreCase((String) plan.get(Constants.STATUS)))
                    .collect(Collectors.toList());
            for (Map<String, Object> cbPlan : activeCbPlans) {
                Object contextDataObj = cbPlan.get(Constants.CONTEXT_DATA_REQUEST);
                if (contextDataObj != null) {
                    Map<String, Object> contextDataMap = parseContextData(contextDataObj, mapper);

                    // Evaluate access rules → skip plan if user has no access
                    if (MapUtils.isNotEmpty(contextDataMap) &&
                            !evaluateContextAccessRule(contextDataMap, userProfile)) {
                        logger.info("User does not have access to cbPlan: {}", cbPlan.get(Constants.PLAN_ID));
                        continue;
                    }
                }
                Map<String, Object> cbPlanDetails = new HashMap<>();
                cbPlanDetails.put(Constants.ID, cbPlan.get(Constants.PLAN_ID));
                cbPlanDetails.put(Constants.END_DATE, cbPlan.get(Constants.END_DATE_REQUEST));
                List<String> courses = (List<String>) cbPlan.get(Constants.CONTENT_LIST);
                cbPlanDetails.put(Constants.IS_APAR,
                        cbPlan.containsKey(Constants.IS_APAR) && cbPlan.get(Constants.IS_APAR) != null
                                ? cbPlan.get(Constants.IS_APAR)
                                : false);

                // Required Fields to be added later if required
                List<Map<String, Object>> courseList = new ArrayList<>();
                for (String courseId : courses) {
                    Map<String, Object> contentDetails = null;
                    if (!courseDetailsMap.containsKey(courseId)) {
                        contentDetails = contentService.readContent(courseId, null);
                        if (MapUtils.isNotEmpty(contentDetails)) {
                            //if (Constants.LIVE.equalsIgnoreCase((String) contentDetails.get(Constants.STATUS))) {
                            if (courseId.contains("_rc")) {
                                if (Constants.VERIFIED.equalsIgnoreCase(userProfile.get(Constants.PROFILE_STATUS_KEY).toLowerCase())){
                                    Map<String, Object> secureSettings = (Map<String, Object>) contentDetails.get(Constants.SECURE_SETTINGS);

                                    if (MapUtils.isNotEmpty(secureSettings)) {
                                        List<String> secureOrganisationList = (List<String>) secureSettings.get(Constants.ORGANISATION);

                                        if (CollectionUtils.isNotEmpty(secureOrganisationList) && secureOrganisationList.contains(userOrgId)) {
                                            courseDetailsMap.put(courseId, contentDetails);
                                        }
                                    }
                                }

                                if (!courseDetailsMap.containsKey(courseId)) {
                                    contentDetails.clear();
                                }
                            } else {
                                courseDetailsMap.put(courseId, contentDetails);
                            }
                        } else {
                            logger.error("Failed to read course details for Id: " + courseId);
                        }
                    } else {
                        continue;
                    }
                    if (MapUtils.isNotEmpty(contentDetails)) {
                        courseList.add(contentDetails);
                    }
                }
                boolean containsLanguageMap = courseList.stream().anyMatch(course ->
                        course.get(Constants.LANGUAGE_MAP_V1) instanceof Map &&
                                !((Map<?, ?>) course.get(Constants.LANGUAGE_MAP_V1)).isEmpty()
                );
                if (containsLanguageMap) {
                    cbPlanDetails.put(Constants.CONTENT_LIST, removeDuplicateCourses(courseList));
                } else {
                    cbPlanDetails.put(Constants.CONTENT_LIST, courseList);
                }
                resultMap.add(cbPlanDetails);
            }
            logger.info("Number of CB Plan Available for the user is " + resultMap.size());
            response.getResult().put(Constants.COUNT, resultMap.size());
            response.getResult().put(Constants.CONTENT, resultMap);
        } catch (Exception e) {
            logger.error("Failed to lookup for user cb plan details. Exception: " + e.getMessage(), e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private Map<String, Object> parseContextData(Object contextDataObj, ObjectMapper mapper) throws IOException {
        if (contextDataObj == null) {
            return Collections.emptyMap();
        }

        String rawJson = contextDataObj.toString().trim();

        // Case 1: If it's already a JSON object (starts with { ... }), parse directly
        if (rawJson.startsWith("{") && rawJson.endsWith("}")) {
            return mapper.readValue(rawJson, new TypeReference<Map<String, Object>>() {});
        }

        // Case 2: If it's an escaped JSON string (starts with "{...}" including quotes)
        if (rawJson.startsWith("\"") && rawJson.endsWith("\"")) {
            // First parse into a TextNode, then get its actual value
            JsonNode textNode = mapper.readTree(rawJson);
            String unwrappedJson = textNode.asText();

            return mapper.readValue(unwrappedJson, new TypeReference<Map<String, Object>>() {});
        }

        // Fallback
        return Collections.emptyMap();
    }




    public List<Map<String, Object>> removeDuplicateCourses(List<Map<String, Object>> courseList) {
        Set<String> seenIdentifiers = new HashSet<>();
        List<Map<String, Object>> finalList = new ArrayList<>();
        for (Map<String, Object> course : courseList) {
            String identifier = (String) course.get(Constants.IDENTIFIER);
            if (seenIdentifiers.contains(identifier)) {
                continue;
            }
            Map<String, Object> languageMap = new HashMap<>();
            Object langObj = course.get(Constants.LANGUAGE_MAP_V1);
            if (langObj instanceof Map<?, ?>) {
                languageMap = (Map<String, Object>) langObj;
            }
            Set<String> languageIdentifiers = new HashSet<>();
            for (Object value : languageMap.values()) {
                if (value instanceof Map<?, ?>) {
                    Map<String, Object> langDetails = (Map<String, Object>) value;
                    String langId = (String) langDetails.get(Constants.ID);
                    if (langId != null) {
                        languageIdentifiers.add(langId);
                    }
                }
            }
            finalList.add(course);
            seenIdentifiers.addAll(languageIdentifiers);
        }
        return finalList;
    }


    private void setUserProfile(Map<String, String> userProfile, Map<String, Object> userBasicProfile) throws JsonProcessingException {
        if (org.apache.commons.collections4.MapUtils.isEmpty(userBasicProfile)) {
            log.warn("User basic profile is empty for userId: {}", userProfile.get(Constants.ID));
            return;
        }
        userProfile.put(Constants.USER, (String) userBasicProfile.get(Constants.ID));
        userProfile.put(Constants.ROOT_ORG_ID, (String) userBasicProfile.get(Constants.ROOT_ORG_ID.toLowerCase()));
        Object rawValue = userBasicProfile.get(Constants.PROFILE_DETAILS.toLowerCase());
        Map<String, Object> profileDetails;

        if (rawValue instanceof String) {
            profileDetails = mapper.readValue((String) rawValue, new TypeReference<Map<String, Object>>() {});
        } else if (rawValue instanceof Map) {
            profileDetails = (Map<String, Object>) rawValue;
        } else {
            throw new IllegalArgumentException("Unsupported type for profileDetails: " + rawValue);
        }

        if (!org.apache.commons.collections4.MapUtils.isEmpty(profileDetails)) {
            List<Map<String, Object>> professionalDetailList = (List<Map<String, Object>>) profileDetails
                    .get(Constants.PROFESSIONAL_DETAILS);
            if (CollectionUtils.isNotEmpty(professionalDetailList)) {
                Map<String, Object> professionalDetails = professionalDetailList.get(0);
                userProfile.put(Constants.DESIGNATION, (String) professionalDetails.get(Constants.DESIGNATION));
                userProfile.put(Constants.GROUP, (String) professionalDetails.get(Constants.GROUP));
            }
            userProfile.put(Constants.PROFILE_STATUS_KEY.toLowerCase(),
                    (String) profileDetails.get(Constants.PROFILE_STATUS_KEY));
            Map<String, Object> cadreDetails = (Map<String, Object>) profileDetails.get(Constants.CADRE_DETAILS);

            if (org.apache.commons.collections4.MapUtils.isNotEmpty(cadreDetails)) {
                userProfile.put(Constants.CADRE, (String) cadreDetails.get(Constants.CADRE_NAME));
                userProfile.put(Constants.SERVICE, (String) cadreDetails.get(Constants.CIVIL_SERVICE_NAME));
                if (cadreDetails.containsKey(Constants.CADRE_BATCH)) {
                    userProfile.put(Constants.BATCH, String.valueOf(cadreDetails.get(Constants.CADRE_BATCH)));
                }
                if (cadreDetails.containsKey(Constants.CENTRAL_DEPUTATION)) {
                    userProfile.put(Constants.CENTRAL_DEPUTATION, String.valueOf( cadreDetails.get(Constants.CENTRAL_DEPUTATION)));
                }
            }
        }
    }

    private boolean evaluateContextAccessRule(Map<String, Object> accessSettingIdMap,
                                              Map<String, String> userProfile) {
        if (MapUtils.isEmpty(accessSettingIdMap) || MapUtils.isEmpty(userProfile)) {
            log.error("Access setting map or user profile is empty");
            return false;
        }

        // Step 1: fetch accessControl
        Map<String, Object> accessControl = (Map<String, Object>) accessSettingIdMap.get(Constants.ACCESS_CONTROL);
        if (MapUtils.isEmpty(accessControl)) {
            log.warn("No accessControl found in accessSettingIdMap");
            return false;
        }

        // Step 2: fetch userGroups
        List<Map<String, Object>> userGroups = (List<Map<String, Object>>) accessControl.get(Constants.USER_GROUPS);
        if (CollectionUtils.isEmpty(userGroups)) {
            log.warn("No userGroups found under accessControl");
            return false;
        }
        if (CollectionUtils.isEmpty(userGroups)) {
            return false;
        }

        // Iterate through all groups: user must match at least one fully
        for (Map<String, Object> userGroup : userGroups) {
            String userGroupName = (String) userGroup.get(Constants.USER_GROUP_NAME); // adjust constant if you have
            boolean isUserHasAccess = true;

            List<Map<String, Object>> criteriaList =
                    (List<Map<String, Object>>) userGroup.get(Constants.USER_GROUP_CRITERIA_LIST);

            if (CollectionUtils.isEmpty(criteriaList)) {
                continue; // no criteria = skip group
            }

            for (Map<String, Object> criteria : criteriaList) {
                String criteriaKey = (String) criteria.get(Constants.CRITERIA_KEY);
                List<String> criteriaValues = (List<String>) criteria.get(Constants.CRITERIA_VALUE);

                String userCriteriaValue = userProfile.get(criteriaKey.toLowerCase());

                if (StringUtils.isEmpty(userCriteriaValue) || !criteriaValues.contains(userCriteriaValue)) {
                    log.debug("User does not match criteria key: {} in group: {}", criteriaKey, userGroupName);
                    isUserHasAccess = false;
                    break;
                }
            }

            if (isUserHasAccess) {
                log.info("User matches all criteria in userGroup: {}", userGroupName);
                return true;
            }
        }

        return false;
    }


}
