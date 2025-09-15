package com.igot.cb.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.user.UserUtilityService;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
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
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class CbPlanLearnerServiceImpl {

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

    public CbPlanLearnerServiceImpl(AccessTokenValidator accessTokenValidator, CassandraOperation cassandraOperation) {
        this.accessTokenValidator = accessTokenValidator;
        this.cassandraOperation = cassandraOperation;
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
                response.setResponseCode(HttpStatus.BAD_REQUEST);
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
                    Map<String, Object> contextDataMap = parseContextData(contextDataObj);

                    // Evaluate access rules → skip plan if user has no access
                    if (MapUtils.isNotEmpty(contextDataMap) &&
                            !evaluateContextAccessRule(contextDataMap, userProfile)) {
                        logger.info("User does not have access to cbPlan: {}", cbPlan.get(Constants.PLAN_ID));
                        continue;
                    }
                }
                Map<String, Object> cbPlanDetails = new HashMap<>();
                cbPlanDetails.put(Constants.ID, cbPlan.get(Constants.PLAN_ID));
                cbPlanDetails.put(Constants.END_DATE_REQUEST, cbPlan.get(Constants.END_DATE_REQUEST));
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

    private Map<String, Object> parseContextData(Object contextDataObj) {
        if (!(contextDataObj instanceof String)) {
            return Collections.emptyMap();
        }

        try {
            String json = (String) contextDataObj;
            return mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            logger.warn("Failed to parse contextData: {}", contextDataObj, e);
            return Collections.emptyMap();
        }
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

                String userCriteriaValue = userProfile.get(criteriaKey);

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



