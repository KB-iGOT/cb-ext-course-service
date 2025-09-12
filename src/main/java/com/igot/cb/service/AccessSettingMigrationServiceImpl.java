package com.igot.cb.service;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.igot.cb.elasticsearch.service.EsUtilService;
import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.igot.cb.cache.IdMapCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.BitSetDeserializer;
import com.igot.cb.util.BitSetSerializer;
import com.igot.cb.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class AccessSettingMigrationServiceImpl {

    @Value("${elastic.required.field.cb.plan.json.path}")
    private String elasticCbPlanJsonPath;
    @Value("${cb.plan.v2.index}")
    private String cpPlanIndex;

    private final CassandraOperation cassandraOperation;
    private final ContentInfoServiceImpl contentService;
    private final IdMapCacheMgr idMapCacheMgr;
    private final ObjectMapper objectMapper;
    private final EsUtilService esUtilService;

    public AccessSettingMigrationServiceImpl(CassandraOperation cassandraOperation,
                                             ContentInfoServiceImpl contentService,
                                             IdMapCacheMgr idMapCacheMgr, EsUtilService esUtilService) {
        this.cassandraOperation = cassandraOperation;
        this.contentService = contentService;
        this.idMapCacheMgr = idMapCacheMgr;
        this.esUtilService = esUtilService;
        this.objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(BitSet.class, new BitSetSerializer());
        module.addDeserializer(BitSet.class, new BitSetDeserializer());
        objectMapper.registerModule(module);
    }

    public ApiResponse migrateAccessSettingRules() {
        ApiResponse response = ApiResponse.createDefaultResponse("migrateAccessSettingRules");

        try {
            List<Map<String, Object>> accessSettingRuleMapList = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD_COURSE, Constants.ACCESS_SETTINGS_RULES_TABLE, null,
                    null, null);
            for (Map<String, Object> accessSettingMap : accessSettingRuleMapList) {
                String contextId = (String) accessSettingMap.get(Constants.CONTEXT_ID);
                if (processAccessSettingRule(accessSettingMap)) {
                    cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD_COURSE,
                            Constants.ACCESS_SETTINGS_RULES_TABLE_V2, accessSettingMap);
                    response.getResult().put(contextId, Constants.SUCCESS);
                } else {
                    response.getResult().put(contextId, Constants.FAILED);
                }
            }
        } catch (Exception e) {
            log.error("Error occurred while migrating access setting rules: {}", e.getMessage(), e);
            response.updateErrorDetails("Migration failed due to an error", HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    public ApiResponse migrateCBPlanAccessSettingRules() {
        ApiResponse response = ApiResponse.createDefaultResponse("migrateCBPlanAccessSettingRules");
        AtomicInteger migrated = new AtomicInteger();
        AtomicInteger skipped = new AtomicInteger();
        List<String> errors = new ArrayList<>();
        try {
            List<Map<String, Object>> cbPlanListMap = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.CB_PLAN_TABLE, null,
                    null, null);
            
            for (Map<String, Object> cbPlanMap : cbPlanListMap) {
                String status = String.valueOf(cbPlanMap.get(Constants.STATUS));
                Map<String, Object> cbPlanV2Map = new HashMap<>();
                if (status.equalsIgnoreCase(Constants.DRAFT)) {
                    String draftDataJson = (String) cbPlanMap.get(Constants.DRAFT_DATA);
                    if (draftDataJson != null && !draftDataJson.isEmpty()) {
                        try {
                            Map<String, Object> draftData = objectMapper.readValue(draftDataJson, Map.class);
                            cbPlanV2Map.put(Constants.NAME, draftData.get(Constants.NAME));
                            String endDateString = (String) draftData.get(Constants.END_DATE_KEY);
                            if (endDateString != null) {
                                parseToInstant(endDateString, cbPlanV2Map);
                            }
                            List<String> contentList = (List<String>) draftData.get(Constants.CONTENT_LIST);
                            cbPlanV2Map.put(Constants.CONTENT_LIST, contentList != null ? contentList : new ArrayList<>());
                            cbPlanV2Map.put(Constants.CONTENT_TYPE, draftData.get(Constants.CONTENT_TYPE));

                        } catch (Exception e) {
                            log.error("Error deserializing draftData JSON: {}", e.getMessage());
                        }
                    }
                } else {
                    cbPlanV2Map.put(Constants.NAME, (String) cbPlanMap.get(Constants.NAME));
                    cbPlanV2Map.put(Constants.END_DATE_KEY, (Instant) cbPlanMap.get(Constants.END_DATE_KEY));
                    cbPlanV2Map.put(Constants.CONTENT_LIST, (List<String>) cbPlanMap.get(Constants.CONTENT_LIST));
                    cbPlanV2Map.put(Constants.CONTENT_TYPE, (String) cbPlanMap.get(Constants.CONTENT_TYPE));
                }

                String orgId = (String) cbPlanMap.get(Constants.ORG_ID);
                String cbPlanId = String.valueOf(cbPlanMap.get(Constants.ID));
                String assignmentType = (String) cbPlanMap.get(Constants.ASSIGNMENT_TYPE);
                List<String> assignmentTypeInfo = (List<String>) cbPlanMap.get(Constants.ASSIGNMENT_TYPE_INFO);

                cbPlanV2Map.put(Constants.PLAN_ID, cbPlanId);
                cbPlanV2Map.put(Constants.ORG_SCOPE, Constants.SINGLE);
                cbPlanV2Map.put(Constants.ORG_ID_LIST, Collections.singletonList(orgId));
                cbPlanV2Map.put(Constants.CREATED_AT, (Instant) cbPlanMap.get(Constants.CREATED_AT_KEY));
                cbPlanV2Map.put(Constants.CREATED_BY, (String) cbPlanMap.get(Constants.CREATED_BY));
                cbPlanV2Map.put(Constants.DRAFT_DATA_KEY, (String) cbPlanMap.get(Constants.DRAFT_DATA));

                Boolean isApar = (Boolean) cbPlanMap.get(Constants.IS_APAR);
                cbPlanV2Map.put(Constants.IS_APAR, isApar != null ? isApar : Boolean.FALSE);

                cbPlanV2Map.put(Constants.PUBLISHED_AT, (Instant) cbPlanMap.get(Constants.PUBLISHED_AT_KEY));
                cbPlanV2Map.put(Constants.PUBLISHED_BY, (String) cbPlanMap.get(Constants.CB_PUBLISHED_BY));
                cbPlanV2Map.put(Constants.STATUS, (String) cbPlanMap.get(Constants.STATUS));
                cbPlanV2Map.put(Constants.COMMENT, (String) cbPlanMap.get(Constants.COMMENT));
                cbPlanV2Map.put(Constants.UPDATED_AT, (Instant) cbPlanMap.get(Constants.UPDATED_AT));
                cbPlanV2Map.put(Constants.UPDATED_BY, (String) cbPlanMap.get(Constants.UPDATED_BY));

                String contextData = buildContextData(cbPlanId, orgId, assignmentType, assignmentTypeInfo);
                cbPlanV2Map.put(Constants.CONTEXT_DATA, contextData);
                ApiResponse dbResponse = (ApiResponse) cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD,
                            Constants.TABLE_CB_PLAN_V2, cbPlanV2Map);
                if (Constants.SUCCESS.equalsIgnoreCase((String) dbResponse.get(Constants.RESPONSE))) {
                    cbPlanV2Map.put(Constants.ID, String.valueOf(cbPlanId));
                    Map<String, Object> sanitizedMap = sanitizeForElastic(cbPlanV2Map);
                    esUtilService.addDocument(cpPlanIndex, Constants.INDEX_TYPE, String.valueOf(cbPlanId), sanitizedMap, elasticCbPlanJsonPath);
                    migrated.incrementAndGet();
                } else {
                    skipped.incrementAndGet();
                    errors.add("planId=" + cbPlanId + ", error = " + dbResponse.get(Constants.ERROR_MESSAGE));
                    log.error("Error occurred while inserting record into CB Plan V2 table: {}", dbResponse.get(Constants.ERROR_MESSAGE));
                }
            }
        } catch (Exception e) {
            log.error("Error occurred while migrating access setting rules: {}", e.getMessage(), e);
            response.updateErrorDetails("Migration failed due to an error", HttpStatus.INTERNAL_SERVER_ERROR);
        }
        response.getResult().put("Successful", migrated.get());
        response.getResult().put("Skipped", skipped.get());
        response.getResult().put("Errors", errors);
        return response;
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

    @SuppressWarnings("unchecked")
    public boolean processAccessSettingRule(Map<String, Object> accessSettingMap) throws Exception {
        String contextId = (String) accessSettingMap.get(Constants.CONTEXT_ID);
        String contextData = (String) accessSettingMap.get(Constants.CONTEXT_DATA);

        if (!StringUtils.hasText(contextData)) {
            log.warn("Context Data is missing for access setting map: {}", contextId);
            return false;
        }

        String contextIdType = contentService.readCourseCategoryForContent(contextId);
        accessSettingMap.put(Constants.CONTEXT_ID_TYPE, contextIdType);

        Map<String, Object> contextDataMap = objectMapper.readValue(contextData,
                new TypeReference<Map<String, Object>>() {
                });
        if (MapUtils.isEmpty(contextDataMap)) {
            log.error("Failed to parse context data for contextId: {}", contextId);
            return false;
        }

        Map<String, Object> accessControl = (Map<String, Object>) contextDataMap
                .get(Constants.ACCESS_CONTROL);
        if (MapUtils.isEmpty(accessControl)) {
            log.error("Access control data is missing for contextId: {}", contextId);
            return false;
        }

        // We do have accessControlMap, let's create accessControlIdMap
        Map<String, Object> accessControlIdMap = new HashMap<>();
        boolean isSuccess = updateContextDataWithIdMap(contextId, accessControl, accessControlIdMap);

        if (!isSuccess) {
            log.error("Failed to update context data with ID map for contextId: {}", contextId);
            return false;
        }
        if (((List<Map<String, Object>>) accessControl
                .get(Constants.USER_GROUPS))
                .size() != ((List<Map<String, Object>>) accessControlIdMap.get(Constants.USER_GROUPS)).size()) {
            log.error("User groups are missing in access control id map for contextId: {}", contextId);
            return false;
        }
        contextDataMap.put(Constants.ACCESS_CONTROL_ID, accessControlIdMap);
        accessSettingMap.put(Constants.CONTEXT_DATA, objectMapper.writeValueAsString(contextDataMap));
        return true;
    }

    @SuppressWarnings("unchecked")
    protected boolean updateContextDataWithIdMap(String contextId, Map<String, Object> accessControl,
                                                 Map<String, Object> accessControlIdMap) {
        List<Map<String, Object>> userGroupsList = (List<Map<String, Object>>) accessControl
                .get(Constants.USER_GROUPS);
        List<Map<String, Object>> userGroupIdMapList = new ArrayList<>();
        accessControlIdMap.put(Constants.USER_GROUPS, userGroupIdMapList);
        accessControlIdMap.put(Constants.VERSION, 1);
        if (CollectionUtils.isEmpty(userGroupsList)) {
            log.error("User groups are missing in access control for contextId: {}", contextId);
            return true;
        }

        for (Map<String, Object> userGroup : userGroupsList) {
            String userGroupId = (String) userGroup.get(Constants.USER_GROUP_ID);
            Map<String, Object> userGroupIdMap = new HashMap<>();
            userGroupIdMap.put(Constants.USER_GROUP_ID, userGroupId);
            userGroupIdMap.put(Constants.USER_GROUP_NAME, userGroup.get(Constants.USER_GROUP_NAME));
            List<Map<String, Object>> criteriaIdMapList = new ArrayList<>();
            List<Map<String, Object>> criteriaList = (List<Map<String, Object>>) userGroup
                    .get(Constants.USER_GROUP_CRITERIA_LIST);

            for (Map<String, Object> criteria : criteriaList) {
                String criteriaKey = (String) criteria.get(Constants.CRITERIA_KEY);
                List<String> criteriaValues = (List<String>) criteria.get(Constants.CRITERIA_VALUE);

                if (CollectionUtils.isEmpty(criteriaValues)) {
                    log.error("Criteria values are missing for criteriaKey: {} in userGroupId: {}", criteriaKey,
                            userGroupId);
                    return false;
                }

                Map<String, Integer> idResultMap = idMapCacheMgr.getId(criteriaValues);
                if (MapUtils.isEmpty(idResultMap)) {
                    log.error("Failed to fetch criteria ID for criteriaKey: {} in userGroupId: {}", criteriaKey,
                            userGroupId);
                    return false;
                }
                if (criteriaValues.size() != idResultMap.size()) {
                    log.error("Criteria values size mismatch for criteriaKey: {} in userGroupId: {}", criteriaKey,
                            userGroupId);
                    return false;
                }
                Map<String, Object> criteriaIdMap = new HashMap<>();
                criteriaIdMap.put(Constants.CRITERIA_KEY, criteriaKey);
                criteriaIdMap.put(Constants.CRITERIA_VALUE, createBitSetForAttribute(idResultMap.values()));

                criteriaIdMapList.add(criteriaIdMap);
            }
            userGroupIdMap.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaIdMapList);
            userGroupIdMapList.add(userGroupIdMap);
        }
        return true;
    }

    /**
     * Creates a BitSet for the given attribute values.
     * 
     * @param attributeValues Collection of Integer values representing the
     *                        attribute.
     * @return BitSet representing the attribute values.
     */
    BitSet createBitSetForAttribute(Collection<Integer> attributeValues) {
        BitSet bitSet = new BitSet();
        for (Integer part : attributeValues) {
            try {
                bitSet.set(part);
            } catch (Exception ex) {
                log.error("Failed to set the bit map positing for value: {}", part, ex);
                throw ex;
            }
        }
        return bitSet;
    }

    private String buildContextData(String cbPlanId, String orgId, String assignmentType, List<String> assignmentTypeInfo)
            throws JsonProcessingException {

        // accessControl.userGroups[0]
        Map<String, Object> userGroup = new HashMap<>();
        UUID userGroupId = UUID.randomUUID();
        userGroup.put(Constants.USER_GROUP_ID, userGroupId.toString());
        userGroup.put(Constants.USER_GROUP_NAME, "User Group 1");

        List<Map<String, Object>> criteriaList = new ArrayList<>();

        // Always add rootOrgId
        criteriaList.add(criteriaEntry(Constants.ROOT_ORG_ID, Collections.singletonList(orgId)));

        if ("Designation".equalsIgnoreCase(assignmentType)) {
            if (!CollectionUtils.isEmpty(assignmentTypeInfo)) {
                criteriaList.add(criteriaEntry(Constants.DESIGNATION, assignmentTypeInfo));
            }
        } else if ("CustomUser".equalsIgnoreCase(assignmentType)) {
            if (!CollectionUtils.isEmpty(assignmentTypeInfo)) {
                criteriaList.add(criteriaEntry(Constants.USER, assignmentTypeInfo));
            }
        } else if ("AllUser".equalsIgnoreCase(assignmentType)) {
            // Nothing extra
        } else {
            // Unknown type: keep only org criteria; optionally, you can log/warn
        }

        userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);

        Map<String, Object> accessControl = new HashMap<>();
        accessControl.put(Constants.VERSION, 1);
        accessControl.put(Constants.USER_GROUPS, Collections.singletonList(userGroup));

        // Final contextData
        Map<String, Object> contextData = new HashMap<>();
        contextData.put(Constants.ACCESS_CONTROL, accessControl);
        

        // We do have accessControlMap, let's create accessControlIdMap
        Map<String, Object> accessControlIdMap = new HashMap<>();
        boolean isSuccess = updateContextDataWithIdMap(cbPlanId, accessControl, accessControlIdMap);

        if (!isSuccess) {
            log.error("Failed to update context data with ID map for cbPlanId: {}", cbPlanId);
            return "";
        }
        if (((List<Map<String, Object>>) accessControl
                .get(Constants.USER_GROUPS))
                .size() != ((List<Map<String, Object>>) accessControlIdMap.get(Constants.USER_GROUPS)).size()) {
            log.error("User groups are missing in access control id map for cbPlanId: {}", cbPlanId);
            return "";
        }
        contextData.put(Constants.ACCESS_CONTROL_ID, accessControlIdMap);
        return objectMapper.writeValueAsString(contextData);
    }

    private Map<String, Object> criteriaEntry(String key, List<String> values) {
        Map<String, Object> m = new HashMap<>();
        m.put(Constants.CRITERIA_KEY, key);
        m.put(Constants.CRITERIA_VALUE, values);
        return m;
    }

    private void parseToInstant(String endDateString, Map<String, Object> cbPlanV2Map) {
        Instant endDateInstant = null;
        try {
            LocalDate localDate = LocalDate.parse(endDateString);
            LocalDateTime localDateTime = localDate.atStartOfDay();  // Set time to 00:00:00
            endDateInstant = localDateTime.toInstant(ZoneOffset.UTC);  // Convert to Instant
        } catch (Exception e) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
                OffsetDateTime odt = OffsetDateTime.parse(endDateString, formatter);
                endDateInstant = odt.toInstant();
            } catch (Exception ex) {
                log.error("Error parsing endDate string: {}", endDateString, ex);
            }
        }
        if (endDateInstant != null) {
            cbPlanV2Map.put(Constants.END_DATE_KEY, endDateInstant);
        }
    }
}
