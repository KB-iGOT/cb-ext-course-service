package com.igot.cb.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.igot.common.ApiResponse;
import org.igot.common.CustomException;
import org.igot.common.cassandra.CassandraOperation;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PayloadValidation;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class AccessSettingsServiceImpl {

  private final PayloadValidation payloadValidation;
  private final CassandraOperation cassandraOperation;
  private final AccessSettingMigrationServiceImpl accessSettingMigrationService;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public AccessSettingsServiceImpl(CassandraOperation cassandraOperation, PayloadValidation payloadValidation,
      AccessSettingMigrationServiceImpl accessSettingMigrationService) {
    this.cassandraOperation = cassandraOperation;
    this.payloadValidation = payloadValidation;
    this.accessSettingMigrationService = accessSettingMigrationService;
  }

  public ApiResponse upsert(Map<String, Object> userGroupDetails) {
    log.info("AccessSettingsService::create:inside");
    ApiResponse response = ApiResponse.createDefaultResponse(Constants.ACCESS_SETTINGS_CREATE_API);
    if (userGroupDetails == null || userGroupDetails.isEmpty()) {
      log.error("User group details are null or empty");
      setFailedResponse(response, "User group details cannot be null or empty");
      return response;
    }
    String errMsg = payloadValidation.validateAccessControlPayload(userGroupDetails);
    if (StringUtils.isNotBlank(errMsg)) {
      setFailedResponse(response, errMsg);
      return response;
    }
    try {
      Map<String, Object> createPayloadWithUuid = createUserGroupIds(userGroupDetails);
      Map<String, Object> accessRuleData = new HashMap<>();
      accessRuleData.put(Constants.CONTEXT_ID, userGroupDetails.get(Constants.CONTENT_ID));
      accessRuleData.put(Constants.CONTEXT_DATA, objectMapper.writeValueAsString(createPayloadWithUuid));
      accessRuleData.put(Constants.IS_ARCHIVED, false);
      if (accessSettingMigrationService.processAccessSettingRule(accessRuleData)) {
        cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD_COURSE,
            Constants.ACCESS_SETTINGS_RULES_TABLE_V2, accessRuleData);
        response.getResult().put(Constants.MSG, Constants.CREATED_RULES);
        // Remove all other keys, and put a single object after message
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.ACCESS_CONTROL, createPayloadWithUuid.get(Constants.ACCESS_CONTROL));
        // Remove all keys except message, then put the payload as a single entry
        response.getResult().putAll(payload);
        return response;
      } else {
        log.error("Failed to process access setting rule");
        setFailedResponse(response, "Failed to process access setting rule to id-map",
            HttpStatus.INTERNAL_SERVER_ERROR);
        return response;
      }
    } catch (Exception e) {
      log.error("Error while upserting access settings", e);
      setFailedResponse(response, "Failed to create access settings: " + e.getMessage(),
          HttpStatus.INTERNAL_SERVER_ERROR);
      return response;
    }
  }

  public ApiResponse read(String contentId) {
    log.info("AccessSettingsService::read:inside");
    ApiResponse response = ApiResponse.createDefaultResponse(Constants.API_ACCESS_RULE_READ);
    try {
      Map<String, Object> propertyMap = new HashMap<>();
      propertyMap.put(Constants.CONTEXT_ID, contentId);
      List<String> fields = new ArrayList<>();
      fields.add(Constants.CONTEXT_ID);
      fields.add(Constants.CONTEXT_DATA);
      fields.add(Constants.IS_ARCHIVED);
      List<Map<String, Object>> accessSettingRule = cassandraOperation.getRecordsByProperties(
          Constants.KEYSPACE_SUNBIRD_COURSE, Constants.ACCESS_SETTINGS_RULES_TABLE_V2, propertyMap,
          fields, null);
      if (!accessSettingRule.isEmpty()) {
        Map<String, Object> record = accessSettingRule.get(0);
        Boolean status = (Boolean) record.get(Constants.IS_ARCHIVED_KEY);
        if (Boolean.FALSE.equals(status)) {
          Object contextDataObj = record.get(Constants.CONTEXT_DATA_KEY);
          String contextDataJson = (contextDataObj instanceof String strValue) ? strValue : null;
          if (StringUtils.isNotEmpty(contextDataJson)) {
            try {
              Map<String, Object> contextDataMap = objectMapper.readValue(
                  contextDataJson, new TypeReference<Map<String, Object>>() {
                  });
              if (!contextDataMap.isEmpty()) {
                contextDataMap.remove(Constants.ACCESS_CONTROL_ID);
              }
              response.setResult(contextDataMap);
              return response;
            } catch (Exception e) {
              log.error("Failed to parse CONTEXT_DATA JSON", e);
              throw new CustomException(
                  Constants.ERROR,
                  "error while processing",
                  HttpStatus.INTERNAL_SERVER_ERROR);
            }
          } else {
            setFailedResponse(response, Constants.ACCESS_SETTINGS_NOT_FOUND, HttpStatus.NOT_FOUND);
            return response;
          }
        }
        setFailedResponse(response, Constants.ACCESS_SETTINGS_NOT_FOUND, HttpStatus.NOT_FOUND);
        return response;
      }
      setFailedResponse(response, Constants.ACCESS_SETTINGS_NOT_FOUND, HttpStatus.NOT_FOUND);
      return response;
    } catch (Exception e) {
      log.error("Error while reading accessRule:", e);
      throw new CustomException(
          Constants.ERROR,
          "error while processing",
          HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  public ApiResponse delete(String contentId) {
    log.info("AccessSettingsService::delete:inside");
    ApiResponse response = ApiResponse.createDefaultResponse(Constants.API_ACCESS_RULE_READ);
    if (StringUtils.isBlank(contentId)) {
      log.error("Content ID is null or empty");
      setFailedResponse(response, "Content ID cannot be null or empty");
      return response;
    }
    try {
      Map<String, Object> accessRuleData = new HashMap<>();
      accessRuleData.put(Constants.CONTEXT_ID, contentId);
      accessRuleData.put(Constants.CONTEXT_DATA, "");
      accessRuleData.put(Constants.IS_ARCHIVED, false);
      cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD_COURSE,
          Constants.ACCESS_SETTINGS_RULES_TABLE_V2, accessRuleData);
      response.setResponseCode(HttpStatus.OK);
      response.getResult().put(Constants.MSG, "Access settings deleted successfully");
      return response;
    } catch (Exception e) {
      log.error("Error while deleting accessRule:", e);
      setFailedResponse(response, "Failed to delete access settings: " + e);
      return response;
    }
  }

  private void setFailedResponse(ApiResponse response, String errorMessage) {
    setFailedResponse(response, errorMessage, HttpStatus.BAD_REQUEST);
  }

  private void setFailedResponse(ApiResponse response, String errorMessage, HttpStatus httpStatus) {
    response.getParams().setStatus(Constants.FAILED);
    response.setResponseCode(httpStatus);
    response.getParams().setErrMsg(errorMessage);
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> createUserGroupIds(Map<String, Object> payload) {
    Object accessControlObj = payload.get(Constants.ACCESS_CONTROL);
    if (accessControlObj instanceof Map) {
      Map<String, Object> accessControl = (Map<String, Object>) accessControlObj;
      Object userGroupsObj = accessControl.get(Constants.USER_GROUPS);
      if (userGroupsObj instanceof List) {
        List<Map<String, Object>> userGroups = (List<Map<String, Object>>) userGroupsObj;
        for (Map<String, Object> userGroup : userGroups) {
          String id = (String) userGroup.get(Constants.USER_GROUP_ID);
          if (StringUtils.isBlank(id)) {
            userGroup.put(Constants.USER_GROUP_ID, UUID.randomUUID().toString());
          }
        }
      }
    }
    return payload;
  }
}