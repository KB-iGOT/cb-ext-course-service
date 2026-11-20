package com.igot.cb.service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.cassandra.CassandraPropertyReader;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PayloadValidation;
import com.igot.cb.util.UserGroupUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * Provider (marketplace content partner) level access settings: user groups with licence
 * allocation, and licence notification settings. One row per partner in
 * sunbird_courses.provider_access_settings; contextdata, and notificationdata with alertthresholds,
 * are written independently so the two SPV tabs never overwrite each other. Every write also inserts the
 * saved row into provider_access_settings_audit.
 */
@Service
@Slf4j
public class ProviderAccessSettingsServiceImpl {

  private static final int MAX_THRESHOLD = 100;

  private final CassandraOperation cassandraOperation;
  private final PayloadValidation payloadValidation;
  private final AccessSettingMigrationServiceImpl accessSettingMigrationService;
  private final OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
  private final CbExtServerProperties serverProperties;
  private final UserAndOrgServiceImpl userAndOrgService;
  private final AccessTokenValidator accessTokenValidator;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Pattern emailPattern;

  public ProviderAccessSettingsServiceImpl(CassandraOperation cassandraOperation,
                                           PayloadValidation payloadValidation,
                                           AccessSettingMigrationServiceImpl accessSettingMigrationService,
                                           OutboundRequestHandlerServiceImpl outboundRequestHandlerService,
                                           CbExtServerProperties serverProperties,
                                           UserAndOrgServiceImpl userAndOrgService,
                                           AccessTokenValidator accessTokenValidator) {
    this.cassandraOperation = cassandraOperation;
    this.payloadValidation = payloadValidation;
    this.accessSettingMigrationService = accessSettingMigrationService;
    this.outboundRequestHandlerService = outboundRequestHandlerService;
    this.serverProperties = serverProperties;
    this.userAndOrgService = userAndOrgService;
    this.accessTokenValidator = accessTokenValidator;
    this.emailPattern = Pattern.compile(serverProperties.getProviderLicenceAlertEmailRegex());
  }

  @SuppressWarnings("unchecked")
  public ApiResponse upsert(Map<String, Object> request, String authToken) {
    log.info("ProviderAccessSettingsService::upsert:inside");
    ApiResponse response = ApiResponse.createDefaultResponse(Constants.API_PROVIDER_ACCESS_SETTINGS_UPSERT);
    String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
    if (StringUtils.isEmpty(userId)) {
      return response;
    }
    String errMsg = payloadValidation.validateProviderAccessControlPayload(request);
    if (StringUtils.isNotBlank(errMsg)) {
      setFailedResponse(response, errMsg, HttpStatus.BAD_REQUEST);
      return response;
    }
    String partnerId = (String) request.get(Constants.PARTNER_ID);
    Map<String, Object> accessControl = (Map<String, Object>) request.get(Constants.ACCESS_CONTROL);
    List<Map<String, Object>> userGroups = accessControl.get(Constants.USER_GROUPS) instanceof List
            ? (List<Map<String, Object>>) accessControl.get(Constants.USER_GROUPS)
            : new ArrayList<>();
    errMsg = validateUserGroups(userGroups);
    if (StringUtils.isNotBlank(errMsg)) {
      setFailedResponse(response, errMsg, HttpStatus.BAD_REQUEST);
      return response;
    }
    try {
      Map<String, Object> settingsRow = readSettingsRow(partnerId);
      List<Map<String, Object>> existingGroups = getUserGroups(parseJson((String) settingsRow.get(Constants.CONTEXT_DATA)));
      Map<String, Map<String, Object>> existingById = new HashMap<>();
      for (Map<String, Object> group : existingGroups) {
        existingById.put((String) group.get(Constants.USER_GROUP_ID), group);
      }
      errMsg = validateAgainstExistingGroups(userGroups, existingById);
      if (StringUtils.isBlank(errMsg)) {
        errMsg = validateAgainstProvider(partnerId, userGroups, existingGroups);
      }
      if (StringUtils.isNotBlank(errMsg)) {
        setFailedResponse(response, errMsg, HttpStatus.BAD_REQUEST);
        return response;
      }

      // The request is in priority order (validated); groups without an id are new.
      List<Map<String, Object>> activeGroups = new ArrayList<>();
      Set<String> incomingIds = new HashSet<>();
      for (Map<String, Object> userGroup : userGroups) {
        String userGroupId = (String) userGroup.get(Constants.USER_GROUP_ID);
        if (StringUtils.isBlank(userGroupId)) {
          userGroupId = UUID.randomUUID().toString();
        }
        incomingIds.add(userGroupId);
        Map<String, Object> group = new LinkedHashMap<>();
        group.put(Constants.USER_GROUP_ID, userGroupId);
        group.put(Constants.USER_GROUP_NUMBER, groupNumber(userGroup));
        group.put(Constants.LICENCE_ALLOCATION, ((Number) userGroup.get(Constants.LICENCE_ALLOCATION)).longValue());
        group.put(Constants.STATUS, Constants.USER_GROUP_STATUS_ACTIVE);
        group.put(Constants.USER_GROUP_CRITERIA_LIST, userGroup.get(Constants.USER_GROUP_CRITERIA_LIST));
        activeGroups.add(group);
      }

      List<Map<String, Object>> allGroups = new ArrayList<>(activeGroups);
      for (Map<String, Object> group : existingGroups) {
        if (!incomingIds.contains((String) group.get(Constants.USER_GROUP_ID))) {
          group.put(Constants.STATUS, Constants.USER_GROUP_STATUS_DELETED);
          allGroups.add(group);
        }
      }

      Map<String, Object> accessControlIdMap = new HashMap<>();
      Map<String, Object> activeAccessControl = new HashMap<>();
      activeAccessControl.put(Constants.USER_GROUPS, activeGroups);
      if (!accessSettingMigrationService.updateContextDataWithIdMap(partnerId, activeAccessControl, accessControlIdMap)
              || activeGroups.size() != ((List<?>) accessControlIdMap.get(Constants.USER_GROUPS)).size()) {
        log.error("Failed to build accessControlId for partnerId: {}", partnerId);
        setFailedResponse(response, Constants.ERR_ACCESS_CONTROL_ID_MAP_FAILED, HttpStatus.INTERNAL_SERVER_ERROR);
        return response;
      }

      Map<String, Object> newAccessControl = new LinkedHashMap<>();
      newAccessControl.put(Constants.VERSION, accessControl.getOrDefault(Constants.VERSION, 1));
      newAccessControl.put(Constants.USER_GROUPS, allGroups);
      Map<String, Object> contextData = new LinkedHashMap<>();
      contextData.put(Constants.PARTNER_ID, partnerId);
      contextData.put(Constants.ACCESS_CONTROL, newAccessControl);
      contextData.put(Constants.ACCESS_CONTROL_ID, accessControlIdMap);

      String contextDataJson = objectMapper.writeValueAsString(contextData);
      cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD_COURSE, Constants.PROVIDER_ACCESS_SETTINGS_TABLE,
              Map.of(Constants.CONTEXT_DATA, contextDataJson),
              Map.of(Constants.PARTNER_ID_COL, partnerId));
      settingsRow.put(Constants.CONTEXT_DATA, contextDataJson);
      writeAudit(partnerId, settingsRow, userId);
      response.getResult().put(Constants.MSG, Constants.PROVIDER_ACCESS_SETTINGS_SAVED);
      return response;
    } catch (Exception e) {
      log.error("Error while upserting provider access settings for partnerId: {}", partnerId, e);
      setFailedResponse(response, String.format(Constants.ERR_PROVIDER_ACCESS_SETTINGS_SAVE, e.getMessage()),
              HttpStatus.INTERNAL_SERVER_ERROR);
      return response;
    }
  }

  /**
   * The provider's active user groups in priority order, each with the licences consumed by it, and
   * the provider's licence summary: Over All Limit, consumed in total, consumed inside the groups,
   * and unaudited (consumed outside every group).
   */
  public ApiResponse read(String partnerId, String authToken) {
    log.info("ProviderAccessSettingsService::read:inside");
    ApiResponse response = ApiResponse.createDefaultResponse(Constants.API_PROVIDER_ACCESS_SETTINGS_READ);
    String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
    if (StringUtils.isEmpty(userId)) {
      return response;
    }
    try {
      Map<String, Object> partnerData = readContentPartnerData(partnerId);
      if (!isValidPartner(partnerData, partnerId, response)) {
        return response;
      }
      Map<String, Object> contextData = readContextData(partnerId);
      List<Map<String, Object>> userGroups = getUserGroups(contextData);
      Consumption consumption = readConsumption(partnerId, userGroups);
      List<Map<String, Object>> activeGroups = activeGroupsInOrder(userGroups);
      long allocatedToGroups = 0L;
      for (Map<String, Object> group : activeGroups) {
        long allocation = asLong(group.get(Constants.LICENCE_ALLOCATION));
        long consumed = consumption.of(group.get(Constants.USER_GROUP_ID));
        group.put(Constants.CONSUMED, consumed);
        group.put(Constants.REMAINING, Math.max(0L, allocation - consumed));
        allocatedToGroups += allocation;
      }
      Map<String, Object> resultAccessControl = new LinkedHashMap<>();
      resultAccessControl.put(Constants.VERSION,
              contextData.get(Constants.ACCESS_CONTROL) instanceof Map<?, ?> accessControl
                      && accessControl.get(Constants.VERSION) != null ? accessControl.get(Constants.VERSION) : 1);
      resultAccessControl.put(Constants.USER_GROUPS, activeGroups);

      response.getResult().put(Constants.PARTNER_ID, partnerId);
      response.getResult().put(Constants.MAX_USER_GROUPS, serverProperties.getProviderAccessSettingsMaxUserGroups());
      response.getResult().put(Constants.ACCESS_CONTROL, resultAccessControl);
      response.getResult().put(Constants.LICENCE,
              licenceSummary(overAllLimitOf(partnerData), consumption, allocatedToGroups));
      return response;
    } catch (Exception e) {
      log.error("Error while reading provider access settings for partnerId: {}", partnerId, e);
      setFailedResponse(response, Constants.ERR_PROVIDER_ACCESS_SETTINGS_READ, HttpStatus.INTERNAL_SERVER_ERROR);
      return response;
    }
  }

  @SuppressWarnings("unchecked")
  public ApiResponse notificationUpsert(Map<String, Object> request, String authToken) {
    log.info("ProviderAccessSettingsService::notificationUpsert:inside");
    ApiResponse response = ApiResponse.createDefaultResponse(Constants.API_PROVIDER_NOTIFICATION_UPSERT);
    String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
    if (StringUtils.isEmpty(userId)) {
      return response;
    }
    if (MapUtils.isEmpty(request)) {
      setFailedResponse(response, Constants.USER_GROUPDETAILS_ERR_VALIDATION_MSG, HttpStatus.BAD_REQUEST);
      return response;
    }
    List<String> errList = new ArrayList<>();
    if (!(request.get(Constants.PARTNER_ID) instanceof String partnerId) || StringUtils.isBlank(partnerId)) {
      errList.add(Constants.PARTNER_ID);
    }
    if (!(request.get(Constants.ALERTS_ENABLED) instanceof Boolean)) {
      errList.add(Constants.ALERTS_ENABLED);
    }
    Object recipientsObj = request.getOrDefault(Constants.RECIPIENTS, new ArrayList<>());
    if (!(recipientsObj instanceof List)) {
      errList.add(Constants.RECIPIENTS);
    }
    Object thresholdsObj = request.get(Constants.THRESHOLDS);
    if (thresholdsObj != null && !(thresholdsObj instanceof List)) {
      errList.add(Constants.THRESHOLDS);
    }
    if (!errList.isEmpty()) {
      setFailedResponse(response, Constants.MSG_MISSING_OR_INVALID_FIELDS + errList, HttpStatus.BAD_REQUEST);
      return response;
    }
    String partnerId = (String) request.get(Constants.PARTNER_ID);
    List<Object> recipients = (List<Object>) recipientsObj;
    int maxRecipients = serverProperties.getProviderLicenceAlertMaxRecipients();
    if (recipients.size() > maxRecipients) {
      setFailedResponse(response, String.format(Constants.ERR_MAX_RECIPIENTS, maxRecipients), HttpStatus.BAD_REQUEST);
      return response;
    }
    // Thresholds left out of the request keep the saved list.
    Set<Integer> newThresholds = null;
    if (thresholdsObj != null) {
      newThresholds = new TreeSet<>();
      String errMsg = validateThresholds((List<?>) thresholdsObj, newThresholds);
      if (StringUtils.isNotBlank(errMsg)) {
        setFailedResponse(response, errMsg, HttpStatus.BAD_REQUEST);
        return response;
      }
    }
    try {
      if (!isValidPartner(readContentPartnerData(partnerId), partnerId, response)) {
        return response;
      }
      Map<String, Object> settingsRow = readSettingsRow(partnerId);
      Map<String, String> existingAddedOn = new HashMap<>();
      for (Map<String, Object> recipient : getRecipients(parseJson((String) settingsRow.get(Constants.NOTIFICATION_DATA)))) {
        existingAddedOn.put(String.valueOf(recipient.get(Constants.EMAIL)).toLowerCase(),
                (String) recipient.get(Constants.ADDED_ON));
      }
      String now = Instant.now().toString();
      Set<String> seen = new HashSet<>();
      List<Map<String, Object>> newRecipients = new ArrayList<>();
      for (Object recipientObj : recipients) {
        Map<String, Object> recipient = recipientObj instanceof Map ? (Map<String, Object>) recipientObj : Map.of();
        String email = StringUtils.trimToEmpty(String.valueOf(recipient.getOrDefault(Constants.EMAIL, StringUtils.EMPTY)));
        if (!emailPattern.matcher(email).matches()) {
          setFailedResponse(response, String.format(Constants.ERR_INVALID_EMAIL, email), HttpStatus.BAD_REQUEST);
          return response;
        }
        if (!seen.add(email.toLowerCase())) {
          setFailedResponse(response, String.format(Constants.ERR_DUPLICATE_EMAIL, email), HttpStatus.BAD_REQUEST);
          return response;
        }
        Map<String, Object> newRecipient = new LinkedHashMap<>();
        String name = StringUtils.trimToNull((String) recipient.get(Constants.NAME));
        if (name != null) {
          newRecipient.put(Constants.NAME, name);
        }
        newRecipient.put(Constants.EMAIL, email);
        newRecipient.put(Constants.ADDED_ON, existingAddedOn.getOrDefault(email.toLowerCase(), now));
        newRecipients.add(newRecipient);
      }
      Map<String, Object> notificationData = new LinkedHashMap<>();
      notificationData.put(Constants.ALERTS_ENABLED, request.get(Constants.ALERTS_ENABLED));
      notificationData.put(Constants.RECIPIENTS, newRecipients);

      String notificationDataJson = objectMapper.writeValueAsString(notificationData);
      Map<String, Object> updatedColumns = new HashMap<>();
      updatedColumns.put(Constants.NOTIFICATION_DATA, notificationDataJson);
      Map<Integer, Boolean> sentFlags = thresholdsOf(settingsRow);
      if (newThresholds != null && !newThresholds.equals(sentFlags.keySet())) {
        Map<Integer, Boolean> newSentFlags = new TreeMap<>();
        newThresholds.forEach(threshold -> newSentFlags.put(threshold, Boolean.TRUE.equals(sentFlags.get(threshold))));
        updatedColumns.put(Constants.ALERT_THRESHOLDS_COL, newSentFlags);
      }
      cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD_COURSE, Constants.PROVIDER_ACCESS_SETTINGS_TABLE,
              updatedColumns, Map.of(Constants.PARTNER_ID_COL, partnerId));
      settingsRow.putAll(updatedColumns);
      writeAudit(partnerId, settingsRow, userId);
      response.getResult().put(Constants.MSG, Constants.PROVIDER_NOTIFICATION_SAVED);
      return response;
    } catch (Exception e) {
      log.error("Error while upserting licence notification settings for partnerId: {}", partnerId, e);
      setFailedResponse(response, String.format(Constants.ERR_PROVIDER_NOTIFICATION_SAVE, e.getMessage()),
              HttpStatus.INTERNAL_SERVER_ERROR);
      return response;
    }
  }

  public ApiResponse notificationRead(String partnerId, String authToken) {
    log.info("ProviderAccessSettingsService::notificationRead:inside");
    ApiResponse response = ApiResponse.createDefaultResponse(Constants.API_PROVIDER_NOTIFICATION_READ);
    String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
    if (StringUtils.isEmpty(userId)) {
      return response;
    }
    try {
      if (!isValidPartner(readContentPartnerData(partnerId), partnerId, response)) {
        return response;
      }
      Map<String, Object> settingsRow = readSettingsRow(partnerId);
      Map<String, Object> notificationData = parseJson((String) settingsRow.get(Constants.NOTIFICATION_DATA));
      Map<Integer, Boolean> sentFlags = thresholdsOf(settingsRow);
      List<Integer> thresholds = new ArrayList<>(sentFlags.keySet());
      // Whether each threshold's alert has been sent.
      Map<String, Boolean> sent = new LinkedHashMap<>();
      sentFlags.forEach((threshold, isSent) -> sent.put(String.valueOf(threshold), isSent));
      response.getResult().put(Constants.PARTNER_ID, partnerId);
      response.getResult().put(Constants.ALERTS_ENABLED,
              Boolean.TRUE.equals(notificationData.get(Constants.ALERTS_ENABLED)));
      response.getResult().put(Constants.MAX_RECIPIENTS, serverProperties.getProviderLicenceAlertMaxRecipients());
      response.getResult().put(Constants.MAX_THRESHOLDS, serverProperties.getProviderLicenceAlertMaxThresholds());
      response.getResult().put(Constants.THRESHOLDS, thresholds);
      response.getResult().put(Constants.SENT, sent);
      response.getResult().put(Constants.RECIPIENTS, getRecipients(notificationData));
      return response;
    } catch (Exception e) {
      log.error("Error while reading licence notification settings for partnerId: {}", partnerId, e);
      setFailedResponse(response, Constants.ERR_PROVIDER_NOTIFICATION_READ, HttpStatus.INTERNAL_SERVER_ERROR);
      return response;
    }
  }

  /**
   * Matches the learner against the provider's ACTIVE user groups in priority order, using the
   * id-mapped criteria (accessControlId). Licence availability is not checked here: the
   * enrolment service checks matchingGroupLicences against its counters when it charges a group.
   */
  @SuppressWarnings("unchecked")
  public ApiResponse validate(Map<String, Object> request) {
    log.info("ProviderAccessSettingsService::validate:inside");
    ApiResponse response = ApiResponse.createDefaultResponse(Constants.API_PROVIDER_ACCESS_VALIDATE);
    List<String> errList = new ArrayList<>();
    for (String field : List.of(Constants.PARTNER_ID, Constants.USER_ID)) {
      if (MapUtils.isEmpty(request) || !(request.get(field) instanceof String value) || StringUtils.isBlank(value)) {
        errList.add(field);
      }
    }
    if (!errList.isEmpty()) {
      setFailedResponse(response, Constants.MSG_MISSING_OR_INVALID_FIELDS + errList, HttpStatus.BAD_REQUEST);
      return response;
    }
    String partnerId = (String) request.get(Constants.PARTNER_ID);
    String userId = (String) request.get(Constants.USER_ID);
    try {
      Map<String, Object> contextData = readContextData(partnerId);
      List<Map<String, Object>> matchedGroups = new ArrayList<>();
      if (contextData.get(Constants.ACCESS_CONTROL_ID) instanceof Map<?, ?> accessControlId
              && accessControlId.get(Constants.USER_GROUPS) instanceof List<?> idGroups) {
        Map<String, Map<String, Object>> idGroupById = new HashMap<>();
        for (Map<String, Object> idGroup : (List<Map<String, Object>>) idGroups) {
          idGroupById.put((String) idGroup.get(Constants.USER_GROUP_ID), idGroup);
        }
        Map<String, Integer> userProfile = userAndOrgService.getUserProfile(userId);
        activeGroupsInOrder(getUserGroups(contextData)).stream()
                .filter(group -> matchesUserGroup(idGroupById.get((String) group.get(Constants.USER_GROUP_ID)), userProfile))
                .forEach(matchedGroups::add);
      }
      List<String> matchingGroups = matchedGroups.stream()
              .map(group -> (String) group.get(Constants.USER_GROUP_ID))
              .toList();
      List<Map<String, Object>> matchingGroupLicences = matchedGroups.stream()
              .map(group -> Map.of(Constants.USER_GROUP_ID, group.get(Constants.USER_GROUP_ID),
                      Constants.LICENCE_ALLOCATION, group.getOrDefault(Constants.LICENCE_ALLOCATION, 0)))
              .toList();
      boolean eligible = !matchingGroups.isEmpty();
      response.getResult().put(Constants.ELIGIBLE, eligible);
      response.getResult().put(Constants.LICENCE_STATE,
              eligible ? Constants.LICENCE_STATE_ENROL : Constants.LICENCE_STATE_NOT_AVAILABLE);
      response.getResult().put(Constants.USER_GROUP_ID, eligible ? matchingGroups.get(0) : null);
      response.getResult().put(Constants.MATCHING_GROUPS, matchingGroups);
      response.getResult().put(Constants.MATCHING_GROUP_LICENCES, matchingGroupLicences);
      return response;
    } catch (Exception e) {
      log.error("Error while validating provider access for partnerId: {}, userId: {}", partnerId, userId, e);
      setFailedResponse(response, Constants.ERR_PROVIDER_ACCESS_VALIDATE, HttpStatus.INTERNAL_SERVER_ERROR);
      return response;
    }
  }

  /**
   * Fits the groups' unused licences into the licences left under the given (new) Over All Limit.
   * Called by cb-pores-service before it saves a provider whose preference turns Yes, or whose Over
   * All Limit is lowered while the preference is Yes. The shortfall (groups' unused licences minus
   * licences left) is cut from User Group 1's unused licences first, then User Group 2, and so on;
   * an allocation never goes below the licences the group has consumed.
   */
  public ApiResponse rebalance(Map<String, Object> request) {
    log.info("ProviderAccessSettingsService::rebalance:inside");
    ApiResponse response = ApiResponse.createDefaultResponse(Constants.API_PROVIDER_ACCESS_REBALANCE);
    List<String> errList = new ArrayList<>();
    if (MapUtils.isEmpty(request) || !(request.get(Constants.PARTNER_ID) instanceof String partnerId)
            || StringUtils.isBlank(partnerId)) {
      errList.add(Constants.PARTNER_ID);
    }
    if (MapUtils.isEmpty(request) || !isWholeNumberAtLeast(request.get(Constants.OVER_ALL_LIMIT), 0)) {
      errList.add(Constants.OVER_ALL_LIMIT);
    }
    if (!errList.isEmpty()) {
      setFailedResponse(response, Constants.MSG_MISSING_OR_INVALID_FIELDS + errList, HttpStatus.BAD_REQUEST);
      return response;
    }
    String partnerId = (String) request.get(Constants.PARTNER_ID);
    long overAllLimit = ((Number) request.get(Constants.OVER_ALL_LIMIT)).longValue();
    try {
      Map<String, Object> settingsRow = readSettingsRow(partnerId);
      Map<String, Object> contextData = parseJson((String) settingsRow.get(Constants.CONTEXT_DATA));
      List<Map<String, Object>> userGroups = getUserGroups(contextData);
      Consumption consumption = readConsumption(partnerId, userGroups);
      List<Map<String, Object>> activeGroups = activeGroupsInOrder(userGroups);
      long unusedInGroups = activeGroups.stream().mapToLong(group -> unused(group, consumption)).sum();
      long licencesLeft = Math.max(0L, overAllLimit - consumption.total());
      long shortfall = Math.max(0L, unusedInGroups - licencesLeft);

      long toCut = shortfall;
      List<Map<String, Object>> resultGroups = new ArrayList<>();
      for (Map<String, Object> group : activeGroups) {
        long cut = Math.min(unused(group, consumption), toCut);
        long allocation = asLong(group.get(Constants.LICENCE_ALLOCATION)) - cut;
        toCut -= cut;
        group.put(Constants.LICENCE_ALLOCATION, allocation);
        Map<String, Object> resultGroup = new LinkedHashMap<>();
        resultGroup.put(Constants.USER_GROUP_ID, group.get(Constants.USER_GROUP_ID));
        resultGroup.put(Constants.USER_GROUP_NUMBER, groupNumber(group));
        resultGroup.put(Constants.LICENCE_ALLOCATION, allocation);
        resultGroup.put(Constants.CUT, cut);
        resultGroups.add(resultGroup);
      }
      // accessControlId holds only the groups' id-mapped conditions, so it is kept as stored.
      if (shortfall > 0) {
        String contextDataJson = objectMapper.writeValueAsString(contextData);
        cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD_COURSE, Constants.PROVIDER_ACCESS_SETTINGS_TABLE,
                Map.of(Constants.CONTEXT_DATA, contextDataJson),
                Map.of(Constants.PARTNER_ID_COL, partnerId));
        settingsRow.put(Constants.CONTEXT_DATA, contextDataJson);
        writeAudit(partnerId, settingsRow, Constants.AUDIT_UPDATED_BY_SYSTEM);
      }
      response.getResult().put(Constants.PARTNER_ID, partnerId);
      response.getResult().put(Constants.SHORTFALL, shortfall);
      response.getResult().put(Constants.USER_GROUPS, resultGroups);
      return response;
    } catch (Exception e) {
      log.error("Error while rebalancing licence allocations for partnerId: {}", partnerId, e);
      setFailedResponse(response, Constants.ERR_PROVIDER_ACCESS_REBALANCE, HttpStatus.INTERNAL_SERVER_ERROR);
      return response;
    }
  }

  /**
   * A group's licences not yet consumed: its allocation minus its consumed licences, at least 0.
   */
  private long unused(Map<String, Object> group, Consumption consumption) {
    return Math.max(0L, asLong(group.get(Constants.LICENCE_ALLOCATION)) - consumption.of(group.get(Constants.USER_GROUP_ID)));
  }

  /**
   * Every criterion of the group must contain the learner's id for that key (AND within a group).
   */
  @SuppressWarnings("unchecked")
  private boolean matchesUserGroup(Map<String, Object> idGroup, Map<String, Integer> userProfile) {
    if (MapUtils.isEmpty(idGroup) || MapUtils.isEmpty(userProfile)
            || !(idGroup.get(Constants.USER_GROUP_CRITERIA_LIST) instanceof List<?> criteriaList)
            || criteriaList.isEmpty()) {
      return false;
    }
    for (Map<String, Object> criteria : (List<Map<String, Object>>) criteriaList) {
      Integer userValue = userProfile.get(String.valueOf(criteria.get(Constants.CRITERIA_KEY)).toLowerCase());
      if (userValue == null || !(criteria.get(Constants.CRITERIA_VALUE) instanceof List<?> ids)
              || ids.stream().noneMatch(id -> id instanceof Number number && number.intValue() == userValue)) {
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  private String validateUserGroups(List<Map<String, Object>> userGroups) {
    int maxUserGroups = serverProperties.getProviderAccessSettingsMaxUserGroups();
    if (userGroups.size() > maxUserGroups) {
      return String.format(Constants.ERR_MAX_USER_GROUPS, maxUserGroups);
    }
    String errMsg = UserGroupUtils.validateUserGroupsNoEmptyCriteria(userGroups);
    if (errMsg != null) {
      return errMsg;
    }
    Set<String> allowedKeys = toLowerCaseSet(serverProperties.getProviderAccessSettingsAllowedCriteriaKeysList());
    Set<String> limitedKeys = toLowerCaseSet(serverProperties.getProviderAccessSettingsLimitedCriteriaKeysList());
    int maxCriteriaValues = serverProperties.getProviderAccessSettingsMaxCriteriaValues();
    List<String> errList = new ArrayList<>();
    for (int i = 0; i < userGroups.size(); i++) {
      Map<String, Object> userGroup = userGroups.get(i);
      addInvalidGroupFields(userGroup, i, errList);
      if (!(userGroup.get(Constants.USER_GROUP_CRITERIA_LIST) instanceof List<?> criteriaList)
              || criteriaList.isEmpty()) {
        errList.add(String.format(Constants.USER_GROUP_FIELD_PATH, i, Constants.USER_GROUP_CRITERIA_LIST));
        continue;
      }
      for (Map<String, Object> criteria : (List<Map<String, Object>>) criteriaList) {
        String criteriaKey = String.valueOf(criteria.get(Constants.CRITERIA_KEY));
        if (!allowedKeys.contains(criteriaKey.toLowerCase())) {
          return String.format(Constants.ERR_INVALID_CRITERIA_KEY, criteriaKey);
        }
        if (limitedKeys.contains(criteriaKey.toLowerCase())
                && criteria.get(Constants.CRITERIA_VALUE) instanceof List<?> values
                && values.size() > maxCriteriaValues) {
          return String.format(Constants.ERR_CRITERIA_VALUES_LIMIT, criteriaKey, maxCriteriaValues);
        }
      }
    }
    if (!errList.isEmpty()) {
      return Constants.MSG_MISSING_OR_INVALID_FIELDS + errList;
    }
    errMsg = validateGroupNumbersAndAllocations(userGroups);
    return errMsg != null ? errMsg : validateUserCriteriaAlone(userGroups);
  }

  /**
   * A group that selects learners by user can't have any other condition.
   */
  @SuppressWarnings("unchecked")
  private String validateUserCriteriaAlone(List<Map<String, Object>> userGroups) {
    for (Map<String, Object> userGroup : userGroups) {
      List<Map<String, Object>> criteriaList = (List<Map<String, Object>>) userGroup.get(Constants.USER_GROUP_CRITERIA_LIST);
      boolean hasUserCriteria = criteriaList.stream()
              .anyMatch(criteria -> Constants.USER.equalsIgnoreCase(String.valueOf(criteria.get(Constants.CRITERIA_KEY))));
      if (hasUserCriteria && criteriaList.size() > 1) {
        return String.format(Constants.ERR_USER_CRITERIA_NOT_ALONE, groupLabel(userGroup));
      }
    }
    return null;
  }

  /**
   * licenceAllocation and userGroup must be whole numbers, at least 0 and 1. A zero allocation is
   * reported by validateGroupNumbersAndAllocations with the group's label.
   */
  private void addInvalidGroupFields(Map<String, Object> userGroup, int index, List<String> errList) {
    if (!isWholeNumberAtLeast(userGroup.get(Constants.LICENCE_ALLOCATION), 0)) {
      errList.add(String.format(Constants.USER_GROUP_FIELD_PATH, index, Constants.LICENCE_ALLOCATION));
    }
    if (!isWholeNumberAtLeast(userGroup.get(Constants.USER_GROUP_NUMBER), 1)) {
      errList.add(String.format(Constants.USER_GROUP_FIELD_PATH, index, Constants.USER_GROUP_NUMBER));
    }
  }

  private boolean isWholeNumberAtLeast(Object value, long min) {
    return value instanceof Number number && number.doubleValue() >= min
            && number.doubleValue() == Math.floor(number.doubleValue());
  }

  /**
   * userGroup is the group's position, which is also its matching priority and its label
   * ("User Group n"): the groups must be numbered 1, 2, 3... in array order, so after a group is
   * deleted the groups below it move up. Every group needs at least one licence.
   */
  private String validateGroupNumbersAndAllocations(List<Map<String, Object>> userGroups) {
    for (int i = 0; i < userGroups.size(); i++) {
      Map<String, Object> userGroup = userGroups.get(i);
      if (groupNumber(userGroup) != i + 1) {
        return Constants.ERR_USER_GROUP_NUMBER_ORDER;
      }
      if (((Number) userGroup.get(Constants.LICENCE_ALLOCATION)).longValue() == 0) {
        return String.format(Constants.ERR_LICENCE_ALLOCATION_ZERO, groupLabel(userGroup));
      }
    }
    return null;
  }

  /**
   * A group sent with a userGroupId must be one of the provider's saved, not deleted, groups, with
   * its conditions unchanged. Groups keep their creation order: saved groups stay in their saved
   * order and new groups are added after them.
   */
  private String validateAgainstExistingGroups(List<Map<String, Object>> userGroups,
          Map<String, Map<String, Object>> existingById) {
    for (Map<String, Object> userGroup : userGroups) {
      String userGroupId = (String) userGroup.get(Constants.USER_GROUP_ID);
      if (StringUtils.isBlank(userGroupId)) {
        continue;
      }
      Map<String, Object> existing = existingById.get(userGroupId);
      if (existing == null || isDeleted(existing)) {
        return String.format(Constants.ERR_INVALID_USER_GROUP_ID, userGroupId);
      }
      if (!conditionsOf(userGroup).equals(conditionsOf(existing))) {
        return String.format(Constants.ERR_USER_GROUP_CONDITIONS_LOCKED, groupLabel(existing));
      }
    }
    return validateGroupOrder(userGroups, existingById);
  }

  private String validateGroupOrder(List<Map<String, Object>> userGroups,
          Map<String, Map<String, Object>> existingById) {
    int lastSavedNumber = 0;
    boolean newGroupSeen = false;
    for (Map<String, Object> userGroup : userGroups) {
      String userGroupId = (String) userGroup.get(Constants.USER_GROUP_ID);
      if (StringUtils.isBlank(userGroupId)) {
        newGroupSeen = true;
        continue;
      }
      int savedNumber = groupNumber(existingById.get(userGroupId));
      if (newGroupSeen || savedNumber <= lastSavedNumber) {
        return Constants.ERR_USER_GROUP_REORDER;
      }
      lastSavedNumber = savedNumber;
    }
    return null;
  }

  /**
   * The group's conditions as criteriaKey (lower case) to its set of values, so order and case of
   * the key don't count as a change.
   */
  private Map<String, Set<String>> conditionsOf(Map<String, Object> group) {
    Map<String, Set<String>> conditions = new HashMap<>();
    if (group.get(Constants.USER_GROUP_CRITERIA_LIST) instanceof List<?> criteriaList) {
      for (Object criteriaObj : criteriaList) {
        if (criteriaObj instanceof Map<?, ?> criteria) {
          Set<String> values = conditions.computeIfAbsent(
                  String.valueOf(criteria.get(Constants.CRITERIA_KEY)).toLowerCase(), key -> new HashSet<>());
          if (criteria.get(Constants.CRITERIA_VALUE) instanceof List<?> criteriaValues) {
            criteriaValues.forEach(value -> values.add(String.valueOf(value)));
          }
        }
      }
    }
    return conditions;
  }

  private int groupNumber(Map<String, Object> group) {
    return group.get(Constants.USER_GROUP_NUMBER) instanceof Number number ? number.intValue() : 0;
  }

  private String groupLabel(Map<String, Object> group) {
    return String.format(Constants.USER_GROUP_LABEL, groupNumber(group));
  }

  /**
   * Licences given to learners can't be revoked, so a group that has charged any licence can't be
   * left out of the request (deleted).
   */
  private String findDeletedGroupInUse(List<Map<String, Object>> userGroups,
          List<Map<String, Object>> existingGroups, Map<String, Long> consumedByGroup) {
    Set<Object> keptIds = new HashSet<>();
    userGroups.forEach(group -> keptIds.add(group.get(Constants.USER_GROUP_ID)));
    for (Map<String, Object> group : existingGroups) {
      long consumed = consumedByGroup.getOrDefault((String) group.get(Constants.USER_GROUP_ID), 0L);
      if (!isDeleted(group) && !keptIds.contains(group.get(Constants.USER_GROUP_ID)) && consumed > 0) {
        return String.format(Constants.ERR_USER_GROUP_IN_USE_DELETE, groupLabel(group), consumed);
      }
    }
    return null;
  }

  private String validateAgainstProvider(String partnerId, List<Map<String, Object>> userGroups,
          List<Map<String, Object>> existingGroups) {
    Map<String, Object> partnerData = readContentPartnerData(partnerId);
    if (MapUtils.isEmpty(partnerData)) {
      return String.format(Constants.ERR_INVALID_PARTNER_ID, partnerId);
    }
    long overAllLimit = overAllLimitOf(partnerData);
    if (overAllLimit <= 0) {
      return Constants.ERR_OVER_ALL_LIMIT_NOT_SET;
    }
    Consumption consumption = readConsumption(partnerId, existingGroups);
    String deletedInUse = findDeletedGroupInUse(userGroups, existingGroups, consumption.byGroup());
    if (deletedInUse != null) {
      return deletedInUse;
    }
    long totalAllocation = 0L;
    for (Map<String, Object> group : userGroups) {
      long allocation = ((Number) group.get(Constants.LICENCE_ALLOCATION)).longValue();
      long groupConsumed = consumption.of(group.get(Constants.USER_GROUP_ID));
      if (allocation < groupConsumed) {
        return String.format(Constants.ERR_LICENCE_ALLOCATION_BELOW_CONSUMED, groupLabel(group), groupConsumed);
      }
      totalAllocation += allocation;
    }
    // Unaudited licences (consumed outside every group) are no longer available to allocate.
    long available = overAllLimit - consumption.unaudited();
    if (totalAllocation > available) {
      return String.format(Constants.ERR_LICENCE_ALLOCATION_EXCEEDS_LIMIT, available);
    }
    return null;
  }

  /**
   * Paid licences consumed by the provider in total, and by each user group (userGroupId to count).
   */
  private record Consumption(long total, Map<String, Long> byGroup) {

    long of(Object userGroupId) {
      return byGroup.getOrDefault(userGroupId, 0L);
    }

    long inGroups() {
      return byGroup.values().stream().mapToLong(Long::longValue).sum();
    }

    /**
     * Licences consumed outside every group: before the groups existed or while the preference was No.
     */
    long unaudited() {
      return Math.max(0L, total - inGroups());
    }
  }

  /**
   * One read for the provider total (TOTAL_ENROLMENTS row) and one for the given groups
   * (USER_GROUP_ENROLMENTS rows, scope_id IN the group ids), all paid.
   */
  private Consumption readConsumption(String partnerId, List<Map<String, Object>> userGroups) {
    long total = 0L;
    for (Map<String, Object> row : readPaidCounters(partnerId, Constants.SCOPE_TYPE_TOTAL_ENROLMENTS, partnerId)) {
      total = asLong(row.get(Constants.VALUE));
    }
    Map<String, Long> byGroup = new HashMap<>();
    List<String> userGroupIds = userGroups.stream()
            .map(group -> (String) group.get(Constants.USER_GROUP_ID))
            .filter(StringUtils::isNotBlank)
            .toList();
    if (!userGroupIds.isEmpty()) {
      for (Map<String, Object> row : readPaidCounters(partnerId, Constants.SCOPE_TYPE_USER_GROUP_ENROLMENTS, userGroupIds)) {
        byGroup.put((String) row.get(Constants.SCOPE_ID_COL), asLong(row.get(Constants.VALUE)));
      }
    }
    return new Consumption(total, byGroup);
  }

  private List<Map<String, Object>> readPaidCounters(String partnerId, String scopeType, Object scopeId) {
    Map<String, Object> propertyMap = new HashMap<>();
    propertyMap.put(Constants.PARTNER_ID_COL, partnerId);
    propertyMap.put(Constants.SCOPE_TYPE_COL, scopeType);
    propertyMap.put(Constants.SCOPE_ID_COL, scopeId);
    propertyMap.put(Constants.COURSE_TYPE_COL, Constants.COURSE_TYPE_PAID);
    return cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD_COURSE,
            Constants.TABLE_USER_EXTERNAL_ENROLMENTS_COUNTER, propertyMap,
            List.of(Constants.SCOPE_ID_COL, Constants.VALUE), null);
  }

  private Map<String, Object> licenceSummary(long overAllLimit, Consumption consumption, long allocatedToGroups) {
    Map<String, Object> licence = new LinkedHashMap<>();
    licence.put(Constants.OVER_ALL_LIMIT, overAllLimit);
    licence.put(Constants.CONSUMED, consumption.total());
    licence.put(Constants.REMAINING, Math.max(0L, overAllLimit - consumption.total()));
    licence.put(Constants.ALLOCATED_TO_GROUPS, allocatedToGroups);
    licence.put(Constants.CONSUMED_IN_GROUPS, consumption.inGroups());
    licence.put(Constants.UNAUDITED, consumption.unaudited());
    licence.put(Constants.UNALLOCATED, Math.max(0L, overAllLimit - consumption.unaudited() - allocatedToGroups));
    return licence;
  }

  /**
   * Inserts the settings row as saved into the audit table. A failed insert is logged and does
   * not fail the save.
   */
  private void writeAudit(String partnerId, Map<String, Object> savedRow, String updatedBy) {
    Map<String, Object> auditRow = new LinkedHashMap<>();
    auditRow.put(Constants.PARTNER_ID_COL, partnerId);
    auditRow.put(Constants.AUDIT_ID_COL, Uuids.timeBased());
    auditRow.put(Constants.CONTEXT_DATA, savedRow.get(Constants.CONTEXT_DATA));
    auditRow.put(Constants.NOTIFICATION_DATA, savedRow.get(Constants.NOTIFICATION_DATA));
    auditRow.put(Constants.ALERT_THRESHOLDS_COL, savedRow.get(Constants.ALERT_THRESHOLDS_COL));
    auditRow.put(Constants.COL_UPDATEDBY, updatedBy);
    auditRow.put(Constants.UPDATED_ON_COL, Instant.now());
    Object result = cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD_COURSE,
            Constants.PROVIDER_ACCESS_SETTINGS_AUDIT_TABLE, auditRow);
    if (!(result instanceof ApiResponse insertResponse)
            || !Constants.SUCCESS.equals(insertResponse.get(Constants.RESPONSE))) {
      log.error("Failed to write provider access settings audit for partnerId: {}", partnerId);
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> readContentPartnerData(String partnerId) {
    String url = serverProperties.getCbPoresServiceHost() + serverProperties.getContentPartnerReadEndPoint() + partnerId;
    Map<String, Object> partnerResponse = (Map<String, Object>) outboundRequestHandlerService.fetchResult(url);
    if (MapUtils.isEmpty(partnerResponse)
            || !Constants.OK.equalsIgnoreCase(String.valueOf(partnerResponse.get(Constants.RESPONSE_CODE)))) {
      log.error("Failed to read content partner: {}", partnerId);
      return Map.of();
    }
    Map<String, Object> result = (Map<String, Object>) partnerResponse.get(Constants.RESULT);
    if (MapUtils.isEmpty(result) || !(result.get(Constants.DATA) instanceof Map)) {
      return Map.of();
    }
    return (Map<String, Object>) result.get(Constants.DATA);
  }

  private Map<String, Object> readContextData(String partnerId) throws Exception {
    return parseJson((String) readSettingsRow(partnerId).get(Constants.CONTEXT_DATA));
  }

  /**
   * The provider's settings row: column name to its saved JSON (alertthresholds: its saved map);
   * a column never saved is absent.
   */
  private Map<String, Object> readSettingsRow(String partnerId) {
    Map<String, Object> propertyMap = new HashMap<>();
    propertyMap.put(Constants.PARTNER_ID_COL, partnerId);
    List<String> columns = List.of(Constants.CONTEXT_DATA, Constants.NOTIFICATION_DATA, Constants.ALERT_THRESHOLDS_COL);
    List<Map<String, Object>> records = cassandraOperation.getRecordsByProperties(
            Constants.KEYSPACE_SUNBIRD_COURSE, Constants.PROVIDER_ACCESS_SETTINGS_TABLE, propertyMap,
            columns, null);
    Map<String, Object> settingsRow = new HashMap<>();
    if (CollectionUtils.isNotEmpty(records)) {
      for (String column : columns) {
        Object value = records.get(0).get(CassandraPropertyReader.getInstance().readProperty(column));
        if ((value instanceof String json && StringUtils.isNotBlank(json))
                || (value instanceof Map<?, ?> map && !map.isEmpty())) {
          settingsRow.put(column, value);
        }
      }
    }
    return settingsRow;
  }

  private Map<String, Object> parseJson(String json) throws Exception {
    if (json == null) {
      return new HashMap<>();
    }
    return objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {
    });
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> getUserGroups(Map<String, Object> contextData) {
    if (contextData.get(Constants.ACCESS_CONTROL) instanceof Map<?, ?> accessControl
            && accessControl.get(Constants.USER_GROUPS) instanceof List<?> userGroups) {
      return (List<Map<String, Object>>) userGroups;
    }
    return new ArrayList<>();
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> getRecipients(Map<String, Object> notificationData) {
    if (notificationData.get(Constants.RECIPIENTS) instanceof List<?> recipients) {
      return (List<Map<String, Object>>) recipients;
    }
    return new ArrayList<>();
  }

  private boolean isDeleted(Map<String, Object> userGroup) {
    return Constants.USER_GROUP_STATUS_DELETED.equals(userGroup.get(Constants.STATUS));
  }

  private List<Map<String, Object>> activeGroupsInOrder(List<Map<String, Object>> userGroups) {
    return new ArrayList<>(userGroups.stream()
            .filter(group -> !isDeleted(group))
            .sorted(Comparator.comparingInt(this::groupNumber))
            .toList());
  }

  private long overAllLimitOf(Map<String, Object> partnerData) {
    return asLong(partnerData.get(Constants.OVER_ALL_LIMIT));
  }

  private long asLong(Object value) {
    return value instanceof Number number ? number.longValue() : 0L;
  }

  /**
   * The provider's saved alert thresholds, each mapped to whether its alert has been sent
   * (ascending), or the default thresholds, none sent, until it saves its own.
   */
  private Map<Integer, Boolean> thresholdsOf(Map<String, Object> settingsRow) {
    Map<Integer, Boolean> thresholds = new TreeMap<>();
    if (settingsRow.get(Constants.ALERT_THRESHOLDS_COL) instanceof Map<?, ?> saved) {
      saved.forEach((threshold, sent) ->
              thresholds.put(((Number) threshold).intValue(), Boolean.TRUE.equals(sent)));
    } else {
      getThresholds().forEach(threshold -> thresholds.put(threshold, Boolean.FALSE));
    }
    return thresholds;
  }

  /**
   * Checks 1 to the configured maximum thresholds, each a unique whole number from 1 to 100, and
   * puts them into sortedThresholds. Returns the error message, or null when valid.
   */
  private String validateThresholds(List<?> thresholds, Set<Integer> sortedThresholds) {
    int maxThresholds = serverProperties.getProviderLicenceAlertMaxThresholds();
    if (thresholds.isEmpty() || thresholds.size() > maxThresholds) {
      return String.format(Constants.ERR_THRESHOLDS_COUNT, maxThresholds);
    }
    for (Object threshold : thresholds) {
      if (!(threshold instanceof Integer value) || value < 1 || value > MAX_THRESHOLD) {
        return Constants.ERR_INVALID_THRESHOLD;
      }
      if (!sortedThresholds.add(value)) {
        return String.format(Constants.ERR_DUPLICATE_THRESHOLD, value);
      }
    }
    return null;
  }

  private List<Integer> getThresholds() {
    return toLowerCaseSet(serverProperties.getProviderLicenceAlertThresholdsList()).stream()
            .map(Integer::parseInt)
            .sorted()
            .toList();
  }

  private Set<String> toLowerCaseSet(List<String> values) {
    Set<String> result = new HashSet<>();
    values.stream().map(String::trim).filter(StringUtils::isNotBlank).forEach(v -> result.add(v.toLowerCase()));
    return result;
  }

  private boolean isValidPartner(Map<String, Object> partnerData, String partnerId, ApiResponse response) {
    if (MapUtils.isEmpty(partnerData)) {
      setFailedResponse(response, String.format(Constants.ERR_INVALID_PARTNER_ID, partnerId), HttpStatus.BAD_REQUEST);
      return false;
    }
    return true;
  }

  private void setFailedResponse(ApiResponse response, String errorMessage, HttpStatus httpStatus) {
    response.getParams().setStatus(Constants.FAILED);
    response.setResponseCode(httpStatus);
    response.getParams().setErrMsg(errorMessage);
  }
}
