package com.igot.cb.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

@Slf4j
@Service
public class PayloadValidation {


  @SuppressWarnings("unchecked")
  public String validateAccessControlPayload(Map<String, Object> payload) {
    List<String> errList = new ArrayList<>();

    if (MapUtils.isEmpty(payload)) {
         log.error("User group details are null or empty");
         return Constants.USER_GROUPDETAILS_ERR_VALIDATION_MSG;
    }
    // Check contentId
    if (ObjectUtils.isEmpty(payload.get(Constants.CONTENT_ID)) ||
        !(payload.get(Constants.CONTENT_ID) instanceof String) ||
        StringUtils.isBlank((String) payload.get(Constants.CONTENT_ID))) {
      errList.add("contentId");
    }

    validateAccessControl(payload, errList);

    if (!errList.isEmpty()) {
      return "Missing or invalid fields: " + errList;
    }
    return "";
  }

  private void validateAccessControl(Map<String, Object> payload, List<String> errList) {
    if (!(payload.get(Constants.ACCESS_CONTROL) instanceof Map<?, ?> accessControl) || accessControl.isEmpty()) {
      errList.add(Constants.ACCESS_CONTROL);
      return;
    }
    if (!(accessControl.get(Constants.USER_GROUPS) instanceof List<?> userGroups)) {
      return;
    }
    for (int i = 0; i < userGroups.size(); i++) {
      if (userGroups.get(i) instanceof Map<?, ?> userGroup
          && userGroup.get(Constants.USER_GROUP_CRITERIA_LIST) instanceof List<?> criteriaList) {
        addInvalidCriteriaValues(criteriaList, i, errList);
      }
    }
  }

  private void addInvalidCriteriaValues(List<?> criteriaList, int groupIndex, List<String> errList) {
    for (int j = 0; j < criteriaList.size(); j++) {
      if (criteriaList.get(j) instanceof Map<?, ?> criteria
          && !isValidCriteriaValue(criteria.get(Constants.CRITERIA_VALUE))) {
        errList.add("userGroups[" + groupIndex + "].userGroupCriteriaList[" + j + "].criteriaValue");
      }
    }
  }

  /**
   * A criteria value is a non-empty list, a boolean or a string.
   */
  private boolean isValidCriteriaValue(Object criteriaValue) {
    if (criteriaValue instanceof List<?> values) {
      return !values.isEmpty();
    }
    return criteriaValue instanceof Boolean || criteriaValue instanceof String;
  }

  public String validateProviderAccessControlPayload(Map<String, Object> payload) {
    List<String> errList = new ArrayList<>();

    if (MapUtils.isEmpty(payload)) {
      log.error("Provider access settings payload is null or empty");
      return Constants.USER_GROUPDETAILS_ERR_VALIDATION_MSG;
    }
    if (!(payload.get(Constants.PARTNER_ID) instanceof String partnerId) || StringUtils.isBlank(partnerId)) {
      errList.add(Constants.PARTNER_ID);
    }
    validateAccessControl(payload, errList);

    if (!errList.isEmpty()) {
      return Constants.MSG_MISSING_OR_INVALID_FIELDS + errList;
    }
    return StringUtils.EMPTY;
  }
}
