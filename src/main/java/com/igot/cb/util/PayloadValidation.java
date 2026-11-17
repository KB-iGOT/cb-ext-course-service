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

  @SuppressWarnings("unchecked")
  private void validateAccessControl(Map<String, Object> payload, List<String> errList) {
    // Check accessControl
    Object accessControlObj = payload.get(Constants.ACCESS_CONTROL);
    if (ObjectUtils.isEmpty(accessControlObj) || !(accessControlObj instanceof Map)) {
      errList.add(Constants.ACCESS_CONTROL);
    } else {
      Map<String, Object> accessControl = (Map<String, Object>) accessControlObj;
      Object userGroupsObj = accessControl.get(Constants.USER_GROUPS);
      if (userGroupsObj instanceof List) {
        List<?> userGroups = (List<?>) userGroupsObj;
        for (int i = 0; i < userGroups.size(); i++) {
          Object ugObj = userGroups.get(i);
          if (ugObj instanceof Map) {
            Map<String, Object> userGroup = (Map<String, Object>) ugObj;
            Object criteriaListObj = userGroup.get(Constants.USER_GROUP_CRITERIA_LIST);
            if (criteriaListObj instanceof List) {
              List<?> criteriaList = (List<?>) criteriaListObj;
              for (int j = 0; j < criteriaList.size(); j++) {
                Object criteriaObj = criteriaList.get(j);
                if (criteriaObj instanceof Map) {
                  Map<String, Object> criteria = (Map<String, Object>) criteriaObj;
                  Object criteriaValueObj = criteria.get(Constants.CRITERIA_VALUE);
                    boolean isError = false;
                    if (criteriaValueObj == null) {
                        isError = true;
                    } else if (criteriaValueObj instanceof List<?> valueList) {
                        if (valueList.isEmpty()) {
                            isError = true;
                        }
                    } else if (!(criteriaValueObj instanceof Boolean) && !(criteriaValueObj instanceof String)) {
                        isError = true;
                    }
                    if (isError) {
                        errList.add("userGroups[" + i + "].userGroupCriteriaList[" + j + "].criteriaValue");
                    }
                }
              }
            }
          }
        }
      }
    }
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
