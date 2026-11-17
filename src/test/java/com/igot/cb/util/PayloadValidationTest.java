package com.igot.cb.util;



import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;


@ExtendWith(MockitoExtension.class)
class PayloadValidationTest {

  private PayloadValidation payloadValidation;

  @BeforeEach
  void setUp() {
    payloadValidation = new PayloadValidation();
  }

  @Test
  void testValidPayload() {
    Map<String, Object> payload = new HashMap<>();
    payload.put(Constants.CONTENT_ID, "content123");

    Map<String, Object> accessControl = new HashMap<>();
    List<Map<String, Object>> userGroups = new ArrayList<>();
    Map<String, Object> userGroup = new HashMap<>();
    List<Map<String, Object>> criteriaList = new ArrayList<>();
    Map<String, Object> criteria = new HashMap<>();
    criteria.put(Constants.CRITERIA_VALUE, Collections.singletonList("val1"));
    criteriaList.add(criteria);
    userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
    userGroups.add(userGroup);
    accessControl.put(Constants.USER_GROUPS, userGroups);

    payload.put(Constants.ACCESS_CONTROL, accessControl);

    String result = payloadValidation.validateAccessControlPayload(payload);
    assertTrue(result.isEmpty(), "Result should be empty for valid payload");
  }

  @Test
  void testMissingContentId() {
    Map<String, Object> payload = new HashMap<>();
    payload.put(Constants.ACCESS_CONTROL, new HashMap<>());

    String result = payloadValidation.validateAccessControlPayload(payload);
    assertTrue(result.contains("contentId"), "Result should mention missing contentId");
  }

  @Test
  void testBlankContentId() {
    Map<String, Object> payload = new HashMap<>();
    payload.put(Constants.CONTENT_ID, "   ");
    payload.put(Constants.ACCESS_CONTROL, new HashMap<>());

    String result = payloadValidation.validateAccessControlPayload(payload);
    assertTrue(result.contains("contentId"));
  }

  @Test
  void testMissingAccessControl() {
    Map<String, Object> payload = new HashMap<>();
    payload.put(Constants.CONTENT_ID, "content123");

    String result = payloadValidation.validateAccessControlPayload(payload);
    assertTrue(result.contains(Constants.ACCESS_CONTROL));
  }

  @Test
  void testInvalidAccessControlType() {
    Map<String, Object> payload = new HashMap<>();
    payload.put(Constants.CONTENT_ID, "content123");
    payload.put(Constants.ACCESS_CONTROL, "notAMap");

    String result = payloadValidation.validateAccessControlPayload(payload);
    assertTrue(result.contains(Constants.ACCESS_CONTROL));
  }

  @Test
  void testMissingUserGroups() {
    Map<String, Object> payload = new HashMap<>();
    payload.put(Constants.CONTENT_ID, "content123");
    Map<String, Object> accessControl = new HashMap<>();
    payload.put(Constants.ACCESS_CONTROL, accessControl);

    String result = payloadValidation.validateAccessControlPayload(payload);
    // Should expect an error for missing accessControl (empty map)
    assertTrue(result.contains(Constants.ACCESS_CONTROL));
  }

  @Test
  void testEmptyCriteriaValue() {
    Map<String, Object> payload = new HashMap<>();
    payload.put(Constants.CONTENT_ID, "content123");

    Map<String, Object> accessControl = new HashMap<>();
    List<Map<String, Object>> userGroups = new ArrayList<>();
    Map<String, Object> userGroup = new HashMap<>();
    List<Map<String, Object>> criteriaList = new ArrayList<>();
    Map<String, Object> criteria = new HashMap<>();
    criteria.put(Constants.CRITERIA_VALUE, new ArrayList<>());
    criteriaList.add(criteria);
    userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, criteriaList);
    userGroups.add(userGroup);
    accessControl.put(Constants.USER_GROUPS, userGroups);

    payload.put(Constants.ACCESS_CONTROL, accessControl);

    String result = payloadValidation.validateAccessControlPayload(payload);
    assertTrue(result.contains("criteriaValue"));
  }

  @Test
  void testProviderPayloadValid() {
    Map<String, Object> payload = new HashMap<>();
    payload.put(Constants.PARTNER_ID, "partner1");
    Map<String, Object> criteria = new HashMap<>();
    criteria.put(Constants.CRITERIA_VALUE, Collections.singletonList("val1"));
    Map<String, Object> userGroup = new HashMap<>();
    userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, List.of(criteria));
    payload.put(Constants.ACCESS_CONTROL, Map.of(Constants.USER_GROUPS, List.of(userGroup)));

    assertTrue(payloadValidation.validateProviderAccessControlPayload(payload).isEmpty());
  }

  @Test
  void testProviderPayloadMissingPartnerIdAndAccessControl() {
    Map<String, Object> payload = new HashMap<>();
    payload.put(Constants.PARTNER_ID, " ");

    String result = payloadValidation.validateProviderAccessControlPayload(payload);
    assertTrue(result.contains(Constants.PARTNER_ID));
    assertTrue(result.contains(Constants.ACCESS_CONTROL));
  }

  @Test
  void testProviderPayloadEmpty() {
    assertEquals(Constants.USER_GROUPDETAILS_ERR_VALIDATION_MSG,
        payloadValidation.validateProviderAccessControlPayload(new HashMap<>()));
  }
}