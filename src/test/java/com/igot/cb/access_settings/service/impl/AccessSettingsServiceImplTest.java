package com.igot.cb.access_settings.service.impl;

import com.igot.cb.access_settings.util.Constants;
import com.igot.cb.access_settings.util.PayloadValidation;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import com.igot.cb.transactional.util.ApiResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.http.HttpStatus;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AccessSettingsServiceImplTest {


  private AccessSettingsServiceImpl service;

  @Mock
  private PayloadValidation payloadValidation;

  @Mock
  private CassandraOperation cassandraOperation;

  @BeforeEach
  void setUp() {
    service = new AccessSettingsServiceImpl(cassandraOperation, payloadValidation);
  }

  @Test
  void testUpsert_NullUserGroupDetails() {
    ApiResponse response = service.upsert(null, "token");
    assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    assertEquals(Constants.FAILED, response.getParams().getStatus());
    assertTrue(response.getParams().getErrMsg().contains("cannot be null or empty"));
  }

  @Test
  void testUpsert_EmptyUserGroupDetails() {
    ApiResponse response = service.upsert(new HashMap<>(), "token");
    assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    assertEquals(Constants.FAILED, response.getParams().getStatus());
    assertTrue(response.getParams().getErrMsg().contains("cannot be null or empty"));
  }

  @Test
  void testUpsert_ValidationError() {
    // Make details non-empty so validation is triggered
    Map<String, Object> details = new HashMap<>();
    details.put(Constants.CONTENT_ID, "cid"); // or any dummy key
    when(payloadValidation.validateAccessControlPayload(details)).thenReturn("validation error");
    ApiResponse response = service.upsert(details, "token");
    assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    assertEquals(Constants.FAILED, response.getParams().getStatus());
    assertTrue(response.getParams().getErrMsg().contains("validation error"));
  }

  @Test
  void testUpsert_Success() {
    String doId = java.util.UUID.randomUUID().toString();
    Map<String, Object> details = new HashMap<>();
    details.put(Constants.CONTENT_ID, doId);

    // Build accessControl map as per the required structure
    Map<String, Object> accessControl = new HashMap<>();
    accessControl.put("version", 1);
    List<Map<String, Object>> userGroups = new ArrayList<>();

    // User Group 1
    Map<String, Object> userGroup1 = new HashMap<>();
    userGroup1.put("userGroupId", "uuid1");
    userGroup1.put("userGroupName", "User Group 1");
    List<Map<String, Object>> criteriaList1 = new ArrayList<>();
    Map<String, Object> rule1 = new HashMap<>();
    rule1.put("ruleGroupKey", "rootOrgId");
    rule1.put("ruleGroupValue", new ArrayList<>());
    Map<String, Object> rule2 = new HashMap<>();
    rule2.put("userGroupKey", "designation");
    rule2.put("userGroupValue", java.util.Arrays.asList("Post Master", "Accountant"));
    criteriaList1.add(rule1);
    criteriaList1.add(rule2);
    userGroup1.put("userGroupCriteriaList", criteriaList1);
    userGroups.add(userGroup1);

    // User Group 2
    Map<String, Object> userGroup2 = new HashMap<>();
    userGroup2.put("userGroupId", "uuid2");
    userGroup2.put("userGroupName", "User Group 2");
    List<Map<String, Object>> criteriaList2 = new ArrayList<>();
    Map<String, Object> rule3 = new HashMap<>();
    rule3.put("userGroupKey", "rootOrgId");
    rule3.put("userGroupValue", java.util.Arrays.asList("orgId3"));
    criteriaList2.add(rule3);
    userGroup2.put("userGroupCriteriaList", criteriaList2);
    userGroups.add(userGroup2);

    // User Group 3
    Map<String, Object> userGroup3 = new HashMap<>();
    userGroup3.put("userGroupId", "uuid3");
    userGroup3.put("userGroupName", "User Group 3");
    List<Map<String, Object>> criteriaList3 = new ArrayList<>();
    Map<String, Object> rule4 = new HashMap<>();
    rule4.put("userGroupKey", "user");
    rule4.put("userGroupValue", java.util.Arrays.asList("userId1", "userId2"));
    criteriaList3.add(rule4);
    userGroup3.put("userGroupCriteriaList", criteriaList3);
    userGroups.add(userGroup3);

    accessControl.put("userGroups", userGroups);
    details.put(Constants.ACCESS_CONTROL, accessControl);

    when(payloadValidation.validateAccessControlPayload(details)).thenReturn("");
    when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap())).thenReturn(null);

    ApiResponse response = service.upsert(details, "token");
    assertEquals(HttpStatus.OK, response.getResponseCode());
    assertEquals(Constants.CREATED_RULES, response.getResult().get(Constants.MSG));
    // assertEquals(doId, response.getResult().get("contentId")); // contentId is not present in response
    assertEquals(accessControl, response.getResult().get("accessControl"));
  }

  @Test
  void testUpsert_Exception() {
    Map<String, Object> details = new HashMap<>();
    details.put(Constants.CONTENT_ID, "cid");
    details.put(Constants.ACCESS_CONTROL, new HashMap<>());
    when(payloadValidation.validateAccessControlPayload(details)).thenReturn("");
    doThrow(new RuntimeException("db error")).when(cassandraOperation).insertRecord(anyString(), anyString(), anyMap());

    ApiResponse response = service.upsert(details, "token");
    assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    assertEquals(Constants.FAILED, response.getParams().getStatus());
    assertTrue(response.getParams().getErrMsg().contains("Failed to create access settings"));
  }

  @SuppressWarnings("unchecked")
  @Test
  void testCreateUserGroupIds_AddsUuid() {
    Map<String, Object> userGroup = new HashMap<>();
    userGroup.put(Constants.USER_GROUP_ID, null);
    List<Map<String, Object>> userGroups = new ArrayList<>();
    userGroups.add(userGroup);
    Map<String, Object> accessControl = new HashMap<>();
    accessControl.put(Constants.USER_GROUPS, userGroups);
    Map<String, Object> payload = new HashMap<>();
    payload.put(Constants.ACCESS_CONTROL, accessControl);

    Map<String, Object> result = service.createUserGroupIds(payload);
    List<Map<String, Object>> resultGroups = (List<Map<String, Object>>)
            ((Map<String, Object>) result.get(Constants.ACCESS_CONTROL)).get(Constants.USER_GROUPS);
    assertNotNull(resultGroups.get(0).get(Constants.USER_GROUP_ID));
  }

  @SuppressWarnings("unchecked")
  @Test
  void testCreateUserGroupIds_WithExistingId() {
    Map<String, Object> userGroup = new HashMap<>();
    userGroup.put(Constants.USER_GROUP_ID, "existing-id");
    List<Map<String, Object>> userGroups = new ArrayList<>();
    userGroups.add(userGroup);
    Map<String, Object> accessControl = new HashMap<>();
    accessControl.put(Constants.USER_GROUPS, userGroups);
    Map<String, Object> payload = new HashMap<>();
    payload.put(Constants.ACCESS_CONTROL, accessControl);

    Map<String, Object> result = service.createUserGroupIds(payload);
    List<Map<String, Object>> resultGroups = (List<Map<String, Object>>)
            ((Map<String, Object>) result.get(Constants.ACCESS_CONTROL)).get(Constants.USER_GROUPS);
    assertEquals("existing-id", resultGroups.get(0).get(Constants.USER_GROUP_ID));
  }

  @Test
  void testCreateUserGroupIds_NoUserGroups() {
    Map<String, Object> accessControl = new HashMap<>();
    Map<String, Object> payload = new HashMap<>();
    payload.put(Constants.ACCESS_CONTROL, accessControl);

    Map<String, Object> result = service.createUserGroupIds(payload);
    assertEquals(accessControl, result.get(Constants.ACCESS_CONTROL));
  }

  @Test
  void testCreateUserGroupIds_NoAccessControl() {
    Map<String, Object> payload = new HashMap<>();
    Map<String, Object> result = service.createUserGroupIds(payload);
    assertEquals(payload, result);
  }

  @Test
  void testRead_HappyPath() {
    Map<String, Object> record = new HashMap<>();
    record.put(Constants.IS_ARCHIVED, false);
    record.put(Constants.CONTEXT_ID, "cid");
    record.put(Constants.CONTEXT_DATA, "{\"foo\":\"bar\"}");

    when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            anyString(), anyString(), anyMap(), anyList(), isNull()))
            .thenReturn(Collections.singletonList(record));

    ApiResponse response = service.read("cid");
    assertEquals(HttpStatus.OK, response.getResponseCode());
    assertEquals("bar", response.getResult().get("foo"));
  }

  @Test
  void testRead_Success() throws Exception {
    String contentId = "cid-123";
    Map<String, Object> dbRecord = new HashMap<>();
    dbRecord.put(Constants.CONTEXT_ID, contentId);
    dbRecord.put(Constants.IS_ARCHIVED, false);
    Map<String, Object> contextData = new HashMap<>();
    contextData.put("foo", "bar");
    String contextDataJson = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(contextData);
    dbRecord.put(Constants.CONTEXT_DATA, contextDataJson);
    List<Map<String, Object>> dbRecords = Collections.singletonList(dbRecord);
    when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            anyString(), anyString(), anyMap(), anyList(), isNull()
    )).thenReturn(dbRecords);

    ApiResponse response = service.read(contentId);
    assertEquals("bar", response.getResult().get("foo"));
  }

  @Test
  void testRead_Success_MultipleRecords_OnlyNonArchived() throws Exception {
    String contentId = "cid-123";
    Map<String, Object> nonArchived = new HashMap<>();
    nonArchived.put(Constants.CONTEXT_ID, contentId);
    nonArchived.put(Constants.IS_ARCHIVED, false);
    Map<String, Object> contextData = new HashMap<>();
    contextData.put("foo", "bar");
    String contextDataJson = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(contextData);
    nonArchived.put(Constants.CONTEXT_DATA, contextDataJson);
    Map<String, Object> archived = new HashMap<>();
    archived.put(Constants.CONTEXT_ID, contentId);
    archived.put(Constants.IS_ARCHIVED, true);
    archived.put(Constants.CONTEXT_DATA, contextDataJson);
    List<Map<String, Object>> dbRecords = Arrays.asList(nonArchived, archived);
    when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            anyString(), anyString(), anyMap(), anyList(), isNull()
    )).thenReturn(dbRecords);

    ApiResponse response = service.read(contentId);
    assertEquals("bar", response.getResult().get("foo"));
  }

  @Test
  void testRead_Failure_NoRecordsFound() {
    String contentId = "cid-404";
    when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            anyString(), anyString(), anyMap(), anyList(), isNull()
    )).thenReturn(Collections.emptyList());

    ApiResponse response = service.read(contentId);
    assertEquals(HttpStatus.NOT_FOUND, response.getResponseCode());
    assertEquals(Constants.FAILED, response.getParams().getStatus());
    assertTrue(response.getParams().getErrMsg().contains("No access settings found"));
  }

  @Test
  void testRead_Failure_ContextDataJsonParseError() {
    String contentId = "cid-err";
    Map<String, Object> dbRecord = new HashMap<>();
    dbRecord.put(Constants.CONTEXT_ID, contentId);
    dbRecord.put(Constants.IS_ARCHIVED, false);
    dbRecord.put(Constants.CONTEXT_DATA, "not-a-json");
    List<Map<String, Object>> dbRecords = Collections.singletonList(dbRecord);
    when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            anyString(), anyString(), anyMap(), anyList(), isNull()
    )).thenReturn(dbRecords);

    Exception ex = assertThrows(com.igot.cb.transactional.util.exceptions.CustomException.class, () -> service.read(contentId));
    assertTrue(ex.getMessage().contains("error while processing"));
  }

  @Test
  void testRead_Failure_ContextDataIsNullOrEmpty() {
    String contentId = "cid-empty";
    Map<String, Object> dbRecordNull = new HashMap<>();
    dbRecordNull.put(Constants.CONTEXT_ID, contentId);
    dbRecordNull.put(Constants.IS_ARCHIVED, false);
    dbRecordNull.put(Constants.CONTEXT_DATA, null);

    Map<String, Object> dbRecordEmpty = new HashMap<>();
    dbRecordEmpty.put(Constants.CONTEXT_ID, contentId);
    dbRecordEmpty.put(Constants.IS_ARCHIVED, false);
    dbRecordEmpty.put(Constants.CONTEXT_DATA, "");

    // Test with null CONTEXT_DATA
    when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            anyString(), anyString(), anyMap(), anyList(), isNull()
    )).thenReturn(Collections.singletonList(dbRecordNull));
    ApiResponse responseNull = service.read(contentId);
    assertEquals(HttpStatus.NOT_FOUND, responseNull.getResponseCode());
    assertEquals(Constants.FAILED, responseNull.getParams().getStatus());

    // Test with empty CONTEXT_DATA
    when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            anyString(), anyString(), anyMap(), anyList(), isNull()
    )).thenReturn(Collections.singletonList(dbRecordEmpty));
    ApiResponse responseEmpty = service.read(contentId);
    assertEquals(HttpStatus.NOT_FOUND, responseEmpty.getResponseCode());
    assertEquals(Constants.FAILED, responseEmpty.getParams().getStatus());
  }

  @Test
  void testRead_Failure_CassandraThrowsException() {
    String contentId = "cid-exc";
    when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            anyString(), anyString(), anyMap(), anyList(), isNull()
    )).thenThrow(new RuntimeException("db error"));

    Exception ex = assertThrows(com.igot.cb.transactional.util.exceptions.CustomException.class, () -> service.read(contentId));
    assertTrue(ex.getMessage().contains("error while processing"));
  }

  @Test
  void testDelete_NullOrEmptyContentId() {
    ApiResponse responseNull = service.delete(null);
    assertEquals(HttpStatus.BAD_REQUEST, responseNull.getResponseCode());
    assertEquals("Content ID cannot be null or empty", responseNull.getParams().getErrMsg());

    ApiResponse responseEmpty = service.delete("");
    assertEquals(HttpStatus.BAD_REQUEST, responseEmpty.getResponseCode());
    assertEquals("Content ID cannot be null or empty", responseEmpty.getParams().getErrMsg());
  }

  @Test
  void testDelete_Success() {
    String contentId = "cid-123";
    Map<String, Object> expectedMap = new HashMap<>();
    expectedMap.put(Constants.CONTEXT_ID, contentId);
    expectedMap.put(Constants.CONTEXT_DATA, "");
    expectedMap.put(Constants.IS_ARCHIVED, false);

    when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap())).thenReturn(null);

    ApiResponse response = service.delete(contentId);

    assertEquals(HttpStatus.OK, response.getResponseCode());
    assertEquals("Access settings deleted successfully", response.getResult().get(Constants.MSG));

  }

  @Test
  void testDelete_Exception() {
    String contentId = "cid-123";
    doThrow(new RuntimeException("db error")).when(cassandraOperation).insertRecord(anyString(), anyString(), anyMap());

    ApiResponse response = service.delete(contentId);

    assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    assertTrue(response.getParams().getErrMsg().contains("Failed to delete access settings"));
  }
}
