package com.igot.cb.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PayloadValidation;
import com.igot.cb.util.CbExtServerProperties;

@ExtendWith(MockitoExtension.class)
class ProviderAccessSettingsServiceImplTest {

  private static final String PARTNER_ID = "partner-1";

  @Mock
  private CassandraOperation cassandraOperation;
  @Mock
  private AccessSettingMigrationServiceImpl accessSettingMigrationService;
  @Mock
  private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
  @Mock
  private RedisCacheMgr redisCacheMgr;
  @Mock
  private UserAndOrgServiceImpl userAndOrgService;
  @Mock
  private AccessTokenValidator accessTokenValidator;

  private ProviderAccessSettingsServiceImpl service;
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void setUp() {
    service = new ProviderAccessSettingsServiceImpl(cassandraOperation, new PayloadValidation(),
        accessSettingMigrationService, outboundRequestHandlerService, redisCacheMgr, serverProperties(), userAndOrgService,
        accessTokenValidator);
    lenient().when(accessTokenValidator.fetchUserIdFromAccessToken(eq("token"), any())).thenReturn("user-1");
    lenient().when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD_COURSE),
        eq(Constants.TABLE_USER_EXTERNAL_ENROLMENTS_COUNTER), anyMap(), anyList(), isNull())).thenReturn(List.of());
  }

  private CbExtServerProperties serverProperties() {
    CbExtServerProperties serverProperties = new CbExtServerProperties();
    serverProperties.setCbPoresServiceHost("http://cb-pores-service:7001");
    serverProperties.setContentPartnerReadEndPoint("/contentpartner/v1/read/");
    serverProperties.setProviderAccessSettingsMaxUserGroups(5);
    serverProperties.setProviderAccessSettingsMaxCriteriaValues(500);
    serverProperties.setProviderAccessSettingsLimitedCriteriaKeys("user,rootorgid,designation");
    serverProperties.setProviderAccessSettingsAllowedCriteriaKeys(
        "user,rootorgid,group,designation,service,batch,cadre,profilestatus");
    serverProperties.setProviderLicenceAlertThresholds("50,75,90,100");
    serverProperties.setProviderLicenceAlertMaxRecipients(100);
    serverProperties.setProviderLicenceAlertMaxThresholds(5);
    serverProperties.setProviderLicenceAlertEmailRegex("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$");
    return serverProperties;
  }

  // ---------- upsert ----------

  @Test
  void upsert_createsNewGroupsAndReturnsOnlyMessage() throws Exception {
    mockPartner(40000);
    mockRow(Constants.CONTEXT_DATA, null);
    mockIdMapSuccess();

    ApiResponse response = service.upsert(request(group(null, 15000, "designation", List.of("auditor"))), "token");

    assertEquals(HttpStatus.OK, response.getResponseCode());
    assertEquals(Map.of(Constants.MSG, Constants.PROVIDER_ACCESS_SETTINGS_SAVED), response.getResult());
    Map<String, Object> saved = savedJson(Constants.CONTEXT_DATA);
    List<Map<String, Object>> groups = groupsOf(saved);
    assertEquals(1, groups.size());
    assertFalse(((String) groups.get(0).get(Constants.USER_GROUP_ID)).isBlank());
    assertEquals(1, groups.get(0).get(Constants.USER_GROUP_NUMBER));
    assertEquals(Constants.USER_GROUP_STATUS_ACTIVE, groups.get(0).get(Constants.STATUS));
    assertTrue(saved.containsKey(Constants.ACCESS_CONTROL_ID));
  }

  @Test
  void upsert_marksGroupsLeftOutAsDeleted() throws Exception {
    mockPartner(40000);
    mockExistingGroups(
        group("g1", 100, "group", List.of("Group A")),
        group("g2", 200, "group", List.of("Group B")));
    mockIdMapSuccess();

    ApiResponse response = service.upsert(request(group("g2", 300, "group", List.of("Group B"))), "token");

    assertEquals(HttpStatus.OK, response.getResponseCode());
    List<Map<String, Object>> groups = groupsOf(savedJson(Constants.CONTEXT_DATA));
    assertEquals(2, groups.size());
    assertEquals("g2", groups.get(0).get(Constants.USER_GROUP_ID));
    assertEquals(300, groups.get(0).get(Constants.LICENCE_ALLOCATION));
    assertEquals(1, groups.get(0).get(Constants.USER_GROUP_NUMBER));
    assertEquals("g1", groups.get(1).get(Constants.USER_GROUP_ID));
    assertEquals(Constants.USER_GROUP_STATUS_DELETED, groups.get(1).get(Constants.STATUS));
  }

  @Test
  void upsert_rejectsUnknownGroupId() throws Exception {
    mockRow(Constants.CONTEXT_DATA, null);

    ApiResponse response = service.upsert(request(group("missing", 10, "group", List.of("A"))), "token");

    assertBadRequest(response, "Invalid userGroupId: missing");
  }

  @Test
  void upsert_rejectsMoreThanMaxGroups() {
    List<Map<String, Object>> groups = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      groups.add(group(null, 1, "group", List.of("A")));
    }
    ApiResponse response = service.upsert(request(groups.toArray(new Map[0])), "token");

    assertBadRequest(response, "Maximum 5 user groups allowed");
  }

  @Test
  void upsert_rejectsInvalidCriteriaKey() {
    ApiResponse response = service.upsert(request(group(null, 1, "apar", List.of("A"))), "token");

    assertBadRequest(response, "Invalid criteriaKey: apar");
  }

  @Test
  void upsert_rejectsTooManyValuesForLimitedKey() {
    List<String> values = new ArrayList<>();
    for (int i = 0; i < 501; i++) {
      values.add("u" + i);
    }
    ApiResponse response = service.upsert(request(group(null, 1, "user", values)), "token");

    assertBadRequest(response, "user allows at most 500 values");
  }

  @Test
  void upsert_rejectsNegativeAllocation() {
    ApiResponse response = service.upsert(request(group(null, -1, "group", List.of("A"))), "token");

    assertBadRequest(response, Constants.MSG_MISSING_OR_INVALID_FIELDS + "[userGroups[0].licenceAllocation]");
  }

  @Test
  void upsert_rejectsWhenOverAllLimitNotSet() {
    mockPartner(0);

    ApiResponse response = service.upsert(request(group(null, 1, "group", List.of("A"))), "token");

    assertBadRequest(response, "Over All Limit not set for provider");
  }

  @Test
  void upsert_rejectsAllocationAboveOverAllLimit() {
    mockPartner(100);

    ApiResponse response = service.upsert(request(
        group(null, 60, "group", List.of("A")),
        group(null, 50, "group", List.of("B"))), "token");

    assertBadRequest(response, "Total licence allocation exceeds available licences (100)");
  }

  @Test
  void upsert_rejectsAllocationBelowLicencesConsumedByTheGroup() throws Exception {
    mockPartner(100);
    mockCounters(30L, Map.of("g1", 5L));
    mockExistingGroups(group("g1", 10, "group", List.of("A")));

    ApiResponse response = service.upsert(request(group("g1", 3, "group", List.of("A"))), "token");

    assertBadRequest(response, "Licence allocation for User Group 1 cannot be lower than the licences already consumed (5)");
  }

  @Test
  void upsert_rejectsAllocationAboveLicencesLeftAfterConsumptionOutsideGroups() throws Exception {
    mockPartner(100);
    mockExistingGroups(group("g1", 20, "group", List.of("A")));
    // 30 consumed in total, 10 of them by g1: 20 were consumed outside the groups, so 80 are left to allocate.
    mockCounters(30L, Map.of("g1", 10L));

    ApiResponse response = service.upsert(request(
        group("g1", 50, "group", List.of("A")),
        group(null, 31, "group", List.of("B"))), "token");

    assertBadRequest(response, "Total licence allocation exceeds available licences (80)");
    verify(cassandraOperation).getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD_COURSE),
        eq(Constants.TABLE_USER_EXTERNAL_ENROLMENTS_COUNTER),
        eq(Map.of(Constants.PARTNER_ID_COL, PARTNER_ID, Constants.SCOPE_TYPE_COL, Constants.SCOPE_TYPE_TOTAL_ENROLMENTS,
            Constants.SCOPE_ID_COL, PARTNER_ID, Constants.COURSE_TYPE_COL, Constants.COURSE_TYPE_PAID)),
        eq(List.of(Constants.SCOPE_ID_COL, Constants.VALUE)), isNull());
    verify(cassandraOperation).getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD_COURSE),
        eq(Constants.TABLE_USER_EXTERNAL_ENROLMENTS_COUNTER),
        eq(Map.of(Constants.PARTNER_ID_COL, PARTNER_ID, Constants.SCOPE_TYPE_COL, Constants.SCOPE_TYPE_USER_GROUP_ENROLMENTS,
            Constants.SCOPE_ID_COL, List.of("g1"), Constants.COURSE_TYPE_COL, Constants.COURSE_TYPE_PAID)),
        eq(List.of(Constants.SCOPE_ID_COL, Constants.VALUE)), isNull());
  }

  @Test
  void upsert_allowsAllocationWithinLicencesLeftAfterConsumption() throws Exception {
    mockPartner(100);
    mockCounters(30L, Map.of("g1", 10L));
    mockExistingGroups(group("g1", 20, "group", List.of("A")));
    mockIdMapSuccess();

    ApiResponse response = service.upsert(request(
        group("g1", 50, "group", List.of("A")),
        group(null, 30, "group", List.of("B"))), "token");

    assertEquals(HttpStatus.OK, response.getResponseCode());
  }

  @Test
  void upsert_rejectsUserConditionCombinedWithAnotherCondition() {
    Map<String, Object> userGroup = group(null, 1, "User", List.of("user-1"));
    userGroup.put(Constants.USER_GROUP_CRITERIA_LIST, List.of(
        Map.of(Constants.CRITERIA_KEY, "User", Constants.CRITERIA_VALUE, List.of("user-1")),
        Map.of(Constants.CRITERIA_KEY, "designation", Constants.CRITERIA_VALUE, List.of("auditor"))));

    ApiResponse response = service.upsert(request(userGroup), "token");

    assertBadRequest(response, "The user condition cannot be combined with other conditions: User Group 1");
  }

  @Test
  void upsert_allowsUserConditionAlone() throws Exception {
    mockPartner(100);
    mockRow(Constants.CONTEXT_DATA, null);
    mockIdMapSuccess();

    ApiResponse response = service.upsert(request(group(null, 1, "user", List.of("user-1", "user-2"))), "token");

    assertEquals(HttpStatus.OK, response.getResponseCode());
  }

  @Test
  void upsert_rejectsZeroAllocation() {
    ApiResponse response = service.upsert(request(group(null, 0, "group", List.of("A"))), "token");

    assertBadRequest(response, "Licence allocation for User Group 1 must be at least 1");
  }

  @Test
  void upsert_rejectsMissingOrInvalidUserGroupNumber() {
    ApiResponse response = service.upsert(request(
        number(group(null, 1, "group", List.of("A")), 0),
        number(group(null, 1, "group", List.of("B")), 2)), "token");

    assertBadRequest(response, Constants.MSG_MISSING_OR_INVALID_FIELDS + "[userGroups[0].userGroup]");
  }

  @Test
  void upsert_rejectsGroupsNotNumberedByPosition() {
    ApiResponse outOfOrder = service.upsert(request(
        number(group(null, 1, "group", List.of("A")), 2),
        number(group(null, 1, "group", List.of("B")), 1)), "token");
    ApiResponse repeated = service.upsert(request(
        number(group(null, 1, "group", List.of("A")), 1),
        number(group(null, 1, "group", List.of("B")), 1)), "token");
    ApiResponse gap = service.upsert(request(
        number(group(null, 1, "group", List.of("A")), 1),
        number(group(null, 1, "group", List.of("B")), 3)), "token");

    assertBadRequest(outOfOrder, Constants.ERR_USER_GROUP_NUMBER_ORDER);
    assertBadRequest(repeated, Constants.ERR_USER_GROUP_NUMBER_ORDER);
    assertBadRequest(gap, Constants.ERR_USER_GROUP_NUMBER_ORDER);
  }

  @Test
  void upsert_addsNewGroupAfterSavedGroupsAndWritesAudit() throws Exception {
    mockPartner(100);
    mockExistingGroups(
        group("g1", 10, "group", List.of("A")),
        group("g2", 10, "group", List.of("B")));
    mockIdMapSuccess();
    ApiResponse inserted = new ApiResponse();
    inserted.put(Constants.RESPONSE, Constants.SUCCESS);
    when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap())).thenReturn(inserted);

    ApiResponse response = service.upsert(request(
        group("g1", 10, "group", List.of("A")),
        group("g2", 10, "group", List.of("B")),
        group(null, 10, "group", List.of("C"))), "token");

    assertEquals(HttpStatus.OK, response.getResponseCode());
    List<Map<String, Object>> groups = groupsOf(savedJson(Constants.CONTEXT_DATA));
    assertEquals(List.of(1, 2, 3), groups.stream().map(g -> g.get(Constants.USER_GROUP_NUMBER)).toList());
    assertFalse(groups.stream().anyMatch(g -> g.containsKey(Constants.USER_GROUP_NAME) || g.containsKey(Constants.PRIORITY)));
    assertEquals(List.of("g1", "g2"), groups.subList(0, 2).stream().map(g -> g.get(Constants.USER_GROUP_ID)).toList());
    Map<String, Object> audit = auditRow();
    assertEquals("user-1", audit.get(Constants.COL_UPDATEDBY));
    assertEquals(3, groupsOf(mapper.readValue((String) audit.get(Constants.CONTEXT_DATA),
        new TypeReference<Map<String, Object>>() {
        })).size());
  }

  @Test
  void upsert_rejectsDeletedGroupId() throws Exception {
    Map<String, Object> deleted = group("g1", 10, "group", List.of("A"));
    deleted.put(Constants.STATUS, Constants.USER_GROUP_STATUS_DELETED);
    mockExistingGroups(deleted);

    ApiResponse response = service.upsert(request(group("g1", 10, "group", List.of("A"))), "token");

    assertBadRequest(response, "Invalid userGroupId: g1");
  }

  @Test
  void upsert_rejectsReorderingSavedGroups() throws Exception {
    mockExistingGroups(
        group("g1", 10, "group", List.of("A")),
        group("g2", 10, "group", List.of("B")));

    ApiResponse response = service.upsert(request(
        group("g2", 10, "group", List.of("B")),
        group("g1", 10, "group", List.of("A"))), "token");

    assertBadRequest(response, Constants.ERR_USER_GROUP_REORDER);
  }

  @Test
  void upsert_rejectsNewGroupBeforeSavedGroups() throws Exception {
    mockExistingGroups(group("g1", 10, "group", List.of("A")));

    ApiResponse response = service.upsert(request(
        group(null, 10, "group", List.of("C")),
        group("g1", 10, "group", List.of("A"))), "token");

    assertBadRequest(response, Constants.ERR_USER_GROUP_REORDER);
  }

  @Test
  void upsert_rejectsChangedConditionsOfSavedGroup() throws Exception {
    mockExistingGroups(group("g1", 10, "group", List.of("A", "B")));

    ApiResponse changedValues = service.upsert(request(group("g1", 10, "group", List.of("A"))), "token");
    ApiResponse changedKey = service.upsert(request(group("g1", 10, "designation", List.of("A", "B"))), "token");

    assertBadRequest(changedValues, "Conditions of User Group 1 cannot be changed");
    assertBadRequest(changedKey, "Conditions of User Group 1 cannot be changed");
  }

  @Test
  void upsert_allowsSameConditionsInAnotherOrderAndKeyCase() throws Exception {
    mockPartner(100);
    mockExistingGroups(group("g1", 10, "group", List.of("A", "B")));
    mockIdMapSuccess();

    ApiResponse response = service.upsert(request(group("g1", 20, "Group", List.of("B", "A"))), "token");

    assertEquals(HttpStatus.OK, response.getResponseCode());
  }

  @Test
  void upsert_savesWhenAuditInsertFails() throws Exception {
    mockPartner(100);
    mockRow(Constants.CONTEXT_DATA, null);
    mockIdMapSuccess();
    ApiResponse failed = new ApiResponse();
    failed.put(Constants.RESPONSE, Constants.FAILED);
    when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap())).thenReturn(failed);

    ApiResponse response = service.upsert(request(group(null, 1, "group", List.of("A"))), "token");

    assertEquals(HttpStatus.OK, response.getResponseCode());
  }

  @Test
  void upsert_rejectsDeletingGroupWithLicencesInUse() throws Exception {
    mockPartner(100);
    mockCounters(5L, Map.of("g1", 1L));
    mockExistingGroups(
        group("g1", 10, "group", List.of("A")),
        group("g2", 10, "group", List.of("B")));

    ApiResponse response = service.upsert(request(group("g2", 10, "group", List.of("B"))), "token");

    assertBadRequest(response, "User Group 1 cannot be deleted because 1 of its licences are in use");
    verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap());
  }

  @Test
  void upsert_renumbersGroupsWhenAnUnusedGroupIsDeleted() throws Exception {
    mockPartner(100);
    mockCounters(0L, Map.of("g2", 0L));
    mockExistingGroups(
        group("g1", 10, "group", List.of("A")),
        group("g2", 10, "group", List.of("B")));
    mockIdMapSuccess();

    ApiResponse response = service.upsert(request(
        group("g2", 10, "group", List.of("B")),
        group(null, 10, "group", List.of("C"))), "token");

    assertEquals(HttpStatus.OK, response.getResponseCode());
    List<Map<String, Object>> groups = groupsOf(savedJson(Constants.CONTEXT_DATA));
    assertEquals("g2", groups.get(0).get(Constants.USER_GROUP_ID));
    assertEquals(List.of(1, 2, 1), groups.stream().map(g -> g.get(Constants.USER_GROUP_NUMBER)).toList());
    assertEquals(Constants.USER_GROUP_STATUS_DELETED, groups.get(2).get(Constants.STATUS));
  }

  @Test
  void upsert_rejectsUnknownPartner() {
    when(outboundRequestHandlerService.fetchResult(anyString()))
        .thenReturn(Map.of(Constants.RESPONSE_CODE, "BAD_REQUEST"));

    ApiResponse response = service.upsert(request(group(null, 1, "group", List.of("A"))), "token");

    assertBadRequest(response, "Invalid partnerId: " + PARTNER_ID);
  }

  @Test
  void upsert_failsWhenIdMappingFails() throws Exception {
    mockPartner(40000);
    mockRow(Constants.CONTEXT_DATA, null);
    when(accessSettingMigrationService.updateContextDataWithIdMap(anyString(), anyMap(), anyMap())).thenReturn(false);

    ApiResponse response = service.upsert(request(group(null, 1, "group", List.of("A"))), "token");

    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap());
  }

  @Test
  void upsert_rejectsInvalidPayload() {
    ApiResponse response = service.upsert(Map.of(Constants.ACCESS_CONTROL, Map.of()), "token");

    assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    assertTrue(response.getParams().getErrMsg().contains(Constants.PARTNER_ID));
  }

  // ---------- read ----------

  @Test
  void read_returnsActiveGroupsInPriorityOrderWithoutAccessControlId() throws Exception {
    Map<String, Object> g1 = group("g1", 100, "group", List.of("A"));
    g1.put(Constants.USER_GROUP_NUMBER, 2);
    Map<String, Object> g2 = group("g2", 100, "group", List.of("B"));
    g2.put(Constants.USER_GROUP_NUMBER, 1);
    Map<String, Object> g3 = group("g3", 100, "group", List.of("C"));
    g3.put(Constants.STATUS, Constants.USER_GROUP_STATUS_DELETED);
    Map<String, Object> contextData = new HashMap<>();
    contextData.put(Constants.ACCESS_CONTROL, Map.of(Constants.VERSION, 1, Constants.USER_GROUPS, List.of(g1, g2, g3)));
    contextData.put(Constants.ACCESS_CONTROL_ID, Map.of());
    mockPartner(1000);
    mockRow(Constants.CONTEXT_DATA, mapper.writeValueAsString(contextData));
    // 130 consumed in total: 100 by g2, 10 by g1 and 20 outside the groups (unaudited).
    mockCounters(130L, Map.of("g2", 100L, "g1", 10L));

    ApiResponse response = service.read(PARTNER_ID, "token");

    assertEquals(HttpStatus.OK, response.getResponseCode());
    assertEquals(5, response.getResult().get(Constants.MAX_USER_GROUPS));
    assertFalse(response.getResult().containsKey(Constants.ACCESS_CONTROL_ID));
    List<Map<String, Object>> groups = groupsOf(response.getResult());
    assertEquals(List.of("g2", "g1"), groups.stream().map(g -> g.get(Constants.USER_GROUP_ID)).toList());
    assertEquals(100L, groups.get(0).get(Constants.CONSUMED));
    assertEquals(0L, groups.get(0).get(Constants.REMAINING));
    assertEquals(10L, groups.get(1).get(Constants.CONSUMED));
    assertEquals(90L, groups.get(1).get(Constants.REMAINING));
    Map<String, Object> licence = new HashMap<>();
    licence.put(Constants.OVER_ALL_LIMIT, 1000L);
    licence.put(Constants.CONSUMED, 130L);
    licence.put(Constants.REMAINING, 870L);
    licence.put(Constants.ALLOCATED_TO_GROUPS, 200L);
    licence.put(Constants.CONSUMED_IN_GROUPS, 110L);
    licence.put(Constants.UNAUDITED, 20L);
    licence.put(Constants.UNALLOCATED, 780L);
    assertEquals(licence, response.getResult().get(Constants.LICENCE));
    verify(outboundRequestHandlerService, times(1)).fetchResult(anyString());
  }

  @Test
  void read_returnsLicenceSummaryWithoutGroupsWhenNoRow() {
    mockPartner(100);
    mockRow(Constants.CONTEXT_DATA, null);
    mockCounters(130L, Map.of());

    ApiResponse response = service.read(PARTNER_ID, "token");

    assertEquals(HttpStatus.OK, response.getResponseCode());
    assertEquals(Map.of(Constants.VERSION, 1, Constants.USER_GROUPS, List.of()),
        response.getResult().get(Constants.ACCESS_CONTROL));
    Map<String, Object> licence = (Map<String, Object>) response.getResult().get(Constants.LICENCE);
    assertEquals(0L, licence.get(Constants.REMAINING));
    assertEquals(130L, licence.get(Constants.UNAUDITED));
    assertEquals(0L, licence.get(Constants.UNALLOCATED));
  }

  @Test
  void read_defaultsVersionAndOverAllLimitWhenNotSet() throws Exception {
    when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(Map.of(
        Constants.RESPONSE_CODE, Constants.OK, Constants.RESULT, Map.of(Constants.DATA, Map.of(Constants.NAME, "P"))));
    mockExistingGroups(group("g1", 10, "group", List.of("A")));

    ApiResponse response = service.read(PARTNER_ID, "token");

    assertEquals(1, ((Map<String, Object>) response.getResult().get(Constants.ACCESS_CONTROL)).get(Constants.VERSION));
    assertEquals(0L, ((Map<String, Object>) response.getResult().get(Constants.LICENCE)).get(Constants.OVER_ALL_LIMIT));
  }

  @Test
  void read_returnsServerErrorOnFailure() {
    mockPartner(100);
    when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD_COURSE),
        eq(Constants.PROVIDER_ACCESS_SETTINGS_TABLE), anyMap(), anyList(), isNull()))
        .thenReturn(List.of(Map.of(Constants.CONTEXT_DATA_KEY, "not json")));

    ApiResponse response = service.read(PARTNER_ID, "token");

    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
  }

  // ---------- rebalance ----------

  @Test
  void rebalance_cutsUnusedLicencesFromGroupOneFirst() throws Exception {
    Map<String, Object> deleted = group("g0", 5, "group", List.of("Z"));
    deleted.put(Constants.STATUS, Constants.USER_GROUP_STATUS_DELETED);
    deleted.put(Constants.USER_GROUP_NUMBER, 1);
    mockExistingGroups(
        group("g1", 50, "group", List.of("A")),
        group("g2", 40, "group", List.of("B")),
        group("g3", 30, "group", List.of("C")),
        deleted);
    // 50 consumed: 10 by g1, 30 by g2, 10 unaudited. New limit 80: 30 left, groups have 40 + 10 + 30 unused.
    mockCounters(50L, Map.of("g1", 10L, "g2", 30L));

    ApiResponse response = service.rebalance(Map.of(Constants.PARTNER_ID, PARTNER_ID, Constants.OVER_ALL_LIMIT, 80));

    assertEquals(HttpStatus.OK, response.getResponseCode());
    assertEquals(50L, response.getResult().get(Constants.SHORTFALL));
    List<Map<String, Object>> groups = (List<Map<String, Object>>) response.getResult().get(Constants.USER_GROUPS);
    assertEquals(List.of("g1", "g2", "g3"), groups.stream().map(g -> g.get(Constants.USER_GROUP_ID)).toList());
    assertEquals(List.of(10L, 30L, 30L), groups.stream().map(g -> g.get(Constants.LICENCE_ALLOCATION)).toList());
    assertEquals(List.of(40L, 10L, 0L), groups.stream().map(g -> g.get(Constants.CUT)).toList());
    List<Map<String, Object>> saved = groupsOf(savedJson(Constants.CONTEXT_DATA));
    assertEquals(4, saved.size());
    assertEquals(10, saved.get(0).get(Constants.LICENCE_ALLOCATION));
    assertEquals(Constants.AUDIT_UPDATED_BY_SYSTEM, auditRow().get(Constants.COL_UPDATEDBY));
  }

  @Test
  void rebalance_savesNothingWhenGroupsFitTheLimit() throws Exception {
    mockExistingGroups(group("g1", 50, "group", List.of("A")));
    mockCounters(10L, Map.of("g1", 10L));

    ApiResponse response = service.rebalance(Map.of(Constants.PARTNER_ID, PARTNER_ID, Constants.OVER_ALL_LIMIT, 50));

    assertEquals(HttpStatus.OK, response.getResponseCode());
    assertEquals(0L, response.getResult().get(Constants.SHORTFALL));
    verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap());
    verify(cassandraOperation, never()).insertRecord(anyString(), anyString(), anyMap());
  }

  @Test
  void rebalance_cutsEveryUnusedLicenceWhenLimitIsBelowConsumed() throws Exception {
    mockExistingGroups(group("g1", 50, "group", List.of("A")));
    mockCounters(60L, Map.of("g1", 20L));

    ApiResponse response = service.rebalance(Map.of(Constants.PARTNER_ID, PARTNER_ID, Constants.OVER_ALL_LIMIT, 40));

    assertEquals(30L, response.getResult().get(Constants.SHORTFALL));
    List<Map<String, Object>> groups = (List<Map<String, Object>>) response.getResult().get(Constants.USER_GROUPS);
    assertEquals(20L, groups.get(0).get(Constants.LICENCE_ALLOCATION));
  }

  @Test
  void rebalance_returnsEmptyWhenNoSettingsSaved() {
    mockRow(Constants.CONTEXT_DATA, null);

    ApiResponse response = service.rebalance(Map.of(Constants.PARTNER_ID, PARTNER_ID, Constants.OVER_ALL_LIMIT, 0));

    assertEquals(HttpStatus.OK, response.getResponseCode());
    assertEquals(0L, response.getResult().get(Constants.SHORTFALL));
    assertEquals(List.of(), response.getResult().get(Constants.USER_GROUPS));
    verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap());
  }

  @Test
  void rebalance_rejectsInvalidRequest() {
    ApiResponse missing = service.rebalance(null);
    ApiResponse invalid = service.rebalance(Map.of(Constants.PARTNER_ID, " ", Constants.OVER_ALL_LIMIT, -1));
    ApiResponse fraction = service.rebalance(Map.of(Constants.PARTNER_ID, PARTNER_ID, Constants.OVER_ALL_LIMIT, 1.5));

    assertBadRequest(missing, Constants.MSG_MISSING_OR_INVALID_FIELDS + "[partnerId, overAllLimit]");
    assertBadRequest(invalid, Constants.MSG_MISSING_OR_INVALID_FIELDS + "[partnerId, overAllLimit]");
    assertBadRequest(fraction, Constants.MSG_MISSING_OR_INVALID_FIELDS + "[overAllLimit]");
  }

  @Test
  void rebalance_returnsServerErrorOnFailure() {
    when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD_COURSE),
        eq(Constants.PROVIDER_ACCESS_SETTINGS_TABLE), anyMap(), anyList(), isNull()))
        .thenReturn(List.of(Map.of(Constants.CONTEXT_DATA_KEY, "not json")));

    ApiResponse response = service.rebalance(Map.of(Constants.PARTNER_ID, PARTNER_ID, Constants.OVER_ALL_LIMIT, 10));

    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    assertEquals(Constants.ERR_PROVIDER_ACCESS_REBALANCE, response.getParams().getErrMsg());
  }

  // ---------- notification upsert ----------

  @Test
  void notificationUpsert_keepsAddedOnForExistingRecipients() throws Exception {
    mockPartner(40000);
    Map<String, Object> existing = Map.of(Constants.ALERTS_ENABLED, false, Constants.RECIPIENTS,
        List.of(Map.of(Constants.EMAIL, "ops@example.com", Constants.ADDED_ON, "2026-10-01T00:00:00Z")));
    mockRow(Constants.NOTIFICATION_DATA, mapper.writeValueAsString(existing));

    ApiResponse response = service.notificationUpsert(notificationRequest(true,
        Map.of(Constants.NAME, " Ops ", Constants.EMAIL, " OPS@example.com "),
        Map.of(Constants.EMAIL, "new@example.com")), "token");

    assertEquals(HttpStatus.OK, response.getResponseCode());
    assertEquals(Map.of(Constants.MSG, Constants.PROVIDER_NOTIFICATION_SAVED), response.getResult());
    Map<String, Object> saved = savedJson(Constants.NOTIFICATION_DATA);
    assertEquals(true, saved.get(Constants.ALERTS_ENABLED));
    List<Map<String, Object>> recipients = (List<Map<String, Object>>) saved.get(Constants.RECIPIENTS);
    assertEquals("Ops", recipients.get(0).get(Constants.NAME));
    assertEquals("OPS@example.com", recipients.get(0).get(Constants.EMAIL));
    assertEquals("2026-10-01T00:00:00Z", recipients.get(0).get(Constants.ADDED_ON));
    assertFalse(recipients.get(1).containsKey(Constants.NAME));
    assertTrue(recipients.get(1).get(Constants.ADDED_ON) instanceof String);
    Map<String, Object> audit = auditRow();
    assertEquals(PARTNER_ID, audit.get(Constants.PARTNER_ID_COL));
    assertTrue(audit.get(Constants.AUDIT_ID_COL) instanceof UUID);
    assertTrue(audit.get(Constants.UPDATED_ON_COL) instanceof Instant);
    assertNull(audit.get(Constants.CONTEXT_DATA));
    assertTrue(((String) audit.get(Constants.NOTIFICATION_DATA)).contains("new@example.com"));
  }

  @Test
  void notificationUpsert_rejectsInvalidEmail() {
    mockPartner(40000);
    mockRow(Constants.NOTIFICATION_DATA, null);

    ApiResponse response = service.notificationUpsert(notificationRequest(true, Map.of(Constants.EMAIL, "bad")), "token");

    assertBadRequest(response, "Enter a valid email address: bad");
  }

  @Test
  void notificationUpsert_rejectsDuplicateEmailIgnoringCase() {
    mockPartner(40000);
    mockRow(Constants.NOTIFICATION_DATA, null);

    ApiResponse response = service.notificationUpsert(notificationRequest(true,
        Map.of(Constants.EMAIL, "a@example.com"), Map.of(Constants.EMAIL, "A@Example.com")), "token");

    assertBadRequest(response, "That email is already on the list: A@Example.com");
  }

  @Test
  void notificationUpsert_rejectsTooManyRecipients() {
    List<Map<String, Object>> recipients = new ArrayList<>();
    for (int i = 0; i < 101; i++) {
      recipients.add(Map.of(Constants.EMAIL, "user" + i + "@example.com"));
    }
    ApiResponse response = service.notificationUpsert(notificationRequest(true, recipients.toArray(new Map[0])), "token");

    assertBadRequest(response, "Maximum 100 recipients");
  }

  @Test
  void notificationUpsert_rejectsMissingFields() {
    ApiResponse response = service.notificationUpsert(Map.of(Constants.RECIPIENTS, "x"), "token");

    assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    String errMsg = response.getParams().getErrMsg();
    assertTrue(errMsg.contains(Constants.PARTNER_ID));
    assertTrue(errMsg.contains(Constants.ALERTS_ENABLED));
    assertTrue(errMsg.contains(Constants.RECIPIENTS));
  }

  @Test
  @SuppressWarnings("unchecked")
  void notificationUpsert_savesThresholdsWithOnOffAndDeletesMarksOfRemovedOrOffThresholds() {
    mockPartner(40000);
    mockRow(Constants.NOTIFICATION_DATA, null);
    Map<String, Object> request = notificationRequest(true, Map.of(Constants.EMAIL, "ops@example.com"));
    request.put(Constants.THRESHOLDS, List.of(threshold(90, true), threshold(50, true), threshold(100, false)));

    ApiResponse response = service.notificationUpsert(request, "token");

    assertEquals(HttpStatus.OK, response.getResponseCode());
    ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
    verify(cassandraOperation).updateRecord(eq(Constants.KEYSPACE_SUNBIRD_COURSE),
        eq(Constants.PROVIDER_ACCESS_SETTINGS_TABLE), captor.capture(),
        eq(Map.of(Constants.PARTNER_ID_COL, PARTNER_ID)));
    // Only the notification columns are written: contextdata is never part of this update.
    assertEquals(Set.of(Constants.NOTIFICATION_DATA, Constants.ALERT_THRESHOLDS_COL), captor.getValue().keySet());
    Map<Integer, Boolean> saved = (Map<Integer, Boolean>) captor.getValue().get(Constants.ALERT_THRESHOLDS_COL);
    assertEquals(Map.of(50, true, 90, true, 100, false), saved);
    assertEquals(List.of(50, 90, 100), List.copyOf(saved.keySet()));
    assertTrue(captor.getValue().get(Constants.NOTIFICATION_DATA) instanceof String);
    // Nothing saved before, so the defaults 50, 75, 90, 100 (all on) were in use: 75 is removed
    // and 100 is switched off, so both sent marks are deleted.
    verify(redisCacheMgr).deleteFromCache(Constants.LICENCE_ALERT_FIRED_KEY_PREFIX + PARTNER_ID + "_75",
        Constants.LICENCE_ALERT_FIRED_KEY_PREFIX + PARTNER_ID + "_100");
    assertEquals(Map.of(50, true, 90, true, 100, false), auditRow().get(Constants.ALERT_THRESHOLDS_COL));
  }

  @Test
  void notificationUpsert_keepsSavedThresholdsWhenLeftOut() throws Exception {
    mockPartner(40000);
    mockRow(Map.of(Constants.ALERT_THRESHOLDS_COL, Map.of(60, true, 80, false)));

    ApiResponse response = service.notificationUpsert(notificationRequest(false), "token");

    assertEquals(HttpStatus.OK, response.getResponseCode());
    assertEquals(false, savedJson(Constants.NOTIFICATION_DATA).get(Constants.ALERTS_ENABLED));
    verifyNoInteractions(redisCacheMgr);
    assertEquals(Map.of(60, true, 80, false), auditRow().get(Constants.ALERT_THRESHOLDS_COL));
  }

  @Test
  void notificationUpsert_deletesNoMarksWhenNoSavedThresholdIsRemovedOrSwitchedOff() {
    mockPartner(40000);
    mockRow(Map.of(Constants.ALERT_THRESHOLDS_COL, Map.of(50, true)));
    Map<String, Object> request = notificationRequest(true);
    request.put(Constants.THRESHOLDS, List.of(threshold(50, true), threshold(60, false)));

    ApiResponse response = service.notificationUpsert(request, "token");

    assertEquals(HttpStatus.OK, response.getResponseCode());
    verifyNoInteractions(redisCacheMgr);
  }

  @Test
  void notificationUpsert_rejectsNoThresholdsOrMoreThanMax() {
    Map<String, Object> request = notificationRequest(true);
    request.put(Constants.THRESHOLDS, List.of());
    assertBadRequest(service.notificationUpsert(request, "token"), "Enter between 1 and 5 thresholds");

    request.put(Constants.THRESHOLDS, List.of(threshold(10, true), threshold(20, true), threshold(30, true),
        threshold(40, true), threshold(50, true), threshold(60, true)));
    assertBadRequest(service.notificationUpsert(request, "token"), "Enter between 1 and 5 thresholds");
  }

  @Test
  void notificationUpsert_rejectsThresholdsThatAreNotWholeNumbersFrom1To100() {
    Map<String, Object> request = notificationRequest(true);
    for (Object value : List.of(0, 101, 50.5, "50")) {
      request.put(Constants.THRESHOLDS, List.of(Map.of(Constants.THRESHOLD, value, Constants.ENABLED, true)));
      assertBadRequest(service.notificationUpsert(request, "token"), Constants.ERR_INVALID_THRESHOLD);
    }
    // An entry that is not an object, e.g. the old shape [50, 75].
    request.put(Constants.THRESHOLDS, List.of(50));
    assertBadRequest(service.notificationUpsert(request, "token"), Constants.ERR_INVALID_THRESHOLD);
  }

  @Test
  void notificationUpsert_rejectsThresholdWithoutEnabledFlag() {
    Map<String, Object> request = notificationRequest(true);
    request.put(Constants.THRESHOLDS, List.of(threshold(50, true), Map.of(Constants.THRESHOLD, 75)));
    assertBadRequest(service.notificationUpsert(request, "token"),
        Constants.MSG_MISSING_OR_INVALID_FIELDS + List.of("thresholds[1].enabled"));

    request.put(Constants.THRESHOLDS, List.of(Map.of(Constants.THRESHOLD, 75, Constants.ENABLED, "yes")));
    assertBadRequest(service.notificationUpsert(request, "token"),
        Constants.MSG_MISSING_OR_INVALID_FIELDS + List.of("thresholds[0].enabled"));
  }

  @Test
  void notificationUpsert_rejectsDuplicateThreshold() {
    Map<String, Object> request = notificationRequest(true);
    request.put(Constants.THRESHOLDS, List.of(threshold(50, true), threshold(75, true), threshold(50, false)));

    assertBadRequest(service.notificationUpsert(request, "token"), "Duplicate threshold: 50");
  }

  @Test
  void notificationUpsert_rejectsThresholdsThatAreNotAList() {
    Map<String, Object> request = notificationRequest(true);
    request.put(Constants.THRESHOLDS, "50,75");

    assertBadRequest(service.notificationUpsert(request, "token"),
        Constants.MSG_MISSING_OR_INVALID_FIELDS + List.of(Constants.THRESHOLDS));
  }

  // ---------- notification read ----------

  @Test
  void notificationRead_returnsSavedSettings() throws Exception {
    mockPartner(40000);
    Map<String, Object> saved = Map.of(Constants.ALERTS_ENABLED, true, Constants.RECIPIENTS,
        List.of(Map.of(Constants.EMAIL, "ops@example.com", Constants.ADDED_ON, "2026-10-01T00:00:00Z")));
    mockRow(Constants.NOTIFICATION_DATA, mapper.writeValueAsString(saved));

    ApiResponse response = service.notificationRead(PARTNER_ID, "token");

    assertEquals(HttpStatus.OK, response.getResponseCode());
    assertEquals(true, response.getResult().get(Constants.ALERTS_ENABLED));
    assertEquals(100, response.getResult().get(Constants.MAX_RECIPIENTS));
    assertEquals(5, response.getResult().get(Constants.MAX_THRESHOLDS));
    assertEquals(defaultThresholds(), response.getResult().get(Constants.THRESHOLDS));
    assertFalse(response.getResult().containsKey("sent"));
    assertEquals(1, ((List<?>) response.getResult().get(Constants.RECIPIENTS)).size());
  }

  @Test
  void notificationRead_returnsSavedThresholdsInOrder() {
    mockPartner(40000);
    mockRow(Map.of(Constants.ALERT_THRESHOLDS_COL, Map.of(80, false, 60, true)));

    ApiResponse response = service.notificationRead(PARTNER_ID, "token");

    assertEquals(HttpStatus.OK, response.getResponseCode());
    assertEquals(List.of(threshold(60, true), threshold(80, false)), response.getResult().get(Constants.THRESHOLDS));
    verifyNoInteractions(redisCacheMgr);
  }

  @Test
  void notificationRead_treatsBlankDataAndEmptyThresholdsAsNotSaved() {
    mockPartner(40000);
    mockRow(Map.of(Constants.NOTIFICATION_DATA, " ", Constants.ALERT_THRESHOLDS_COL, Map.of()));

    ApiResponse response = service.notificationRead(PARTNER_ID, "token");

    assertEquals(HttpStatus.OK, response.getResponseCode());
    assertEquals(false, response.getResult().get(Constants.ALERTS_ENABLED));
    assertEquals(defaultThresholds(), response.getResult().get(Constants.THRESHOLDS));
  }

  @Test
  void notificationRead_returnsDefaultsWhenNotSaved() {
    mockPartner(40000);
    mockRow(Constants.NOTIFICATION_DATA, null);

    ApiResponse response = service.notificationRead(PARTNER_ID, "token");

    assertEquals(HttpStatus.OK, response.getResponseCode());
    assertEquals(false, response.getResult().get(Constants.ALERTS_ENABLED));
    assertEquals(defaultThresholds(), response.getResult().get(Constants.THRESHOLDS));
    assertEquals(List.of(), response.getResult().get(Constants.RECIPIENTS));
  }

  // ---------- validate ----------

  @Test
  void validate_returnsMatchingActiveGroupsInPriorityOrder() throws Exception {
    mockRow(Constants.CONTEXT_DATA, mapper.writeValueAsString(validateContextData()));
    when(userAndOrgService.getUserProfile("user-1")).thenReturn(Map.of("designation", 10, "group", 20));

    ApiResponse response = service.validate(Map.of(Constants.PARTNER_ID, PARTNER_ID, Constants.USER_ID, "user-1"));

    assertEquals(HttpStatus.OK, response.getResponseCode());
    assertEquals(true, response.getResult().get(Constants.ELIGIBLE));
    assertEquals(Constants.LICENCE_STATE_ENROL, response.getResult().get(Constants.LICENCE_STATE));
    assertEquals("g2", response.getResult().get(Constants.USER_GROUP_ID));
    assertEquals(List.of("g2", "g1"), response.getResult().get(Constants.MATCHING_GROUPS));
    assertEquals(List.of(
        Map.of(Constants.USER_GROUP_ID, "g2", Constants.LICENCE_ALLOCATION, 10),
        Map.of(Constants.USER_GROUP_ID, "g1", Constants.LICENCE_ALLOCATION, 10)),
        response.getResult().get(Constants.MATCHING_GROUP_LICENCES));
  }

  @Test
  void validate_returnsNotAvailableWhenNoGroupMatches() throws Exception {
    mockRow(Constants.CONTEXT_DATA, mapper.writeValueAsString(validateContextData()));
    when(userAndOrgService.getUserProfile("user-1")).thenReturn(Map.of("designation", 99));

    ApiResponse response = service.validate(Map.of(Constants.PARTNER_ID, PARTNER_ID, Constants.USER_ID, "user-1"));

    assertEquals(HttpStatus.OK, response.getResponseCode());
    assertEquals(false, response.getResult().get(Constants.ELIGIBLE));
    assertEquals(Constants.LICENCE_STATE_NOT_AVAILABLE, response.getResult().get(Constants.LICENCE_STATE));
    assertNull(response.getResult().get(Constants.USER_GROUP_ID));
    assertEquals(List.of(), response.getResult().get(Constants.MATCHING_GROUPS));
  }

  @Test
  void validate_returnsNotAvailableWhenNoSettingsSaved() {
    mockRow(Constants.CONTEXT_DATA, null);

    ApiResponse response = service.validate(Map.of(Constants.PARTNER_ID, PARTNER_ID, Constants.USER_ID, "user-1"));

    assertEquals(false, response.getResult().get(Constants.ELIGIBLE));
    verify(userAndOrgService, never()).getUserProfile(anyString());
  }

  @Test
  void validate_rejectsMissingFields() {
    ApiResponse response = service.validate(Map.of(Constants.PARTNER_ID, " "));

    assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    assertTrue(response.getParams().getErrMsg().contains(Constants.PARTNER_ID));
    assertTrue(response.getParams().getErrMsg().contains(Constants.USER_ID));
  }

  @Test
  void validate_rejectsNullRequest() {
    ApiResponse response = service.validate(null);

    assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
  }

  @Test
  void validate_returnsServerErrorOnFailure() {
    when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), isNull()))
        .thenThrow(new RuntimeException("db down"));

    ApiResponse response = service.validate(Map.of(Constants.PARTNER_ID, PARTNER_ID, Constants.USER_ID, "user-1"));

    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
  }

  /**
   * g1 (priority 2): designation 10. g2 (priority 1): designation 10 AND group 20.
   * g3 is DELETED, g4 has no criteria, g5 has no id-mapped entry.
   */
  private Map<String, Object> validateContextData() {
    Map<String, Object> g1 = group("g1", 10, "designation", List.of("auditor"));
    g1.put(Constants.USER_GROUP_NUMBER, 2);
    Map<String, Object> g2 = group("g2", 10, "designation", List.of("auditor"));
    g2.put(Constants.USER_GROUP_NUMBER, 1);
    Map<String, Object> g3 = group("g3", 10, "designation", List.of("auditor"));
    g3.put(Constants.STATUS, Constants.USER_GROUP_STATUS_DELETED);
    Map<String, Object> g4 = group("g4", 10, "designation", List.of("auditor"));
    Map<String, Object> g5 = group("g5", 10, "designation", List.of("auditor"));
    List<Map<String, Object>> idGroups = List.of(
        idGroup("g1", Map.of(Constants.CRITERIA_KEY, "Designation", Constants.CRITERIA_VALUE, List.of(5, 10))),
        idGroup("g2", Map.of(Constants.CRITERIA_KEY, "designation", Constants.CRITERIA_VALUE, List.of(10)),
            Map.of(Constants.CRITERIA_KEY, "group", Constants.CRITERIA_VALUE, List.of(20))),
        idGroup("g3", Map.of(Constants.CRITERIA_KEY, "designation", Constants.CRITERIA_VALUE, List.of(10))),
        idGroup("g4"),
        Map.of(Constants.USER_GROUP_ID, "other"));
    Map<String, Object> contextData = new HashMap<>();
    contextData.put(Constants.ACCESS_CONTROL, Map.of(Constants.USER_GROUPS, List.of(g1, g2, g3, g4, g5)));
    contextData.put(Constants.ACCESS_CONTROL_ID, Map.of(Constants.USER_GROUPS, idGroups));
    return contextData;
  }

  @SafeVarargs
  private static Map<String, Object> idGroup(String id, Map<String, Object>... criteria) {
    return Map.of(Constants.USER_GROUP_ID, id, Constants.USER_GROUP_CRITERIA_LIST, List.of(criteria));
  }

  // ---------- token and partner checks ----------

  @Test
  void allApis_rejectExpiredToken() {
    when(accessTokenValidator.fetchUserIdFromAccessToken(eq("expired"), any())).thenAnswer(invocation -> {
      ApiResponse response = invocation.getArgument(1);
      response.setResponseCode(HttpStatus.UNAUTHORIZED);
      return null;
    });

    assertEquals(HttpStatus.UNAUTHORIZED, service.upsert(Map.of(), "expired").getResponseCode());
    assertEquals(HttpStatus.UNAUTHORIZED, service.read(PARTNER_ID, "expired").getResponseCode());
    assertEquals(HttpStatus.UNAUTHORIZED, service.notificationUpsert(Map.of(), "expired").getResponseCode());
    assertEquals(HttpStatus.UNAUTHORIZED, service.notificationRead(PARTNER_ID, "expired").getResponseCode());
    verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), any());
  }

  @Test
  void read_tokenWithoutUserId_returnsWithoutReading() {
    when(accessTokenValidator.fetchUserIdFromAccessToken(eq("no-sub"), any())).thenReturn("");

    ApiResponse response = service.read(PARTNER_ID, "no-sub");

    assertTrue(response.getResult().isEmpty());
    verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), any());
  }

  @Test
  void readAndNotificationApis_rejectUnknownPartner() {
    when(outboundRequestHandlerService.fetchResult(anyString()))
        .thenReturn(Map.of(Constants.RESPONSE_CODE, "BAD_REQUEST"));

    assertBadRequest(service.read(PARTNER_ID, "token"), "Invalid partnerId: " + PARTNER_ID);
    assertBadRequest(service.notificationRead(PARTNER_ID, "token"), "Invalid partnerId: " + PARTNER_ID);
    assertBadRequest(service.notificationUpsert(notificationRequest(true), "token"), "Invalid partnerId: " + PARTNER_ID);
    verify(cassandraOperation, never()).getRecordsByProperties(anyString(), anyString(), anyMap(), anyList(), any());
    verify(cassandraOperation, never()).updateRecord(anyString(), anyString(), anyMap(), anyMap());
  }

  // ---------- helpers ----------

  private void mockPartner(long overAllLimit) {
    Map<String, Object> data = new HashMap<>();
    data.put(Constants.OVER_ALL_LIMIT, overAllLimit);
    when(outboundRequestHandlerService.fetchResult("http://cb-pores-service:7001/contentpartner/v1/read/" + PARTNER_ID))
        .thenReturn(Map.of(
        Constants.RESPONSE_CODE, Constants.OK,
        Constants.RESULT, Map.of(Constants.DATA, data)));
  }

  private void mockRow(String column, String json) {
    List<Map<String, Object>> rows = new ArrayList<>();
    if (json != null) {
      Map<String, Object> row = new HashMap<>();
      row.put(Constants.PARTNER_ID_COL, PARTNER_ID);
      // Cassandra rows come back with keys renamed by cassandratablecolumn.properties
      row.put(Constants.CONTEXT_DATA.equals(column) ? Constants.CONTEXT_DATA_KEY : column, json);
      rows.add(row);
    }
    when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD_COURSE),
        eq(Constants.PROVIDER_ACCESS_SETTINGS_TABLE), anyMap(), anyList(), isNull())).thenReturn(rows);
  }

  /**
   * The provider's settings row with the given columns, as Cassandra returns it.
   */
  private void mockRow(Map<String, Object> columns) {
    Map<String, Object> row = new HashMap<>(columns);
    row.put(Constants.PARTNER_ID_COL, PARTNER_ID);
    when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD_COURSE),
        eq(Constants.PROVIDER_ACCESS_SETTINGS_TABLE), anyMap(), anyList(), isNull())).thenReturn(List.of(row));
  }

  /**
   * The provider's paid TOTAL_ENROLMENTS row and its USER_GROUP_ENROLMENTS rows (userGroupId to value).
   */
  private void mockCounters(long total, Map<String, Long> consumedByGroup) {
    lenient().when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD_COURSE),
        eq(Constants.TABLE_USER_EXTERNAL_ENROLMENTS_COUNTER), argThat(scopeType(Constants.SCOPE_TYPE_TOTAL_ENROLMENTS)),
        anyList(), isNull())).thenReturn(List.of(Map.of(Constants.SCOPE_ID_COL, PARTNER_ID, Constants.VALUE, total)));
    List<Map<String, Object>> groupRows = consumedByGroup.entrySet().stream()
        .map(entry -> Map.<String, Object>of(Constants.SCOPE_ID_COL, entry.getKey(), Constants.VALUE, entry.getValue()))
        .toList();
    lenient().when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD_COURSE),
        eq(Constants.TABLE_USER_EXTERNAL_ENROLMENTS_COUNTER), argThat(scopeType(Constants.SCOPE_TYPE_USER_GROUP_ENROLMENTS)),
        anyList(), isNull())).thenReturn(groupRows);
  }

  private static org.mockito.ArgumentMatcher<Map<String, Object>> scopeType(String scopeType) {
    return propertyMap -> propertyMap != null && scopeType.equals(propertyMap.get(Constants.SCOPE_TYPE_COL));
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> auditRow() {
    ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
    verify(cassandraOperation).insertRecord(eq(Constants.KEYSPACE_SUNBIRD_COURSE),
        eq(Constants.PROVIDER_ACCESS_SETTINGS_AUDIT_TABLE), captor.capture());
    return captor.getValue();
  }

  @SuppressWarnings("unchecked")
  private void mockIdMapSuccess() {
    when(accessSettingMigrationService.updateContextDataWithIdMap(anyString(), anyMap(), anyMap()))
        .thenAnswer(inv -> {
          Map<String, Object> accessControl = inv.getArgument(1);
          Map<String, Object> idMap = inv.getArgument(2);
          idMap.put(Constants.VERSION, 1);
          idMap.put(Constants.USER_GROUPS, new ArrayList<>((List<Object>) accessControl.get(Constants.USER_GROUPS)));
          return true;
        });
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> savedJson(String column) throws Exception {
    ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
    verify(cassandraOperation).updateRecord(eq(Constants.KEYSPACE_SUNBIRD_COURSE),
        eq(Constants.PROVIDER_ACCESS_SETTINGS_TABLE), captor.capture(),
        eq(Map.of(Constants.PARTNER_ID_COL, PARTNER_ID)));
    assertEquals(1, captor.getValue().size());
    return mapper.readValue((String) captor.getValue().get(column), new TypeReference<>() {
    });
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> groupsOf(Map<String, Object> holder) {
    return (List<Map<String, Object>>) ((Map<String, Object>) holder.get(Constants.ACCESS_CONTROL))
        .get(Constants.USER_GROUPS);
  }

  private static Map<String, Object> group(String id, long allocation, String key, List<String> values) {
    Map<String, Object> criteria = new HashMap<>();
    criteria.put(Constants.CRITERIA_KEY, key);
    criteria.put(Constants.CRITERIA_VALUE, values);
    Map<String, Object> group = new HashMap<>();
    if (id != null) {
      group.put(Constants.USER_GROUP_ID, id);
    }
    group.put(Constants.LICENCE_ALLOCATION, allocation);
    group.put(Constants.USER_GROUP_CRITERIA_LIST, List.of(criteria));
    return group;
  }

  private static Map<String, Object> number(Map<String, Object> group, int userGroup) {
    group.put(Constants.USER_GROUP_NUMBER, userGroup);
    return group;
  }

  @SafeVarargs
  private static Map<String, Object> request(Map<String, Object>... groups) {
    Map<String, Object> request = new HashMap<>();
    request.put(Constants.PARTNER_ID, PARTNER_ID);
    request.put(Constants.ACCESS_CONTROL, Map.of(Constants.VERSION, 1, Constants.USER_GROUPS, numbered(groups)));
    return request;
  }

  /**
   * The groups numbered 1..n (userGroup) from their position, unless a group already has a number.
   */
  @SafeVarargs
  private static List<Map<String, Object>> numbered(Map<String, Object>... groups) {
    for (int i = 0; i < groups.length; i++) {
      groups[i].putIfAbsent(Constants.USER_GROUP_NUMBER, i + 1);
    }
    return List.of(groups);
  }

  @SafeVarargs
  private void mockExistingGroups(Map<String, Object>... groups) throws Exception {
    mockRow(Constants.CONTEXT_DATA, mapper.writeValueAsString(
        Map.of(Constants.ACCESS_CONTROL, Map.of(Constants.USER_GROUPS, numbered(groups)))));
  }

  private static Map<String, Object> threshold(int threshold, boolean enabled) {
    Map<String, Object> entry = new LinkedHashMap<>();
    entry.put(Constants.THRESHOLD, threshold);
    entry.put(Constants.ENABLED, enabled);
    return entry;
  }

  private static List<Map<String, Object>> defaultThresholds() {
    return List.of(threshold(50, true), threshold(75, true), threshold(90, true), threshold(100, true));
  }

  @SafeVarargs
  private static Map<String, Object> notificationRequest(boolean alertsEnabled, Map<String, Object>... recipients) {
    Map<String, Object> request = new HashMap<>();
    request.put(Constants.PARTNER_ID, PARTNER_ID);
    request.put(Constants.ALERTS_ENABLED, alertsEnabled);
    request.put(Constants.RECIPIENTS, List.of(recipients));
    return request;
  }

  private static void assertBadRequest(ApiResponse response, String errMsg) {
    assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    assertEquals(Constants.FAILED, response.getParams().getStatus());
    assertEquals(errMsg, response.getParams().getErrMsg());
  }
}
