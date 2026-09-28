package com.igot.cb.cbplan.service.impl.v4;

import com.igot.cb.common.ServerProperties;
import com.igot.cb.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanContentSyncServiceV4ImplTest {

    private static final String CA_LINKED_ID = "do_ca_12345";
    private static final String LEARNING_BASE_URL = "http://learning-service";
    private static final String SYSTEM_UPDATE_API = "/content/v3/system/update/";
    private static final String V4_CONTENT_ID_1 = "do_content_001";
    private static final String V4_CONTENT_ID_2 = "do_content_002";
    private static final String V3_CONTENT_ID = "do_v3_plain_001";
    private static final String PLAN_IDENTIFIER = "plan_identifier_001";
    private static final String PLAN_NAME = "Test Plan";
    private static final String PLAN_YEAR = "2025";
    private static final String ORG_NAME = "Test Org";

    @Mock
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @Mock
    private ServerProperties serverProperties;

    @Mock
    private CbPlanContentLookupServiceV4Impl contentLookupService;

    @Mock
    private CbExtServerProperties cbExtServerProperties;

    private CbPlanContentSyncServiceV4Impl syncService;

    @BeforeEach
    void setUp() {
        syncService = new CbPlanContentSyncServiceV4Impl(
                outboundRequestHandlerService, serverProperties, contentLookupService, cbExtServerProperties);
    }


    @Test
    void constructor_withValidDependencies_createsInstance() {
        assertThat(syncService).isNotNull();
    }


    @Test
    void syncContentNodeTrainingPlan_nullCaLinkedId_skipsSync() throws Exception {
        syncService.syncContentNodeTrainingPlan(null, List.of(V4_CONTENT_ID_1));
        verify(contentLookupService, never()).getContentMetadata(anyString());
    }

    @Test
    void syncContentNodeTrainingPlan_blankCaLinkedId_skipsSync() throws Exception {
        syncService.syncContentNodeTrainingPlan("  ", List.of(V4_CONTENT_ID_1));
        verify(contentLookupService, never()).getContentMetadata(anyString());
    }


    @Test
    void syncContentNodeTrainingPlan_asyncMode_noExceptionThrown() {
        when(cbExtServerProperties.isCbPlanContentSyncAsync()).thenReturn(true);
        assertThatNoException().isThrownBy(() ->
                syncService.syncContentNodeTrainingPlan(CA_LINKED_ID, List.of(V4_CONTENT_ID_1)));
    }


    private void stubSyncMode() {
        when(cbExtServerProperties.isCbPlanContentSyncAsync()).thenReturn(false);
    }

    private void stubPatchUrl() {
        when(serverProperties.getLearningServiceVmBaseUrl()).thenReturn(LEARNING_BASE_URL);
        when(serverProperties.getSystemUpdateAPI()).thenReturn(SYSTEM_UPDATE_API);
    }

    private Map<String, Object> buildTrainingPlanV2() {
        Map<String, Object> trainingPlanV2 = new HashMap<>();
        trainingPlanV2.put(Constants.IDENTIFIER, PLAN_IDENTIFIER);
        trainingPlanV2.put(Constants.NAME, PLAN_NAME);
        trainingPlanV2.put(Constants.PLAN_YEAR, PLAN_YEAR);
        trainingPlanV2.put(Constants.ORG_NAME, ORG_NAME);
        return trainingPlanV2;
    }

    private Map<String, Object> buildContentMetadata(Map<String, Object> trainingPlanV2) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(Constants.TRAINING_PLAN_V2, trainingPlanV2);
        return metadata;
    }

    private Map<String, Object> okPatchResponse() {
        Map<String, Object> response = new HashMap<>();
        response.put(Constants.RESPONSE_CODE, Constants.OK);
        return response;
    }


    @Test
    void syncContentNodeTrainingPlan_syncMode_contentNodeNotFound_skipsPatching() throws Exception {
        stubSyncMode();
        doReturn(new HashMap<>()).when(contentLookupService).getContentMetadata(CA_LINKED_ID);

        syncService.syncContentNodeTrainingPlan(CA_LINKED_ID, List.of(V4_CONTENT_ID_1));

        verify(outboundRequestHandlerService, never()).fetchResultUsingPatch(anyString(), anyMap(), anyMap());
    }

    @Test
    void syncContentNodeTrainingPlan_syncMode_getContentMetadataReturnsNull_skipsPatching() throws Exception {
        stubSyncMode();
        doReturn(null).when(contentLookupService).getContentMetadata(CA_LINKED_ID);

        syncService.syncContentNodeTrainingPlan(CA_LINKED_ID, List.of(V4_CONTENT_ID_1));

        verify(outboundRequestHandlerService, never()).fetchResultUsingPatch(anyString(), anyMap(), anyMap());
    }

    @Test
    void syncContentNodeTrainingPlan_syncMode_missingTrainingPlanV2_skipsPatching() throws Exception {
        stubSyncMode();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("someOtherField", "value");
        doReturn(metadata).when(contentLookupService).getContentMetadata(CA_LINKED_ID);

        syncService.syncContentNodeTrainingPlan(CA_LINKED_ID, List.of(V4_CONTENT_ID_1));

        verify(outboundRequestHandlerService, never()).fetchResultUsingPatch(anyString(), anyMap(), anyMap());
    }


    @Test
    void syncContentNodeTrainingPlan_syncMode_getContentMetadataThrows_handledGracefully() throws Exception {
        stubSyncMode();
        doThrow(new RuntimeException("Network timeout")).when(contentLookupService).getContentMetadata(CA_LINKED_ID);

        syncService.syncContentNodeTrainingPlan(CA_LINKED_ID, List.of(V4_CONTENT_ID_1));

        verify(outboundRequestHandlerService, never()).fetchResultUsingPatch(anyString(), anyMap(), anyMap());
    }


    @Test
    @SuppressWarnings("unchecked")
    void syncContentNodeTrainingPlan_syncMode_buildsCorrectPatchUrl() throws Exception {
        stubSyncMode();
        stubPatchUrl();
        doReturn(buildContentMetadata(buildTrainingPlanV2())).when(contentLookupService).getContentMetadata(CA_LINKED_ID);
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), anyMap(), anyMap()))
                .thenReturn(okPatchResponse());

        ArgumentCaptor<String> urlCaptor = ArgumentCaptor.forClass(String.class);
        syncService.syncContentNodeTrainingPlan(CA_LINKED_ID, List.of(V4_CONTENT_ID_1));

        verify(outboundRequestHandlerService).fetchResultUsingPatch(urlCaptor.capture(), anyMap(), anyMap());
        assertThat(urlCaptor.getValue()).isEqualTo(LEARNING_BASE_URL + SYSTEM_UPDATE_API + CA_LINKED_ID);
    }


    @Test
    @SuppressWarnings("unchecked")
    void syncContentNodeTrainingPlan_syncMode_v4ContentList_patchesCorrectPayload() throws Exception {
        stubSyncMode();
        stubPatchUrl();
        doReturn(buildContentMetadata(buildTrainingPlanV2())).when(contentLookupService).getContentMetadata(CA_LINKED_ID);
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), anyMap(), anyMap()))
                .thenReturn(okPatchResponse());

        List<String> contentList = List.of(
                "{\"identifier\":\"" + V4_CONTENT_ID_1 + "\",\"mandatory\":true}",
                "{\"identifier\":\"" + V4_CONTENT_ID_2 + "\",\"mandatory\":false}"
        );

        ArgumentCaptor<Map<String, Object>> bodyCaptor = ArgumentCaptor.forClass(Map.class);
        syncService.syncContentNodeTrainingPlan(CA_LINKED_ID, contentList);

        verify(outboundRequestHandlerService).fetchResultUsingPatch(anyString(), bodyCaptor.capture(), anyMap());

        Map<String, Object> request = (Map<String, Object>) bodyCaptor.getValue().get(Constants.REQUEST);
        Map<String, Object> content = (Map<String, Object>) request.get(Constants.CONTENT);
        Map<String, Object> trainingPlanV2 = (Map<String, Object>) content.get(Constants.TRAINING_PLAN_V2);
        List<Map<String, Object>> parsedContentList = (List<Map<String, Object>>) trainingPlanV2.get(Constants.CONTENT_LIST);

        assertThat(parsedContentList).hasSize(2);
        assertThat(parsedContentList.get(0))
                .containsEntry(Constants.IDENTIFIER, V4_CONTENT_ID_1)
                .containsEntry(Constants.MANDATORY, true);
        assertThat(parsedContentList.get(1))
                .containsEntry(Constants.IDENTIFIER, V4_CONTENT_ID_2)
                .containsEntry(Constants.MANDATORY, false);
    }


    @Test
    @SuppressWarnings("unchecked")
    void syncContentNodeTrainingPlan_syncMode_v3PlainIdContentList_wrappedWithMandatoryFalse() throws Exception {
        stubSyncMode();
        stubPatchUrl();
        doReturn(buildContentMetadata(buildTrainingPlanV2())).when(contentLookupService).getContentMetadata(CA_LINKED_ID);
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), anyMap(), anyMap()))
                .thenReturn(okPatchResponse());

        ArgumentCaptor<Map<String, Object>> bodyCaptor = ArgumentCaptor.forClass(Map.class);
        syncService.syncContentNodeTrainingPlan(CA_LINKED_ID, List.of(V3_CONTENT_ID));

        verify(outboundRequestHandlerService).fetchResultUsingPatch(anyString(), bodyCaptor.capture(), anyMap());

        Map<String, Object> request = (Map<String, Object>) bodyCaptor.getValue().get(Constants.REQUEST);
        Map<String, Object> content = (Map<String, Object>) request.get(Constants.CONTENT);
        Map<String, Object> trainingPlanV2 = (Map<String, Object>) content.get(Constants.TRAINING_PLAN_V2);
        List<Map<String, Object>> parsedContentList = (List<Map<String, Object>>) trainingPlanV2.get(Constants.CONTENT_LIST);

        assertThat(parsedContentList).hasSize(1);
        assertThat(parsedContentList.get(0))
                .containsEntry(Constants.IDENTIFIER, V3_CONTENT_ID)
                .containsEntry(Constants.MANDATORY, Boolean.FALSE);
    }


    @Test
    @SuppressWarnings("unchecked")
    void syncContentNodeTrainingPlan_syncMode_emptyContentList_patchesWithEmptyList() throws Exception {
        stubSyncMode();
        stubPatchUrl();
        doReturn(buildContentMetadata(buildTrainingPlanV2())).when(contentLookupService).getContentMetadata(CA_LINKED_ID);
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), anyMap(), anyMap()))
                .thenReturn(okPatchResponse());

        ArgumentCaptor<Map<String, Object>> bodyCaptor = ArgumentCaptor.forClass(Map.class);
        syncService.syncContentNodeTrainingPlan(CA_LINKED_ID, List.of());

        verify(outboundRequestHandlerService).fetchResultUsingPatch(anyString(), bodyCaptor.capture(), anyMap());

        Map<String, Object> request = (Map<String, Object>) bodyCaptor.getValue().get(Constants.REQUEST);
        Map<String, Object> content = (Map<String, Object>) request.get(Constants.CONTENT);
        Map<String, Object> trainingPlanV2 = (Map<String, Object>) content.get(Constants.TRAINING_PLAN_V2);
        List<Map<String, Object>> parsedContentList = (List<Map<String, Object>>) trainingPlanV2.get(Constants.CONTENT_LIST);

        assertThat(parsedContentList).isEmpty();
    }


    @Test
    @SuppressWarnings("unchecked")
    void syncContentNodeTrainingPlan_syncMode_preservesExistingTrainingPlanV2Fields() throws Exception {
        stubSyncMode();
        stubPatchUrl();
        doReturn(buildContentMetadata(buildTrainingPlanV2())).when(contentLookupService).getContentMetadata(CA_LINKED_ID);
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), anyMap(), anyMap()))
                .thenReturn(okPatchResponse());

        ArgumentCaptor<Map<String, Object>> bodyCaptor = ArgumentCaptor.forClass(Map.class);
        syncService.syncContentNodeTrainingPlan(CA_LINKED_ID,
                List.of("{\"identifier\":\"" + V4_CONTENT_ID_1 + "\",\"mandatory\":true}"));

        verify(outboundRequestHandlerService).fetchResultUsingPatch(anyString(), bodyCaptor.capture(), anyMap());

        Map<String, Object> request = (Map<String, Object>) bodyCaptor.getValue().get(Constants.REQUEST);
        Map<String, Object> content = (Map<String, Object>) request.get(Constants.CONTENT);
        Map<String, Object> trainingPlanV2 = (Map<String, Object>) content.get(Constants.TRAINING_PLAN_V2);

        assertThat(trainingPlanV2)
                .containsEntry(Constants.IDENTIFIER, PLAN_IDENTIFIER)
                .containsEntry(Constants.NAME, PLAN_NAME)
                .containsEntry(Constants.PLAN_YEAR, PLAN_YEAR)
                .containsEntry(Constants.ORG_NAME, ORG_NAME);
    }


    @Test
    void syncContentNodeTrainingPlan_syncMode_nonOkPatchResponse_noException() throws Exception {
        stubSyncMode();
        stubPatchUrl();
        doReturn(buildContentMetadata(buildTrainingPlanV2())).when(contentLookupService).getContentMetadata(CA_LINKED_ID);
        Map<String, Object> failResponse = new HashMap<>();
        failResponse.put(Constants.RESPONSE_CODE, "CLIENT_ERROR");
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), anyMap(), anyMap()))
                .thenReturn(failResponse);

        syncService.syncContentNodeTrainingPlan(CA_LINKED_ID, List.of(V4_CONTENT_ID_1));

        verify(outboundRequestHandlerService).fetchResultUsingPatch(anyString(), anyMap(), anyMap());
    }

    @Test
    void syncContentNodeTrainingPlan_syncMode_nullPatchResponse_noException() throws Exception {
        stubSyncMode();
        stubPatchUrl();
        doReturn(buildContentMetadata(buildTrainingPlanV2())).when(contentLookupService).getContentMetadata(CA_LINKED_ID);
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), anyMap(), anyMap()))
                .thenReturn(null);

        syncService.syncContentNodeTrainingPlan(CA_LINKED_ID, List.of(V4_CONTENT_ID_1));

        verify(outboundRequestHandlerService).fetchResultUsingPatch(anyString(), anyMap(), anyMap());
    }
}
