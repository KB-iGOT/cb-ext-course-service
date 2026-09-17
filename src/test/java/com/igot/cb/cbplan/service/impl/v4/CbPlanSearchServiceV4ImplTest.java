package com.igot.cb.cbplan.service.impl.v4;

import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CbPlanSearchServiceV4ImplTest {

    private static final String TEST_AUTH_TOKEN = "test_token";
    private static final String TEST_USER_ID = "user_123";
    private static final String TEST_ORG_ID = "org_456";
    private static final String TEST_INDEX = "cbplan_index";
    private static final String TEST_JSON_PATH = "jsonPath";

    @Mock
    private EsUtilService esUtilService;
    @Mock
    private CbExtServerProperties serverProperties;
    @Mock
    private CbPlanValidationServiceV4Impl validationService;

    private CbPlanSearchServiceV4Impl searchService;

    @BeforeEach
    void setUp() {
        searchService = new CbPlanSearchServiceV4Impl(esUtilService, serverProperties, validationService);

        lenient().when(serverProperties.getCpPlanIndex()).thenReturn(TEST_INDEX);
        lenient().when(serverProperties.getElasticCbPlanJsonPath()).thenReturn(TEST_JSON_PATH);
    }

    @Test
    void searchCbPlan_withV3ContentList_shouldConvertToV4WithMandatoryFalse() {
        ApiRequest request = createApiRequest();
        List<Map<String, Object>> esData = createEsDataWithV3ContentList();
        SearchResult searchResult = new SearchResult();
        searchResult.setData(esData);

        when(validationService.validateAndExtractUserId(eq(TEST_AUTH_TOKEN), any())).thenReturn(TEST_USER_ID);
        when(esUtilService.searchDocumentsV2(eq(TEST_INDEX), any(SearchCriteria.class), eq(TEST_JSON_PATH)))
                .thenReturn(searchResult);

        ApiResponse response = searchService.searchCbPlan(request, TEST_ORG_ID, TEST_AUTH_TOKEN);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(HttpStatus.OK, response.getResponseCode());

        SearchResult resultData = (SearchResult) response.getResult().get(Constants.RESULT);
        assertNotNull(resultData);
        List<Map<String, Object>> data = resultData.getData();
        assertEquals(1, data.size());

        List<Map<String, Object>> contentList = (List<Map<String, Object>>) data.get(0).get(Constants.CONTENT_LIST);
        assertNotNull(contentList);
        assertEquals(2, contentList.size());

        assertEquals("do_114376977434968064182", contentList.get(0).get(Constants.IDENTIFIER));
        assertEquals(false, contentList.get(0).get(Constants.MANDATORY));

        assertEquals("do_114378386987417600180", contentList.get(1).get(Constants.IDENTIFIER));
        assertEquals(false, contentList.get(1).get(Constants.MANDATORY));
    }

    @Test
    void searchCbPlan_withV4ContentList_shouldPreserveMandatoryField() {
        ApiRequest request = createApiRequest();
        List<Map<String, Object>> esData = createEsDataWithV4ContentList();
        SearchResult searchResult = new SearchResult();
        searchResult.setData(esData);

        when(validationService.validateAndExtractUserId(eq(TEST_AUTH_TOKEN), any())).thenReturn(TEST_USER_ID);
        when(esUtilService.searchDocumentsV2(eq(TEST_INDEX), any(SearchCriteria.class), eq(TEST_JSON_PATH)))
                .thenReturn(searchResult);

        ApiResponse response = searchService.searchCbPlan(request, TEST_ORG_ID, TEST_AUTH_TOKEN);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(HttpStatus.OK, response.getResponseCode());

        SearchResult resultData = (SearchResult) response.getResult().get(Constants.RESULT);
        assertNotNull(resultData);
        List<Map<String, Object>> data = resultData.getData();
        assertEquals(1, data.size());

        List<Map<String, Object>> contentList = (List<Map<String, Object>>) data.get(0).get(Constants.CONTENT_LIST);
        assertNotNull(contentList);
        assertEquals(2, contentList.size());

        assertEquals("do_114467322178428928111", contentList.get(0).get(Constants.IDENTIFIER));
        assertEquals(true, contentList.get(0).get(Constants.MANDATORY));

        assertEquals("do_114378386987417600180", contentList.get(1).get(Constants.IDENTIFIER));
        assertEquals(false, contentList.get(1).get(Constants.MANDATORY));
    }

    @Test
    void searchCbPlan_withEmptyContentList_shouldReturnEmptyList() {
        ApiRequest request = createApiRequest();
        List<Map<String, Object>> esData = createEsDataWithEmptyContentList();
        SearchResult searchResult = new SearchResult();
        searchResult.setData(esData);

        when(validationService.validateAndExtractUserId(eq(TEST_AUTH_TOKEN), any())).thenReturn(TEST_USER_ID);
        when(esUtilService.searchDocumentsV2(eq(TEST_INDEX), any(SearchCriteria.class), eq(TEST_JSON_PATH)))
                .thenReturn(searchResult);

        ApiResponse response = searchService.searchCbPlan(request, TEST_ORG_ID, TEST_AUTH_TOKEN);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        SearchResult resultData = (SearchResult) response.getResult().get(Constants.RESULT);
        List<Map<String, Object>> data = resultData.getData();
        assertEquals(1, data.size());

        List<?> contentList = (List<?>) data.get(0).get(Constants.CONTENT_LIST);
        assertNotNull(contentList);
        assertTrue(contentList.isEmpty());
    }

    @Test
    void searchCbPlan_withoutContentList_shouldNotTransform() {
        ApiRequest request = createApiRequest();
        List<Map<String, Object>> esData = createEsDataWithoutContentList();
        SearchResult searchResult = new SearchResult();
        searchResult.setData(esData);

        when(validationService.validateAndExtractUserId(eq(TEST_AUTH_TOKEN), any())).thenReturn(TEST_USER_ID);
        when(esUtilService.searchDocumentsV2(eq(TEST_INDEX), any(SearchCriteria.class), eq(TEST_JSON_PATH)))
                .thenReturn(searchResult);

        ApiResponse response = searchService.searchCbPlan(request, TEST_ORG_ID, TEST_AUTH_TOKEN);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        SearchResult resultData = (SearchResult) response.getResult().get(Constants.RESULT);
        List<Map<String, Object>> data = resultData.getData();
        assertEquals(1, data.size());
        assertFalse(data.get(0).containsKey(Constants.CONTENT_LIST));
    }

    @Test
    void searchCbPlan_withInvalidUserId_shouldReturnFailedResponse() {
        ApiRequest request = createApiRequest();

        when(validationService.validateAndExtractUserId(eq(TEST_AUTH_TOKEN), any())).thenReturn("");

        searchService.searchCbPlan(request, TEST_ORG_ID, TEST_AUTH_TOKEN);

        verify(esUtilService, never()).searchDocumentsV2(anyString(), any(), anyString());
    }

    @Test
    void searchCbPlan_withEmptySearchResults_shouldNotTransform() {
        ApiRequest request = createApiRequest();
        SearchResult searchResult = new SearchResult();
        searchResult.setData(Collections.emptyList());

        when(validationService.validateAndExtractUserId(eq(TEST_AUTH_TOKEN), any())).thenReturn(TEST_USER_ID);
        when(esUtilService.searchDocumentsV2(eq(TEST_INDEX), any(SearchCriteria.class), eq(TEST_JSON_PATH)))
                .thenReturn(searchResult);

        ApiResponse response = searchService.searchCbPlan(request, TEST_ORG_ID, TEST_AUTH_TOKEN);

        assertNotNull(response);
        assertFalse(response.getResult().containsKey(Constants.RESULT));
    }

    @Test
    void searchCbPlan_withException_shouldReturnInternalServerError() {
        ApiRequest request = createApiRequest();

        when(validationService.validateAndExtractUserId(eq(TEST_AUTH_TOKEN), any())).thenReturn(TEST_USER_ID);
        when(esUtilService.searchDocumentsV2(eq(TEST_INDEX), any(SearchCriteria.class), eq(TEST_JSON_PATH)))
                .thenThrow(new RuntimeException("ES connection failed"));

        ApiResponse response = searchService.searchCbPlan(request, TEST_ORG_ID, TEST_AUTH_TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals("Error while processing search", response.getParams().getErr());
    }

    @Test
    void searchCbPlan_withOrgIdFilter_shouldAddOrgIdToSearchCriteria() {
        ApiRequest request = createApiRequestWithOrgIdFilter(true);
        SearchResult searchResult = new SearchResult();
        searchResult.setData(Collections.emptyList());

        when(validationService.validateAndExtractUserId(eq(TEST_AUTH_TOKEN), any())).thenReturn(TEST_USER_ID);
        when(esUtilService.searchDocumentsV2(eq(TEST_INDEX), any(SearchCriteria.class), eq(TEST_JSON_PATH)))
                .thenReturn(searchResult);

        searchService.searchCbPlan(request, TEST_ORG_ID, TEST_AUTH_TOKEN);

        verify(esUtilService).searchDocumentsV2(eq(TEST_INDEX), argThat(criteria -> {
            Map<String, Object> filter = criteria.getFilter();
            return filter != null && TEST_ORG_ID.equals(filter.get(Constants.ORG_ID_LIST));
        }), eq(TEST_JSON_PATH));
    }

    @Test
    void searchCbPlan_withOrgIdFilterDisabled_shouldNotAddOrgIdToSearchCriteria() {
        ApiRequest request = createApiRequestWithOrgIdFilter(false);
        SearchResult searchResult = new SearchResult();
        searchResult.setData(Collections.emptyList());

        when(validationService.validateAndExtractUserId(eq(TEST_AUTH_TOKEN), any())).thenReturn(TEST_USER_ID);
        when(esUtilService.searchDocumentsV2(eq(TEST_INDEX), any(SearchCriteria.class), eq(TEST_JSON_PATH)))
                .thenReturn(searchResult);

        searchService.searchCbPlan(request, TEST_ORG_ID, TEST_AUTH_TOKEN);

        verify(esUtilService).searchDocumentsV2(eq(TEST_INDEX), argThat(criteria -> {
            Map<String, Object> filter = criteria.getFilter();
            return filter == null || !filter.containsKey(Constants.ORG_ID_LIST);
        }), eq(TEST_JSON_PATH));
    }

    @Test
    void searchCbPlan_withV3SingleItemContentList_shouldConvertToV4() {
        ApiRequest request = createApiRequest();
        List<Map<String, Object>> esData = new ArrayList<>();
        Map<String, Object> searchRecord = new HashMap<>();
        searchRecord.put("id", "plan_1");
        searchRecord.put(Constants.CONTENT_LIST, List.of("do_123456"));
        esData.add(searchRecord);

        SearchResult searchResult = new SearchResult();
        searchResult.setData(esData);

        when(validationService.validateAndExtractUserId(eq(TEST_AUTH_TOKEN), any())).thenReturn(TEST_USER_ID);
        when(esUtilService.searchDocumentsV2(eq(TEST_INDEX), any(SearchCriteria.class), eq(TEST_JSON_PATH)))
                .thenReturn(searchResult);

        ApiResponse response = searchService.searchCbPlan(request, TEST_ORG_ID, TEST_AUTH_TOKEN);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(HttpStatus.OK, response.getResponseCode());

        SearchResult resultData = (SearchResult) response.getResult().get(Constants.RESULT);
        assertNotNull(resultData);
        List<Map<String, Object>> data = resultData.getData();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) data.get(0).get(Constants.CONTENT_LIST);

        assertEquals(1, contentList.size());
        assertEquals("do_123456", contentList.get(0).get(Constants.IDENTIFIER));
        assertEquals(false, contentList.get(0).get(Constants.MANDATORY));
    }

    @Test
    void searchCbPlan_withMultipleRecordsV3AndV4_shouldTransformAll() {
        ApiRequest request = createApiRequest();
        List<Map<String, Object>> esData = new ArrayList<>();

        Map<String, Object> v3Record = new HashMap<>();
        v3Record.put("id", "plan_v3");
        v3Record.put(Constants.CONTENT_LIST, List.of("do_v3_1", "do_v3_2"));
        esData.add(v3Record);

        Map<String, Object> v4Record = new HashMap<>();
        v4Record.put("id", "plan_v4");
        v4Record.put(Constants.CONTENT_LIST, List.of(
                "{\"identifier\":\"do_v4_1\",\"mandatory\":true}",
                "{\"identifier\":\"do_v4_2\",\"mandatory\":false}"
        ));
        esData.add(v4Record);

        SearchResult searchResult = new SearchResult();
        searchResult.setData(esData);

        when(validationService.validateAndExtractUserId(eq(TEST_AUTH_TOKEN), any())).thenReturn(TEST_USER_ID);
        when(esUtilService.searchDocumentsV2(eq(TEST_INDEX), any(SearchCriteria.class), eq(TEST_JSON_PATH)))
                .thenReturn(searchResult);

        ApiResponse response = searchService.searchCbPlan(request, TEST_ORG_ID, TEST_AUTH_TOKEN);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(HttpStatus.OK, response.getResponseCode());

        SearchResult resultData = (SearchResult) response.getResult().get(Constants.RESULT);
        assertNotNull(resultData);
        List<Map<String, Object>> data = resultData.getData();
        assertEquals(2, data.size());

        List<Map<String, Object>> v3ContentList = (List<Map<String, Object>>) data.get(0).get(Constants.CONTENT_LIST);
        assertEquals(2, v3ContentList.size());
        assertEquals(false, v3ContentList.get(0).get(Constants.MANDATORY));

            List<Map<String, Object>> v4ContentList = (List<Map<String, Object>>) data.get(1).get(Constants.CONTENT_LIST);
        assertEquals(2, v4ContentList.size());
        assertEquals(true, v4ContentList.get(0).get(Constants.MANDATORY));
        assertEquals(false, v4ContentList.get(1).get(Constants.MANDATORY));
    }

    private ApiRequest createApiRequest() {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("applyOrgIdFilter", false);
        request.setRequest(requestMap);
        return request;
    }

    private ApiRequest createApiRequestWithOrgIdFilter(boolean applyFilter) {
        ApiRequest request = new ApiRequest();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("applyOrgIdFilter", applyFilter);
        request.setRequest(requestMap);
        return request;
    }

    private List<Map<String, Object>> createEsDataWithV3ContentList() {
        List<Map<String, Object>> esData = new ArrayList<>();
        Map<String, Object> searchRecord = new HashMap<>();
        searchRecord.put("id", "plan_1");
        searchRecord.put("name", "Test Plan");
        searchRecord.put(Constants.CONTENT_LIST, List.of(
                "do_114376977434968064182",
                "do_114378386987417600180"
        ));
        esData.add(searchRecord);
        return esData;
    }

    private List<Map<String, Object>> createEsDataWithV4ContentList() {
        List<Map<String, Object>> esData = new ArrayList<>();
        Map<String, Object> searchRecord = new HashMap<>();
        searchRecord.put("id", "plan_2");
        searchRecord.put("name", "Test Plan V4");
        searchRecord.put(Constants.CONTENT_LIST, List.of(
                "{\"identifier\":\"do_114467322178428928111\",\"mandatory\":true}",
                "{\"identifier\":\"do_114378386987417600180\",\"mandatory\":false}"
        ));
        esData.add(searchRecord);
        return esData;
    }

    private List<Map<String, Object>> createEsDataWithEmptyContentList() {
        List<Map<String, Object>> esData = new ArrayList<>();
        Map<String, Object> searchRecord = new HashMap<>();
        searchRecord.put("id", "plan_3");
        searchRecord.put(Constants.CONTENT_LIST, Collections.emptyList());
        esData.add(searchRecord);
        return esData;
    }

    private List<Map<String, Object>> createEsDataWithoutContentList() {
        List<Map<String, Object>> esData = new ArrayList<>();
        Map<String, Object> searchRecord = new HashMap<>();
        searchRecord.put("id", "plan_4");
        searchRecord.put("name", "Plan without contentList");
        esData.add(searchRecord);
        return esData;
    }
}
