package com.igot.cb.elasticsearch.service;

import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EsUtilServiceTest {

    @Mock
    private EsUtilService esUtilService;

    @Test
    void testAddDocument() {
        String esIndexName = "test-index";
        String type = "_doc";
        String id = "123";
        Map<String, Object> document = new HashMap<>();
        document.put("name", "test");
        String jsonFilePath = "/test.json";

        String expectedResult = "Success";
        when(esUtilService.addDocument(esIndexName, type, id, document, jsonFilePath)).thenReturn(expectedResult);

        String result = esUtilService.addDocument(esIndexName, type, id, document, jsonFilePath);

        assertEquals(expectedResult, result);
        verify(esUtilService).addDocument(esIndexName, type, id, document, jsonFilePath);
    }

    @Test
    void testUpdateDocument() {
        String index = "test-index";
        String indexType = "_doc";
        String entityId = "123";
        Map<String, Object> document = new HashMap<>();
        document.put("name", "updated");
        String jsonFilePath = "/test.json";

        String expectedResult = "Updated";
        when(esUtilService.updateDocument(index, indexType, entityId, document, jsonFilePath)).thenReturn(expectedResult);

        String result = esUtilService.updateDocument(index, indexType, entityId, document, jsonFilePath);

        assertEquals(expectedResult, result);
        verify(esUtilService).updateDocument(index, indexType, entityId, document, jsonFilePath);
    }

    @Test
    void testSearchDocuments() {
        String esIndexName = "test-index";
        SearchCriteria searchCriteria = new SearchCriteria();
        String elasticCbPlanJsonPath = "/test.json";

        SearchResult expectedResult = new SearchResult();
        when(esUtilService.searchDocuments(esIndexName, searchCriteria, elasticCbPlanJsonPath)).thenReturn(expectedResult);

        SearchResult result = esUtilService.searchDocuments(esIndexName, searchCriteria, elasticCbPlanJsonPath);

        assertEquals(expectedResult, result);
        verify(esUtilService).searchDocuments(esIndexName, searchCriteria, elasticCbPlanJsonPath);
    }
}