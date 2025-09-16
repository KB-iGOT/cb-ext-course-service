package com.igot.cb.elasticsearch.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.exceptions.CustomException;
import com.igot.cb.elasticsearch.config.EsConfig;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class EsUtilServiceImplTest {

    @Mock
    private EsConfig esConfig;

    @Mock
    private ElasticsearchClient elasticsearchClient;

    @Mock
    private ObjectMapper objectMapper;

    private EsUtilServiceImpl esUtilService;

    @BeforeEach
    void setUp() {
        esUtilService = new EsUtilServiceImpl(esConfig, elasticsearchClient);
        ReflectionTestUtils.setField(esUtilService, "objectMapper", objectMapper);
    }

    @Test
    void testAddDocumentException() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("name", "test");

        // Mock to throw exception which will be caught and return null
        when(objectMapper.readValue(any(InputStream.class), any(com.fasterxml.jackson.core.type.TypeReference.class)))
                .thenThrow(new RuntimeException("Test exception"));

        String result = esUtilService.addDocument("test-index", "_doc", "1", document, "/test.json");
        
        assertNull(result);
    }









    @Test
    void testSearchDocumentsSuccess() throws Exception {
        SearchCriteria criteria = createBasicSearchCriteria();
        
        SearchResponse<Object> searchResponse = createMockSearchResponse();
        when(elasticsearchClient.search(any(co.elastic.clients.elasticsearch.core.SearchRequest.class), eq(Object.class)))
                .thenReturn(searchResponse);

        SearchResult result = esUtilService.searchDocuments("test-index", criteria, "/test.json");
        
        assertNotNull(result);
        assertEquals(1, result.getData().size());
        assertEquals(1L, result.getTotalCount());
    }

    @Test
    void testSearchDocumentsIOException() throws Exception {
        SearchCriteria criteria = createBasicSearchCriteria();
        
        when(elasticsearchClient.search(any(co.elastic.clients.elasticsearch.core.SearchRequest.class), eq(Object.class)))
                .thenThrow(new IOException("Search failed"));

        SearchResult result = esUtilService.searchDocuments("test-index", criteria, "/test.json");
        
        assertNull(result);
    }

    @Test
    void testReadJsonSchemaException() {
        assertThrows(CustomException.class, () -> {
            EsUtilServiceImpl.readJsonSchema("/nonexistent.json");
        });
    }

    private SearchCriteria createBasicSearchCriteria() {
        SearchCriteria criteria = new SearchCriteria();
        criteria.setPageNumber(0);
        criteria.setPageSize(10);
        criteria.setSearchString("test");
        
        Map<String, Object> filter = new HashMap<>();
        filter.put("status", "active");
        criteria.setFilter((HashMap<String, Object>) filter);
        
        return criteria;
    }

    private SearchResponse<Object> createMockSearchResponse() {
        SearchResponse<Object> searchResponse = mock(SearchResponse.class);
        HitsMetadata<Object> hits = mock(HitsMetadata.class);
        TotalHits totalHits = mock(TotalHits.class);
        Hit<Object> hit = mock(Hit.class);
        
        Map<String, Object> source = new HashMap<>();
        source.put("id", "1");
        source.put("name", "test");
        
        when(hit.source()).thenReturn(source);
        when(hits.hits()).thenReturn(Arrays.asList(hit));
        when(totalHits.value()).thenReturn(1L);
        when(hits.total()).thenReturn(totalHits);
        when(searchResponse.hits()).thenReturn(hits);
        when(searchResponse.aggregations()).thenReturn(new HashMap<>());
        
        return searchResponse;
    }
}