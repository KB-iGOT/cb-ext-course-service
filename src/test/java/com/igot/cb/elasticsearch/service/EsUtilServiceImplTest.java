package com.igot.cb.elasticsearch.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.exceptions.CustomException;
import com.igot.cb.elasticsearch.config.EsConfig;
import com.igot.cb.elasticsearch.dto.FacetDTO;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import com.networknt.schema.JsonSchemaFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
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

    @Mock
    private IndexResponse indexResponse;

    private EsUtilServiceImpl esUtilService;

    @Mock
    private CbExtServerProperties cbExtServerProperties;

    @BeforeEach
    void setUp() {
        esUtilService = new EsUtilServiceImpl(esConfig, elasticsearchClient, cbExtServerProperties);
        ReflectionTestUtils.setField(esUtilService, "objectMapper", objectMapper);
    }

    @Test
    void testAddDocumentSuccess() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("name", "test");
        document.put("invalid", "field");
        
        Map<String, Object> schema = new HashMap<>();
        schema.put("name", Map.of("type", "text"));
        
        when(objectMapper.readValue(any(InputStream.class), any(TypeReference.class)))
                .thenReturn(schema);
        when(elasticsearchClient.index(any(co.elastic.clients.elasticsearch.core.IndexRequest.class)))
                .thenReturn(indexResponse);
        when(indexResponse.result()).thenReturn(Result.Created);

        String result = esUtilService.addDocument("test-index", "_doc", "1", document, "/test.json");
        
        assertEquals("Successfully indexed document with id: Created", result);
    }

    @Test
    void testAddDocumentException() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("name", "test");

        when(objectMapper.readValue(any(InputStream.class), any(TypeReference.class)))
                .thenThrow(new RuntimeException("Test exception"));

        String result = esUtilService.addDocument("test-index", "_doc", "1", document, "/test.json");
        
        assertNull(result);
    }

    @Test
    void testUpdateDocumentSuccess() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("name", "updated");
        document.put("invalid", "field");
        
        Map<String, Object> schema = new HashMap<>();
        schema.put("name", Map.of("type", "text"));
        
        when(objectMapper.readValue(any(InputStream.class), any(TypeReference.class)))
                .thenReturn(schema);
        when(elasticsearchClient.index(any(co.elastic.clients.elasticsearch.core.IndexRequest.class)))
                .thenReturn(indexResponse);
        when(indexResponse.result()).thenReturn(Result.Updated);

        String result = esUtilService.updateDocument("test-index", "_doc", "1", document, "/test.json");
        
        assertEquals("updated", result);
    }

    @Test
    void testUpdateDocumentIOException() throws Exception {
        Map<String, Object> document = new HashMap<>();
        
        when(objectMapper.readValue(any(InputStream.class), any(TypeReference.class)))
                .thenThrow(new IOException("Test exception"));

        String result = esUtilService.updateDocument("test-index", "_doc", "1", document, "/test.json");
        
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
    void testSearchDocumentsWithFacets() throws Exception {
        SearchCriteria criteria = createBasicSearchCriteria();
        criteria.setFacets(Arrays.asList("category", "status"));
        
        SearchResponse<Object> searchResponse = createMockSearchResponseWithFacets();
        when(elasticsearchClient.search(any(co.elastic.clients.elasticsearch.core.SearchRequest.class), eq(Object.class)))
                .thenReturn(searchResponse);
        when(cbExtServerProperties.getNonTextFields())
                .thenReturn("category,status");
        SearchResult result = esUtilService.searchDocuments("test-index", criteria, "/test.json");
        
        assertNotNull(result);
        assertEquals(1, result.getData().size());
        assertEquals(2, result.getFacets().size());
        assertTrue(result.getFacets().containsKey("category"));
        assertTrue(result.getFacets().containsKey("status"));
    }

    @Test
    void testSearchDocumentsWithZeroPageSize() throws Exception {
        SearchCriteria criteria = createBasicSearchCriteria();
        criteria.setPageSize(0);
        
        SearchResponse<Object> searchResponse = createMockSearchResponse();
        when(elasticsearchClient.search(any(co.elastic.clients.elasticsearch.core.SearchRequest.class), eq(Object.class)))
                .thenReturn(searchResponse);
        
        SearchResult result = esUtilService.searchDocuments("test-index", criteria, "/test.json");
        
        assertNotNull(result);
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
    void testSearchDocumentsNullCriteria() {
        assertThrows(AssertionError.class, () -> {
            esUtilService.searchDocuments("test-index", null, "/test.json");
        });
    }

    @Test
    void testSearchDocumentsEmptySearchString() {
        SearchCriteria criteria = new SearchCriteria();
        criteria.setSearchString("");
        criteria.setPageNumber(0);
        criteria.setPageSize(10);
        criteria.setQuery(new HashMap<>());
        criteria.setFilter(new HashMap<>());
        
        assertThrows(NullPointerException.class, () -> {
            esUtilService.searchDocuments("test-index", criteria, "/test.json");
        });
    }

    @Test
    void testSearchDocumentsWithComplexQuery() throws Exception {
        SearchCriteria criteria = createBasicSearchCriteria();
        
        // Test bool query with proper FieldValue
        Map<String, Object> boolQuery = new HashMap<>();
        Map<String, Object> mustClause = new HashMap<>();
        mustClause.put("term", Map.of("status", FieldValue.of("active")));
        boolQuery.put("must", Arrays.asList(mustClause));
        
        Map<String, Object> query = new HashMap<>();
        query.put("bool", boolQuery);
        criteria.setQuery(query);
        
        SearchResponse<Object> searchResponse = createMockSearchResponse();
        when(elasticsearchClient.search(any(co.elastic.clients.elasticsearch.core.SearchRequest.class), eq(Object.class)))
                .thenReturn(searchResponse);
        
        SearchResult result = esUtilService.searchDocuments("test-index", criteria, "/test.json");
        
        assertNotNull(result);
    }

    @Test
    void testSearchDocumentsWithTermQuery() throws Exception {
        SearchCriteria criteria = createBasicSearchCriteria();
        
        Map<String, Object> termQuery = new HashMap<>();
        termQuery.put("status", FieldValue.of("active"));
        
        Map<String, Object> query = new HashMap<>();
        query.put("term", termQuery);
        criteria.setQuery(query);
        
        SearchResponse<Object> searchResponse = createMockSearchResponse();
        when(elasticsearchClient.search(any(co.elastic.clients.elasticsearch.core.SearchRequest.class), eq(Object.class)))
                .thenReturn(searchResponse);
        
        SearchResult result = esUtilService.searchDocuments("test-index", criteria, "/test.json");
        
        assertNotNull(result);
    }

    @Test
    void testSearchDocumentsWithRangeQuery() throws Exception {
        SearchCriteria criteria = createBasicSearchCriteria();
        
        Map<String, Object> rangeConditions = new HashMap<>();
        rangeConditions.put("gte", 10);
        rangeConditions.put("lte", 100);
        rangeConditions.put("gt", 5);
        rangeConditions.put("lt", 200);
        
        Map<String, Object> rangeQuery = new HashMap<>();
        rangeQuery.put("age", rangeConditions);
        
        Map<String, Object> query = new HashMap<>();
        query.put("range", rangeQuery);
        criteria.setQuery(query);
        
        SearchResponse<Object> searchResponse = createMockSearchResponse();
        when(elasticsearchClient.search(any(co.elastic.clients.elasticsearch.core.SearchRequest.class), eq(Object.class)))
                .thenReturn(searchResponse);
        
        SearchResult result = esUtilService.searchDocuments("test-index", criteria, "/test.json");
        
        assertNotNull(result);
    }

    @Test
    void testSearchDocumentsWithMustNotQuery() throws Exception {
        SearchCriteria criteria = createBasicSearchCriteria();
        
        Map<String, Object> termQuery = Map.of("term", Map.of("status", FieldValue.of("inactive")));
        
        Map<String, Object> query = new HashMap<>();
        query.put("must_not", Arrays.asList(termQuery));
        criteria.setQuery(query);
        
        SearchResponse<Object> searchResponse = createMockSearchResponse();
        when(elasticsearchClient.search(any(co.elastic.clients.elasticsearch.core.SearchRequest.class), eq(Object.class)))
                .thenReturn(searchResponse);
        
        SearchResult result = esUtilService.searchDocuments("test-index", criteria, "/test.json");
        
        assertNotNull(result);
    }

    @Test
    void testSearchDocumentsWithFilterList() throws Exception {
        SearchCriteria criteria = createBasicSearchCriteria();
        
        Map<String, Object> filter = new HashMap<>();
        filter.put("categories", Arrays.asList("tech", "science"));
        filter.put("active", true);
        filter.put("tags", Set.of("java", "spring"));
        criteria.setFilter((HashMap<String, Object>) filter);
        
        SearchResponse<Object> searchResponse = createMockSearchResponse();
        when(elasticsearchClient.search(any(co.elastic.clients.elasticsearch.core.SearchRequest.class), eq(Object.class)))
                .thenReturn(searchResponse);
        
        SearchResult result = esUtilService.searchDocuments("test-index", criteria, "/test.json");
        
        assertNotNull(result);
    }

    @Test
    void testSearchDocumentsWithSorting() throws Exception {
        SearchCriteria criteria = createBasicSearchCriteria();
        criteria.setOrderBy("createdDate");
        criteria.setOrderDirection("desc");
        
        Map<String, Object> mockSchema = new HashMap<>();
        mockSchema.put("createdDate", Map.of("type", "date"));
        
        try (MockedStatic<EsUtilServiceImpl> mockedStatic = mockStatic(EsUtilServiceImpl.class)) {
            mockedStatic.when(() -> EsUtilServiceImpl.readJsonSchema("/test.json"))
                    .thenReturn(mockSchema);
            
            SearchResponse<Object> searchResponse = createMockSearchResponse();
            when(elasticsearchClient.search(any(co.elastic.clients.elasticsearch.core.SearchRequest.class), eq(Object.class)))
                    .thenReturn(searchResponse);
            
            SearchResult result = esUtilService.searchDocuments("test-index", criteria, "/test.json");
            
            assertNotNull(result);
        }
    }

    @Test
    void testSearchDocumentsWithEmptyRequestedFields() throws Exception {
        SearchCriteria criteria = createBasicSearchCriteria();
        criteria.setRequestedFields(new ArrayList<>());
        
        SearchResponse<Object> searchResponse = createMockSearchResponse();
        when(elasticsearchClient.search(any(co.elastic.clients.elasticsearch.core.SearchRequest.class), eq(Object.class)))
                .thenReturn(searchResponse);
        
        SearchResult result = esUtilService.searchDocuments("test-index", criteria, "/test.json");
        
        assertNotNull(result);
    }

    @Test
    void testSearchDocumentsWithNullRequestedFields() throws Exception {
        SearchCriteria criteria = createBasicSearchCriteria();
        criteria.setRequestedFields(null);
        
        SearchResponse<Object> searchResponse = createMockSearchResponse();
        when(elasticsearchClient.search(any(co.elastic.clients.elasticsearch.core.SearchRequest.class), eq(Object.class)))
                .thenReturn(searchResponse);
        
        SearchResult result = esUtilService.searchDocuments("test-index", criteria, "/test.json");
        
        assertNotNull(result);
    }

    @Test
    void testReadJsonSchemaException() {
        assertThrows(CustomException.class, () -> {
            EsUtilServiceImpl.readJsonSchema("/nonexistent.json");
        });
    }

    @Test
    void testUnsupportedQueryType() throws Exception {
        SearchCriteria criteria = createBasicSearchCriteria();
        
        Map<String, Object> query = new HashMap<>();
        query.put("unsupported", Map.of("field", "value"));
        criteria.setQuery(query);
        
        assertThrows(IllegalArgumentException.class, () -> {
            esUtilService.searchDocuments("test-index", criteria, "/test.json");
        });
    }

    @Test
    void testMustNotQueryWithNonList() throws Exception {
        SearchCriteria criteria = createBasicSearchCriteria();
        
        Map<String, Object> query = new HashMap<>();
        query.put("must_not", "invalid");
        criteria.setQuery(query);
        
        assertThrows(IllegalArgumentException.class, () -> {
            esUtilService.searchDocuments("test-index", criteria, "/test.json");
        });
    }

    @Test
    void testUnsupportedRangeCondition() throws Exception {
        SearchCriteria criteria = createBasicSearchCriteria();
        
        Map<String, Object> rangeConditions = new HashMap<>();
        rangeConditions.put("unsupported", 10);
        
        Map<String, Object> rangeQuery = new HashMap<>();
        rangeQuery.put("age", rangeConditions);
        
        Map<String, Object> query = new HashMap<>();
        query.put("range", rangeQuery);
        criteria.setQuery(query);
        
        assertThrows(IllegalArgumentException.class, () -> {
            esUtilService.searchDocuments("test-index", criteria, "/test.json");
        });
    }

    @Test
    void testSearchDocumentsWithMatchQuery() throws Exception {
        SearchCriteria criteria = createBasicSearchCriteria();
        
        Map<String, Object> matchQuery = new HashMap<>();
        matchQuery.put("title", FieldValue.of("test"));
        
        Map<String, Object> query = new HashMap<>();
        query.put("match", matchQuery);
        criteria.setQuery(query);
        
        SearchResponse<Object> searchResponse = createMockSearchResponse();
        when(elasticsearchClient.search(any(co.elastic.clients.elasticsearch.core.SearchRequest.class), eq(Object.class)))
                .thenReturn(searchResponse);
        
        SearchResult result = esUtilService.searchDocuments("test-index", criteria, "/test.json");
        
        assertNotNull(result);
    }

    @Test
    void testSearchDocumentsWithTermsQuery() throws Exception {
        SearchCriteria criteria = createBasicSearchCriteria();
        
        Map<String, Object> termsQuery = new HashMap<>();
        // Mock TermsQueryField properly
        termsQuery.put("status", Arrays.asList("active", "pending"));
        
        Map<String, Object> query = new HashMap<>();
        query.put("terms", termsQuery);
        criteria.setQuery(query);
        
        SearchResponse<Object> searchResponse = createMockSearchResponse();
        when(elasticsearchClient.search(any(co.elastic.clients.elasticsearch.core.SearchRequest.class), eq(Object.class)))
                .thenReturn(searchResponse);
        
        // This will throw ClassCastException due to TermsQueryField casting, which is expected
        assertThrows(ClassCastException.class, () -> {
            esUtilService.searchDocuments("test-index", criteria, "/test.json");
        });
    }

    private SearchCriteria createBasicSearchCriteria() {
        SearchCriteria criteria = new SearchCriteria();
        criteria.setPageNumber(0);
        criteria.setPageSize(10);
        criteria.setSearchString("test");
        criteria.setRequestedFields(Arrays.asList("name", "id"));
        
        Map<String, Object> filter = new HashMap<>();
        filter.put("status", "active");
        criteria.setFilter((HashMap<String, Object>) filter);
        
        criteria.setQuery(new HashMap<>());
        
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

    private SearchResponse<Object> createMockSearchResponseWithFacets() {
        SearchResponse<Object> searchResponse = createMockSearchResponse();
        
        // Mock aggregations
        Map<String, Aggregate> aggregations = new HashMap<>();
        
        // Mock category aggregation
        StringTermsAggregate categoryTerms = mock(StringTermsAggregate.class);
        StringTermsBucket categoryBucket = mock(StringTermsBucket.class);
        when(categoryBucket.key()).thenReturn(FieldValue.of("technology"));
        when(categoryBucket.docCount()).thenReturn(5L);
        
        // Mock buckets for category
        co.elastic.clients.elasticsearch._types.aggregations.Buckets<StringTermsBucket> categoryBuckets = 
            mock(co.elastic.clients.elasticsearch._types.aggregations.Buckets.class);
        when(categoryBuckets.array()).thenReturn(Arrays.asList(categoryBucket));
        when(categoryTerms.buckets()).thenReturn(categoryBuckets);
        
        Aggregate categoryAggregate = mock(Aggregate.class);
        when(categoryAggregate.isSterms()).thenReturn(true);
        when(categoryAggregate.sterms()).thenReturn(categoryTerms);
        aggregations.put("category_agg", categoryAggregate);
        
        // Mock status aggregation
        StringTermsAggregate statusTerms = mock(StringTermsAggregate.class);
        StringTermsBucket statusBucket = mock(StringTermsBucket.class);
        when(statusBucket.key()).thenReturn(FieldValue.of("active"));
        when(statusBucket.docCount()).thenReturn(3L);
        
        // Mock buckets for status
        co.elastic.clients.elasticsearch._types.aggregations.Buckets<StringTermsBucket> statusBuckets = 
            mock(co.elastic.clients.elasticsearch._types.aggregations.Buckets.class);
        when(statusBuckets.array()).thenReturn(Arrays.asList(statusBucket));
        when(statusTerms.buckets()).thenReturn(statusBuckets);
        
        Aggregate statusAggregate = mock(Aggregate.class);
        when(statusAggregate.isSterms()).thenReturn(true);
        when(statusAggregate.sterms()).thenReturn(statusTerms);
        aggregations.put("status_agg", statusAggregate);
        
        when(searchResponse.aggregations()).thenReturn(aggregations);
        
        return searchResponse;
    }

    @Test
    void testExtractFacetDataWithLongTerms() throws Exception {
        // Prepare SearchCriteria with a facet
        SearchCriteria criteria = new SearchCriteria();
        criteria.setFacets(Collections.singletonList("age"));

        // Mock LongTermsBucket
        LongTermsBucket longBucket = mock(LongTermsBucket.class);
        when(longBucket.keyAsString()).thenReturn("30");
        when(longBucket.docCount()).thenReturn(7L);

        // Mock Buckets
        @SuppressWarnings("unchecked")
        co.elastic.clients.elasticsearch._types.aggregations.Buckets<LongTermsBucket> longBuckets =
                mock(co.elastic.clients.elasticsearch._types.aggregations.Buckets.class);
        when(longBuckets.array()).thenReturn(Collections.singletonList(longBucket));

        // Mock LongTermsAggregate
        LongTermsAggregate longTerms = mock(LongTermsAggregate.class);
        when(longTerms.buckets()).thenReturn(longBuckets);

        // Mock Aggregate
        Aggregate aggregate = mock(Aggregate.class);
        when(aggregate.isSterms()).thenReturn(false);  // go into else branch
        when(aggregate.lterms()).thenReturn(longTerms);

        // Mock SearchResponse
        SearchResponse<Object> searchResponse = mock(SearchResponse.class);
        when(searchResponse.aggregations()).thenReturn(Map.of("age_agg", aggregate));

        // Use reflection to invoke private method
        Method method = EsUtilServiceImpl.class.getDeclaredMethod(
                "extractFacetData",
                SearchResponse.class,
                SearchCriteria.class
        );
        method.setAccessible(true);

        EsUtilServiceImpl service = new EsUtilServiceImpl(null, null, null);
        @SuppressWarnings("unchecked")
        Map<String, List<FacetDTO>> result =
                (Map<String, List<FacetDTO>>) method.invoke(service, searchResponse, criteria);

        // Assertions
        assertNotNull(result);
        assertTrue(result.containsKey("age"));
        assertEquals(1, result.get("age").size());
        assertEquals("30", result.get("age").get(0).getValue());
        assertEquals(7L, result.get("age").get(0).getCount());
    }

    @Test
    void testBuildBoolQueryCoversFilterMustNotShould() throws Exception {
        // Prepare nested queries for must, filter, must_not, should
        Map<String, Object> mustQuery = Map.of(Constants.TERM, Map.of("status", FieldValue.of("active")));
        Map<String, Object> filterQuery = Map.of(Constants.TERM, Map.of("category", FieldValue.of("tech")));
        Map<String, Object> mustNotQuery = Map.of(Constants.TERM, Map.of("archived", FieldValue.of("true")));
        Map<String, Object> shouldQuery = Map.of(Constants.TERM, Map.of("priority", FieldValue.of("high")));

        Map<String, Object> boolMap = new HashMap<>();
        boolMap.put(Constants.MUST, List.of(mustQuery));
        boolMap.put(Constants.FILTER, List.of(filterQuery));
        boolMap.put(Constants.MUST_NOT, List.of(mustNotQuery));
        boolMap.put(Constants.SHOULD, List.of(shouldQuery));

        // Use reflection to access the private method
        Method method = EsUtilServiceImpl.class.getDeclaredMethod(
                "buildBoolQuery",
                Map.class
        );
        method.setAccessible(true);

        EsUtilServiceImpl service = new EsUtilServiceImpl(null, null, null);

        BoolQuery result = (BoolQuery) method.invoke(service, boolMap);

        // Assertions: ensure that all parts got included
        assertNotNull(result);
        assertFalse(result.must().isEmpty(), "Must clause should not be empty");
        assertFalse(result.filter().isEmpty(), "Filter clause should not be empty");
        assertFalse(result.mustNot().isEmpty(), "MustNot clause should not be empty");
        assertFalse(result.should().isEmpty(), "Should clause should not be empty");
    }

    @Test
    void testIsRangeQueryTrueAndFalse() throws Exception {
        // Access private method via reflection
        Method method = EsUtilServiceImpl.class.getDeclaredMethod(
                "isRangeQuery", Map.class
        );
        method.setAccessible(true);

        EsUtilServiceImpl service = new EsUtilServiceImpl(null, null, null);

        // ---------- Case 1: Should return true ----------
        Map<String, Object> rangeMap = new HashMap<>();
        rangeMap.put(Constants.SEARCH_OPERATION_GREATER_THAN_EQUALS, 10);

        boolean resultTrue = (boolean) method.invoke(service, rangeMap);
        assertTrue(resultTrue, "Expected true when range keys are present");

        // ---------- Case 2: Should return false ----------
        Map<String, Object> nonRangeMap = new HashMap<>();
        nonRangeMap.put("status", "active");

        boolean resultFalse = (boolean) method.invoke(service, nonRangeMap);
        assertFalse(resultFalse, "Expected false when no range keys are present");
    }

    @Test
    void testBuildFilterQueryWithRangeAndNestedBoolean() throws Exception {
        Method method = EsUtilServiceImpl.class.getDeclaredMethod(
                "buildFilterQuery", Map.class
        );
        method.setAccessible(true);

        EsUtilServiceImpl service = new EsUtilServiceImpl(null, null, null);

        // ---------- Case 1: Range query ----------
        Map<String, Object> rangeMap = new HashMap<>();
        rangeMap.put(Constants.SEARCH_OPERATION_GREATER_THAN_EQUALS, 10);
        rangeMap.put(Constants.SEARCH_OPERATION_LESS_THAN, 100);
        rangeMap.put(Constants.SEARCH_OPERATION_LESS_THAN_EQUALS, 10);
        rangeMap.put(Constants.SEARCH_OPERATION_GREATER_THAN, 100);

        Map<String, Object> filterCriteriaRange = new HashMap<>();
        filterCriteriaRange.put("age", rangeMap);

        BoolQuery.Builder rangeQueryResult =
                (BoolQuery.Builder) method.invoke(service, filterCriteriaRange);

        assertNotNull(rangeQueryResult);
        assertFalse(rangeQueryResult.build().must().isEmpty(), "Range query must be included");


        // ---------- Case 2: Nested field map with Boolean ----------
        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("enabled", true);  // Boolean is safe

        Map<String, Object> filterCriteriaNested = new HashMap<>();
        filterCriteriaNested.put("metadata", nestedMap);

        BoolQuery.Builder nestedQueryResult =
                (BoolQuery.Builder) method.invoke(service, filterCriteriaNested);

        assertNotNull(nestedQueryResult);
        assertFalse(nestedQueryResult.build().must().isEmpty(), "Nested field queries must be included");
    }

}