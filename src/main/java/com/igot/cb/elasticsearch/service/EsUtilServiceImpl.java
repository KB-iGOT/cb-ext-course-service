package com.igot.cb.elasticsearch.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.SourceConfig;
import co.elastic.clients.json.JsonData;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.elasticsearch.dto.FacetDTO;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import com.networknt.schema.JsonSchemaFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.igot.common.CustomException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@Slf4j
@SuppressWarnings({"unchecked","deprecation"}) // deprecation: legacy factory usage for ES6 compatibility; unchecked: dynamic query map casting
public class EsUtilServiceImpl implements EsUtilService {
    private final ElasticsearchClient elasticsearchClient;
    private final CbExtServerProperties cbExtServerProperties;
    private ObjectMapper objectMapper;

    private static final Map<String, Map<String, Object>> schemaCache = new ConcurrentHashMap<>();

    public EsUtilServiceImpl(ElasticsearchClient elasticsearchClient, CbExtServerProperties cbExtServerProperties, ObjectMapper objectMapper) {
        this.cbExtServerProperties = cbExtServerProperties;
        this.elasticsearchClient = elasticsearchClient;
        this.objectMapper = objectMapper;
    }

    @Override
    public String addDocument(
            String esIndexName, String type, String id, Map<String, Object> document, String jsonFilePath) {
        log.info("EsUtilServiceImpl :: addDocument");
        try {
            JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance();
            InputStream schemaStream = schemaFactory.getClass().getResourceAsStream(jsonFilePath);
            Map<String, Object> map = objectMapper.readValue(schemaStream,
                    new TypeReference<Map<String, Object>>() {
                    });
            Iterator<Map.Entry<String, Object>> iterator = document.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                String key = entry.getKey();
                if (!map.containsKey(key)) {
                    iterator.remove();
                }
            }
            IndexRequest<Map<String,Object>> indexRequest = new IndexRequest.Builder<Map<String, Object>>()
                    .index(esIndexName)
                    .id(id)
                    .document(document)
                    .refresh(Refresh.True)
                    .build();
            IndexResponse response = elasticsearchClient.index(indexRequest);
            return "Successfully indexed document with id: " + response.result();
        } catch (Exception e) {
            log.error("Issue while Indexing to es: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public String updateDocument(
            String index, String indexType, String entityId, Map<String, Object> updatedDocument, String jsonFilePath) {
        try {
            // 1. Filter incoming map using schema (same logic as addDocument)
            JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance();
            try (InputStream schemaStream = schemaFactory.getClass().getResourceAsStream(jsonFilePath)) {
                Map<String, Object> schemaMap = objectMapper.readValue(schemaStream, new TypeReference<Map<String, Object>>() {});
                updatedDocument.entrySet().removeIf(e -> !schemaMap.containsKey(e.getKey()));
            }

            Map<String, Object> merged = new HashMap<>();
            boolean existingFound = false;
            try {
                GetResponse<Object> existing = elasticsearchClient.get(builder -> builder.index(index).id(entityId), Object.class);
                if (existing.found()) {
                    Object src = existing.source();
                    if (src instanceof Map) {
                        merged.putAll((Map<String,Object>) src);
                        existingFound = true;
                    }
                }
            } catch (Exception getEx) {
                log.debug("ES get (for merge) failed for index={}, id={}, treating as upsert. Cause: {}", index, entityId, getEx.getMessage());
            }

            // 2. Merge (overwrite / add updated fields only)
            merged.putAll(updatedDocument);

            // 3. Index (acts as create or replace). We want refresh so subsequent reads see changes.
            IndexRequest<Map<String,Object>> indexRequest = new IndexRequest.Builder<Map<String, Object>>()
                    .index(index)
                    .id(entityId)
                    .document(merged)
                    .refresh(Refresh.True)
                    .build();
            IndexResponse response = elasticsearchClient.index(indexRequest);
            return (existingFound ? "updated" : "created") + ":" + response.result().jsonValue();
        } catch (Exception e) {
            log.error("Error performing merge+index update for index={}, id={}: {}", index, entityId, e.getMessage(), e);
            return null;
        }
    }

    @Override
    public SearchResult searchDocuments(String esIndexName, SearchCriteria searchCriteria, String jsonFilePath) {
        SearchRequest.Builder searchRequestBuilder = buildSearchRequest(searchCriteria, jsonFilePath);
        assert searchRequestBuilder != null;
        searchRequestBuilder.index(esIndexName);
        try {
            if (searchCriteria != null) {
                int pageNumber = searchCriteria.getPageNumber();
                int pageSize = searchCriteria.getPageSize();
                int from = pageNumber * pageSize;
                searchRequestBuilder.from(from);
                if (pageSize > 0) {
                    searchRequestBuilder.size(pageSize);
                }
            }
            SearchRequest searchRequest = searchRequestBuilder.build();
            log.info("Final search query: {}", searchRequest.toString());
            SearchResponse<Object> paginatedSearchResponse =
                    elasticsearchClient.search(searchRequest, Object.class);
            List<Map<String, Object>> paginatedResult = extractPaginatedResult(paginatedSearchResponse);
            Map<String, List<FacetDTO>> fieldAggregations =
                    extractFacetData(paginatedSearchResponse, searchCriteria);
            SearchResult searchResult = new SearchResult();
            searchResult.setData(paginatedResult);
            searchResult.setFacets(fieldAggregations);
            long totalHits = 0L;
            var hitsMeta = paginatedSearchResponse.hits();
            if (hitsMeta != null) {
                var totalObj = hitsMeta.total();
                if (totalObj != null) {
                    totalHits = totalObj.value();
                }
            }
            searchResult.setTotalCount(totalHits);
            return searchResult;
        } catch (IOException e) {
            log.error("Error while fetching details from elastic search");
            return null;
        }
    }

    private Map<String, List<FacetDTO>> extractFacetData(
            SearchResponse<Object> searchResponse, SearchCriteria searchCriteria) {

        Map<String, List<FacetDTO>> fieldAggregations = new HashMap<>();

        if (searchCriteria.getFacets() == null) {
            return fieldAggregations;
        }

        for (String field : searchCriteria.getFacets()) {
            Aggregate aggregate = searchResponse.aggregations().get(field + "_agg");
            if (aggregate == null) continue;

            List<FacetDTO> facetList = extractFacetList(aggregate);
            if (!facetList.isEmpty()) {
                fieldAggregations.put(field, facetList);
            }
        }
        return fieldAggregations;
    }

    private List<FacetDTO> extractFacetList(Aggregate aggregate) {
        return aggregate.isSterms()
                ? extractStringFacets(aggregate.sterms().buckets().array())
                : extractLongFacets(aggregate.lterms().buckets().array());
    }

    private List<FacetDTO> extractStringFacets(List<StringTermsBucket> buckets) {
        List<FacetDTO> list = new ArrayList<>();
        for (StringTermsBucket bucket : buckets) {
            String key = bucket.key().stringValue();
            if (!key.isEmpty()) {
                list.add(new FacetDTO(key, bucket.docCount()));
            }
        }
        return list;
    }

    private List<FacetDTO> extractLongFacets(List<LongTermsBucket> buckets) {
        List<FacetDTO> list = new ArrayList<>();
        for (LongTermsBucket bucket : buckets) {
            list.add(new FacetDTO(bucket.keyAsString(), bucket.docCount()));
        }
        return list;
    }


    private List<Map<String, Object>> extractPaginatedResult(SearchResponse<Object> paginatedSearchResponse) {
        List<Map<String, Object>> paginatedResult = new ArrayList<>();
        for (Hit<Object> hit : paginatedSearchResponse.hits().hits()) {
            paginatedResult.add((Map<String, Object>) hit.source());
        }
        return paginatedResult;
    }

    private SearchRequest.Builder buildSearchRequest(SearchCriteria searchCriteria, String JsonFilePath) {
        log.info("Building search query");
        if (searchCriteria == null || searchCriteria.toString().isEmpty()) {
            log.error("Search criteria body is missing");
            return null;
        }
        BoolQuery.Builder boolQueryBuilder = buildFilterQuery(searchCriteria.getFilter());
        SearchRequest.Builder searchSourceBuilder = new SearchRequest.Builder();
        searchSourceBuilder.query(boolQueryBuilder.build()._toQuery());
        addSortToSearchSourceBuilder(searchCriteria, searchSourceBuilder, JsonFilePath);
        addRequestedFieldsToSearchSourceBuilder(searchCriteria, searchSourceBuilder);
        String searchString = searchCriteria.getSearchString();
        if (isNotBlank(searchString)) {
            boolQueryBuilder.must(
                    Query.of(q -> q.match(m -> m.field(Constants.NAME).query(searchString)))
            );
        }
        addFacetsToSearchSourceBuilder(searchCriteria.getFacets(), searchSourceBuilder);
        Query queryPart = buildQueryPart(searchCriteria.getQuery());
        boolQueryBuilder.must(queryPart);
        log.info("final search query result {}", searchSourceBuilder);
        return searchSourceBuilder;
    }

    private Query buildQueryPart(Map<String, Object> queryMap) {
        log.info("Search:: buildQueryPart");
        if (queryMap == null || queryMap.isEmpty()) {
            return QueryBuilders.matchAll().build()._toQuery();
        }
        for (Map.Entry<String, Object> entry : queryMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            switch (key) {
                case Constants.BOOL:
                    return buildBoolQuery((Map<String, Object>) value)._toQuery();
                case Constants.TERM:
                    return buildTermQuery((Map<String, Object>) value);
                case Constants.TERMS:
                    return buildTermsQuery((Map<String, Object>) value);
                case Constants.MATCH:
                    return buildMatchQuery((Map<String, Object>) value);
                case Constants.RANGE:
                    return buildRangeQuery((Map<String, Object>) value);
                case Constants.MUST_NOT:
                    if (value instanceof List) {
                        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
                        for (Object item : (List<?>) value) {
                            if (item instanceof Map) {
                                boolQueryBuilder.mustNot(buildQueryPart((Map<String, Object>) item));
                            }
                        }
                        return boolQueryBuilder.build()._toQuery();
                    } else {
                        throw new IllegalArgumentException("must_not value should be a list of conditions");
                    }
                default:
                    throw new IllegalArgumentException(Constants.UNSUPPORTED_QUERY + key);
            }
        }
        return null;
    }

    private Query buildMatchQuery(Map<String, Object> matchMap) {
        log.info("search:: buildMatchQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        for (Map.Entry<String, Object> entry : matchMap.entrySet()) {
            boolQueryBuilder.must(QueryBuilders.match(m -> m.field(entry.getKey()).query((FieldValue) entry.getValue())));
        }
        return boolQueryBuilder.build()._toQuery();
    }

    private Query buildTermsQuery(Map<String, Object> termsMap) {
        log.info("search:: buildTermsQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        for (Map.Entry<String, Object> entry : termsMap.entrySet()) {
            boolQueryBuilder.must(QueryBuilders.terms(t -> t.field(entry.getKey()).terms((TermsQueryField) entry.getValue())));
        }
        return boolQueryBuilder.build()._toQuery();
    }

    private Query buildRangeQuery(Map<String, Object> rangeMap) {
        log.info("search:: buildRangeQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        for (Map.Entry<String, Object> entry : rangeMap.entrySet()) {
            Map<String, Object> rangeConditions = (Map<String, Object>) entry.getValue();
            RangeQuery.Builder rangeQueryBuilder = new RangeQuery.Builder().field(entry.getKey());
            rangeConditions.forEach((condition, value) -> {
                switch (condition) {
                    case "gt":
                        rangeQueryBuilder.gt(JsonData.of(value));
                        break;
                    case "gte":
                        rangeQueryBuilder.gte(JsonData.of(value));
                        break;
                    case "lt":
                        rangeQueryBuilder.lt(JsonData.of(value));
                        break;
                    case "lte":
                        rangeQueryBuilder.lte(JsonData.of(value));
                        break;
                    default:
                        throw new IllegalArgumentException(Constants.UNSUPPORTED_RANGE + condition);
                }
            });
            boolQueryBuilder.must(rangeQueryBuilder.build()._toQuery());
        }
        return boolQueryBuilder.build()._toQuery();
    }

    private BoolQuery buildBoolQuery(Map<String, Object> boolMap) {
        log.info("Search:: builderBoolQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        if (boolMap.containsKey(Constants.MUST)) {
            List<Map<String, Object>> mustList = (List<Map<String, Object>>) boolMap.get("must");
            mustList.forEach(must -> boolQueryBuilder.must(buildQueryPart(must)));
        }
        if (boolMap.containsKey(Constants.FILTER)) {
            List<Map<String, Object>> filterList = (List<Map<String, Object>>) boolMap.get("filter");
            filterList.forEach(filter -> boolQueryBuilder.filter(buildQueryPart(filter)));
        }
        if (boolMap.containsKey(Constants.MUST_NOT)) {
            List<Map<String, Object>> mustNotList = (List<Map<String, Object>>) boolMap.get("must_not");
            mustNotList.forEach(mustNot -> boolQueryBuilder.mustNot(buildQueryPart(mustNot)));
        }
        if (boolMap.containsKey(Constants.SHOULD)) {
            List<Map<String, Object>> shouldList = (List<Map<String, Object>>) boolMap.get("should");
            shouldList.forEach(should -> boolQueryBuilder.should(buildQueryPart(should)));
        }
        return boolQueryBuilder.build();
    }

    private Query buildTermQuery(Map<String, Object> termMap) {
        log.info("search::buildTermQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        for (Map.Entry<String, Object> entry : termMap.entrySet()) {
            boolQueryBuilder.must(QueryBuilders.term(t -> t.field(entry.getKey()).value((FieldValue) entry.getValue())));
        }
        return boolQueryBuilder.build()._toQuery();
    }

    private void addFacetsToSearchSourceBuilder(
            List<String> facets, SearchRequest.Builder searchRequestBuilder) {
        if (facets != null && !facets.isEmpty()) {
            Map<String, Aggregation> aggregationMap = new HashMap<>();

            for (String field : facets) {
                Aggregation aggregation;
                if (cbExtServerProperties.getNonTextFields().contains(field)) {
                    aggregation = Aggregation.of(a -> a.terms(
                            t -> t.field(field).size(250)));
                } else {
                    aggregation = Aggregation.of(a -> a.terms(
                            t -> t.field(field + ".keyword").size(250)));
                }
                aggregationMap.put(field + "_agg", aggregation);
            }
            searchRequestBuilder.aggregations(aggregationMap);
        }
    }

    private BoolQuery.Builder buildFilterQuery(Map<String, Object> filterCriteriaMap) {
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        List<Query> mustNotQueries = new ArrayList<>();
        List<Query> boolQueries = new ArrayList<>();
        if (filterCriteriaMap != null) {
            filterCriteriaMap.forEach(
                    (field, value) -> {
                        if (field.equals("must_not") && value instanceof ArrayList) {
                            mustNotQueries.add(Query.of(q ->q.termsSet(t->t.field(field).terms((ArrayList<String>) value))));
                        } else if (value instanceof Boolean) {
                            boolQueries.add(Query.of(q ->q.term(t->t.field(field).value((boolean)value))));
                        } else if (value instanceof List<?>) {
                            List<FieldValue> termsList = ((List<?>) value).stream()
                                    .map(v -> FieldValue.of(v.toString()))
                                    .collect(Collectors.toList());
                            if (cbExtServerProperties.getNonTextFields().contains(field)) {
                                boolQueryBuilder.must(Query.of(q -> q.terms(t -> t.field(field).terms(terms -> terms.value(termsList)))));
                            } else {
                                boolQueryBuilder.must(Query.of(q -> q.terms(t -> t.field(field + Constants.KEYWORD).terms(terms -> terms.value(termsList)))));
                            }
                        } else if (value instanceof String strValue) {
                            boolQueryBuilder.must(Query.of(q -> q.terms(t ->
                                    t.field(field + Constants.KEYWORD)
                                            .terms(terms -> terms.value(List.of(FieldValue.of(strValue))))
                            )));
                        } else if (value instanceof Set) {
                            Set<String> termsSet = (Set<String>) value;
                            List<FieldValue> termsList = termsSet.stream()
                                    .map(FieldValue::of)
                                    .toList();
                            boolQueryBuilder.must(Query.of(q -> q.terms(t -> t.field(field + Constants.KEYWORD).terms(terms -> terms.value(termsList)))));
                        } else if (value instanceof Map) {
                            Map<String, Object> nestedMap = (Map<String, Object>) value;
                            if (isRangeQuery(nestedMap)) {
                                // Handle range query
                                BoolQuery.Builder rangeOrNullQuery = QueryBuilders.bool();
                                RangeQuery.Builder rangeQuery = QueryBuilders.range().field(field);
                                nestedMap.forEach((rangeOperator, rangeValue) -> {
                                    switch (rangeOperator) {
                                        case Constants.SEARCH_OPERATION_GREATER_THAN_EQUALS:
                                            rangeQuery.gte(JsonData.of(rangeValue));
                                            break;
                                        case Constants.SEARCH_OPERATION_LESS_THAN_EQUALS:
                                            rangeQuery.lte(JsonData.of(rangeValue));
                                            break;
                                        case Constants.SEARCH_OPERATION_GREATER_THAN:
                                            rangeQuery.gt(JsonData.of(rangeValue));
                                            break;
                                        case Constants.SEARCH_OPERATION_LESS_THAN:
                                            rangeQuery.lt(JsonData.of(rangeValue));
                                            break;
                                        default:
                                            throw new IllegalArgumentException(Constants.UNSUPPORTED_RANGE + rangeValue);
                                    }
                                });
                                rangeOrNullQuery.should(rangeQuery.build()._toQuery());
                                rangeOrNullQuery.should(Query.of(q -> q.bool(b -> b.mustNot(Query.of(qn -> qn.exists(e -> e.field(field)))))));
                                boolQueryBuilder.must(rangeOrNullQuery.build()._toQuery());
                            } else {
                                nestedMap.forEach((nestedField, nestedValue) -> {
                                    String fullPath = field + "." + nestedField;
                                    if (nestedValue instanceof Boolean booleanValue) {
                                        boolQueryBuilder.must(Query.of(q -> q.term(t -> t.field(fullPath).value(booleanValue))));
                                    } else if (nestedValue instanceof String strValue) {
                                        List<FieldValue> termList = Collections.singletonList(FieldValue.of(strValue));
                                        boolQueryBuilder.must(Query.of(q -> q.terms(t -> t.field(fullPath + Constants.KEYWORD).terms((TermsQueryField) termList))));
                                    } else if (nestedValue instanceof ArrayList) {
                                        boolQueryBuilder.must(Query.of(q -> q.terms(t -> t.field(fullPath + Constants.KEYWORD).terms((TermsQueryField) nestedValue))));
                                    }
                                });
                            }
                        }
                    });
            mustNotQueries.forEach(mustNotQuery -> boolQueryBuilder.mustNot(mustNotQuery));
            boolQueries.forEach(boolQuery -> boolQueryBuilder.must(boolQuery));
        }
        return boolQueryBuilder;
    }

    private boolean isRangeQuery(Map<String, Object> nestedMap) {
        return nestedMap.keySet().stream().anyMatch(key -> key.equals(Constants.SEARCH_OPERATION_GREATER_THAN_EQUALS) ||
                key.equals(Constants.SEARCH_OPERATION_LESS_THAN_EQUALS) || key.equals(Constants.SEARCH_OPERATION_GREATER_THAN) ||
                key.equals(Constants.SEARCH_OPERATION_LESS_THAN));
    }

    private boolean isNotBlank(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private void addSortToSearchSourceBuilder( SearchCriteria searchCriteria,
                                               SearchRequest.Builder searchRequestBuilder,
                                               String jsonFilePath) {

        if (isNotBlank(searchCriteria.getOrderBy()) && isNotBlank(searchCriteria.getOrderDirection())) {
            String sortField = searchCriteria.getOrderBy();
            Map<String, Object> schemaMap = readJsonSchema(jsonFilePath);
            Map<String, Object> fieldMap = (Map<String, Object>) schemaMap.get(sortField);

            if (MapUtils.isEmpty(fieldMap) ||
                    (!Constants.NUMBER.equals(fieldMap.get(Constants.TYPE)) && !Constants.LONG.equals(fieldMap.get(Constants.TYPE)) && !Constants.DATE.equals(fieldMap.get(Constants.TYPE)))) {
                sortField += Constants.KEYWORD;
            }

            String finalSortField = sortField;
            searchRequestBuilder.sort(s -> s.field(f -> f
                    .field(finalSortField)
                    .order(Constants.ASC.equalsIgnoreCase(searchCriteria.getOrderDirection()) ? SortOrder.Asc : SortOrder.Desc)
            ));
        }
    }

    private void addRequestedFieldsToSearchSourceBuilder(
            SearchCriteria searchCriteria, SearchRequest.Builder searchRequestBuilder) {
        if (searchCriteria.getRequestedFields() == null) {
            // Get all fields in response
            searchRequestBuilder.source(SourceConfig.of(sc -> sc.fetch(true)));
        } else {
            if (searchCriteria.getRequestedFields().isEmpty()) {
                log.error("Please specify at least one field to include in the results.");
            }
            searchRequestBuilder.source(SourceConfig.of(sc -> sc.filter(filter -> filter.includes(searchCriteria.getRequestedFields()))));
        }
    }


    public static Map<String, Object> readJsonSchema(String jsonFilePath) {
        if (schemaCache.containsKey(jsonFilePath)) {
            return schemaCache.get(jsonFilePath);
        }

        try (InputStream schemaStream = JsonSchemaFactory.getInstance().getClass().getResourceAsStream(jsonFilePath)) {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> schemaMap = objectMapper.readValue(schemaStream, new TypeReference<Map<String, Object>>() {});
            schemaCache.put(jsonFilePath, schemaMap);
            return schemaMap;
        } catch (Exception e) {
            log.error("Error reading json schema", e);
            throw new CustomException("error reading json schema", e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}