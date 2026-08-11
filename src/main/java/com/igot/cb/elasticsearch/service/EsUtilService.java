package com.igot.cb.elasticsearch.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;

import java.util.Map;

public interface EsUtilService {

    String addDocument(String esIndexName, String type, String id, Map<String, Object> document, String JsonFilePath);

    String addDocument(ElasticsearchClient client, String esIndexName, String type, String id, Map<String, Object> document, String JsonFilePath);

    String updateDocument(String index, String indexType, String entityId, Map<String, Object> document, String JsonFilePath);

    SearchResult searchDocuments(String esIndexName, SearchCriteria searchCriteria, String elasticCbPlanJsonPath) throws Exception;

    Map<String, Object> getDocumentById(String esIndexName, String id);

    Map<String, Object> getDocumentById(ElasticsearchClient client, String esIndexName, String id);
}
