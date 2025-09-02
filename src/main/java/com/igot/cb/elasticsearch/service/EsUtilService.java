package com.igot.cb.elasticsearch.service;

import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;

import java.util.Map;

public interface EsUtilService {

    String addDocument(String esIndexName, String type, String id, Map<String, Object> document, String JsonFilePath);

    String updateDocument(String index, String indexType, String entityId, Map<String, Object> document, String JsonFilePath);

    SearchResult searchDocuments(String esIndexName, SearchCriteria searchCriteria, String elasticCbPlanJsonPath) throws Exception;
}
