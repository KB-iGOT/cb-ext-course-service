package com.igot.cb.elasticsearch.dto;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class SearchResult {
    private List<Map<String, Object>> data;
    private Map<String, List<FacetDTO>> facets;
    private long totalCount;
    List<Map<String, Object>> additionalInfo;
}
