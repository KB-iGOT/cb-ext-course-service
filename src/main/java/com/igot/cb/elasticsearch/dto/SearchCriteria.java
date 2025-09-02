package com.igot.cb.elasticsearch.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class SearchCriteria {
    private HashMap<String, Object> filter;

    private List<String> requestedFields;

    private int pageNumber;

    private int pageSize;

    private String orderBy;

    private String orderDirection;

    private String searchString;

    private List<String> facets;

    private Map<String, Object> query;

    private boolean overrideCache;
}
