package com.igot.cb.elasticsearch.dto;

import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class SearchResultTest {

    @Test
    void testDefaultConstructor() {
        SearchResult result = new SearchResult();
        
        assertNull(result.getData());
        assertNull(result.getFacets());
        assertEquals(0, result.getTotalCount());
        assertNull(result.getAdditionalInfo());
    }

    @Test
    void testAllArgsConstructor() {
        List<Map<String, Object>> data = new ArrayList<>();
        Map<String, Object> item = new HashMap<>();
        item.put("id", "1");
        item.put("name", "test");
        data.add(item);
        
        Map<String, List<FacetDTO>> facets = new HashMap<>();
        List<FacetDTO> categoryFacets = Arrays.asList(new FacetDTO("course", 5L));
        facets.put("category", categoryFacets);
        
        List<Map<String, Object>> additionalInfo = new ArrayList<>();
        
        SearchResult result = new SearchResult(data, facets, 100, additionalInfo);
        
        assertEquals(data, result.getData());
        assertEquals(facets, result.getFacets());
        assertEquals(100, result.getTotalCount());
        assertEquals(additionalInfo, result.getAdditionalInfo());
    }

    @Test
    void testSettersAndGetters() {
        SearchResult result = new SearchResult();
        
        List<Map<String, Object>> data = new ArrayList<>();
        Map<String, Object> item1 = new HashMap<>();
        item1.put("id", "1");
        item1.put("title", "Java Course");
        data.add(item1);
        
        Map<String, Object> item2 = new HashMap<>();
        item2.put("id", "2");
        item2.put("title", "Python Course");
        data.add(item2);
        
        Map<String, List<FacetDTO>> facets = new HashMap<>();
        List<FacetDTO> levelFacets = Arrays.asList(
            new FacetDTO("beginner", 10L),
            new FacetDTO("intermediate", 5L)
        );
        facets.put("level", levelFacets);
        
        List<Map<String, Object>> additionalInfo = new ArrayList<>();
        Map<String, Object> info = new HashMap<>();
        info.put("aggregation", "summary");
        additionalInfo.add(info);
        
        result.setData(data);
        result.setFacets(facets);
        result.setTotalCount(50);
        result.setAdditionalInfo(additionalInfo);
        
        assertEquals(data, result.getData());
        assertEquals(facets, result.getFacets());
        assertEquals(50, result.getTotalCount());
        assertEquals(additionalInfo, result.getAdditionalInfo());
        assertEquals(2, result.getData().size());
        assertEquals("Java Course", result.getData().get(0).get("title"));
    }
}