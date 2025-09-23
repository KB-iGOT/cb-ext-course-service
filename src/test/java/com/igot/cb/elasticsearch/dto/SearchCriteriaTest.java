package com.igot.cb.elasticsearch.dto;

import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class SearchCriteriaTest {

    @Test
    void testDefaultConstructor() {
        SearchCriteria criteria = new SearchCriteria();
        
        assertNull(criteria.getFilter());
        assertNull(criteria.getRequestedFields());
        assertEquals(0, criteria.getPageNumber());
        assertEquals(0, criteria.getPageSize());
        assertNull(criteria.getOrderBy());
        assertNull(criteria.getOrderDirection());
        assertNull(criteria.getSearchString());
        assertNull(criteria.getFacets());
        assertNull(criteria.getQuery());
        assertFalse(criteria.isOverrideCache());
    }

    @Test
    void testAllArgsConstructor() {
        HashMap<String, Object> filter = new HashMap<>();
        filter.put("status", "active");
        
        List<String> requestedFields = Arrays.asList("id", "name");
        List<String> facets = Arrays.asList("category", "type");
        Map<String, Object> query = new HashMap<>();
        query.put("match", "test");
        
        SearchCriteria criteria = new SearchCriteria(
            filter, requestedFields, 1, 10, "name", "asc", "test", facets, query, true
        );
        
        assertEquals(filter, criteria.getFilter());
        assertEquals(requestedFields, criteria.getRequestedFields());
        assertEquals(1, criteria.getPageNumber());
        assertEquals(10, criteria.getPageSize());
        assertEquals("name", criteria.getOrderBy());
        assertEquals("asc", criteria.getOrderDirection());
        assertEquals("test", criteria.getSearchString());
        assertEquals(facets, criteria.getFacets());
        assertEquals(query, criteria.getQuery());
        assertTrue(criteria.isOverrideCache());
    }

    @Test
    void testSettersAndGetters() {
        SearchCriteria criteria = new SearchCriteria();
        
        HashMap<String, Object> filter = new HashMap<>();
        filter.put("category", "course");
        
        List<String> fields = Arrays.asList("title", "description");
        List<String> facets = Arrays.asList("level");
        Map<String, Object> query = new HashMap<>();
        
        criteria.setFilter(filter);
        criteria.setRequestedFields(fields);
        criteria.setPageNumber(2);
        criteria.setPageSize(20);
        criteria.setOrderBy("createdDate");
        criteria.setOrderDirection("desc");
        criteria.setSearchString("java");
        criteria.setFacets(facets);
        criteria.setQuery(query);
        criteria.setOverrideCache(true);
        
        assertEquals(filter, criteria.getFilter());
        assertEquals(fields, criteria.getRequestedFields());
        assertEquals(2, criteria.getPageNumber());
        assertEquals(20, criteria.getPageSize());
        assertEquals("createdDate", criteria.getOrderBy());
        assertEquals("desc", criteria.getOrderDirection());
        assertEquals("java", criteria.getSearchString());
        assertEquals(facets, criteria.getFacets());
        assertEquals(query, criteria.getQuery());
        assertTrue(criteria.isOverrideCache());
    }
}