package com.igot.cb.elasticsearch.dto;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class FacetDTOTest {

    @Test
    void testDefaultConstructor() {
        FacetDTO facet = new FacetDTO();
        
        assertNull(facet.getValue());
        assertNull(facet.getCount());
    }

    @Test
    void testAllArgsConstructor() {
        String value = "category1";
        Long count = 15L;
        
        FacetDTO facet = new FacetDTO(value, count);
        
        assertEquals(value, facet.getValue());
        assertEquals(count, facet.getCount());
    }

    @Test
    void testSettersAndGetters() {
        FacetDTO facet = new FacetDTO();
        
        facet.setValue("technology");
        facet.setCount(25L);
        
        assertEquals("technology", facet.getValue());
        assertEquals(25L, facet.getCount());
    }

    @Test
    void testWithZeroCount() {
        FacetDTO facet = new FacetDTO("empty", 0L);
        
        assertEquals("empty", facet.getValue());
        assertEquals(0L, facet.getCount());
    }

    @Test
    void testWithNullValues() {
        FacetDTO facet = new FacetDTO(null, null);
        
        assertNull(facet.getValue());
        assertNull(facet.getCount());
    }
}