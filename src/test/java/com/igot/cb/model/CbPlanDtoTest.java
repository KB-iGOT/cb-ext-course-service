package com.igot.cb.model;

import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class CbPlanDtoTest {

    @Test
    void testGettersAndSetters() {
        CbPlanDto dto = new CbPlanDto();
        
        dto.setId("plan123");
        dto.setName("Test Plan");
        dto.setContentType("Course");
        
        List<String> contentList = Arrays.asList("content1", "content2");
        dto.setContentList(contentList);
        
        dto.setOrgScope("Single");
        
        Map<String, Object> contextData = new HashMap<>();
        contextData.put("key", "value");
        dto.setContextData(contextData);
        
        Date endDate = new Date();
        dto.setEndDate(endDate);
        
        dto.setIsApar(true);
        
        List<String> orgIdList = Arrays.asList("org1", "org2");
        dto.setOrgIdList(orgIdList);
        
        assertEquals("plan123", dto.getId());
        assertEquals("Test Plan", dto.getName());
        assertEquals("Course", dto.getContentType());
        assertEquals(contentList, dto.getContentList());
        assertEquals("Single", dto.getOrgScope());
        assertEquals(contextData, dto.getContextData());
        assertEquals(endDate, dto.getEndDate());
        assertTrue(dto.getIsApar());
        assertEquals(orgIdList, dto.getOrgIdList());
    }

    @Test
    void testDefaultValues() {
        CbPlanDto dto = new CbPlanDto();
        
        assertNull(dto.getId());
        assertNull(dto.getName());
        assertNull(dto.getContentType());
        assertNull(dto.getContentList());
        assertNull(dto.getOrgScope());
        assertNull(dto.getContextData());
        assertNull(dto.getEndDate());
        assertNull(dto.getIsApar());
        assertNull(dto.getOrgIdList());
    }

    @Test
    void testBooleanValues() {
        CbPlanDto dto = new CbPlanDto();
        
        dto.setIsApar(false);
        assertFalse(dto.getIsApar());
        
        dto.setIsApar(null);
        assertNull(dto.getIsApar());
    }
}