package com.igot.cb.cbplan.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CbPlanReadResponseDtoTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testBuilderPopulatesAllFields() throws Exception {
        Instant createdAt = Instant.parse("2026-01-01T00:00:00Z");
        Instant publishedAt = Instant.parse("2026-02-01T00:00:00Z");
        Instant endDate = Instant.parse("2026-12-31T18:29:59Z");
        JsonNode contextData = MAPPER.readTree("{\"accessControl\":{\"userGroups\":[]}}");
        List<String> contentList = List.of("course1");
        CbPlanReadResponseDto dto = CbPlanReadResponseDto.builder()
                .id("plan123")
                .name("My Plan")
                .planYear("2026-27")
                .endDate(endDate)
                .isApar(true)
                .contentType("Course")
                .planType("AI CBP")
                .createdAt(createdAt)
                .cbPublishedAt(publishedAt)
                .status("Live")
                .createdBy("user1")
                .createdByName("John")
                .contextData(contextData)
                .contentList(contentList)
                .build();
        assertEquals("plan123", dto.getId());
        assertEquals("My Plan", dto.getName());
        assertEquals("2026-27", dto.getPlanYear());
        assertEquals(endDate, dto.getEndDate());
        assertTrue(dto.getIsApar());
        assertEquals("Course", dto.getContentType());
        assertEquals("AI CBP", dto.getPlanType());
        assertEquals(createdAt, dto.getCreatedAt());
        assertEquals(publishedAt, dto.getCbPublishedAt());
        assertEquals("Live", dto.getStatus());
        assertEquals("user1", dto.getCreatedBy());
        assertEquals("John", dto.getCreatedByName());
        assertEquals(contextData, dto.getContextData());
        assertEquals(contentList, dto.getContentList());
    }

    @Test
    void testBuilderLeavesUnsetFieldsNull() {
        CbPlanReadResponseDto dto = CbPlanReadResponseDto.builder().id("plan123").build();
        assertEquals("plan123", dto.getId());
        assertNull(dto.getName());
        assertNull(dto.getEndDate());
        assertNull(dto.getContextData());
        assertNull(dto.getContentList());
    }

    @Test
    void testNoArgsConstructorAndSetters() {
        CbPlanReadResponseDto dto = new CbPlanReadResponseDto();
        dto.setId("plan999");
        dto.setName("Updated Plan");
        dto.setIsApar(false);
        dto.setStatus("draft");
        assertEquals("plan999", dto.getId());
        assertEquals("Updated Plan", dto.getName());
        assertEquals(Boolean.FALSE, dto.getIsApar());
        assertEquals("draft", dto.getStatus());
    }

    @Test
    void testAllArgsConstructor() {
        CbPlanReadResponseDto dto = new CbPlanReadResponseDto("plan1", "name", "2026-27",
                Instant.EPOCH, false, "Course", "type", Instant.EPOCH, Instant.EPOCH,
                "Live", "user1", "John", null, List.of(), "ca_123");
        assertEquals("plan1", dto.getId());
        assertEquals("2026-27", dto.getPlanYear());
        assertNull(dto.getContextData());
        assertTrue(((List<?>) dto.getContentList()).isEmpty());
        assertEquals("ca_123", dto.getCaLinkedId());
    }

    @Test
    void testDeserializationIgnoresUnknownProperties() throws Exception {
        String json = "{\"id\":\"plan123\",\"name\":\"Plan\",\"someRemovedField\":\"value\"}";
        CbPlanReadResponseDto dto = MAPPER.readValue(json, CbPlanReadResponseDto.class);
        assertNotNull(dto);
        assertEquals("plan123", dto.getId());
        assertEquals("Plan", dto.getName());
    }
}
