package com.igot.cb.cbplan.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CbPlanDictionaryCacheEntryTest {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Test
    void testNoArgsConstructorAndSetters() {
        CbPlanDictionaryCacheEntry entry = new CbPlanDictionaryCacheEntry();
        Map<String, List<CbPlanContentOccurrence>> aparMap = new LinkedHashMap<>();
        aparMap.put("content1", List.of(new CbPlanContentOccurrence("plan1", Instant.EPOCH, "Mandatory")));
        Map<String, List<CbPlanContentOccurrence>> nonAparMap = new LinkedHashMap<>();
        entry.setAparContentList(aparMap);
        entry.setNonAparContentList(nonAparMap);
        entry.setAparCount(1);
        entry.setNonAparCount(0);
        assertEquals(aparMap, entry.getAparContentList());
        assertEquals(nonAparMap, entry.getNonAparContentList());
        assertEquals(1, entry.getAparCount());
        assertEquals(0, entry.getNonAparCount());
    }

    @Test
    void testAllArgsConstructor() {
        Map<String, List<CbPlanContentOccurrence>> aparMap = new LinkedHashMap<>();
        Map<String, List<CbPlanContentOccurrence>> nonAparMap = new LinkedHashMap<>();
        CbPlanDictionaryCacheEntry entry = new CbPlanDictionaryCacheEntry(aparMap, nonAparMap, 5, 3);
        assertEquals(aparMap, entry.getAparContentList());
        assertEquals(nonAparMap, entry.getNonAparContentList());
        assertEquals(5, entry.getAparCount());
        assertEquals(3, entry.getNonAparCount());
    }

    @Test
    void testEqualsAndHashCode() {
        CbPlanDictionaryCacheEntry first =
                new CbPlanDictionaryCacheEntry(new LinkedHashMap<>(), new LinkedHashMap<>(), 0, 0);
        CbPlanDictionaryCacheEntry second =
                new CbPlanDictionaryCacheEntry(new LinkedHashMap<>(), new LinkedHashMap<>(), 0, 0);
        CbPlanDictionaryCacheEntry different =
                new CbPlanDictionaryCacheEntry(new LinkedHashMap<>(), new LinkedHashMap<>(), 1, 0);
        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertNotEquals(first, different);
    }

    @Test
    void testJsonRoundTripMatchesRedisCacheContract() throws Exception {
        Map<String, List<CbPlanContentOccurrence>> aparMap = new LinkedHashMap<>();
        aparMap.put("course1", List.of(new CbPlanContentOccurrence("plan1", Instant.parse("2026-08-12T10:15:30Z"), "Mandatory")));
        CbPlanDictionaryCacheEntry original =
                new CbPlanDictionaryCacheEntry(aparMap, new LinkedHashMap<>(), 1, 0);
        String json = MAPPER.writeValueAsString(original);
        CbPlanDictionaryCacheEntry restored = MAPPER.readValue(json, CbPlanDictionaryCacheEntry.class);
        assertTrue(json.contains("aparContentList"));
        assertTrue(json.contains("nonAparCount"));
        assertEquals(original, restored);
        assertEquals("plan1", restored.getAparContentList().get("course1").get(0).getPlanId());
    }
}
