package com.igot.cb.cbplan.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class CbPlanContentOccurrenceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Test
    void testNoArgsConstructorDefaultsToNull() {
        CbPlanContentOccurrence occurrence = new CbPlanContentOccurrence();
        assertNull(occurrence.getPlanId());
        assertNull(occurrence.getEndDate());
    }

    @Test
    void testSetters() {
        CbPlanContentOccurrence occurrence = new CbPlanContentOccurrence();
        Instant endDate = Instant.now();
        occurrence.setPlanId("plan123");
        occurrence.setEndDate(endDate);
        assertEquals("plan123", occurrence.getPlanId());
        assertEquals(endDate, occurrence.getEndDate());
    }

    @Test
    void testAllArgsConstructor() {
        Instant endDate = Instant.now();
        CbPlanContentOccurrence occurrence = new CbPlanContentOccurrence("plan456", endDate);
        assertEquals("plan456", occurrence.getPlanId());
        assertEquals(endDate, occurrence.getEndDate());
    }

    @Test
    void testEqualsAndHashCode() {
        Instant endDate = Instant.now();
        CbPlanContentOccurrence first = new CbPlanContentOccurrence("plan789", endDate);
        CbPlanContentOccurrence second = new CbPlanContentOccurrence("plan789", endDate);
        CbPlanContentOccurrence different = new CbPlanContentOccurrence("planOther", endDate);
        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertNotEquals(first, different);
    }

    @Test
    void testToStringContainsPlanId() {
        CbPlanContentOccurrence occurrence = new CbPlanContentOccurrence("plan123", Instant.EPOCH);
        assertNotNull(occurrence.toString());
    }

    @Test
    void testJsonRoundTripPreservesValues() throws Exception {
        Instant endDate = Instant.parse("2026-08-12T10:15:30Z");
        CbPlanContentOccurrence original = new CbPlanContentOccurrence("plan123", endDate);
        String json = MAPPER.writeValueAsString(original);
        CbPlanContentOccurrence restored = MAPPER.readValue(json, CbPlanContentOccurrence.class);
        assertEquals(original, restored);
        assertEquals(endDate, restored.getEndDate());
    }
}
