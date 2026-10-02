package com.igot.cb.usergroups.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CriteriaValueDeserializerTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Test
    void deserialize_withStringArray_shouldReturnListOfStrings() throws Exception {
        String json = "{\"criteriaKey\":\"batch\",\"criteriaValue\":[\"2022\",\"2023\",\"2024\"]}";

        CriteriaItem result = objectMapper.readValue(json, CriteriaItem.class);

        assertEquals(List.of("2022", "2023", "2024"), result.criteriaValue());
    }

    @Test
    void deserialize_withBareBoolean_shouldReturnSingleElementList() throws Exception {
        String json = "{\"criteriaKey\":\"isOnCentralDeputation\",\"criteriaValue\":true}";

        CriteriaItem result = objectMapper.readValue(json, CriteriaItem.class);

        assertEquals(List.of("true"), result.criteriaValue());
    }

    @Test
    void deserialize_withBareFalseBoolean_shouldReturnSingleElementList() throws Exception {
        String json = "{\"criteriaKey\":\"isOnCentralDeputation\",\"criteriaValue\":false}";

        CriteriaItem result = objectMapper.readValue(json, CriteriaItem.class);

        assertEquals(List.of("false"), result.criteriaValue());
    }

    @Test
    void deserialize_withBooleanInsideArray_shouldReturnStringifiedElement() throws Exception {
        String json = "{\"criteriaKey\":\"isOnCentralDeputation\",\"criteriaValue\":[true]}";

        CriteriaItem result = objectMapper.readValue(json, CriteriaItem.class);

        assertEquals(List.of("true"), result.criteriaValue());
    }

    @Test
    void deserialize_withBareString_shouldReturnSingleElementList() throws Exception {
        String json = "{\"criteriaKey\":\"service\",\"criteriaValue\":\"ias\"}";

        CriteriaItem result = objectMapper.readValue(json, CriteriaItem.class);

        assertEquals(List.of("ias"), result.criteriaValue());
    }

    @Test
    void deserialize_withStringArrayMultipleValues_shouldPreserveOrder() throws Exception {
        String json = "{\"criteriaKey\":\"service\",\"criteriaValue\":[\"ias\",\"ips\",\"ifos\"]}";

        CriteriaItem result = objectMapper.readValue(json, CriteriaItem.class);

        assertEquals(List.of("ias", "ips", "ifos"), result.criteriaValue());
    }

    @Test
    void deserialize_withSingleOrgIdArray_shouldReturnSingleElementList() throws Exception {
        String json = "{\"criteriaKey\":\"rootOrgId\",\"criteriaValue\":[\"0133783095823810560\"]}";

        CriteriaItem result = objectMapper.readValue(json, CriteriaItem.class);

        assertEquals(List.of("0133783095823810560"), result.criteriaValue());
    }

    @Test
    void deserialize_withMixedArrayBooleanAndString_shouldReturnAllAsStrings() throws Exception {
        String json = "{\"criteriaKey\":\"test\",\"criteriaValue\":[\"val1\",true,\"val2\"]}";

        CriteriaItem result = objectMapper.readValue(json, CriteriaItem.class);

        assertEquals(List.of("val1", "true", "val2"), result.criteriaValue());
    }

    @Test
    void deserialize_withEmptyArray_shouldReturnEmptyList() throws Exception {
        String json = "{\"criteriaKey\":\"rootOrgId\",\"criteriaValue\":[]}";

        CriteriaItem result = objectMapper.readValue(json, CriteriaItem.class);

        assertNotNull(result.criteriaValue());
        assertTrue(result.criteriaValue().isEmpty());
    }
}
