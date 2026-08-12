package com.igot.cb.cbplan.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Cache entry for user CB Plan dictionary responses.
 * Stores computed APAR/non-APAR grouping results (excludes enrichment).
 *
 * @version 3.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CbPlanDictionaryCacheEntry {

    /**
     * Map of APAR content IDs to their plan occurrences.
     */
    @JsonProperty("aparContentList")
    private Map<String, List<CbPlanContentOccurrence>> aparContentList;

    /**
     * Map of non-APAR content IDs to their plan occurrences.
     */
    @JsonProperty("nonAparContentList")
    private Map<String, List<CbPlanContentOccurrence>> nonAparContentList;

    /**
     * Count of unique APAR content IDs.
     */
    @JsonProperty("aparCount")
    private int aparCount;

    /**
     * Count of unique non-APAR content IDs.
     */
    @JsonProperty("nonAparCount")
    private int nonAparCount;
}
