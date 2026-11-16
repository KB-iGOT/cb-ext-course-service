package com.igot.cb.cbplan.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Represents a single occurrence of a content item in a CB Plan.
 * Used to track which plans contain a particular course and when they expire.
 *
 * @author CB Ext Team
 * @version 3.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CbPlanContentOccurrence {

    /**
     * CB Plan ID where this content appears.
     */
    @JsonProperty("planId")
    private String planId;

    /**
     * End date of the plan (when this content assignment expires).
     */
    @JsonProperty("endDate")
    private Instant endDate;

    /**
     * Type of the plan (e.g., AICBP).
     */
    @JsonProperty("planType")
    private String planType;
}
