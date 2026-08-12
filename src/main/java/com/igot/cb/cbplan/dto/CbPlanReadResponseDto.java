package com.igot.cb.cbplan.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * DTO for CB Plan V3 read response.
 * Contains all plan details including enriched content information.
 *
 * @version 3.0
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CbPlanReadResponseDto {

    /**
     * CB Plan unique identifier.
     */
    private String id;

    /**
     * Plan name.
     */
    private String name;

    /**
     * Financial year for the plan (e.g., "2026-27").
     */
    private String planYear;

    /**
     * Plan end date.
     */
    private Instant endDate;

    /**
     * Whether this is an APAR (Annual Performance Appraisal Report) plan.
     */
    private Boolean isApar;

    /**
     * Content type (e.g., "Course", "Program").
     */
    private String contentType;

    /**
     * Plan type classification.
     */
    private String planType;

    /**
     * Timestamp when the plan was created.
     */
    private Instant createdAt;

    /**
     * Timestamp when the plan was published.
     */
    private Instant cbPublishedAt;

    /**
     * Plan status (DRAFT, LIVE, ARCHIVED).
     */
    private String status;

    /**
     * User ID of the creator.
     */
    private String createdBy;

    /**
     * Name of the creator.
     */
    private String createdByName;

    /**
     * Context data containing access control settings and user groups.
     */
    private JsonNode contextData;

    /**
     * List of enriched content items with metadata.
     * Each map contains content details like identifier, name, description, etc.
     */
    private List<Map<String, Object>> contentList;
}
