package com.igot.cb.cbplan.service.impl.v4;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.igot.cb.cbplan.dto.CbPlanReadResponseDto;
import com.igot.cb.model.CbPlanDto;
import com.igot.cb.util.Constants;

import lombok.extern.slf4j.Slf4j;

/**
 * Builds CB Plan V4 read responses. Renders contextData exactly as stored,
 * matching V3's read behaviour: a V3-created plan's inline userGroupName is
 * returned as-is, and a V4-created plan's userGroupId reference is returned
 * as-is, with no enrichment or lookup performed on read.
 *
 * @version 4.0
 */
@Service
@Slf4j
public class CbPlanReadServiceV4Impl {
    private final ObjectMapper mapper;

    public CbPlanReadServiceV4Impl() {
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Builds CB Plan read data from a raw Cassandra record.
     * Extracts plan details from either draftData (for LIVE plans with draft)
     * or direct fields, and returns contextData exactly as stored.
     *
     * @param cbPlan   raw CB Plan record from Cassandra
     * @param cbPlanId CB Plan ID
     * @return plan data DTO
     * @throws JsonProcessingException if draft_data JSON parsing fails
     */
    public CbPlanReadResponseDto buildEnrichedPlanData(Map<String, Object> cbPlan, String cbPlanId)
            throws JsonProcessingException {
        log.debug("CbPlanReadServiceV4.buildEnrichedPlanData: Entry - cbPlanId={}", cbPlanId);
        String draftData = (String) cbPlan.get(Constants.DRAFT_DATA);
        String status = (String) cbPlan.get(Constants.STATUS);
        boolean hasDraftData = StringUtils.isNotBlank(draftData) && !Constants.EMPTY_JSON.equals(draftData);
        boolean isLiveStatus = Constants.LIVE.equalsIgnoreCase(status);
        if (hasDraftData && isLiveStatus) {
            log.debug("CbPlanReadServiceV4.buildEnrichedPlanData: Reading from pending draftData - cbPlanId={}", cbPlanId);
        }
        PlanFields fields = hasDraftData && isLiveStatus ? extractFromDraft(draftData) : extractFromRecord(cbPlan);
        return buildDto(cbPlan, cbPlanId, status, fields);
    }

    /**
     * Extracts plan fields from a pending draftData blob (LIVE plan with an unpublished update).
     *
     * @param draftData draftData JSON string
     * @return extracted plan fields
     * @throws JsonProcessingException if draftData is not valid CbPlanDto JSON
     */
    private PlanFields extractFromDraft(String draftData) throws JsonProcessingException {
        CbPlanDto cbPlanDto = mapper.readValue(draftData, CbPlanDto.class);
        Instant endDate = Objects.nonNull(cbPlanDto.getEndDate()) ? cbPlanDto.getEndDate().toInstant() : null;
        boolean isApar = Objects.nonNull(cbPlanDto.getIsApar()) && cbPlanDto.getIsApar();
        Object contentList = extractContentList(cbPlanDto.getContentList());
        return new PlanFields(cbPlanDto.getName(), endDate, isApar, contentList);
    }

    /**
     * Extracts plan fields directly from the Cassandra record's own columns.
     *
     * @param cbPlan raw CB Plan record from Cassandra
     * @return extracted plan fields
     */
    private PlanFields extractFromRecord(Map<String, Object> cbPlan) {
        String name = (String) cbPlan.get(Constants.NAME);
        Instant endDate = (Instant) cbPlan.get(Constants.END_DATE_REQUEST);
        boolean isApar = Boolean.TRUE.equals(cbPlan.get(Constants.IS_APAR));
        return new PlanFields(name, endDate, isApar, extractContentList(cbPlan.get(Constants.CONTENT_LIST)));
    }

    /**
     * Extracts contentList from Cassandra and returns it as-is for backward compatibility.
     * V3 format (plain IDs): returns List<String> as-is
     * V4 format (JSON strings): parses and returns List<Map<String, Object>>
     *
     * @param contentListObj raw contentList value from Cassandra
     * @return contentList in original format (V3 or V4), empty list when absent
     */
    private Object extractContentList(Object contentListObj) {
        if (!(contentListObj instanceof List<?> list) || list.isEmpty()) {
            return new ArrayList<>();
        }

        Object firstItem = list.get(0);
        if (Objects.isNull(firstItem)) {
            return list;
        }

        if (isV4Format(firstItem)) {
            return parseV4ContentList(list);
        }
        return list;
    }

    /**
     * Assembles the final read response DTO from the raw record, resolved status,
     * and the fields extracted from either draftData or the record itself.
     *
     * @param cbPlan   raw CB Plan record from Cassandra
     * @param cbPlanId CB Plan ID
     * @param status   plan status
     * @param fields   fields extracted by {@link #extractFromDraft} or {@link #extractFromRecord}
     * @return assembled read response DTO
     */
    private CbPlanReadResponseDto buildDto(Map<String, Object> cbPlan, String cbPlanId, String status,
                                           PlanFields fields) {
        return CbPlanReadResponseDto.builder()
                .id(cbPlanId)
                .name(fields.name())
                .planYear((String) cbPlan.get(Constants.PLAN_YEAR))
                .endDate(fields.endDate())
                .isApar(fields.isApar())
                .contentType((String) cbPlan.get(Constants.CONTENT_TYPE))
                .planType((String) cbPlan.get(Constants.PLAN_TYPE))
                .createdAt((Instant) cbPlan.get(Constants.CREATED_AT_REQ))
                .cbPublishedAt((Instant) cbPlan.get(Constants.CB_PUBLISHED_AT))
                .status(status)
                .createdBy((String) cbPlan.get(Constants.CREATED_BY))
                .createdByName(StringUtils.EMPTY)
                .contextData(parseContextDataToJsonNode(cbPlan.get(Constants.CONTEXT_DATA_REQUEST)))
                .contentList(fields.contentList())
                .caLinkedId((String) cbPlan.get(Constants.CA_LINKED_ID_DB))
                .build();
    }

    /**
     * Parses contextData from the Cassandra object to a JsonNode, unmodified.
     *
     * @param contextData raw contextData from Cassandra
     * @return JsonNode representation, or null if absent/unparseable
     */
    private JsonNode parseContextDataToJsonNode(Object contextData) {
        if (Objects.isNull(contextData)) {
            return null;
        }
        try {
            return mapper.readTree(contextData.toString());
        } catch (JsonProcessingException e) {
            log.warn("CbPlanReadServiceV4: Failed to parse contextData, returning null", e);
            return null;
        }
    }

    private record PlanFields(String name, Instant endDate, boolean isApar, Object contentList) {
    }

    /**
     * Checks if a content item is in V4 format (JSON string) by attempting to parse it.
     *
     * @param item content item from Cassandra
     * @return true if item is a valid JSON object string (V4 format), false otherwise (V3 format)
     */
    private boolean isV4Format(Object item) {
        if (Objects.isNull(item)) {
            return false;
        }

        String itemStr = String.valueOf(item);
        if (StringUtils.isBlank(itemStr)) {
            return false;
        }

        try {
            Object parsed = mapper.readValue(itemStr, Object.class);
            return parsed instanceof Map;
        } catch (JsonProcessingException e) {
            return false;
        }
    }

    /**
     * Parses V4 contentList format (JSON strings) to List<Map<String, Object>>.
     *
     * @param contentListFromDb list of JSON strings from Cassandra
     * @return list of V4 content objects with identifier and mandatory fields
     */
    private List<Map<String, Object>> parseV4ContentList(List<?> contentListFromDb) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object item : contentListFromDb) {
            if (Objects.isNull(item)) {
                continue;
            }

            String itemStr = String.valueOf(item);
            try {
                Map<String, Object> parsed = mapper.readValue(itemStr,
                    new TypeReference<Map<String, Object>>() {});
                result.add(parsed);
            } catch (JsonProcessingException e) {
                log.warn("CbPlanReadServiceV4.parseV4ContentList: Failed to parse V4 content item, skipping: {}", itemStr, e);
            }
        }
        return result;
    }

    /**
     * Extracts identifiers from V4 serialized contentList for content lookup service.
     * V4 format: ['{"identifier":"do_123","mandatory":true}'] - extracts "do_123"
     * V3 format: ['do_123'] - returns as-is
     *
     * @param contentListRaw raw contentList from Cassandra (V3 or V4 format)
     * @return list of content identifiers
     */
    public List<String> extractIdentifiers(List<String> contentListRaw) {
        if (Objects.isNull(contentListRaw) || contentListRaw.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> identifiers = new ArrayList<>();
        for (String item : contentListRaw) {
            if (StringUtils.isBlank(item)) {
                continue;
            }
            try {
                // Try to parse as V4 JSON format
                Map<String, Object> parsed = mapper.readValue(item, new TypeReference<Map<String, Object>>() {});
                Object identifier = parsed.get(Constants.IDENTIFIER);
                if (Objects.nonNull(identifier)) {
                    identifiers.add(identifier.toString());
                } else {
                    identifiers.add(item);
                }
            } catch (JsonProcessingException e) {
                // Not JSON - V3 plain identifier
                identifiers.add(item);
            }
        }
        return identifiers;
    }
}
