package com.igot.cb.cbplan.service.impl.v4;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.model.ApiRequest;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.model.ApiRespParam;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Service for CB Plan V4 search operations.
 * Handles search query construction and execution.
 *
 * @version 4.0
 */
@Service
@Slf4j
public class CbPlanSearchServiceV4Impl {
    private final EsUtilService esUtilService;
    private final CbExtServerProperties serverProperties;
    private final CbPlanValidationServiceV4Impl validationService;
    private final ObjectMapper mapper;

    public CbPlanSearchServiceV4Impl(EsUtilService esUtilService,
                                     CbExtServerProperties serverProperties,
                                     CbPlanValidationServiceV4Impl validationService) {
        this.esUtilService = esUtilService;
        this.serverProperties = serverProperties;
        this.validationService = validationService;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Searches CB Plans. Client controls all filtering via the request body.
     * Backend constructs SearchCriteria from the generic request.
     *
     * @param request   the API request containing search parameters
     * @param userOrgId the user's organization ID
     * @param authToken the authentication token
     * @return ApiResponse containing search results
     */
    public ApiResponse searchCbPlan(ApiRequest request, String userOrgId, String authToken) {
        log.info("CbPlanSearchServiceV4Impl.searchCbPlan: Searching CB Plans for orgId: {}", userOrgId);
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMMUNITY_SEARCH);
        try {
            String userId = validationService.validateAndExtractUserId(authToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }
            SearchCriteria searchCriteria = mapper.convertValue(request.getRequest(), SearchCriteria.class);
            addOrgIdListFilter(searchCriteria, userOrgId);
            SearchResult searchResult = esUtilService.searchDocumentsV2(
                    serverProperties.getCpPlanIndex(),
                    searchCriteria,
                    serverProperties.getElasticCbPlanJsonPath());
            if (CollectionUtils.isNotEmpty(searchResult.getData())) {
                transformContentListInSearchResults(searchResult.getData());
                response.getResult().put(Constants.RESULT, searchResult);
                response.setParams(new ApiRespParam());
                response.getParams().setStatus(Constants.SUCCESS);
                response.setResponseCode(HttpStatus.OK);
            }
        } catch (Exception e) {
            log.error("CbPlanSearchServiceV4Impl.searchCbPlan: Error occurred while searching for orgId: {}", userOrgId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr("Error while processing search");
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    /**
     * Adds orgIdList filter directly to Elasticsearch query when applyOrgIdFilter flag is true.
     * This approach is more efficient than post-processing as ES filters at query time using indexed fields.
     * Only transfers matching records over the network and provides accurate totalCount from ES.
     *
     * @param searchCriteria the search criteria to modify
     * @param userOrgId      the user's organization ID to filter by
     */
    private void addOrgIdListFilter(SearchCriteria searchCriteria, String userOrgId) {
        if (!searchCriteria.isApplyOrgIdFilter()) {
            log.info("CbPlanSearchServiceV4Impl.addOrgIdListFilter: Skipping orgIdList filter (applyOrgIdFilter=false)");
            return;
        }
        if (StringUtils.isBlank(userOrgId)) {
            log.warn("CbPlanSearchServiceV4Impl.addOrgIdListFilter: userOrgId is blank, skipping filter");
            return;
        }
        if (searchCriteria.getFilter() == null) {
            searchCriteria.setFilter(new HashMap<>());
        }
        searchCriteria.getFilter().put(Constants.ORG_ID_LIST, userOrgId);
        log.info("CbPlanSearchServiceV4Impl.addOrgIdListFilter: Added orgIdList filter for orgId: {}", userOrgId);
    }

    /**
     * Transforms contentList from V4 format (JSON strings) to proper objects in search results.
     * Modifies the search results in place.
     *
     * @param searchResults list of search result records from Elasticsearch
     */
    private void transformContentListInSearchResults(List<Map<String, Object>> searchResults) {
        if (CollectionUtils.isEmpty(searchResults)) {
            return;
        }
        for (Map<String, Object> searchResult : searchResults) {
            if (MapUtils.isNotEmpty(searchResult) && searchResult.containsKey(Constants.CONTENT_LIST)) {
                Object contentListObj = searchResult.get(Constants.CONTENT_LIST);
                if (contentListObj instanceof List<?> contentList) {
                    List<Map<String, Object>> transformedContentList = parseV4ContentList(contentList);
                    if (CollectionUtils.isNotEmpty(transformedContentList)) {
                        searchResult.put(Constants.CONTENT_LIST, transformedContentList);
                    }
                }
            }
        }
    }

    /**
     * Transforms contentList to V4 format (objects with identifier and mandatory fields).
     * Handles both V3 format (plain strings) and V4 format (JSON strings):
     * - V3: ["do_123"] → [{"identifier":"do_123","mandatory":false}]
     * - V4: ["{\"identifier\":\"do_123\",\"mandatory\":true}"] → [{"identifier":"do_123","mandatory":true}]
     *
     * @param contentList list from Elasticsearch (V3 plain strings or V4 JSON strings)
     * @return list of V4 content objects with identifier and mandatory fields
     */
    private List<Map<String, Object>> parseV4ContentList(List<?> contentList) {
        if (CollectionUtils.isEmpty(contentList)) {
            return List.of();
        }
        List<Map<String, Object>> parsedList = new ArrayList<>();
        boolean isV4Format = isV4JsonFormat(contentList);
        for (Object item : contentList) {
            if (item instanceof String itemStr) {
                Map<String, Object> contentItem;

                if (isV4Format) {
                    contentItem = tryParseAsV4ContentItem(itemStr);
                    if (MapUtils.isEmpty(contentItem)) {
                        return List.of();
                    }
                } else {
                    contentItem = convertV3ToV4Format(itemStr);
                }

                parsedList.add(contentItem);
            }
        }
        return parsedList;
    }

    /**
     * Checks if the contentList is in V4 JSON format by examining the first item.
     *
     * @param contentList list to check
     * @return true if V4 format (JSON strings), false if V3 format (plain strings)
     */
    private boolean isV4JsonFormat(List<?> contentList) {
        if (CollectionUtils.isEmpty(contentList)) {
            return false;
        }
        Object firstItem = contentList.get(0);
        if (firstItem instanceof String firstItemStr) {
            Map<String, Object> parsed = tryParseAsV4ContentItem(firstItemStr);
            return MapUtils.isNotEmpty(parsed);
        }

        return false;
    }

    /**
     * Converts V3 format (plain identifier string) to V4 format (object with identifier and mandatory).
     *
     * @param identifier content identifier string (e.g., "do_123")
     * @return V4 content object with mandatory set to false
     */
    private Map<String, Object> convertV3ToV4Format(String identifier) {
        Map<String, Object> contentItem = new HashMap<>();
        contentItem.put(Constants.IDENTIFIER, identifier);
        contentItem.put(Constants.MANDATORY, false);
        return contentItem;
    }

    /**
     * Attempts to parse a string as V4 content item JSON.
     * V4 format must have "identifier" field and optionally "mandatory" field.
     *
     * @param itemStr JSON string to parse
     * @return parsed map if valid V4 format, empty map otherwise
     */
    private Map<String, Object> tryParseAsV4ContentItem(String itemStr) {
        try {
            Map<String, Object> parsed = mapper.readValue(itemStr,
                    new TypeReference<Map<String, Object>>() {});

            if (parsed.containsKey(Constants.IDENTIFIER)) {
                return parsed;
            }

            return Map.of();
        } catch (Exception e) {
            log.debug("CbPlanSearchServiceV4Impl.tryParseAsV4ContentItem: Not V4 format or parse failed for: {}", itemStr);
            return Map.of();
        }
    }

}
