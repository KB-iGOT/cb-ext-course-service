package com.igot.cb.cbplan.service.impl.v4;

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
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

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
            SearchResult searchResult = esUtilService.searchDocuments(
                    serverProperties.getCpPlanIndex(),
                    searchCriteria,
                    serverProperties.getElasticCbPlanJsonPath());
            if (CollectionUtils.isNotEmpty(searchResult.getData())) {
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
}
