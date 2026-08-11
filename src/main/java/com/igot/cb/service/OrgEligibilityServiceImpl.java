package com.igot.cb.service;

import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class OrgEligibilityServiceImpl {

    private final EsUtilService esUtilService;
    private final CbExtServerProperties serverProperties;
    private final ElasticsearchClient orgEligibilityElasticsearchClient;
    private final AccessTokenValidator accessTokenValidator;

    public OrgEligibilityServiceImpl(EsUtilService esUtilService, CbExtServerProperties serverProperties,
            @Qualifier("orgEligibilityElasticsearchClient") ElasticsearchClient orgEligibilityElasticsearchClient,
            AccessTokenValidator accessTokenValidator) {
        this.esUtilService = esUtilService;
        this.serverProperties = serverProperties;
        this.orgEligibilityElasticsearchClient = orgEligibilityElasticsearchClient;
        this.accessTokenValidator = accessTokenValidator;
    }

    public ApiResponse upsertOrgEligibility(Map<String, Object> request, String authToken) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_ORG_ELIGIBILITY_UPSERT);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isBlank(userId)) {
                return response;
            }

            String orgId = (String) request.get(Constants.ORG_ID);
            if (StringUtils.isBlank(orgId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("orgId is missing in request");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            String result = esUtilService.addDocument(orgEligibilityElasticsearchClient,
                    serverProperties.getOrgEligibilityIndex(), Constants.INDEX_TYPE,
                    orgId, request, serverProperties.getElasticOrgEligibilityJsonPath());
            if (StringUtils.isBlank(result)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("Failed to upsert org eligibility for orgId: " + orgId);
                response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

            response.getResult().put(Constants.ORG_ID, orgId);
            response.getResult().put(Constants.STATUS, Constants.SUCCESS);
        } catch (Exception e) {
            log.error("Failed to upsert org eligibility", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    public ApiResponse readOrgEligibility(String orgId, String authToken) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_ORG_ELIGIBILITY_READ);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isBlank(userId)) {
                return response;
            }

            if (StringUtils.isBlank(orgId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("orgId is missing");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            Map<String, Object> document = esUtilService.getDocumentById(orgEligibilityElasticsearchClient,
                    serverProperties.getOrgEligibilityIndex(), orgId);
            if (MapUtils.isEmpty(document)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErr("Org eligibility not found for orgId: " + orgId);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            response.getResult().putAll(document);
        } catch (Exception e) {
            log.error("Failed to read org eligibility for orgId: " + orgId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }
}
