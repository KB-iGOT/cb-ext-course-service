package com.igot.cb.cbplan.service.impl.v4;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.igot.cb.common.ServerProperties;
import com.igot.cb.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;

import lombok.extern.slf4j.Slf4j;

/**
 * Handles async synchronisation of a CB Plan's contentList to the linked
 * Comprehensive Assessment content node's {@code trainingPlan_v2} field
 * via the learning service system update PATCH API.
 * Reads the existing {@code trainingPlan_v2} object first so all non-contentList
 * fields (identifier, name, planYear, endDate, orgName) are preserved.
 *
 * @version 4.0
 */
@Service
@Slf4j
public class CbPlanContentSyncServiceV4Impl {

    private final OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    private final ServerProperties serverProperties;
    private final CbPlanContentLookupServiceV4Impl contentLookupService;
    private final CbExtServerProperties cbExtServerProperties;
    private final ObjectMapper mapper;

    public CbPlanContentSyncServiceV4Impl(OutboundRequestHandlerServiceImpl outboundRequestHandlerService,
                                          ServerProperties serverProperties,
                                          CbPlanContentLookupServiceV4Impl contentLookupService,
                                          CbExtServerProperties cbExtServerProperties) {
        this.outboundRequestHandlerService = outboundRequestHandlerService;
        this.serverProperties = serverProperties;
        this.contentLookupService = contentLookupService;
        this.cbExtServerProperties = cbExtServerProperties;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Updates the {@code contentList} inside the linked content node's {@code trainingPlan_v2}.
     * Reads the existing object first so all other fields are preserved, then PATCHes back only
     * the changed contentList. Whether the call is fire-and-forget or blocking is controlled by
     * {@code cbplan.v4.content.sync.async} (default: {@code true}).
     *
     * @param caLinkedId  do_id of the linked Comprehensive Assessment content node
     * @param contentList plan's contentList as stored in Cassandra (JSON strings or plain IDs)
     */
    public void syncContentNodeTrainingPlan(String caLinkedId, List<String> contentList) {
        if (StringUtils.isBlank(caLinkedId)) {
            return;
        }
        if (cbExtServerProperties.isCbPlanContentSyncAsync()) {
            CompletableFuture.runAsync(() -> syncTrainingPlan(caLinkedId, contentList))
                    .exceptionally(e -> {
                        log.error("syncContentNodeTrainingPlan: Async sync failed - caLinkedId={}", caLinkedId, e);
                        return null;
                    });
        } else {
            syncTrainingPlan(caLinkedId, contentList);
        }
    }

    /**
     * Reads the existing {@code trainingPlan_v2} from the content node, replaces its
     * {@code contentList} with the updated entries, then PATCHes the result back.
     * Skips when the content node is missing or carries no {@code trainingPlan_v2}.
     *
     * @param caLinkedId  do_id of the linked content node
     * @param contentList plan's contentList to sync
     */
    private void syncTrainingPlan(String caLinkedId, List<String> contentList) {
        try {
            Map<String, Object> contentMetadata = contentLookupService.getContentMetadata(caLinkedId);
            if (MapUtils.isEmpty(contentMetadata)) {
                log.warn("syncTrainingPlan: Content node not found - caLinkedId={}", caLinkedId);
                return;
            }
            Map<String, Object> trainingPlanV2 = (Map<String, Object>) contentMetadata.get(Constants.TRAINING_PLAN_V2);
            if (MapUtils.isEmpty(trainingPlanV2)) {
                log.warn("syncTrainingPlan: No trainingPlan_v2 on content node - caLinkedId={}", caLinkedId);
                return;
            }
            Map<String, Object> updatedTrainingPlan = new HashMap<>(trainingPlanV2);
            updatedTrainingPlan.put(Constants.CONTENT_LIST, toTrainingPlanFormat(contentList));
            patchTrainingPlanV2(caLinkedId, updatedTrainingPlan);
        } catch (Exception e) {
            log.error("syncTrainingPlan: Failed - caLinkedId={}", caLinkedId, e);
        }
    }

    /**
     * Builds the PATCH payload and calls the learning service system update API.
     *
     * @param caLinkedId     do_id of the content node to update
     * @param trainingPlanV2 updated {@code trainingPlan_v2} object to write
     */
    private void patchTrainingPlanV2(String caLinkedId, Map<String, Object> trainingPlanV2) {
        String url = serverProperties.getLearningServiceVmBaseUrl()
                + serverProperties.getSystemUpdateAPI()
                + caLinkedId;
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.TRAINING_PLAN_V2, trainingPlanV2);
        Map<String, Object> requestWrapper = new HashMap<>();
        requestWrapper.put(Constants.CONTENT, content);
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.REQUEST, requestWrapper);
        Map<String, Object> response = outboundRequestHandlerService.fetchResultUsingPatch(
                url, requestBody, Collections.emptyMap());
        String responseCode = Objects.nonNull(response) ? (String) response.get(Constants.RESPONSE_CODE) : null;
        if (Constants.OK.equalsIgnoreCase(responseCode)) {
            log.info("patchTrainingPlanV2: Synced contentList - caLinkedId={}, contentCount={}",
                    caLinkedId, ((List<?>) trainingPlanV2.get(Constants.CONTENT_LIST)).size());
        } else {
            log.warn("patchTrainingPlanV2: Non-OK response - caLinkedId={}, responseCode={}", caLinkedId, responseCode);
        }
    }

    /**
     * Converts a Cassandra-format contentList to the {@code {identifier, mandatory}} object list
     * expected by {@code trainingPlan_v2.contentList}. V4 items are JSON strings; V3 items are plain IDs.
     *
     * @param contentList raw contentList entries from Cassandra
     * @return list of content objects ready for the PATCH payload
     */
    private List<Map<String, Object>> toTrainingPlanFormat(List<String> contentList) {
        if (CollectionUtils.isEmpty(contentList)) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> result = new ArrayList<>();
        for (String item : contentList) {
            Map<String, Object> parsed = parseContentItem(item);
            if (MapUtils.isNotEmpty(parsed)) {
                result.add(parsed);
            }
        }
        return result;
    }

    /**
     * Parses a single contentList entry. V4 entries are JSON strings deserialised directly;
     * V3 plain IDs are wrapped with {@code mandatory: false}.
     *
     * @param item raw contentList entry
     * @return parsed content map with at least identifier and mandatory fields
     */
    private Map<String, Object> parseContentItem(String item) {
        try {
            return mapper.readValue(item, new TypeReference<Map<String, Object>>() {
            });
        } catch (JsonProcessingException e) {
            log.debug("parseContentItem: Plain ID format, wrapping - item={}", item, e);
            Map<String, Object> map = new HashMap<>();
            map.put(Constants.IDENTIFIER, item);
            map.put(Constants.MANDATORY, Boolean.FALSE);
            return map;
        }
    }
}
