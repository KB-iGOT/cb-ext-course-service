package com.igot.cb.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.common.ServerProperties;
import com.igot.cb.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.util.Constants;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Component
public class VideoOnDemandKafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(VideoOnDemandKafkaConsumer.class);

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private ServerProperties serverProperties;

    @Autowired
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @KafkaListener(topics = "${spring.kafka.content.metadata.update.topic.name}", groupId = "${spring.kafka.content.metadata.update.consumer.group.id}")
    public void contentMetadataUpdateConsumerForVOD(ConsumerRecord<String, String> data) {
        logger.debug("Processing Kafka message from topic: {}", data.topic());

        if (StringUtils.isBlank(data.value())) {
            logger.warn("Received empty message from Kafka topic: {}", data.topic());
            return;
        }

        CompletableFuture.runAsync(() -> processMetadataUpdate(data.value()))
                .exceptionally(ex -> {
                    logger.error("Failed to process metadata update asynchronously for topic: {}", data.topic(), ex);
                    return null;
                });
    }

    private void processMetadataUpdate(String messageValue) {
        try {
            Map<String, String> data = mapper.readValue(messageValue, new TypeReference<Map<String, String>>() {
            });

            if (!isValidRequest(data)) {
                logger.warn("Invalid request data: missing identifier or streaming URL");
                return;
            }

            String identifier = data.get(Constants.IDENTIFIER);
            String streamingUrl = data.get(Constants.STREAMING_URI);

            updateContentMetadata(identifier, streamingUrl);

        } catch (JsonProcessingException e) {
            logger.error("Failed to parse Kafka message: {}", messageValue, e);
        } catch (Exception e) {
            logger.error("Unexpected error processing metadata update for message: {}", messageValue, e);
        }
    }

    private boolean isValidRequest(Map<String, String> data) {
        if (data == null || data.isEmpty()) {
            return false;
        }

        String identifier = data.get(Constants.IDENTIFIER);
        String streamingUrl = data.get(Constants.STREAMING_URI);

        return StringUtils.isNotBlank(identifier) && StringUtils.isNotBlank(streamingUrl);
    }

    private void updateContentMetadata(String identifier, String streamingUrl) {
        logger.debug("Updating content metadata for identifier: {}", identifier);

        try {
            String url = buildUpdateUrl(identifier);
            Map<String, String> headers = createHeaders();
            Map<String, Object> requestPayload = buildRequestPayload(streamingUrl);

            Map<String, Object> response = outboundRequestHandlerService.fetchResultUsingPatch(url, requestPayload, headers);

            handleUpdateResponse(identifier, response);

        } catch (Exception e) {
            logger.error("Failed to update content metadata for identifier: {}", identifier, e);
        }
    }

    private String buildUpdateUrl(String identifier) {
        return serverProperties.getLearningServiceVmBaseUrl() +
                serverProperties.getSystemUpdateAPI() +
                identifier;
    }

    private Map<String, String> createHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
        return headers;
    }

    private Map<String, Object> buildRequestPayload(String streamingUrl) {
        String transformedUrl = streamingUrl.replaceAll(
                serverProperties.getVodBucketPrefix(),
                serverProperties.getVodStreamUrlPrefix()
        );

        Map<String, Object> content = new HashMap<>();
        content.put(Constants.STREAMING_URL, transformedUrl);

        Map<String, Object> contentRequest = new HashMap<>();
        contentRequest.put(Constants.CONTENT, content);

        Map<String, Object> request = new HashMap<>();
        request.put(Constants.REQUEST, contentRequest);

        return request;
    }

    private void handleUpdateResponse(String identifier, Map<String, Object> response) {
        if (response == null || response.isEmpty()) {
            logger.warn("Received empty response for identifier: {}", identifier);
            return;
        }

        String responseCode = (String) response.get(Constants.RESPONSE_CODE);

        if (Constants.OK.equalsIgnoreCase(responseCode)) {
            logger.info("Successfully updated metadata for identifier: {}", identifier);
        } else {
            logger.warn("Failed to update metadata for identifier: {}, response code: {}", identifier, responseCode);
        }
    }
}
