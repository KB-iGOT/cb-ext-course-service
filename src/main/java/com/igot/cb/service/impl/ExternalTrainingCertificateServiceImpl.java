package com.igot.cb.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class ExternalTrainingCertificateServiceImpl {

    @Autowired
    CbExtServerProperties serverProperties;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper mapper;

    public void generateCertificateEventAndPushToKafka(Map<String, Object> userDetailsMap, Map<String, Object> eventDetailsMap) throws JsonProcessingException {
        String eventJson = generateCertificateEvent(userDetailsMap, eventDetailsMap);
        String topic = serverProperties.getUserIssueCertificateForEventTopic();
        kafkaTemplate.send(topic, (String) userDetailsMap.get(Constants.USER_ID), eventJson);
    }

    public String generateCertificateEvent(Map<String, Object> userDetailsMap, Map<String, Object> eventDetailsMap) throws JsonProcessingException {

        Map<String, Object> event = new HashMap<>();
        double eventCompletionPercentage = 100.0;
        String userId = getRequiredString(userDetailsMap, Constants.USER_ID);
        String batchId = getRequiredString(eventDetailsMap, Constants.BATCH_ID);
        String eventId = getRequiredString(eventDetailsMap, Constants.EVENT_ID);
        String issuedDate = getRequiredString(eventDetailsMap, Constants.ISSUED_DATE);
        String recipientName = getRequiredString(userDetailsMap, Constants.FIRSTNAME);
        String orgId = getRequiredString(userDetailsMap, Constants.ROOT_ORG_ID);
        String baseUrl = serverProperties.getDomainHost();
        String certTemplate = getRequiredString(eventDetailsMap, Constants.CERT_TEMPLATE);
        String templateId = getRequiredString(eventDetailsMap, Constants.CERT_TEMPLATE_ID);
        String providerName = getRequiredString(eventDetailsMap, Constants.SOURCE_NAME);
        String eventName = getRequiredString(eventDetailsMap, Constants.EVENT_NAME);
        // Actor
        Map<String, Object> actor = new HashMap<>();
        actor.put("id", "Certificate Generator");
        actor.put("type", "System");
        event.put("actor", actor);

        event.put("eid", "BE_JOB_REQUEST");

        // edata
        Map<String, Object> edata = new HashMap<>();
        edata.put("eventType", "externalTraining");
        edata.put("name", "external training certificate template");
        edata.put("coursePosterImage", serverProperties.getExternalTrainingDefaultPosterImage());
        edata.put("tag", batchId);

        Map<String, Object> criteria = new HashMap<>();
        criteria.put("narrative", "external training certificate template");
        edata.put("criteria", criteria);

        edata.put("issuedDate", issuedDate);

        // Recipient Data
        List<Map<String, Object>> data = new ArrayList<>();
        Map<String, Object> recipient = new HashMap<>();
        recipient.put("recipientName", recipientName);
        recipient.put("recipientId", userId);
        data.add(recipient);
        edata.put("data", data);

        edata.put("parentCollections", new ArrayList<>());
        edata.put("primaryCategory", "externalTraining");

        // Issuer
        Map<String, Object> issuer = new HashMap<>();
        issuer.put("name", "in");
        issuer.put("url", "https://igotkarmayogi.gov.in");
        edata.put("issuer", issuer);

        edata.put("orgId", orgId);
        edata.put("basePath", baseUrl + "certs");

        // Signatory
        List<Map<String, Object>> signatoryList = new ArrayList<>();
        Map<String, Object> signatory = new HashMap<>();
        signatory.put("image", "https://diksha.gov.in/gj/header-logo.png");
        signatory.put("name", "Govt Of India");
        signatory.put("id", "in");
        signatory.put("designation", "Home Minister");
        signatoryList.add(signatory);
        edata.put("signatoryList", signatoryList);

        edata.put("svgTemplate", certTemplate);
        edata.put("templateId", templateId);

        // Related
        Map<String, Object> related = new HashMap<>();
        related.put("batchId", batchId);
        related.put("eventId", eventId);
        related.put("type", "external training certificate template");
        edata.put("related", related);

        edata.put("providerName", providerName);
        edata.put("oldId", "");
        edata.put("userId", userId);
        edata.put("eventCompletionPercentage", eventCompletionPercentage);
        edata.put("courseName", eventName);

        event.put("edata", edata);

        // Timestamp
        event.put("ets", eventDetailsMap.get("ets"));

        // Context
        Map<String, Object> context = new HashMap<>();
        Map<String, Object> pdata = new HashMap<>();
        pdata.put("ver", "1.0");
        pdata.put("id", "org.sunbird.learning.platform");
        context.put("pdata", pdata);
        event.put("context", context);

        event.put("mid", "LMS." + UUID.randomUUID());

        // Object
        Map<String, Object> object = new HashMap<>();
        object.put("id", userId);
        object.put("type", "GenerateCertificate");
        event.put("object", object);

        return mapper.writeValueAsString(event);
    }

    private String getRequiredString(Map<String, Object> map, String key) {
        if (MapUtils.isEmpty(map)) {
            throw new IllegalArgumentException("Input map is null or empty");
        }
        Object value = map.get(key);
        if (Objects.isNull(value)) {
            throw new IllegalArgumentException("Missing required field: " + key);
        }
        String str = StringUtils.trimToEmpty(value.toString());
        if (StringUtils.isBlank(str)) {
            throw new IllegalArgumentException("Empty value for field: " + key);
        }
        return str;
    }
}