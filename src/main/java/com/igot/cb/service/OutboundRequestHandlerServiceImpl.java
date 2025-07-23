package com.igot.cb.service;

import java.lang.reflect.Type;
import java.util.Map;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.igot.cb.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class OutboundRequestHandlerServiceImpl {
	private final RestTemplate restTemplate;
	private final ObjectMapper objectMapper;

	public OutboundRequestHandlerServiceImpl(RestTemplate restTemplate) {
		this.restTemplate = restTemplate;
		this.objectMapper = new ObjectMapper();
		this.objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
	}

	public Object fetchResult(String uri) {

		Object response = null;
		try {
			if (log.isDebugEnabled()) {
				StringBuilder str = new StringBuilder(this.getClass().getCanonicalName())
						.append(Constants.FETCH_RESULT_CONSTANT).append(System.lineSeparator());
				str.append(Constants.URI_CONSTANT).append(uri).append(System.lineSeparator());
				log.debug(str.toString());
			}
			response = restTemplate.getForObject(uri, Map.class);
		} catch (HttpClientErrorException e) {
			log.error("Failed to call rest URL: {}", uri, e);
			try {
				response = objectMapper.readValue(e.getResponseBodyAsString(),
						new TypeReference<Map<String, Object>>() {
						});
			} catch (Exception e1) {
				log.debug("Failed to parse error response: {}", e.getResponseBodyAsString(), e1);
			}
			log.error("Error received: " + e.getResponseBodyAsString(), e);
		} catch (Exception e) {
			log.error("Failed to call rest URL: {}", uri, e);
			try {
				log.warn("Error Response: " + objectMapper.writeValueAsString(response));
			} catch (Exception e1) {
				log.debug("Failed to parse error response: ", e1);
			}
		}
		return response;
	}

	public <T> T fetchResultUsingExchange(String uri, ParameterizedTypeReference<T> responseType) {
		Object response = null;
		try {
			if (log.isDebugEnabled()) {
				StringBuilder str = new StringBuilder(this.getClass().getCanonicalName())
						.append(Constants.FETCH_RESULT_CONSTANT).append(System.lineSeparator());
				str.append(Constants.URI_CONSTANT).append(uri).append(System.lineSeparator());
				log.debug(str.toString());
			}
			ResponseEntity<T> responseEntity = restTemplate.exchange(uri, HttpMethod.GET, null, responseType);
			return responseEntity.getBody();
		} catch (HttpClientErrorException e) {
			log.error("Failed to call rest URL: {}, received error: {}", uri, e.getResponseBodyAsString(), e);
			try {
				response = objectMapper.readValue(e.getResponseBodyAsString(),
						new TypeReference<Map<String, Object>>() {
						});
			} catch (Exception e1) {
				log.debug("Failed to parse error response: {}", e.getResponseBodyAsString(), e1);
			}
			log.error("Error received: " + e.getResponseBodyAsString(), e);
		} catch (Exception e) {
			log.error("Failed to call rest URL: {}", uri, e);
			try {
				log.warn("Error Response: " + objectMapper.writeValueAsString(response));
			} catch (Exception e1) {
				log.debug("Failed to parse error response: ", e1);
			}
		}
		return null;
	}
}
