package com.igot.cb.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.ProviderAccessSettingsServiceImpl;
import com.igot.cb.util.Constants;

@RestController
@RequestMapping("/accessSettings/provider")
public class ProviderAccessSettingsController {

  private final ProviderAccessSettingsServiceImpl providerAccessSettingsService;

  public ProviderAccessSettingsController(ProviderAccessSettingsServiceImpl providerAccessSettingsService) {
    this.providerAccessSettingsService = providerAccessSettingsService;
  }

  @PutMapping("/v1/upsert")
  public ResponseEntity<ApiResponse> upsert(@RequestBody Map<String, Object> request,
      @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
    ApiResponse response = providerAccessSettingsService.upsert(request, authToken);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @GetMapping("/v1/read/{partnerId}")
  public ResponseEntity<ApiResponse> read(@PathVariable("partnerId") String partnerId,
      @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
    ApiResponse response = providerAccessSettingsService.read(partnerId, authToken);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @PutMapping("/v1/notification/upsert")
  public ResponseEntity<ApiResponse> notificationUpsert(@RequestBody Map<String, Object> request,
      @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
    ApiResponse response = providerAccessSettingsService.notificationUpsert(request, authToken);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  // Service-to-service: called by cb_external_enrollment_service when providerPreference is true.
  @PostMapping("/v1/validate")
  public ResponseEntity<ApiResponse> validate(@RequestBody Map<String, Object> request) {
    ApiResponse response = providerAccessSettingsService.validate(request);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  // Service-to-service: called by cb-pores-service before it saves a provider update.
  @PostMapping("/v1/rebalance")
  public ResponseEntity<ApiResponse> rebalance(@RequestBody Map<String, Object> request) {
    ApiResponse response = providerAccessSettingsService.rebalance(request);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @GetMapping("/v1/notification/read/{partnerId}")
  public ResponseEntity<ApiResponse> notificationRead(@PathVariable("partnerId") String partnerId,
      @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
    ApiResponse response = providerAccessSettingsService.notificationRead(partnerId, authToken);
    return new ResponseEntity<>(response, response.getResponseCode());
  }
}
