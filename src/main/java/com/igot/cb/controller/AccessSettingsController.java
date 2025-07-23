package com.igot.cb.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.AccessSettingMigrationServiceImpl;
import com.igot.cb.service.AccessSettingsServiceImpl;
import com.igot.cb.util.Constants;

@RestController
@RequestMapping("/accessSettings")
public class AccessSettingsController {

  private final AccessSettingsServiceImpl accessSettingsService;
  private final AccessSettingMigrationServiceImpl accessSettingMigrationService;

  public AccessSettingsController(AccessSettingsServiceImpl accessSettingsService, AccessSettingMigrationServiceImpl accessSettingMigrationService) {
    this.accessSettingsService = accessSettingsService;
    this.accessSettingMigrationService = accessSettingMigrationService;
  }

  //createand update API
  @PutMapping("/v1/upsert")
  public ResponseEntity<ApiResponse> upsert(@RequestBody Map<String, Object> userGroupDetails,
      @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
    ApiResponse response = accessSettingsService.upsert(userGroupDetails, authToken);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @GetMapping("/read/{contentId}")
  public ResponseEntity<ApiResponse> read(@PathVariable("contentId") String contentId) {
    ApiResponse response = accessSettingsService.read(contentId);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @DeleteMapping("/v1/delete/{contentId}")
  public ResponseEntity<ApiResponse> delete(@PathVariable("contentId") String contentId) {
    ApiResponse response = accessSettingsService.delete(contentId);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @GetMapping("/v1/migrate")
  public ResponseEntity<ApiResponse> migrateAccessSettingRules() {
    ApiResponse response = accessSettingMigrationService.migrateAccessSettingRules();
    return new ResponseEntity<>(response, response.getResponseCode());
  }
}
