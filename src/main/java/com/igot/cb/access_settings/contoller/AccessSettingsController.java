package com.igot.cb.access_settings.contoller;

import com.igot.cb.access_settings.service.AccessSettingsService;
import com.igot.cb.transactional.util.ApiResponse;
import com.igot.cb.transactional.util.Constants;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@RestController
@RequestMapping("/accessSetttings")
public class AccessSettingsController {

  @SuppressWarnings("unused")
  @Autowired
  private AccessSettingsService accessSettingsService;

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

}
