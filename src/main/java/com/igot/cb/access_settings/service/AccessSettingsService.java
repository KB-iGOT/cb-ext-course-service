package com.igot.cb.access_settings.service;

import com.igot.cb.transactional.util.ApiResponse;
import java.util.Map;

public interface AccessSettingsService {

  ApiResponse upsert(Map<String, Object> userGroupDetails, String authToken);

  ApiResponse read(String contentId);

  ApiResponse delete(String contentId);
}
