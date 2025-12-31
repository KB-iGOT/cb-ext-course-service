package com.igot.cb.service;

import com.igot.cb.model.ApiResponse;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

public interface NotificationService {

    ApiResponse notifyAssignmentUploaded(Map<String, Object> requestData, String authToken);

    ApiResponse notifyAssignmentEvaluate(Map<String, Object> requestData, String authToken);

    ApiResponse notifyAssignmentSubmit(Map<String, Object> requestData, String authToken);

    void sendNotificationForContentRetirement(String contentId, String contentName, LocalDate retirementDate, List<String> userId, String notificationType);
}
