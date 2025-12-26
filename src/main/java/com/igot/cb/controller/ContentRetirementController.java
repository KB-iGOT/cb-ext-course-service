package com.igot.cb.controller;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.ContentRetirementService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/admin/content/retirement")
public class ContentRetirementController {

    private final ContentRetirementService contentRetirementService;

    public ContentRetirementController(ContentRetirementService contentRetirementService) {
        this.contentRetirementService = contentRetirementService;
    }

    @GetMapping("/schedule")
    public ResponseEntity<ApiResponse> runManually() {
        ApiResponse response = contentRetirementService.processDueRetirements();
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/notify/users")
    public ResponseEntity<String> triggerNotifications() {

        //notificationService.triggerNotificationJob();

        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body("Notification job accepted");
    }

}

