package com.igot.cb.health.controller;

import com.igot.cb.health.service.HealthService;
import com.igot.cb.model.ApiResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class HealthController {

    private final HealthService healthService;

    @GetMapping("/health")
    public ResponseEntity<ApiResponse> healthCheck() {
        ApiResponse response = healthService.checkHealthStatus();
        return ResponseEntity.status(response.getResponseCode()).body(response);
    }

    @GetMapping("/liveness")
    public ResponseEntity<String> livenessCheck() {
        return new ResponseEntity<>("Status ok", HttpStatus.OK);
    }
}
