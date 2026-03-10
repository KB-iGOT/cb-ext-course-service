package com.igot.cb.health.controller;

import com.igot.cb.health.service.HealthService;
import com.igot.cb.model.ApiResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HealthControllerTest {

    @Mock
    private HealthService healthService;

    @InjectMocks
    private HealthController healthController;

    @Test
    void healthCheck_ShouldReturnApiResponse() {

        ApiResponse response = new ApiResponse();
        response.setResponseCode(HttpStatus.OK);

        when(healthService.checkHealthStatus()).thenReturn(response);

        ResponseEntity<ApiResponse> result = healthController.healthCheck();

        assertEquals(HttpStatus.OK, result.getStatusCode());
        assertEquals(response, result.getBody());

        verify(healthService, times(1)).checkHealthStatus();
    }

    @Test
    void livenessCheck_ShouldReturnStatusOk() {

        ResponseEntity<String> response = healthController.livenessCheck();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("Status ok", response.getBody());
    }
}
