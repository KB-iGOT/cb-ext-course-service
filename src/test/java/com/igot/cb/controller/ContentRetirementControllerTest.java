package com.igot.cb.controller;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.ContentRetirementService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ContentRetirementControllerTest {

    @Mock
    private ContentRetirementService contentRetirementService;

    private ContentRetirementController contentRetirementController;

    @BeforeEach
    void setUp() {
        contentRetirementController = new ContentRetirementController(contentRetirementService);
    }

    @Test
    void runManually_ShouldCallServiceAndReturnResponse() {
        ApiResponse mockResponse = new ApiResponse();
        when(contentRetirementService.processDueRetirements()).thenReturn(mockResponse);

        ResponseEntity<ApiResponse> response = contentRetirementController.runManually();

        verify(contentRetirementService).processDueRetirements();
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockResponse, response.getBody());
    }

    @Test
    void triggerNotifications_ShouldReturnCreatedStatus() {
        ResponseEntity<String> response = contentRetirementController.triggerNotifications();

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals("Notification job accepted", response.getBody());
    }

    @Test
    void constructor_ShouldInitializeService() {
        assertNotNull(contentRetirementController);
    }

    @Test
    void triggerNotificationsToSpv_ShouldCallServiceAndReturnCreatedStatus() {
        ResponseEntity<String> response =
                contentRetirementController.triggerNotificationsToSpv();
        verify(contentRetirementService)
                .sendContentRetirementNotificationsToSpv();

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals("Notification job accepted", response.getBody());
    }

}