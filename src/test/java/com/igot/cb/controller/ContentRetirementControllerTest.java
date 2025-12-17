package com.igot.cb.controller;

import com.igot.cb.service.ContentRetirementService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    void runManually_ShouldTriggerRetirementAndReturnOk() {
        ResponseEntity<String> response = contentRetirementController.runManually();

        verify(contentRetirementService).processDueRetirements();
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("Content retirement job triggered", response.getBody());
    }
}