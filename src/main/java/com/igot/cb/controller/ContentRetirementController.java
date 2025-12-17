package com.igot.cb.controller;

import com.igot.cb.service.ContentRetirementService;
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

    @GetMapping("/run")
    public ResponseEntity<String> runManually() {
        contentRetirementService.processDueRetirements();
        return ResponseEntity.ok("Content retirement job triggered");
    }
}

