package com.igot.cb.controller;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.ContentStateServiceImpl;
import com.igot.cb.service.LearningPathwayRetireService;
import com.igot.cb.util.Constants;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/learningpathway")
public class LearningPathwayRetireController {

    private final LearningPathwayRetireService learningPathwayRetireService;

    public LearningPathwayRetireController(LearningPathwayRetireService learningPathwayRetireService ) {
        this.learningPathwayRetireService=learningPathwayRetireService;
    }

    @GetMapping("/v1/retire/{contentId}")
    public ResponseEntity<ApiResponse> retireLearningPathway(@PathVariable("contentId") String contentId,
                                                             @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = learningPathwayRetireService.retireLearningPathway(authToken,contentId);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
