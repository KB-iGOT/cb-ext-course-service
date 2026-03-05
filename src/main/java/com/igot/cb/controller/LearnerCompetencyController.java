package com.igot.cb.controller;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.CompetencyService;
import com.igot.cb.util.Constants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@RequiredArgsConstructor
public class LearnerCompetencyController {


    private final CompetencyService competencyService;

    /**
     * GET endpoint to fetch user competency data.
     *
     * @param authToken The authentication token (from x-authenticated-user-token header)
     * @return ResponseEntity containing the competency data or Send Kafka event message
     */
    @GetMapping("/learner/v1/competency")
    public ResponseEntity<Object> getLearnerCompetency(
            @RequestHeader(value = Constants.X_AUTH_TOKEN, required = true) String authToken) {

        ApiResponse response = competencyService.fetchUserCompetency(authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}

