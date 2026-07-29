package com.igot.cb.controller;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.ProgramCoordinatorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
public class ProgramCoordinatorController {

    @Autowired
    private ProgramCoordinatorService programCoordinatorService;

    @PostMapping("/v1/program/{programId}/coordinator")
    public ResponseEntity<ApiResponse> upsertProgramCoordinator(
            @RequestHeader("x-authenticated-user-token") String authUserToken,
            @PathVariable("programId") String programId,
            @RequestBody List<Map<String, String>> incomingCoordinators) throws IOException {

        ApiResponse response = programCoordinatorService.upsertCoordinators(programId, incomingCoordinators, authUserToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/v1/program/{programId}/coordinators")
    public ResponseEntity<ApiResponse> getProgramCoordinators(
            @PathVariable("programId") String programId, @RequestHeader("x-authenticated-user-token") String authUserToken) throws IOException {

        ApiResponse response = programCoordinatorService.getProgramCoordinators(programId, authUserToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

}
