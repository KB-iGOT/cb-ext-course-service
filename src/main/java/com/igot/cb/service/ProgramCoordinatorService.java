package com.igot.cb.service;

import com.igot.cb.model.ApiResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ProgramCoordinatorService {

    ApiResponse upsertCoordinators(String programId, List<Map<String, String>> incomingCoordinators, String token) throws IOException;

    ApiResponse getProgramCoordinators(String programId, String token) throws IOException;
}
