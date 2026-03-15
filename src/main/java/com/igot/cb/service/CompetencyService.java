package com.igot.cb.service;

import com.igot.cb.model.ApiResponse;

public interface CompetencyService {

    ApiResponse fetchUserCompetency(String authToken);
}

