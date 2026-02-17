package com.igot.cb.service;

import com.igot.cb.model.ApiResponse;
import org.springframework.stereotype.Service;

@Service
public interface LearningPathwayRetireService {

    public ApiResponse retireLearningPathway(String userToken, String contentId);

}
