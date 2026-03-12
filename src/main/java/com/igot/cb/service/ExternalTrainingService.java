package com.igot.cb.service;

import com.igot.cb.model.ApiResponse;
import org.springframework.core.io.Resource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.multipart.MultipartFile;

public interface ExternalTrainingService {

    public ApiResponse externalTrainingUserBulkUpload(MultipartFile multipartFile, String eventId, String batchId);
    public ApiResponse externalTrainingUserBulkUploadStatus(String eventId, String batchId);
    public ResponseEntity<Resource> downloadFile(String fileName);
}
