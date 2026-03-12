package com.igot.cb.controller;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.ExternalTrainingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@RestController
@RequestMapping("/externalTraining/v1")
public class ExternalTrainingController {


    @Autowired
    ExternalTrainingService externalTrainingService;

    @PostMapping("/bulkUpload")
    public ResponseEntity<?> externalTrainingUserBulkUpload(@RequestParam("file") MultipartFile multipartFile, @RequestParam(value = "eventId") String eventId, @RequestParam("batchId") String batchId) throws IOException {
        ApiResponse uploadResponse = externalTrainingService.externalTrainingUserBulkUpload(multipartFile, eventId, batchId);
        return new ResponseEntity<>(uploadResponse, uploadResponse.getResponseCode());

    }
    @GetMapping("/bulkUpload/status")
    public ResponseEntity<?> externalTrainingUserBulkUploadStatus(@RequestParam("eventId") String eventId, @RequestParam("batchId") String batchId) {
        ApiResponse response = externalTrainingService.externalTrainingUserBulkUploadStatus(eventId, batchId);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/bulkUpload/download/{fileName}")
    public ResponseEntity<?> downloadFile(@PathVariable("fileName") String fileName) {
        return externalTrainingService.downloadFile(fileName);
    }

}
