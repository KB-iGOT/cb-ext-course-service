package com.igot.cb.controller;

import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.ExternalTrainingService;
import com.igot.cb.util.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@RestController
@RequestMapping("/externaltraining/v1")
public class ExternalTrainingController {


    @Autowired
    ExternalTrainingService externalTrainingService;

    @PostMapping("/bulkupload/{eventId}/{batchId}")
    public ResponseEntity<?> externalTrainingUserBulkUpload(@RequestParam("file") MultipartFile multipartFile, @PathVariable(value = "eventId") String eventId, @PathVariable("batchId") String batchId, @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) throws IOException {
        ApiResponse uploadResponse = externalTrainingService.externalTrainingUserBulkUpload(multipartFile, eventId, batchId, authToken);
        return new ResponseEntity<>(uploadResponse, uploadResponse.getResponseCode());

    }
    @GetMapping("/bulkupload/status")
    public ResponseEntity<?> externalTrainingUserBulkUploadStatus(@RequestParam("eventId") String eventId, @RequestParam("batchId") String batchId, @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        ApiResponse response = externalTrainingService.externalTrainingUserBulkUploadStatus(eventId, batchId, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/bulkupload/download/{fileName}")
    public ResponseEntity<?> downloadFile(@PathVariable("fileName") String fileName, @RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        return externalTrainingService.downloadFile(fileName, authToken);
    }

    @GetMapping("/bulkupload/sample")
    public ResponseEntity<?> downloadBulkUploadSampleFile() {
        return externalTrainingService.downloadBulkUploadSampleFile();
    }

}
