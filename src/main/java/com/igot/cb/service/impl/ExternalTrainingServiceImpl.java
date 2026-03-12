package com.igot.cb.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.ExternalTrainingService;
import com.igot.cb.storage.service.StorageService;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.*;

@Service
public class ExternalTrainingServiceImpl implements ExternalTrainingService {

    private final Logger logger = LoggerFactory.getLogger(ExternalTrainingServiceImpl.class);

    @Autowired
    StorageService storageService;

    @Autowired
    CbExtServerProperties serverConfig;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    CassandraOperation cassandraOperation;

    @Autowired
    AccessTokenValidator accessTokenValidator;

    @Autowired
    private ObjectMapper mapper;

    @Override
    public ApiResponse externalTrainingUserBulkUpload(MultipartFile mFile, String eventId, String batchId) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_EXTERNAL_TRAINING_USER_BULK_UPLOAD);
        try {

            String userOrgId = "";
            String errMsg = validateEventDetailsAndCSVFile(eventId, batchId, mFile);
            if (StringUtils.isNotEmpty(errMsg)) {
                setErrorData(response, errMsg);
                return response;
            }

            if (isFileExistForProcessing(userOrgId, eventId, batchId)) {
                setErrorData(response, "Failed to upload for another request as previous request is in processing state, please try after some time.");
                return response;
            }

            ApiResponse uploadResponse = storageService.uploadFile(mFile, serverConfig.getExternalTrainingBulkUploadContainerName());
            if (!HttpStatus.OK.equals(uploadResponse.getResponseCode())) {
                setErrorData(response, String.format("Failed to upload file. Error: %s",
                        uploadResponse.getParams().getErrMsg()));
                return response;
            }

            Map<String, Object> uploadedFile = new HashMap<>();
            uploadedFile.put(Constants.CONTEXT_ID_CAMEL, eventId);
            uploadedFile.put(Constants.IDENTIFIER, UUID.randomUUID().toString());
            uploadedFile.put(Constants.FILE_NAME, uploadResponse.getResult().get(Constants.NAME));
            uploadedFile.put(Constants.FILE_PATH, uploadResponse.getResult().get(Constants.URL));
            uploadedFile.put(Constants.CREATED_ON, new Timestamp(System.currentTimeMillis()));
            uploadedFile.put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);

            ApiResponse insertResponse = (ApiResponse) cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD,
                    serverConfig.getExternalTrainingBulkUploadTable(), uploadedFile);

            if (!Constants.SUCCESS.equalsIgnoreCase((String) insertResponse.get(Constants.RESPONSE))) {
                setErrorData(response, "Failed to update database with event user bulk onboard file details.");
                return response;
            }

            uploadedFile.put(Constants.ORD_ID, userOrgId);
            uploadedFile.put(Constants.EVENT_ID, eventId);
            uploadedFile.put(Constants.BATCH_ID, batchId);
            kafkaTemplate.send(serverConfig.getExternalTrainingBulkUploadTopic(), mapper.writeValueAsString(uploadedFile));

            response.getParams().setStatus(Constants.SUCCESS);
            response.setResponseCode(HttpStatus.OK);
            response.getResult().putAll(uploadedFile);
        } catch (Exception e) {
            setErrorData(response,
                    String.format("Failed to process event user bulk onboard request. Error: ", e.getMessage()));
        }
        return response;
    }

    private void setErrorData(ApiResponse response, String errMsg) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrMsg(errMsg);
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    private boolean isFileExistForProcessing(String orgId, String eventId, String batchId) {
        Map<String, Object> bulkUploadPrimaryKey = new HashMap<String, Object>();
        bulkUploadPrimaryKey.put(Constants.ORD_ID, orgId);
        bulkUploadPrimaryKey.put(Constants.EVENT_ID, eventId);
        bulkUploadPrimaryKey.put(Constants.BATCH_ID, batchId);
        List<String> fields = Arrays.asList(Constants.ORG_ID, Constants.EVENT_ID, Constants.BATCH_ID, Constants.STATUS);

        List<Map<String, Object>> bulkUploadMdoList = cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD, serverConfig.getExternalTrainingBulkUploadTable(), bulkUploadPrimaryKey, fields, null);
        if (CollectionUtils.isEmpty(bulkUploadMdoList)) {
            return false;
        }
        return bulkUploadMdoList.stream()
                .anyMatch(entry -> Constants.STATUS_IN_PROGRESS_UPPERCASE.equalsIgnoreCase((String) entry.get(Constants.STATUS)));
    }

    private String validateEventDetailsAndCSVFile(String eventId, String batchId, MultipartFile mFile) {
        String errMsg;
        // Validate event details first
        errMsg = validateEventDetails(eventId, batchId);
        if (StringUtils.isNotEmpty(errMsg)) {
            logger.error("Validation failed for event details: {}", errMsg);
            return errMsg;
        }
        // Validate the CSV file next
        errMsg = validateCsvFile(mFile);
        if (StringUtils.isNotEmpty(errMsg)) {
            logger.error("Validation failed for CSV file: {}", errMsg);
            return errMsg;
        }
        return errMsg;
    }


    private String validateEventDetails(String eventId, String batchId) {
        String errMsg = "";
        logger.debug("Fetching event batch details for eventId: {} and batchId: {}", eventId, batchId);
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(Constants.EVENT_ID, eventId);
        propertiesMap.put(Constants.BATCH_ID, batchId);

        try {
            List<Map<String, Object>> eventBatchDetails = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD_COURSE,
                    Constants.EVENT_BATCH_TABLE,
                    propertiesMap,
                    null,
                    null
            );

            if (CollectionUtils.isEmpty(eventBatchDetails)) {
                errMsg = String.format("No event batch details found for eventId: %s and batchId: %s", eventId, batchId);
                logger.error(errMsg);
                return errMsg;
            }
        } catch (Exception e) {
            errMsg = String.format("Error while fetching event batch details for eventId: %s and batchId: %s", eventId, batchId);
            logger.error(errMsg, e);
            return errMsg;
        }

        return errMsg;
    }

    public String validateCsvFile(MultipartFile file) {
        String errMsg = "";
        // Check if the file is not null and not empty
        if (Objects.isNull(file) || file.isEmpty()) {
            errMsg = "File is empty or not provided.";
            return errMsg;
        }
        // Extract the file name and extension
        String fileName = file.getOriginalFilename();
        if (Objects.isNull(fileName)) {
            errMsg = "File name is invalid.";
            return errMsg;
        }
        // Validate the extension
        String extension = FilenameUtils.getExtension(fileName);
        if (!"csv".equalsIgnoreCase(extension)) {
            errMsg = "Invalid file type. Only CSV files are allowed.";
            return errMsg;
        }
        return errMsg;
    }

    @Override
    public ApiResponse externalTrainingUserBulkUploadStatus(String eventId, String batchId) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_EXTERNAL_TRAINING_USER_BULK_UPLOAD_STATUS);
        try {
            Map<String, Object> propertyMap = new HashMap<>();
            if (StringUtils.isNotEmpty(eventId)) {
                propertyMap.put(Constants.CONTEXT_ID_CAMEL, eventId);
            }
            List<Map<String, Object>> bulkUploadList = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD,
                    serverConfig.getExternalTrainingBulkUploadTable(), propertyMap, null, null);
            response.getParams().setStatus(Constants.SUCCESS);
            response.setResponseCode(HttpStatus.OK);
            response.getResult().put(Constants.CONTENT, bulkUploadList);
            response.getResult().put(Constants.COUNT, bulkUploadList != null ? bulkUploadList.size() : 0);
        } catch (Exception e) {
            setErrorData(response,
                    String.format("Failed to get user event bulk onboard request status. Error: ", e.getMessage()));
        }
        return response;
    }

    @Override
    public ResponseEntity<Resource> downloadFile(String fileName) {
        try {
            storageService.downloadFile(fileName, serverConfig.getExternalTrainingBulkUploadContainerName());
            Path tmpPath = Paths.get(Constants.LOCAL_BASE_PATH + fileName);
            ByteArrayResource resource = new ByteArrayResource(Files.readAllBytes(tmpPath));
            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"");
            return ResponseEntity.ok()
                    .headers(headers)
                    .contentLength(tmpPath.toFile().length())
                    .contentType(MediaType.parseMediaType(MediaType.MULTIPART_FORM_DATA_VALUE))
                    .body(resource);
        } catch (IOException e) {
            logger.error("Failed to read the downloaded file: " + fileName + ", Exception: ", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        } finally {
            try {
                File file = new File(Constants.LOCAL_BASE_PATH + fileName);
                if (file.exists()) {
                    file.delete();
                }
            } catch (Exception e1) {
            }
        }
    }
}
