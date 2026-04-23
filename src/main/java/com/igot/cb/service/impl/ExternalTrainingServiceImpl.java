package com.igot.cb.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.ExternalTrainingService;
import com.igot.cb.service.UserAndOrgServiceImpl;
import com.igot.cb.storage.service.StorageService;
import com.igot.cb.user.UserUtilityService;
import com.igot.cb.util.AccessTokenValidator;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
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

    @Autowired
    private UserAndOrgServiceImpl userAndOrgService;

    @Autowired
    private UserUtilityService userUtilityService;

    @Override
    public ApiResponse externalTrainingUserBulkUpload(MultipartFile mFile, String eventId, String batchId, String authToken) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_EXTERNAL_TRAINING_USER_BULK_UPLOAD);
        try {

            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isBlank(userId)) {
                return response;
            }

            String userOrgId = getRootOrgFromUser(userId, response);
            if (Constants.FAILED.equalsIgnoreCase(response.getParams().getStatus())) {
                return response;
            }

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
            uploadedFile.put(Constants.ORD_ID, userOrgId);
            uploadedFile.put(Constants.CONTEXT_ID_CAMEL, eventId);
            uploadedFile.put(Constants.BATCH_ID, batchId);
            uploadedFile.put(Constants.IDENTIFIER, UUID.randomUUID().toString());
            uploadedFile.put(Constants.FILE_NAME, uploadResponse.getResult().get(Constants.NAME));
            uploadedFile.put(Constants.FILE_PATH, uploadResponse.getResult().get(Constants.URL));
            uploadedFile.put(Constants.CREATED_BY, userId);
            uploadedFile.put(Constants.CREATED_ON, Instant.now());
            uploadedFile.put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);

            ApiResponse insertResponse = (ApiResponse) cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD,
                    serverConfig.getExternalTrainingBulkUploadTable(), uploadedFile);

            if (!Constants.SUCCESS.equalsIgnoreCase((String) insertResponse.get(Constants.RESPONSE))) {
                setErrorData(response, "Failed to update database with event user bulk onboard file details.");
                return response;
            }

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
        bulkUploadPrimaryKey.put(Constants.CONTEXT_ID_KEY, eventId);
        bulkUploadPrimaryKey.put(Constants.BATCH_ID, batchId);
        List<String> fields = Arrays.asList(Constants.ORG_ID, Constants.CONTEXT_ID_KEY, Constants.BATCH_ID, Constants.STATUS);

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

        // 1. File null / empty check
        if (Objects.isNull(file) || file.isEmpty()) {
            return "File is empty or not provided.";
        }
        // 2. File name validation
        String fileName = file.getOriginalFilename();
        if (StringUtils.isBlank(fileName)) {
            return "File name is invalid.";
        }
        // 3. Extension validation
        String extension = FilenameUtils.getExtension(fileName);
        if (!"csv".equalsIgnoreCase(extension)) {
            return "Invalid file type. Only CSV files are allowed.";
        }
        int externalTrainingBatchSize = serverConfig.getExternalTrainingBatchSize();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8))) {
            String header = reader.readLine();
            if (header == null) {
                return "CSV header is missing. Expected header: Email";
            }
            // 4. Header validation
            if (!"Email".equalsIgnoreCase(header.trim())) {
                return "Invalid CSV header. Expected header: Email";
            }
            // 5. Row count validation
            int dataRowCount = 0;
            while (reader.readLine() != null) {
                dataRowCount++;
                if (dataRowCount > externalTrainingBatchSize) {
                    return "CSV file should not contain more than " + externalTrainingBatchSize + " rows.";
                }
            }
            // 6. check if file has only header
            if (dataRowCount == 0) {
                return "CSV file contains no data rows.";
            }
        } catch (Exception e) {
            return "Error while reading CSV file.";
        }
        return "";
    }

    @Override
    public ApiResponse externalTrainingUserBulkUploadStatus(String eventId, String batchId, String authToken) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_EXTERNAL_TRAINING_USER_BULK_UPLOAD_STATUS);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isBlank(userId)) {
                return response;
            }

            String userOrgId = getRootOrgFromUser(userId, response);
            if (Constants.FAILED.equalsIgnoreCase(response.getParams().getStatus())) {
                return response;
            }

            Map<String, Object> propertyMap = new HashMap<>();
            if (StringUtils.isNotEmpty(eventId)) {
                propertyMap.put(Constants.ORD_ID, userOrgId);
                propertyMap.put(Constants.CONTEXT_ID_CAMEL, eventId);
                propertyMap.put(Constants.BATCH_ID, batchId);
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
    public ResponseEntity<?> downloadFile(String fileName, String authToken) {
        try {
            ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_EXTERNAL_TRAINING_USER_BULK_UPLOAD_STATUS);
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isBlank(userId)) {
                return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(response);
            }

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
            logger.error("Failed to read the downloaded file: {}, Exception: ", fileName, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }

    }

    private String getRootOrgFromUser(String userId, ApiResponse response) {
        String rootOrgId = null;
        Map<String, Object> userMap = userAndOrgService.readUserProfileFromDB(userId,
                Arrays.asList(Constants.ID, Constants.ROOT_ORG_ID));
        if (MapUtils.isEmpty(userMap)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams()
                    .setErrMsg("Failed to read user details from DB. UserId: " + userId);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return rootOrgId;
        }
        rootOrgId = (String) userMap.get(Constants.ROOT_ORG_ID);

        return rootOrgId;
    }

    private ResponseEntity<Resource> createErrorResponse(String message, HttpStatus status) {
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);

        return ResponseEntity.status(status)
                .headers(headers)
                .body(new ByteArrayResource(message.getBytes()));
    }

    @Override
    public ResponseEntity<Resource> downloadBulkUploadSampleFile() {
        String fileName = serverConfig.getExternalTrainingUserBulkUploadSampleFileName();
        Path filePath = Paths.get(Constants.LOCAL_BASE_PATH, fileName);
        try {
            storageService.downloadFile(fileName, serverConfig.getExternalTrainingBulkUploadContainerName());
            byte[] fileBytes = Files.readAllBytes(filePath);
            ByteArrayResource resource = new ByteArrayResource(fileBytes);
            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"")
                    .contentLength(fileBytes.length)
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .body(resource);

        } catch (IOException e) {
            logger.error("Failed to read downloaded file: {}", fileName, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        } finally {
            try {
                Files.deleteIfExists(filePath);
            } catch (IOException ex) {
                logger.warn("Failed to delete temp file: {}", filePath);
            }
        }
    }
}
