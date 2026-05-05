package com.igot.cb.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.service.ContentInfoServiceImpl;
import com.igot.cb.service.NotificationService;
import com.igot.cb.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.service.impl.ExternalTrainingCertificateServiceImpl;
import com.igot.cb.storage.service.StorageService;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@Service
public class ExternalTrainingBulkUploadConsumer {

    private final Logger logger = LoggerFactory.getLogger(ExternalTrainingBulkUploadConsumer.class);

    ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    CbExtServerProperties serverProperties;

    @Autowired
    CassandraOperation cassandraOperation;

    @Autowired
    StorageService storageService;

    @Autowired
    OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    private ExternalTrainingCertificateServiceImpl externalTrainingCertificateService;

    @Autowired
    private ContentInfoServiceImpl contentInfoService;

    private final NotificationService notificationService;

    @Autowired
    public ExternalTrainingBulkUploadConsumer(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @KafkaListener(topics = "${external.training.user.bulk.upload.topic}", groupId = "${external.training.user.bulk.upload.topic.group}")
    public void processExternalTrainingBulkUploadMessage(ConsumerRecord<String, String> data) {
        logger.info(
                "ExternalTrainingBulkUploadConsumer::processMessage: Received event to initiate Public user event Bulk Upload Process...");
        logger.info("Received message:: " + data.value());
        try {
            if (StringUtils.isNoneBlank(data.value())) {
                CompletableFuture.runAsync(() -> {
                    try {
                        initiateExternalTrainingBulkUploadProcess(data.value());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            } else {
                logger.error("Error in External Training Bulk Upload Consumer: Invalid Kafka Msg");
            }
        } catch (Exception e) {
            logger.error(String.format("Error in External Training Bulk Upload Consumer: Error Msg :%s", e.getMessage()), e);
        }
    }


    public void initiateExternalTrainingBulkUploadProcess(String inputData) throws IOException {
        logger.info("ExternalTrainingBulkUploadConsumer:: initiateExternalTrainingBulkUploadProcess: Started");
        long startTime = System.currentTimeMillis();
        Map<String, String> inputDataMap = objectMapper.readValue(inputData,
                new TypeReference<HashMap<String, String>>() {
                });

        List<String> errList = validateReceivedKafkaMessage(inputDataMap);
        if (errList.isEmpty()) {
            updateUserBulkUploadStatus(inputDataMap.get(Constants.ORD_ID), inputDataMap.get(Constants.CONTEXT_ID_CAMEL), inputDataMap.get(Constants.BATCH_ID),
                    inputDataMap.get(Constants.IDENTIFIER), Constants.STATUS_IN_PROGRESS_UPPERCASE, 0, 0, 0);
            String fileName = inputDataMap.get(Constants.FILE_NAME);
            logger.info("fileName {} ", fileName);
            storageService.downloadFile(fileName, serverProperties.getExternalTrainingBulkUploadContainerName());
            processExternalTrainingBulkUpload(inputDataMap);
        } else {
            logger.error(String.format("Error in the Kafka Message Received : %s", errList));
        }
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        logger.info("Total time taken to process External Training Bulk Upload : " + totalTime);

    }

    private void processExternalTrainingBulkUpload(Map<String, String> inputData) throws IOException {
        String orgId = inputData.get(Constants.ORD_ID);
        String eventId = inputData.get(Constants.CONTEXT_ID_KEY);
        String batchId = inputData.get(Constants.BATCH_ID);
        String status = "";

        int totalRecordsCount = 0;
        int processedCount = 0;
        int failedCount = 0;
        Map<String, Object> emailUserIdMap = new HashMap<>();
        Map<String, Object> eventDetails = new HashMap<>();
        String columnName = "Email";

        File file = new File(Constants.LOCAL_BASE_PATH + inputData.get(Constants.FILE_NAME));
        if (!file.exists() || file.length() == 0) {
            logger.info("File not downloaded/present.");
            status = Constants.FAILED_UPPERCASE;
            updateStatus(inputData, status, totalRecordsCount, processedCount, failedCount);
            return;
        }

        List<Map<String, String>> updatedRecords = new ArrayList<>();
        List<String> headers;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.newFormat(serverProperties.getBulkUploadCsvDelimiter()).withFirstRecordAsHeader())) {

            headers = new ArrayList<>(csvParser.getHeaderNames());
            cleanHeaders(headers);

            if (!headers.contains("Status")) headers.add("Status");
            if (!headers.contains("Error Details")) headers.add("Error Details");

            int expectedFieldCount = headers.size() - 2; // Exclude "Status" and "Error Details"

            getUserIdList(csvParser, columnName, emailUserIdMap);
            getEventDetails(eventId, batchId, eventDetails);

            try (CSVParser csvParser2 = new CSVParser(
                    new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)),
                    CSVFormat.newFormat(serverProperties.getBulkUploadCsvDelimiter()).withFirstRecordAsHeader()
            )) {
                List<String> notificationUserIds = new ArrayList<>();
                for (CSVRecord record : csvParser2.getRecords()) {
                    totalRecordsCount++;

                    Map<String, String> updatedRecord = processRecord(record, expectedFieldCount, eventId, batchId, emailUserIdMap, eventDetails, notificationUserIds);
                    updatedRecords.add(updatedRecord);
                    if ("FAILED".equalsIgnoreCase(updatedRecord.get("Status"))) {
                        failedCount++;
                    } else {
                        processedCount++;
                    }
                }

                // Send notification for all users at once after processing
                if (!notificationUserIds.isEmpty()) {
                    String trainingName = eventDetails.getOrDefault(Constants.EVENT_NAME, "").toString();
                    notificationService.sendNotificationForExternalTraining(eventId, trainingName, notificationUserIds, Constants.EXTERNAL_TRAINING);
                }
            }

            writeUpdatedCSV(file, headers, updatedRecords);
            status = finalizeStatus(totalRecordsCount, processedCount, failedCount, file);

        } catch (Exception e) {
            logger.error("Error processing file: ", e);
            status = Constants.FAILED_UPPERCASE;
            updateStatus(inputData, status, totalRecordsCount, processedCount, failedCount);
        }

        updateStatus(inputData, status, totalRecordsCount, processedCount, failedCount);
    }

    private void getUserIdList(CSVParser csvParser, String columnName, Map<String, Object> emailUserMap) throws IOException {
        if (csvParser == null) {
            throw new IllegalArgumentException("Invalid input: CSV parser must not be null or empty.");
        }

        List<String> emailList = new ArrayList<>();
        try {
            for (CSVRecord record : csvParser) {
                // Validate if the record contains the required column
                if (record.isMapped(columnName)) {
                    String columnValue = record.get(columnName);
                    if (StringUtils.isNotBlank(columnValue)) {
                        emailList.add(columnValue.toLowerCase().trim());
                    } else {
                        logger.warn("Skipping empty value for column {} in record {}", columnName, record.getRecordNumber());
                    }
                } else {
                    logger.error("Column '{}' not found in CSV file.", columnName);
                    throw new IllegalArgumentException("CSV file does not contain the specified column: " + columnName);
                }
            }
            logger.debug("Fetched {} email addresses from CSV.", emailList.size());

            if (!emailList.isEmpty()) {
                emailUserMap.putAll(getUserInfo(Constants.EMAIL, emailList));
                logger.debug("User IDs successfully retrieved for the email list.");
            } else {
                logger.warn("No valid email addresses found in the CSV.");
            }
        } catch (Exception e) {
            logger.error("Error processing CSV file to fetch user IDs", e);
            throw new IOException("Failed to process CSV file", e);
        }
    }


    /**
     * Cleans up the headers by removing any surrounding quotes.
     */
    private void cleanHeaders(List<String> headers) {
        headers.replaceAll(header -> header.replaceAll("^\"|\"$", ""));
    }

    /**
     * Processes a single CSV record. Returns the updated record with status and error details.
     */
    private Map<String, String> processRecord(CSVRecord record, int expectedFieldCount, String eventId, String batchId, Map<String, Object> emailUserMap, Map<String, Object> eventDetails, List<String> notificationUserIds) {
        Map<String, String> updatedRecord = new LinkedHashMap<>(record.toMap());
        try {
            if (record.size() > expectedFieldCount) {
                markRecordAsFailed(updatedRecord, "Number of fields in the record exceeds expected number. Please check your data.");
                return updatedRecord;
            }

            String email = record.get("Email");
            if (StringUtils.isBlank(email)) {
                markRecordAsFailed(updatedRecord, "Empty email");
                return updatedRecord;
            }
            if (!ProjectUtil.validateEmailPattern(email)) {
                markRecordAsFailed(updatedRecord, "Invalid Email Id");
                return updatedRecord;
            }
            Object userInfoObj = emailUserMap.get(email);
            if (ObjectUtils.isEmpty(userInfoObj)) {
                markRecordAsFailed(updatedRecord, "User does not exist");
                return updatedRecord;
            }

            Map<String, Object> userInfo = (Map<String, Object>) userInfoObj;
            // Validate userInfo
            validateNotNullOrEmpty(userInfo);
            String userId = userInfo.get(Constants.USER_ID).toString();

            Map<String, Object> enrollmentRecord = isEventEnrolmentExist(userId, eventId, batchId);
            if (MapUtils.isNotEmpty(enrollmentRecord)) {
                markRecordAsFailed(updatedRecord, "User enrolled in the batch");
                return updatedRecord;
            }

            // Enroll user
            ApiResponse enrollmentResponse = enrollUser(userId, eventId, batchId, eventDetails);
            if (!Constants.SUCCESS.equalsIgnoreCase((String) enrollmentResponse.get(Constants.RESPONSE))) {
                markRecordAsFailed(updatedRecord, "Failed to enroll");
                return updatedRecord;
            }
            // Trigger certificate event
            externalTrainingCertificateService.generateCertificateEventAndPushToKafka(userInfo, eventDetails);

            // Accumulate userId for notification
            notificationUserIds.add(userId);

            logger.info("Successfully enrolled user: userId = {}, email = {}", userId, email);

        } catch (IllegalArgumentException e) {
            // Validation errors
            logger.warn("Validation failed for record: {}, error: {}", record, e.getMessage());
            markRecordAsFailed(updatedRecord, e.getMessage());

        } catch (JsonProcessingException e) {
            logger.error("JSON processing error for record: {}", record, e);
            markRecordAsFailed(updatedRecord, "Error processing JSON data");

        } catch (Exception e) {
            // Catch-all to avoid breaking batch processing
            logger.error("Unexpected error while processing record: {}", record, e);
            markRecordAsFailed(updatedRecord, "Internal error while processing record");
        }

        return updatedRecord;
    }

    /**
     * Marks a record as failed with an error message.
     */
    private void markRecordAsFailed(Map<String, String> record, String errorMessage) {
        record.put("Status", "FAILED");
        record.put("Error Details", errorMessage);
    }

    /**
     * Writes the updated records to the CSV file.
     */
    private void writeUpdatedCSV(File file, List<String> headers, List<Map<String, String>> updatedRecords) throws IOException {
        try (FileWriter fileWriter = new FileWriter(file);
             BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
             CSVPrinter csvPrinter = new CSVPrinter(bufferedWriter, CSVFormat.newFormat(serverProperties.getBulkUploadCsvDelimiter())
                     .withHeader(headers.toArray(new String[0]))
                     .withRecordSeparator(System.lineSeparator()))) {

            for (Map<String, String> record : updatedRecords) {
                csvPrinter.printRecord(record.values());
            }
        }
    }

    /**
     * Finalizes the status based on the record processing result.
     */
    private String finalizeStatus(int totalRecordsCount, int processedCount, int failedCount, File file) throws IOException {
        String status = uploadTheUpdatedCSVFile(file);
        if (Constants.SUCCESS.equalsIgnoreCase(status) && failedCount == 0 && totalRecordsCount == processedCount && totalRecordsCount >= 1) {
            return Constants.SUCCESS;
        }
        return Constants.FAILED;
    }

    /**
     * Updates the bulk onboarding status.
     */
    private void updateStatus(Map<String, String> inputData, String status, int totalRecordsCount, int successfulRecordsCount, int failedRecordsCount) {
        updateUserBulkUploadStatus(inputData.get(Constants.ORD_ID), inputData.get(Constants.CONTEXT_ID_CAMEL), inputData.get(Constants.BATCH_ID), inputData.get(Constants.IDENTIFIER), status, totalRecordsCount, successfulRecordsCount, failedRecordsCount);
    }


    private Map<String, Object> getUserInfo(String key, List<String> values) {
        int batchSize = 100;
        Map<String, Object> requestBody = new HashMap<>();
        List<Map<String, Object>> contentList = new ArrayList<>();
        Map<String, Object> reqMap = new HashMap<>();

        Map<String, Object> emailUserMap = new HashMap<>();

        String url = serverProperties.getSbUrl() + serverProperties.getUserSearchEndPoint();
        HashMap<String, String> headersValue = new HashMap<>();
        headersValue.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
        for (int i = 0; i < values.size(); i += batchSize) {
            int end = Math.min(i + batchSize, values.size());
            List<String> subList = values.subList(i, end);
            reqMap.put(Constants.FILTERS, new HashMap<String, Object>() {
                {
                    put(key, subList);
                }
            });
            requestBody.put(Constants.REQUEST, reqMap);

            try {
                Map<String, Object> response = outboundRequestHandlerService.fetchResultUsingPost(url, requestBody,
                        headersValue);
                if (response != null && Constants.OK.equalsIgnoreCase((String) response.get(Constants.RESPONSE_CODE))) {
                    Map<String, Object> map = (Map<String, Object>) response.get(Constants.RESULT);
                    if (map.get(Constants.RESPONSE) != null) {
                        Map<String, Object> responseObj = (Map<String, Object>) map.get(Constants.RESPONSE);
                        contentList = (List<Map<String, Object>>) responseObj.get(Constants.CONTENT);
                        if (contentList != null) {
                            contentList.forEach(e -> {
                                Map<String, Object> profileDetails = (Map<String, Object>) e.get(Constants.PROFILE_DETAILS);
                                Map<String, Object> userInfo = new HashMap<>();
                                userInfo.put(Constants.ROOT_ORG_ID, e.get(Constants.ROOT_ORG_ID));
                                userInfo.put(Constants.FIRSTNAME, e.get(Constants.FIRSTNAME));
                                userInfo.put(Constants.USER_ID, e.get(Constants.USER_ID));
                                if (profileDetails != null) {
                                    Map<String, Object> personalDetails = (Map<String, Object>) profileDetails.get(Constants.PERSONAL_DETAILS);
                                    if (personalDetails != null) {
                                        String primaryEmail = (String) personalDetails.get(Constants.PRIMARY_EMAIL);
                                        if (primaryEmail != null && !primaryEmail.trim().isEmpty()) {
                                            emailUserMap.put(primaryEmail.toLowerCase().trim(), userInfo);
                                        }
                                    }
                                }
                            });

                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Error while fetching user details of list of users ", e);
            }
        }
        return emailUserMap;
    }

    private ApiResponse enrollUser(String userId, String eventId, String batchId, Map<String, Object> eventDetails) throws JsonProcessingException {

        int defaultStatus = 2;
        int defaultProgress = 100;
        float defaultCompletionPercentage = 100;
        ApiResponse response;
        try {
            Object startObj = eventDetails.get(Constants.START_DATE);
            Object endObj = eventDetails.get(Constants.END_DATE_CAMEL);

            Instant startDate = null;
            Instant endDate = null;
            if (startObj instanceof Instant) {
                startDate = (Instant) startObj;
            } else if (startObj instanceof Date) {
                startDate = ((Date) startObj).toInstant();
            }

            if (endObj instanceof Instant) {
                endDate = (Instant) endObj;
            } else if (endObj instanceof Date) {
                endDate = ((Date) endObj).toInstant();
            }

            Map<String, Object> request = new HashMap<>();
            request.put(Constants.USER_ID, userId);
            request.put(Constants.CONTEXT_ID_CAMEL, eventId);
            request.put(Constants.BATCH_ID, batchId);
            request.put(Constants.ACTIVE, true);
            request.put(Constants.STATUS, defaultStatus);
            request.put(Constants.PROGRESS, defaultProgress);
            request.put(Constants.COMPLETION_PERCENTAGE, defaultCompletionPercentage);
            request.put(Constants.ENROLLED_DATE_KEY_LOWER, startDate);
            request.put(Constants.DATE_TIME, startDate);
            request.put(Constants.COMPLETED_ON, endDate);
            request.put(Constants.LRC_PROGRESS_DETAILS_COLUMN, prepareLrcProgressDetails(eventDetails));

            logger.info("Attempting to enroll user: userId = {}, eventId = {}, batchId = {}", userId, eventId, batchId);
            response = (ApiResponse) cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD_COURSE, serverProperties.getExternalTrainingEnrolmentsTableName(), request);

            Map<String, Object> batchLookupInsertRequest = new HashMap<>();
            batchLookupInsertRequest.put(Constants.USER_ID, userId);
            batchLookupInsertRequest.put(Constants.BATCH_ID, batchId);
            batchLookupInsertRequest.put(Constants.ACTIVE, true);
            ApiResponse batchLookupInsertResponse = (ApiResponse) cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD_COURSE, serverProperties.getExternalTrainingEnrolmentBatchLookupTableName(), batchLookupInsertRequest);

        } catch (Exception e) {
            logger.error("Exception while enrolling user: userId = {}, eventId = {}, batchId = {}", userId, eventId, batchId, e);
            response = ProjectUtil.createDefaultResponse(Constants.API_EXTERNAL_TRAINING_USER_BULK_UPLOAD);
            response.put(Constants.RESPONSE, Constants.FAILED);
        }

        return response;
    }

    private Map<String, Object> isEventEnrolmentExist(String userId, String eventId, String batchId) {

        Map<String, Object> compositeKey = new HashMap<>();
        compositeKey.put(Constants.USER_ID, userId);
        compositeKey.put(Constants.CONTEXT_ID_KEY, eventId);

        List<Map<String, Object>> enrolmentRecords = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD_COURSE, serverProperties.getExternalTrainingEnrolmentsTableName(), compositeKey, null, null);
        if (CollectionUtils.isEmpty(enrolmentRecords)) {
            return null;
        }
        return enrolmentRecords.get(0);
    }

    private List<String> validateReceivedKafkaMessage(Map<String, String> inputDataMap) {
        StringBuffer str = new StringBuffer();
        List<String> errList = new ArrayList<>();
        if (StringUtils.isEmpty(inputDataMap.get(Constants.CONTEXT_ID_KEY))) {
            errList.add("Event ID is not present");
        }
        if (StringUtils.isEmpty(inputDataMap.get(Constants.BATCH_ID))) {
            errList.add("Batch ID is not present");
        }
        if (StringUtils.isEmpty(inputDataMap.get(Constants.FILE_NAME))) {
            errList.add("Filename is not present");
        }
        if (!errList.isEmpty()) {
            str.append("Failed to Validate event Details. Error Details - [").append(errList.toString()).append("]");
        }
        return errList;
    }

    private String uploadTheUpdatedCSVFile(File file) throws IOException {
        ApiResponse uploadResponse = storageService.uploadFile(file, serverProperties.getExternalTrainingBulkUploadContainerName(), serverProperties.getCloudContainerName());
        if (!HttpStatus.OK.equals(uploadResponse.getResponseCode())) {
            logger.info(String.format("Failed to upload file. Error: %s",
                    uploadResponse.getParams().getErrMsg()));
            return Constants.FAILED;
        }
        return Constants.SUCCESS;
    }

    public void updateUserBulkUploadStatus(String orgId, String contextId, String batchId, String identifier, String status, int totalRecordsCount,
                                           int successfulRecordsCount, int failedRecordsCount) {
        try {
            Map<String, Object> compositeKeys = new HashMap<>();
            compositeKeys.put(Constants.ORD_ID, orgId);
            compositeKeys.put(Constants.CONTEXT_ID_CAMEL, contextId);
            compositeKeys.put(Constants.BATCH_ID, batchId);
            compositeKeys.put(Constants.IDENTIFIER, identifier);
            Map<String, Object> fieldsToBeUpdated = new HashMap<>();
            if (!status.isEmpty()) {
                fieldsToBeUpdated.put(Constants.STATUS, status);
            }
            if (totalRecordsCount >= 0) {
                fieldsToBeUpdated.put(Constants.TOTAL_RECORDS, totalRecordsCount);
            }
            if (successfulRecordsCount >= 0) {
                fieldsToBeUpdated.put(Constants.SUCCESSFUL_RECORDS_COUNT, successfulRecordsCount);
            }
            if (failedRecordsCount >= 0) {
                fieldsToBeUpdated.put(Constants.FAILED_RECORDS_COUNT, failedRecordsCount);
            }
            fieldsToBeUpdated.put(Constants.UPDATE_ON, Instant.now());
            cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, serverProperties.getExternalTrainingBulkUploadTable(),
                    fieldsToBeUpdated, compositeKeys);
        } catch (Exception e) {
            logger.error(String.format("Error in Updating User Bulk Upload Status in Cassandra %s", e.getMessage()), e);
        }
    }

    private void getEventDetails(String eventId, String batchId, Map<String, Object> eventDetails) {
        logger.debug("Fetching event batch details for eventId: {} and batchId: {}", eventId, batchId);
        eventDetails.put(Constants.EVENT_ID, eventId);
        eventDetails.put(Constants.BATCH_ID, batchId);

        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(Constants.EVENT_ID, eventId);
        propertiesMap.put(Constants.BATCH_ID, batchId);
        try {
            List<Map<String, Object>> eventBatchDetails = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD_COURSE, Constants.EVENT_BATCH_TABLE_NAME, propertiesMap, null, null);
            if (CollectionUtils.isNotEmpty(eventBatchDetails)) {
                Map<String, Object> eventBatch = eventBatchDetails.get(0);
                Object startObj = eventBatch.get("start_date");
                Object endObj = eventBatch.get("end_date");

                Date startDate = null;
                Date endDate = null;

                if (startObj instanceof Date) {
                    startDate = (Date) startObj;
                } else if (startObj instanceof Instant) {
                    startDate = Date.from((Instant) startObj);
                }

                if (endObj instanceof Date) {
                    endDate = (Date) endObj;
                } else if (endObj instanceof Instant) {
                    endDate = Date.from((Instant) endObj);
                }
                String batchAttributesStr = (String) eventBatch.get(Constants.BATCH_ATTRIBUTES_COLUMN);
                Map<String, Object> batchAttributes = objectMapper.readValue(batchAttributesStr, new TypeReference<Map<String, Object>>() {
                });
                eventDetails.put(Constants.START_DATE, startDate);
                eventDetails.put(Constants.END_DATE_CAMEL, endDate);

                Object durationObj = batchAttributes.get(Constants.DURATION);
                long durationInSec = 0;
                if (durationObj instanceof Number) {
                    durationInSec = ((Number) durationObj).longValue() * 60;
                }
                eventDetails.put(Constants.DURATION, durationInSec);

                Map<String, Object> readResponse = contentInfoService.readEvent(eventId);
                if (MapUtils.isNotEmpty(readResponse)) {
                    eventDetails.put(Constants.EVENT_NAME, readResponse.get(Constants.NAME));
                    eventDetails.put(Constants.CERT_TEMPLATE, readResponse.get(Constants.CERT_TEMPLATE));
                    eventDetails.put(Constants.CERT_TEMPLATE_ID, readResponse.get(Constants.CERT_TEMPLATE_ID));
                    eventDetails.put(Constants.SOURCE_NAME, readResponse.get(Constants.SOURCE_NAME));

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                    String issuedDate = sdf.format(endDate);
                    eventDetails.put(Constants.ISSUED_DATE, issuedDate);
                    long etsForEvent = ((Date) eventDetails.get(Constants.END_DATE_CAMEL)).getTime();
                    eventDetails.put("ets", etsForEvent);
                } else {
                    throw new RuntimeException("Unable to fetch event details: readResponse is empty");
                }
                validateNotNullOrEmpty(eventDetails);
            } else {
                logger.warn("No event batch details found for eventId: {} and batchId: {}", eventId, batchId);
            }
        } catch (Exception e) {
            logger.error("Error while fetching event batch details for eventId: {} and batchId: {}", eventId, batchId, e);
            throw new RuntimeException("Unable to fetch event details: " + e.getMessage(), e);
        }
    }

    private void validateNotNullOrEmpty(Map<String, Object> data) {
        if (MapUtils.isEmpty(data)) {
            throw new IllegalArgumentException("Input data map is null or empty");
        }
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (Objects.isNull(value)) {
                throw new IllegalArgumentException("Value for key '" + key + "' is null");
            }
            if (value instanceof String && StringUtils.isBlank((String) value)) {
                throw new IllegalArgumentException("Value for key '" + key + "' is empty");
            }
        }
    }

    private String prepareLrcProgressDetails(Map<String, Object> eventDetails) throws JsonProcessingException {
        Map<String, Object> result = new HashMap<>();
        result.put("max_size", eventDetails.get(Constants.DURATION));
        result.put(Constants.DURATION, eventDetails.get(Constants.DURATION));
        result.put("mimeType", "application/html");
        result.put("stateMetaData", eventDetails.get(Constants.DURATION));
        result.put("current", List.of(0));

        return objectMapper.writeValueAsString(result);
    }
}
