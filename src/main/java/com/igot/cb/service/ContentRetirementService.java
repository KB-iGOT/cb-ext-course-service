package com.igot.cb.service;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

@Slf4j
@Service
public class ContentRetirementService {

    private final CassandraOperation cassandraOperation;
    private final ContentInfoServiceImpl contentService;
    private final NotificationService notificationService;
    private final OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    private final CbExtServerProperties props;

    public ContentRetirementService(CassandraOperation cassandraOperation, ContentInfoServiceImpl contentService, NotificationService notificationService, OutboundRequestHandlerServiceImpl outboundRequestHandlerService, CbExtServerProperties props) {
        this.cassandraOperation = cassandraOperation;
        this.contentService = contentService;
        this.notificationService = notificationService;
        this.outboundRequestHandlerService = outboundRequestHandlerService;
        this.props = props ;
    }

    public ApiResponse processDueRetirements() {
        ApiResponse response = ApiResponse.createDefaultResponse("retirement.schedule.cron");

        LocalDate today = LocalDate.now();

        log.info("Running content retirement job for date <= {}", today);

        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(Constants.RETIREMENT_DATE_KEY, today);

        List<String> fields = Arrays.asList(
                Constants.CONTENT_ID_KEY,
                Constants.REQUEST_ID_KEY,
                Constants.RETIREMENT_DATE_KEY,
                Constants.STATUS
        );

        List<Map<String, Object>> records =
                cassandraOperation.getRecordsByProperties(
                        Constants.KEYSPACE_SUNBIRD_COURSE,
                        Constants.CONTENT_RETIREMENT_BY_RETIREMENT_DATE_TABLE,
                        propertiesMap,
                        fields,
                        null
                );

        if (CollectionUtils.isEmpty(records)) {
            log.info("No approved content retirement requests found");
            response.getResult().put(Constants.CONTENT, new ArrayList<>());
            return response;
        }
        List<Map<String, Object>> responseList = new ArrayList<>();
        for (Map<String, Object> retirementRecord : records) {
            String status = (String) retirementRecord.get(Constants.STATUS);

            if (!Constants.APPROVED.equalsIgnoreCase(status == null ? "" : status)) {
                log.debug("Skipping retirement for content {} due to status {}",
                        retirementRecord.get(Constants.CONTENT_ID_KEY),
                        status
                );
                continue;
            }
            LocalDate retirementDate = (LocalDate) retirementRecord.get(Constants.RETIREMENT_DATE);

            if (retirementDate != null && !retirementDate.isAfter(today)) {
                responseList.add(retireContent(retirementRecord));
            }
        }
        response.getResult().put(Constants.CONTENT, responseList);
        return response;
    }

    private Map<String, Object> retireContent(Map<String, Object> retirementRecord) {

        Map<String, Object> result = new HashMap<>();

        String contentId = (String) retirementRecord.get(Constants.CONTENT_ID);
        String requestId = (String) retirementRecord.get(Constants.REQUEST_ID);

        result.put(Constants.CONTENT_ID, contentId);

        try {
            Map<String, Object> contentRetireStatusMap =
                    contentService.retireContent(contentId);

            if (MapUtils.isNotEmpty(contentRetireStatusMap)) {

                Map<String, Object> updateMap = new HashMap<>();
                updateMap.put(Constants.STATUS, Constants.RETIRED);
                updateMap.put(Constants.UPDATED_AT_KEY, Instant.now());

                Map<String, Object> whereClause = new HashMap<>();
                whereClause.put(Constants.CONTENT_ID_KEY, contentId);
                whereClause.put(Constants.REQUEST_ID_KEY, requestId);

                cassandraOperation.updateRecord(
                        Constants.KEYSPACE_SUNBIRD_COURSE,
                        Constants.CONTENT_RETIREMENT_REQUEST_TABLE,
                        updateMap,
                        whereClause
                );

                log.info("Content retired successfully: {}", contentId);

                result.put(Constants.RETIRED, true);
                result.put(Constants.MESSAGE, "Content retired successfully");

                LocalDate retirementDate = (LocalDate) retirementRecord.get(Constants.RETIREMENT_DATE);
                validateAndSendInAppLearerNotification(contentId, Constants.CONTENT_RETIRED, retirementDate);

            } else {
                log.warn("Retirement API returned empty response for {}", contentId);

                result.put(Constants.RETIRED, false);
                result.put(Constants.MESSAGE, "Retirement API returned empty response");
            }

        } catch (Exception ex) {
            log.error("Failed to retire content {}", contentId, ex);

            result.put(Constants.RETIRED, false);
            result.put(Constants.MESSAGE, ex.getMessage());
        }

        return result;
    }


    public void sendContentRetirementNotifications() {

        LocalDate today = LocalDate.now();

        log.info("Running content retirement notification job for {}", today);

        Map<String, Object> retirementDateFilter =  new HashMap<>();
        retirementDateFilter.put(Constants.RETIREMENT_DATE_KEY, today);

        Map<String, Object> approvedDateFilter =  new HashMap<>();
        approvedDateFilter.put(Constants.APPROVED_DATE, today);

        List<Map<String, Object>> retirementRequestsByRetirementDate =
                cassandraOperation.getRecordsByProperties(
                        Constants.KEYSPACE_SUNBIRD_COURSE,
                        Constants.CONTENT_RETIREMENT_BY_RETIREMENT_DATE_TABLE,
                        retirementDateFilter,
                        Arrays.asList(
                                Constants.CONTENT_ID_KEY,
                                Constants.APPROVED_DATE,
                                Constants.RETIREMENT_DATE_NOTIFICATION,
                                Constants.STATUS
                        ),
                        null
                );
        List<Map<String, Object>> retirementRequestsByApproveDate =
                cassandraOperation.getRecordsByProperties(
                        Constants.KEYSPACE_SUNBIRD_COURSE,
                        Constants.CONTENT_RETIREMENT_BY_APPROVED_DATE_TABLE,
                        approvedDateFilter,
                        Arrays.asList(
                                Constants.CONTENT_ID_KEY,
                                Constants.APPROVED_DATE,
                                Constants.RETIREMENT_DATE_NOTIFICATION,
                                Constants.STATUS
                        ),
                        null
                );

        sendApprovedRetirementNotifications(retirementRequestsByApproveDate, today);
        if (CollectionUtils.isEmpty(retirementRequestsByRetirementDate)) {
            log.info("No approved retirement requests found by retire_date");
        }

        for (Map<String, Object> record : retirementRequestsByRetirementDate) {
            String status = (String) record.get(Constants.STATUS);
            if (!Constants.APPROVED.equalsIgnoreCase(status == null ? "" : status)) {
                log.debug("Skipping retirement notification for content {} as status {}", record.get(Constants.CONTENT_ID_KEY), status);
                continue;
            }

            String contentId = (String) record.get(Constants.CONTENT_ID);
            LocalDate retirementDate = (LocalDate) record.get(Constants.RETIREMENT_DATE);

            String notificationType = null;
            if (retirementDate != null && retirementDate.equals(today.plusDays(1))) {
                notificationType = Constants.REMINDER_NOTIFICATION_ONE_DAY;
            } else if (retirementDate != null && retirementDate.equals(today.plusDays(7))) {
                notificationType = Constants.REMINDER_NOTIFICATION_SEVEN_DAY;
            }

            if (!StringUtils.hasText(notificationType)) {
                continue;
            }

            log.info("Triggering {} notification for content {}", notificationType, contentId);
            validateAndSendInAppLearerNotification(contentId, notificationType, retirementDate);

        }
    }

    public void sendContentRetirementNotificationsToSpv() {
        LocalDate today = LocalDate.now();
        log.info("Running content retirement notification job for spv admins {}", today);

        Map<String, Object> properties =  new HashMap<>();
        properties.put(Constants.CREATED_DATE , today);

        List<Map<String, Object>> retirementRequests =
                cassandraOperation.getRecordsByProperties(
                        Constants.KEYSPACE_SUNBIRD_COURSE,
                        Constants.CONTENT_RETIREMENT_BY_CREATED_DATE_TABLE,
                        properties,
                        Arrays.asList(
                                Constants.CONTENT_ID_KEY,
                                Constants.CREATED_DATE,
                                Constants.USER_ID_RAISED_FIELD,
                                Constants.RETIREMENT_DATE_KEY
                        ),
                        1000
                );
        if (CollectionUtils.isEmpty(retirementRequests)) {
            log.info("No approved retirement requests found");
            return;
        }
        List<Map<String, String>> spvPublishers = fetchSpvPublishers();
        List<String> spvPublisherUserIds = new ArrayList<>();
        List<String> spvPublisherEmails = new ArrayList<>();
        for (Map<String, String> publisher : spvPublishers) {
            String userId = publisher.get(Constants.USER_ID);
            String email  = publisher.get(Constants.EMAIL);
            if (StringUtils.hasText(userId)) {
                spvPublisherUserIds.add(userId);
            }
            if (StringUtils.hasText(email)) {
                spvPublisherEmails.add(email);
            }
        }
        Set<String> finalRecipients = new HashSet<>(spvPublisherUserIds);
        for (Map<String, Object> record : retirementRequests) {
            String contentId = (String) record.get(Constants.CONTENT_ID);
            Object createdObj = record.get(Constants.CREATED_DATE);
            LocalDate createdDate = null;
            if (createdObj instanceof Instant instant) {
                createdDate = instant.atZone(ZoneId.systemDefault()).toLocalDate();
            } else if (createdObj instanceof LocalDate localDate) {
                createdDate = localDate;
            }
            String requestedBy = (String) record.get(Constants.USER_ID_RAISED_FIELD);
            if (createdDate == null || !createdDate.equals(today)) continue;
            if (finalRecipients.isEmpty()) continue;
            log.info("Triggering retirement approved notification for content {}", contentId);
            Map<String, Object> content =
                    contentService.readContent(contentId, List.of("name"));
            String contentName =
                    (String) content.get("name");
            Object retirementDateObj = record.get(Constants.RETIREMENT_DATE);
            LocalDate retirementDate = null;
            if (retirementDateObj instanceof Instant instant) {
                retirementDate = instant.atZone(ZoneId.systemDefault()).toLocalDate();
            } else if (retirementDateObj instanceof LocalDate localDate) {
                retirementDate = localDate;
            }
            notificationService.sendNotificationForContentRetirementSpv(
                    contentId,  contentName,
                    new ArrayList<>(finalRecipients),
                    Constants.CONTENT_RETIREMENT_SCHEDULED_NOTIFICATION, retirementDate, spvPublisherEmails, requestedBy
            );
        }
    }

    private List<Map<String, String>> fetchSpvPublishers() {
        List<Map<String, String>> publishers = new ArrayList<>();
        Map<String, Object> filters = Map.of(
                "organisations.roles", List.of("SPV_PUBLISHER"),
                "status", 1
        );
        List<String> userFields = List.of(Constants.USER_ID, Constants.PROFILE_DETAILS_PERSONAL_DETAILS_MAIL) ;
        Map<String, Object> requestObject = Map.of(
                Constants.REQUEST, Map.of(
                        Constants.QUERY, "",
                        Constants.FILTERS, filters,
                        Constants.FIELDS, userFields,
                        Constants.LIMIT, 1000
                )
        );
        Map<String, String> headers = Map.of(
                Constants.CONTENT_TYPE, Constants.APPLICATION_JSON
        );
        String url = props.getSbUrl() + props.getUserSearchEndPoint();
        Map<String, Object> resp =
                outboundRequestHandlerService.fetchResultUsingPost(url, requestObject, headers);

        if (MapUtils.isEmpty(resp) ||
                !"OK".equalsIgnoreCase(String.valueOf(resp.get(Constants.RESPONSE_CODE)))) {
            log.error("[FETCH-SPV][FAILED] Invalid response {}", resp);
            return publishers;
        }
        Object contentsObj = Optional.ofNullable(resp.get(Constants.RESULT))
                .filter(Map.class::isInstance)
                .map(Map.class::cast)
                .map(result -> result.get(Constants.RESPONSE))
                .filter(Map.class::isInstance)
                .map(Map.class::cast)
                .map(response -> response.get(Constants.CONTENT))
                .orElse(null);
        if (!(contentsObj instanceof List<?> contents)) {
            log.warn("[FETCH-SPV][EMPTY] No content in response");
            return publishers;
        }
        for (Object item : contents) {
            if (!(item instanceof Map<?, ?> content)) continue;

            Object userIdObj = content.get(Constants.USER_ID);
            if (!(userIdObj instanceof String userId) || !StringUtils.hasText(userId)) continue;

            Object profileDetailsObj = content.get(Constants.PROFILE_DETAILS);
            if (!(profileDetailsObj instanceof Map<?, ?> profileDetails)) continue;

            Object personalDetailsObj = profileDetails.get(Constants.PERSONAL_DETAILS);
            if (!(personalDetailsObj instanceof Map<?, ?> personalDetails)) continue;

            Object emailObj = personalDetails.get(Constants.PRIMARY_EMAIL);
            if (!(emailObj instanceof String email) || !StringUtils.hasText(email)) continue;

            Map<String, String> record = new HashMap<>();
            record.put(Constants.USER_ID, userId);
            record.put(Constants.EMAIL, email);

            publishers.add(record);
        }
        log.info("[FETCH-SPV][SUCCESS] totalPublishers={}", publishers.size());
        return publishers;
    }

    private void validateAndSendInAppLearerNotification(String contentId, String notificationType, LocalDate retirementDate) {
        try {
            Map<String, Object> content =
                    contentService.readContent(contentId, Arrays.asList("name", "batches"));

            List<Map<String, Object>> batches =
                    (List<Map<String, Object>>) content.get("batches");

            if (CollectionUtils.isEmpty(batches)) {
                log.info("No batches found for content {}", contentId);
                return;
            }

            for (Map<String, Object> batch : batches) {

                String batchId = (String) batch.get(Constants.BATCH_ID);

                List<Map<String, Object>> batchUsers =
                        cassandraOperation.getRecordsByProperties(
                                Constants.KEYSPACE_SUNBIRD_COURSE,
                                Constants.ENROLLMENT_BATCH_LOOKUP,
                                Map.of(Constants.BATCH_ID, batchId),
                                Arrays.asList(Constants.USER_ID),
                                null
                        );

                if (CollectionUtils.isEmpty(batchUsers)) {
                    continue;
                }

                for (Map<String, Object> batchUser : batchUsers) {

                    String userId = (String) batchUser.get(Constants.USER_ID);
                    Map<String, Object> enrolmentProperties = Map.of(
                            Constants.USER_ID, userId,
                            Constants.COURSE_ID, contentId,
                            Constants.BATCH_ID, batchId
                    );

                    List<Map<String, Object>> enrolment =
                            cassandraOperation.getRecordsByProperties(
                                    Constants.KEYSPACE_SUNBIRD_COURSE,
                                    Constants.USER_ENROLMENTS_V2_TABLE,
                                    enrolmentProperties,
                                    null,
                                    null
                            );

                    if (CollectionUtils.isEmpty(enrolment)) {
                        continue;
                    }

                    List<Map<String, Object>> eligibleEnrolments =
                            enrolment.stream()
                                    .filter(Objects::nonNull)
                                    .filter(e -> {
                                        Object statusObj = e.get(Constants.STATUS);
                                        Object activeObj = e.get(Constants.ACTIVE);
                                        Object certificates = e.get(Constants.ISSUED_CERTIFICATES);

                                        return statusObj instanceof Integer
                                                && activeObj instanceof Boolean
                                                && !Objects.equals(statusObj, 2)
                                                && (certificates == null || ((List<?>) certificates).isEmpty())
                                                && Boolean.TRUE.equals(activeObj);
                                    })
                                    .toList();


                    if (CollectionUtils.isEmpty(eligibleEnrolments)) {
                        continue;
                    }
                    String courseName = (String) content.get(Constants.NAME);
                    notificationService.sendNotificationForContentRetirement(
                            contentId,
                            courseName,
                            retirementDate,
                            List.of(userId),
                            notificationType
                    );

                }
            }
        } catch (Exception e) {
            log.error("Error while sending in-app notification for content retirement", e);
        }
    }

    private void sendApprovedRetirementNotifications(List<Map<String, Object>> retirementRequestsByApproveDate, LocalDate today) {

        if (CollectionUtils.isEmpty(retirementRequestsByApproveDate)) {
            log.info("No retirement requests found for approved notification");
            return;
        }
        for (Map<String, Object> records : retirementRequestsByApproveDate) {
            String status = (String) records.get(Constants.STATUS);
            if (!Constants.APPROVED.equalsIgnoreCase(status == null ? "" : status)) {
                log.debug(
                        "Skipping approved notification for content {} due to status {}",
                        records.get(Constants.CONTENT_ID_KEY),
                        status
                );
                continue;
            }
            String contentId = (String) records.get(Constants.CONTENT_ID);
            LocalDate approvedDate = (LocalDate) records.get(Constants.APPROVED_DATE);

            LocalDate retirementDate = (LocalDate) records.get(Constants.RETIREMENT_DATE);
            if (approvedDate != null && approvedDate.equals(today)) {

                log.info("Triggering APPROVED retirement notification for content {}", contentId);
                validateAndSendInAppLearerNotification(
                        contentId,
                        Constants.CONTENT_RETIREMENT_APPROVED_NOTIFICATION,
                        retirementDate
                );
            }
        }
    }

}

