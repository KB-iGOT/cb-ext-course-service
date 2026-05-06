package com.igot.cb.service;

import com.datastax.oss.driver.internal.core.util.CollectionsUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.util.AccessTokenValidator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.user.UserUtilityService;
import com.igot.cb.util.Constants;
import com.igot.cb.util.CbExtServerProperties;
import com.igot.cb.util.ProjectUtil;

import java.io.StringWriter;
import java.time.LocalDate;
import java.util.*;

@Service
@Slf4j
public class NotificationServiceImpl implements NotificationService {
    @Autowired
    private AccessTokenValidator accessTokenValidator;

    @Autowired
    private CassandraOperation cassandraOperation;

    @Autowired
    private UserUtilityService userUtilityService; // kept as fallback

    @Autowired
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @Autowired
    private CbExtServerProperties props;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public ApiResponse notifyAssignmentUploaded(Map<String, Object> requestData, String authToken) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_NOTIFICATION_ASSIGNMENT_UPLOADED);
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
        if (StringUtils.isEmpty(userId)) {
            return response;
        }

        try {
            String validationErr = validateRequest(requestData, null);
            if (StringUtils.isNotBlank(validationErr)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg(validationErr);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.BATCH_ID_KEY, requestData.get(Constants.BATCH_ID));

            List<String> fields = Arrays.asList(Constants.USER_ID_LOWER_CASE, Constants.ACTIVE);
            List<Map<String, Object>> enrollments = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD_COURSE, Constants.ENROLLMENT_BATCH_LOOKUP, propertyMap, fields, null);

            if (CollectionUtils.isEmpty(enrollments)) {
                response.getResult().put(Constants.MESSAGE, "No enrolled users found for given courseId/batchId");
                response.setResponseCode(HttpStatus.OK);
                return response;
            }

            Set<String> userIdsSet = new HashSet<>();

            for (Map<String, Object> enrollment : enrollments) {
                if ((boolean) enrollment.get(Constants.ACTIVE)) {
                    userIdsSet.add((String) enrollment.get(Constants.USER_ID));
                }
            }

            List<String> userIds = new ArrayList<>(userIdsSet);

            List<String> emails = (List<String>) fetchUserEmails(userIds).get(Constants.EMAILS);

            if (CollectionUtils.isEmpty(emails)) {
                response.getResult().put(Constants.MESSAGE, "No enrolled users found for given batchId");
                response.setResponseCode(HttpStatus.OK);
                return response;
            }

            Map<String, Object> params = new HashMap<>();
            params.put(Constants.ASSIGNMENT, requestData.get(Constants.ASSIGNMENT_TITLE));

            Map<String, Object> mailRequestMap = new HashMap<>();
            mailRequestMap.put(Constants.SUBJECT, Constants.ASSIGNMENT_UPLOADED_SUBJECT);
            mailRequestMap.put(Constants.PARAMS, params);
            mailRequestMap.put(Constants.BCC_IDS, emails);
            mailRequestMap.put(Constants.USER_ID, userId);
            Map<String, Object> data = new HashMap<>();
            data.put(Constants.ID, requestData.get(Constants.COURSE_ID));
            data.put(Constants.BATCH_ID, requestData.get(Constants.BATCH_ID));
            Map<String, Object> message = new HashMap<>();
            Map<String, Object> placeHolders = new HashMap<>();
            placeHolders.put(Constants.ASSIGNMENT_TITLE, requestData.get(Constants.ASSIGNMENT_TITLE));
            message.put(Constants.DATA, data);
            message.put(Constants.PLACE_HOLDERS, placeHolders);
            sendInAppNotification(Constants.BP_ASSIGNMENT_UPLOAD, Constants.ALERT, userIds, message);

            notifyUsersByEmail(mailRequestMap, Constants.ASSIGNMENT_UPLOADED_TEMPLATE);
            response.setResponseCode(HttpStatus.OK);
            return response;
        } catch (Exception e) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrMsg(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
    }

    // Consolidated helper to send email notifications using notify service
    private void notifyUsersByEmail(Map<String, Object> mailNotificationDetails, String templateName) {
        try {
            Map<String, Object> notificationRequest = new HashMap<>();
            Map<String, Object> action = new HashMap<>();
            Map<String, Object> template = new HashMap<>();

            template.put(Constants.ID, templateName);
            template.put(Constants.PARAMS, mailNotificationDetails.get(Constants.PARAMS));
            template.put(Constants.TYPE, Constants.EMAIL);
            template.put(Constants.DATA, constructEmailTemplate(templateName, (Map<String, Object>) mailNotificationDetails.get("params")));

            Map<String, Object> config = new HashMap<>();
            Map<String, Object> usermap = new HashMap<>();
            usermap.put(Constants.ID, mailNotificationDetails.get(Constants.USER_ID));
            usermap.put(Constants.TYPE, Constants.USER);
            config.put(Constants.SUBJECT, mailNotificationDetails.get(Constants.SUBJECT));
            config.put(Constants.SENDER, props.getNotificationSupportMail());
            template.put(Constants.CONFIG, config);

            action.put(Constants.TYPE, Constants.EMAIL);
            action.put(Constants.CATEGORY, Constants.EMAIL);
            action.put(Constants.TEMPLATE, template);
            action.put(Constants.CREATED_BY, usermap);

            notificationRequest.put(Constants.TYPE, Constants.EMAIL);
            notificationRequest.put(Constants.PRIORITY, 1);
            if (mailNotificationDetails.containsKey(Constants.IDS) && CollectionUtils.isNotEmpty((List<String>) mailNotificationDetails.get(Constants.IDS))) {
                notificationRequest.put(Constants.IDS, mailNotificationDetails.get(Constants.IDS));
            } else {
                notificationRequest.put(Constants.IDS, Collections.emptyList());
            }

            if (mailNotificationDetails.containsKey(Constants.BCC_IDS) && CollectionUtils.isNotEmpty((List<String>) mailNotificationDetails.get(Constants.BCC_IDS))) {
                notificationRequest.put(Constants.BCC_IDS, mailNotificationDetails.get(Constants.BCC_IDS));
            }

            notificationRequest.put(Constants.ACTION, action);

            Map<String, Object> req = new HashMap<>();
            Map<String, Object> notificationMap = new HashMap<>();
            notificationMap.put(Constants.NOTIFICATIONS, Collections.singletonList(notificationRequest));
            req.put(Constants.REQUEST, notificationMap);
            sendNotification(req);
        } catch (Exception e) {
            log.error("Error while preparing/sending email notification", e);
        }
    }

    private void sendNotification(Map<String, Object> request) {
        StringBuilder builder = new StringBuilder();
        builder.append(props.getNotificationServiceHost()).append(props.getNotificationAsyncPath());
        try {
            log.info("Notification request: " + objectMapper.writeValueAsString(request));
            Map<String, Object> response = outboundRequestHandlerService.fetchResultUsingPost(builder.toString(), request, null);
            log.debug("The email notification is successfully sent, response is: " + response);
        } catch (Exception e) {
            log.error("Exception while posting the data in notification service: ", e);
        }
    }

    private String constructEmailTemplate(String templateName, Map<String, Object> params) {
        String replacedHTML = "";
        try {
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.NAME, templateName);
            List<Map<String, Object>> templateMap = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_EMAIL_TEMPLATE, propertyMap, Collections.singletonList(Constants.TEMPLATE), null);
            String htmlTemplate = templateMap.stream()
                    .findFirst()
                    .map(template -> (String) template.get(Constants.TEMPLATE))
                    .orElse(null);
            VelocityEngine velocityEngine = new VelocityEngine();
            velocityEngine.init();
            VelocityContext context = new VelocityContext();
            if (params != null) {
                for (Map.Entry<String, Object> entry : params.entrySet()) {
                    context.put(entry.getKey(), entry.getValue());
                }
            }
            StringWriter writer = new StringWriter();
            velocityEngine.evaluate(context, writer, Constants.HTMLTemplate, htmlTemplate);
            replacedHTML = writer.toString();
        } catch (Exception e) {
            log.error("Unable to create template ", e);
        }
        return replacedHTML;
    }

    public void sendInAppNotification(String subCategory, String subType, List<String> userIds, Map<String, Object> message) {
        try {
            if (StringUtils.isBlank(subCategory)) {
                log.error("subCategory is required");
            }
            if (StringUtils.isBlank(subType)) {
                log.error("subType is required");
            }

            if (CollectionUtils.isEmpty(userIds)) {
                log.error("userIds cannot be null or empty");
            }

            if (MapUtils.isEmpty(message)) {
                log.error("message cannot be null or empty");
            }

            Map<String, Object> payload = new HashMap<>();
            payload.put(Constants.SUB_CATEGORY, subCategory);
            payload.put(Constants.SUB_TYPE, subType);
            payload.put(Constants.USER_ID_KEYS, userIds);
            payload.put(Constants.MESSAGE, message);
            HashMap<String, String> headers = new HashMap<>();
            headers.put(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
            String requestJson = new ObjectMapper().writeValueAsString(payload);
            String url = props.getCbWrapperNotificationHost() + props.getCbWrapperNotificationPath();
            outboundRequestHandlerService.fetchResultUsingPost(url, requestJson, headers);
            log.debug("Notification sent successfully to users: " + userIds);
        } catch (IllegalArgumentException iae) {
            log.error("Invalid input for sendNotification: {}", iae.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error while sending notification: {}", e.getMessage());
        }
    }

    private String validateRequest(Map<String, Object> request, String extraRequiredField) {
        StringBuilder str = new StringBuilder();
        if (MapUtils.isEmpty(request)) {
            str.append("Request object is empty.");
            return str.toString();
        }

        List<String> errList = new ArrayList<>();
        if (StringUtils.isBlank((String) request.get(Constants.COURSE_ID))) errList.add(Constants.COURSE_ID);
        if (StringUtils.isBlank((String) request.get(Constants.BATCH_ID))) errList.add(Constants.BATCH_ID);
        if (StringUtils.isBlank((String) request.get(Constants.ASSIGNMENT_TITLE)))
            errList.add(Constants.ASSIGNMENT_TITLE);

        if (extraRequiredField != null) {
            if (StringUtils.isBlank((String) request.get(extraRequiredField))) {
                errList.add(extraRequiredField);
            }
        }

        if (!errList.isEmpty()) {
            str.append("Failed Due To Missing Params - ").append(errList).append(".");
        }
        return str.toString();
    }

    private Map<String, Object> fetchUserEmails(List<String> userIds) {
        List<String> emails = new ArrayList<>();
        String firstName = "";

        Map<String, Object> filters = Map.of(Constants.USER_ID, userIds);
        List<String> userFields = List.of(Constants.USER_ID, Constants.PROFILE_DETAILS_PERSONAL_DETAILS);

        Map<String, Object> requestObject = Map.of(
                Constants.REQUEST, Map.of(
                        Constants.FILTERS, filters,
                        Constants.FIELDS, userFields
                )
        );

        Map<String, String> headers = Map.of(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
        String url = props.getSbUrl() + props.getUserSearchEndPoint();

        Map<String, Object> resp = outboundRequestHandlerService.fetchResultUsingPost(url, requestObject, headers);
        if (MapUtils.isEmpty(resp) || !"OK".equalsIgnoreCase(String.valueOf(resp.get(Constants.RESPONSE_CODE)))) {
            return Map.of(Constants.EMAILS, emails, Constants.FIRST_NAME, firstName);
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
            return Map.of(Constants.EMAILS, emails, Constants.FIRST_NAME, firstName);
        }

        for (Object item : contents) {
            if (!(item instanceof Map<?, ?> content)) continue;
            Object personalObj = Optional.ofNullable(content.get(Constants.PROFILE_DETAILS))
                    .filter(Map.class::isInstance)
                    .map(Map.class::cast)
                    .map(p -> p.get(Constants.PERSONAL_DETAILS))
                    .orElse(null);

            if (!(personalObj instanceof Map<?, ?> personal)) continue;

            // Extract email
            Object emailObj = personal.get(Constants.PRIMARY_EMAIL);
            if (emailObj instanceof String email && StringUtils.isNotBlank(email)) {
                emails.add(email);
            }

            // Extract first name (only if single user)
            if (userIds.size() == Constants.ONE) {
                Object nameObj = personal.get(Constants.FIRST_NAME);
                if (nameObj instanceof String name && StringUtils.isNotBlank(name)) {
                    firstName = name;
                }
            }
        }

        return Map.of(Constants.EMAILS, emails, Constants.FIRST_NAME, firstName);
    }

    public ApiResponse notifyAssignmentEvaluate(Map<String, Object> requestData, String authToken) {
        ApiResponse response = new ApiResponse();
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }

            String validateRequest = validateRequest(requestData, Constants.LEARNER_ID);

            if (StringUtils.isNotBlank(validateRequest)) {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg(validateRequest);
                return response;
            }

            Map<String, Object> userData = fetchUserEmails(Collections.singletonList((String) requestData.get(Constants.LEARNER_ID)));
            if (MapUtils.isEmpty(userData)) {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg(Constants.LEARNER_ID_ERR);
                return response;
            }

            Map<String, Object> params = new HashMap<>();
            params.put(Constants.ASSIGNMENT, requestData.get(Constants.ASSIGNMENT_TITLE));
            params.put(Constants.FIRST_NAME, userData.get(Constants.FIRST_NAME));

            Map<String, Object> mailRequestMap = new HashMap<>();
            mailRequestMap.put(Constants.SUBJECT, Constants.ASSIGNMENT_EVALUATE_SUBJECT);
            mailRequestMap.put(Constants.PARAMS, params);
            mailRequestMap.put(Constants.IDS, userData.get(Constants.EMAILS));
            mailRequestMap.put(Constants.USER_ID, userId);
            Map<String, Object> data = new HashMap<>();
            data.put(Constants.ID, requestData.get(Constants.COURSE_ID));
            data.put(Constants.BATCH_ID, requestData.get(Constants.BATCH_ID));
            Map<String, Object> message = new HashMap<>();
            Map<String, Object> placeHolders = new HashMap<>();
            placeHolders.put(Constants.ASSIGNMENT_TITLE, requestData.get(Constants.ASSIGNMENT_TITLE));
            message.put(Constants.DATA, data);
            message.put(Constants.PLACE_HOLDERS, placeHolders);
            sendInAppNotification(Constants.BP_ASSIGNMENT_EVALUATE, Constants.ALERT, Collections.singletonList((String)requestData.get(Constants.LEARNER_ID)), message);

            notifyUsersByEmail(mailRequestMap, Constants.ASSIGNMENT_EVALUATE_TEMPLATE);
            response.setResponseCode(HttpStatus.OK);
            return response;
        } catch (Exception e) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrMsg(e.getMessage());
            return response;
        }
    }

    public ApiResponse notifyAssignmentSubmit(Map<String, Object> requestData, String authToken) {
        ApiResponse response = new ApiResponse();
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken, response);
            if (StringUtils.isEmpty(userId)) {
                return response;
            }

            String validateRequestError = validateRequest(requestData, Constants.INSTRUCTOR_ID);

            if (StringUtils.isNotBlank(validateRequestError)) {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg(validateRequestError);
                return response;
            }

            Map<String, Object> userData = fetchUserEmails(Collections.singletonList((String) requestData.get(Constants.INSTRUCTOR_ID)));
            if (MapUtils.isEmpty(userData)) {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg(Constants.INSTRUCTOR_ID_ERR);
                return response;
            }
            String learnerName = (String) fetchUserEmails(Collections.singletonList(userId)).get(Constants.FIRST_NAME);

            Map<String, Object> params = new HashMap<>();
            params.put(Constants.ASSIGNMENT, requestData.get(Constants.ASSIGNMENT_TITLE));
            params.put(Constants.FIRST_NAME, userData.get(Constants.FIRST_NAME));
            params.put(Constants.LEARNER_NAME, learnerName);

            Map<String, Object> mailRequestMap = new HashMap<>();
            mailRequestMap.put(Constants.SUBJECT, Constants.ASSIGNMENT_SUBMIT_SUBJECT.replace(Constants.LEARNER_NAME_TAG, learnerName));
            mailRequestMap.put(Constants.PARAMS, params);
            mailRequestMap.put(Constants.IDS, userData.get(Constants.EMAILS));
            mailRequestMap.put(Constants.USER_ID, userId);
            Map<String, Object> data = new HashMap<>();
            data.put(Constants.ID, requestData.get(Constants.COURSE_ID));
            data.put(Constants.BATCH_ID, requestData.get(Constants.BATCH_ID));
            Map<String, Object> message = new HashMap<>();
            Map<String, Object> placeHolders = new HashMap<>();
            placeHolders.put(Constants.ASSIGNMENT_TITLE, requestData.get(Constants.ASSIGNMENT_TITLE));
            placeHolders.put(Constants.LEARNER_NAME, learnerName);
            message.put(Constants.DATA, data);
            message.put(Constants.PLACE_HOLDERS, placeHolders);
            sendInAppNotification(Constants.BP_ASSIGNMENT_SUBMIT, Constants.ALERT, Collections.singletonList((String)requestData.get(Constants.INSTRUCTOR_ID)), message);

            notifyUsersByEmail(mailRequestMap, Constants.ASSIGNMENT_SUBMIT_TEMPLATE);
            response.setResponseCode(HttpStatus.OK);
            return response;
        } catch (Exception e) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrMsg(e.getMessage());
            return response;
        }
    }

    /**
     * @param contentName
     * @param userIds
     * @param notificationType
     * @return
     */
    @Override
    public void sendNotificationForContentRetirement(
            String contentId,
            String contentName,
            LocalDate retirementDate,
            List<String> userIds,
            String notificationType) {

        try {
            if (CollectionUtils.isEmpty(userIds) || StringUtils.isEmpty(notificationType)) {
                log.warn("Invalid input for content retirement in-app notification");
                return;
            }

            // Placeholders (ONLY what templates need)
            String subCategory = "";
            Map<String, String> placeHolders = new HashMap<>();
            placeHolders.put(Constants.COURSE_NAME, contentName);
            if (Constants.CONTENT_RETIREMENT_APPROVED_NOTIFICATION.equals(notificationType)) {
                placeHolders.put(Constants.RETIREMENT_DATE, retirementDate.toString());
                subCategory = Constants.APPROVED_CONTENT_RETIREMENT;
            } else if (Constants.REMINDER_NOTIFICATION_SEVEN_DAY.equals(notificationType)) {
                placeHolders.put(Constants.REMINDER_TYPE, "7 Days Before");
                placeHolders.put(Constants.RETIREMENT_DATE, retirementDate.toString());
                subCategory = Constants.SEVEN_DAYS_BEFORE_CONTENT_RETIREMENT;
            } else if (Constants.REMINDER_NOTIFICATION_ONE_DAY.equals(notificationType)) {
                placeHolders.put(Constants.REMINDER_TYPE, "1 Day Before");
                placeHolders.put(Constants.RETIREMENT_DATE, retirementDate.toString());
                subCategory = Constants.ONE_DAYS_BEFORE_CONTENT_RETIREMENT;
            } else {
                subCategory = Constants.CONTENT_RETIRED;
            }

            Map<String, Object> data = new HashMap<>();
            data.put(Constants.ID, contentId);
            data.put(Constants.RETIRED_DATE,(retirementDate.toString()));
            Map<String, Object> message = new HashMap<>();
            message.put(Constants.PLACE_HOLDERS, placeHolders);
            message.put(Constants.DATA, data);

            sendInAppNotification(subCategory, Constants.ALERT, userIds, message);
            log.info("In-app retirement notification [{}] sent for course {}",
                    notificationType, contentName);
        } catch (Exception e) {
            log.error("Error while sending in-app retirement notification", e);
        }
    }

    @Override
    public void sendNotificationForContentRetirementSpv(String contentId, String contentName, ArrayList<String> userIds, String notificationType, LocalDate date, List<String> emails, String requestedBy) {
        try {
            if (CollectionUtils.isEmpty(userIds) || StringUtils.isEmpty(notificationType)) {
                log.warn("Invalid input for content retirement in-app notification");
                return;
            }
            String subCategory = notificationType;
            Map<String, String> placeHolders = new HashMap<>();
            placeHolders.put(Constants.TITLE, contentName);
            placeHolders.put(Constants.DATE_KEY, date.toString());
            Map<String, Object> data = new HashMap<>();
            data.put(Constants.ID, contentId);
            Map<String, Object> message = new HashMap<>();
            message.put(Constants.PLACE_HOLDERS, placeHolders);
            message.put(Constants.DATA, data);
            Map<String, Object> params = new HashMap<>();
            params.put(Constants.COURSE_NAME, contentName);
            params.put(Constants.RETIREMENT_DATE, date.toString());

            Map<String, Object> mailRequestMap = new HashMap<>();
            mailRequestMap.put(Constants.SUBJECT, Constants.RETIREMENT_SCHEDULED_SUBJECT.replace(Constants.COURSE_NAME_TAG, contentName));
            mailRequestMap.put(Constants.PARAMS, params);

            mailRequestMap.put(Constants.BCC_IDS, emails);
            mailRequestMap.put(Constants.USER_ID, requestedBy);
            sendInAppNotification(subCategory, Constants.ALERT, userIds, message);
            notifyUsersByEmail(mailRequestMap, Constants.RETIREMENT_SCHEDULE_TEMPLATE);
            log.info("In-app retirement notification [{}] sent for course {}",
                    notificationType, contentName);
        } catch (Exception e) {
            log.error("Error while sending in-app retirement notification", e);
        }
    }

    @Override
    public void sendNotificationForExternalTraining(String trainingId, String trainingName, List<String> userIds, String notificationType) {
        try {
            if (CollectionUtils.isEmpty(userIds) || StringUtils.isEmpty(notificationType)) {
                log.warn("Invalid input for content retirement in-app notification");
                return;
            }
            String subCategory = notificationType;
            Map<String, String> placeHolders = new HashMap<>();
            placeHolders.put(Constants.COURSE_NAME, trainingName);
            Map<String, Object> data = new HashMap<>();
            data.put(Constants.ID, trainingId);
            Map<String, Object> message = new HashMap<>();
            message.put(Constants.PLACE_HOLDERS, placeHolders);
            message.put(Constants.DATA, data);
            sendInAppNotification(subCategory, Constants.ALERT, userIds, message);
            log.info("In-app retirement notification [{}] sent for course {}",
                    notificationType, trainingName);
        } catch (Exception e) {
            log.error("Error while sending in-app retirement notification", e);
        }

    }
}
