package com.igot.cb.service;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

@Slf4j
@Service
public class ContentRetirementService {

    private final CassandraOperation cassandraOperation;
    private final ContentInfoServiceImpl contentService;

    public ContentRetirementService(CassandraOperation cassandraOperation, ContentInfoServiceImpl contentService) {
        this.cassandraOperation = cassandraOperation;
        this.contentService = contentService;
    }

    public ApiResponse processDueRetirements() {
        ApiResponse response = ApiResponse.createDefaultResponse("retirement.schedule.cron");

        LocalDate today = LocalDate.now();

        log.info("Running content retirement job for date <= {}", today);

        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(Constants.STATUS, Constants.APPROVED);

        List<String> fields = Arrays.asList(
                Constants.CONTENT_ID_KEY,
                Constants.REQUEST_ID_KEY,
                Constants.RETIREMENT_DATE_KEY,
                Constants.STATUS
        );

        List<Map<String, Object>> records =
                cassandraOperation.getRecordsByProperties(
                        Constants.KEYSPACE_SUNBIRD_COURSE,
                        Constants.CONTENT_RETIREMENT_REQUEST_TABLE,
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
}

