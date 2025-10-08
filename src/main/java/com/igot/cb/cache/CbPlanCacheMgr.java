package com.igot.cb.cache;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.expression.spel.ast.BooleanLiteral;
import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class CbPlanCacheMgr {

    @Value("${cb.plan.cache.ttl.minutes:60}")
    private int ttlMinutes;

    @Value("${cb.plan.batch.size:5}") //default fallback to 5 if missing
    private int planBatchSize;

    private final CassandraOperation cassandraOperation;
    private Cache<String, List<Map<String, Object>>> cbPlanCache;

    public CbPlanCacheMgr(CassandraOperation cassandraOperation) {
        this.cassandraOperation = cassandraOperation;
        this.cbPlanCache = Caffeine.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(Duration.ofMinutes(ttlMinutes))
                .build();
    }

    private List<Map<String, Object>> getCbPlanForAll() {
        List<Map<String, Object>> allCbPlanList = cbPlanCache.getIfPresent("all-lookup");
        if (allCbPlanList == null) {
            log.info("No CB Plans for all orgs in Cache, reading from Cassandra");
            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put(Constants.PLAN_YEAR, "ALL");
            allCbPlanList = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ALL_ORG,
                    propertiesMap,
                    new ArrayList<>(),
                    null);
            if (allCbPlanList == null) {
                allCbPlanList = new ArrayList<>();
            }
            allCbPlanList = allCbPlanList.stream()
                    .filter(plan -> Boolean.TRUE.equals(plan.get(Constants.IS_ACTIVE))).collect(Collectors.toList());
            cbPlanCache.put("all", allCbPlanList);
        } else {
            log.info("Cache hit for all orgs: Found {} records", allCbPlanList.size());
        }
        return allCbPlanList;
    }

    private List<Map<String, Object>> getCbPlanForOrgId(String orgId) {
        List<Map<String, Object>> cbPlanList = cbPlanCache.getIfPresent(orgId + "-lookup");
        
        if (cbPlanList == null) {
            log.info("No CB Plans for orgId in Cache: {}, reading from Cassandra", orgId);
            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put(Constants.ORG_ID, orgId);
            cbPlanList = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_CB_PLAN_V2_LOOKUP_BY_ORG,
                    propertiesMap,
                    new ArrayList<>(),
                    null);
            if (cbPlanList == null) {
                cbPlanList = new ArrayList<>();
            }
            cbPlanList = cbPlanList.stream()
                    .filter(plan -> Boolean.TRUE.equals(plan.get(Constants.IS_ACTIVE))).collect(Collectors.toList());
            cbPlanCache.put(orgId + "-lookup", cbPlanList);
            cbPlanList.addAll(getCbPlanForAll());
        } else {
            log.info("Cache hit for orgId: {}, Found {} records", orgId, cbPlanList.size());
        }

        return cbPlanList;
    }

    public List<Map<String, Object>> getCbPlanForAllAndOrgId(String orgId, AtomicBoolean isCacheEnabled) {
        List<Map<String, Object>> activeCbPlans = cbPlanCache.getIfPresent(orgId);
        if (activeCbPlans != null) {
            log.info("Cache hit for orgId: {}, Found {} active CB Plans", orgId, activeCbPlans.size());
            activeCbPlans = new ArrayList<>();
            return activeCbPlans;
        }
        List<Map<String, Object>> cbPlanList = getCbPlanForOrgId(orgId);
        if (cbPlanList.isEmpty()) {
            log.info("No CB Plans found for orgId: {}", orgId);
            cbPlanList = new ArrayList<>();
            cbPlanCache.put(orgId, cbPlanList);
            return cbPlanList;
        }
        cbPlanList = cbPlanList.stream()
                .sorted(Comparator.comparing(m -> (Instant) m.get(Constants.END_DATE_REQUEST),
                        Comparator.reverseOrder()))
                .collect(Collectors.toList());
        List<String> planIds = cbPlanList.stream()
                    .map(plan -> (String) plan.get(Constants.PLAN_ID)).collect(Collectors.toList());
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(Constants.PLAN_ID, planIds);
        List<Map<String, Object>> existingCbPlans = cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_CB_PLAN_V2,
                propertiesMap,
                new ArrayList<>(),
                null);
        if (existingCbPlans == null) {
            log.error("Failed to read cassandra for cb plan, for PlanIds: {}", planIds);
            return new ArrayList<>();
        }

        activeCbPlans = existingCbPlans.stream()
                .filter(plan -> Constants.LIVE.equalsIgnoreCase((String) plan.get(Constants.STATUS))).collect(Collectors.toList());
        //TODO - Need to remove draftData (if available) and also contextData.accessControl
        log.info("Found {} CB Plans for orgId: {}, active count: {}", existingCbPlans.size(), orgId, activeCbPlans.size());
        cbPlanCache.put(orgId, activeCbPlans);
        isCacheEnabled.set(true);
        return activeCbPlans;
    }

    public List<Map<String, Object>> getCbPlansByPlanIdsInBatch(List<String> planIds) {
        List<Map<String, Object>> allCbPlans = new ArrayList<>();

        if (CollectionUtils.isEmpty(planIds)) {
            log.warn("No plan IDs provided for batch fetch.");
            return allCbPlans;
        }

        log.info("Fetching CB Plan details for {} plan IDs in batches of 5", planIds.size());

        // Process in batches of 5

        for (int i = 0; i < planIds.size(); i += planBatchSize) {
            List<String> batch = planIds.subList(i, Math.min(i + planBatchSize, planIds.size()));

            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put(Constants.PLAN_ID, batch);

            try {
                List<Map<String, Object>> batchResult = cassandraOperation.getRecordsByProperties(
                        Constants.KEYSPACE_SUNBIRD,
                        Constants.TABLE_CB_PLAN_V2,
                        propertiesMap,
                        new ArrayList<>(),
                        null
                );

                if (CollectionUtils.isNotEmpty(batchResult)) {
                    allCbPlans.addAll(batchResult);
                    log.info("Fetched {} records for plan IDs batch: {}", batchResult.size(), batch);
                } else {
                    log.warn("No records found for plan IDs batch: {}", batch);
                }

            } catch (Exception e) {
                log.error("Error fetching CB Plans for plan IDs batch {}: {}", batch, e.getMessage(), e);
            }
        }


        log.info("Total CB Plans fetched from Cassandra: {}", allCbPlans.size());
        return allCbPlans;
    }

}
