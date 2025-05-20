package com.igot.cb.transactional.cassandrautils;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;

import com.igot.cb.transactional.util.ApiResponse;
import com.igot.cb.transactional.util.Constants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;


/**
 * @author Mahesh RV
 * @author Ruksana
 */
@Component
public class CassandraOperationImpl implements CassandraOperation {

    private Logger logger = LoggerFactory.getLogger(getClass().getName());

    @Autowired
    CassandraConnectionManager connectionManager;

    private com.datastax.oss.driver.api.querybuilder.select.Select processQuery(String keyspaceName, String tableName, Map<String, Object> propertyMap,
                                                                                List<String> fields) {
        com.datastax.oss.driver.api.querybuilder.select.Select select;
        if (CollectionUtils.isNotEmpty(fields)) {
            select = com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom(keyspaceName, tableName).columns(fields);
        } else {
            select = com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom(keyspaceName, tableName).all();
        }
        if (MapUtils.isEmpty(propertyMap)) {
            return select; // Build and return the query
        }
        for (Map.Entry<String, Object> entry : propertyMap.entrySet()) {
            String columnName = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof List) {
                List<?> valueList = (List<?>) value;
                if (CollectionUtils.isNotEmpty(valueList)) {
                    List<Term> terms = valueList.stream()
                            .map(com.datastax.oss.driver.api.querybuilder.QueryBuilder::literal)
                            .collect(Collectors.toList());
                    select = select.whereColumn(columnName).in(terms);
                }
            } else {
                select = select.whereColumn(columnName).isEqualTo(com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal(value));
            }
        }
        return select;
    }

    @Override
    public Object insertRecord(String keyspaceName, String tableName, Map<String, Object> request) {
        ApiResponse response = new ApiResponse();
        try {
            String query = CassandraUtil.getPreparedStatement(keyspaceName, tableName, request);
            CqlSession session = connectionManager.getSession(keyspaceName);
            com.datastax.oss.driver.api.core.cql.PreparedStatement statement = session.prepare(query);
            com.datastax.oss.driver.api.core.cql.BoundStatement boundStatement = statement.bind(request.values().toArray());
            session.execute(boundStatement);
            response.put(Constants.RESPONSE, Constants.SUCCESS);
        } catch (Exception e) {
            String errMsg = String.format("Exception occurred while inserting record to %s %s", tableName, e.getMessage());
            logger.error("Error inserting record into {}: {}", tableName, e.getMessage());
            response.put(Constants.RESPONSE, Constants.FAILED);
            response.put(Constants.ERROR_MESSAGE, errMsg);
        }
        return response;
    }

    @Override
    public List<Map<String, Object>> getRecordsByPropertiesWithoutFiltering(String keyspaceName, String tableName, Map<String, Object> propertyMap, List<String> fields, Integer limit) {

        List<Map<String, Object>> response = new ArrayList<>();
        try {
            com.datastax.oss.driver.api.querybuilder.select.Select selectQuery = null;
            selectQuery = processQuery(keyspaceName, tableName, propertyMap, fields);

            if (limit != null) selectQuery = selectQuery.limit(limit);
            String queryString = selectQuery.toString();
            SimpleStatement statement = SimpleStatement.newInstance(queryString);
            ResultSet results = connectionManager.getSession(keyspaceName).execute(statement);
            response = CassandraUtil.createResponse(results);

        } catch (Exception e) {
            logger.error("Error fetching records from {}: {}", tableName, e.getMessage());
        }
        return response;
    }

    @Override
    public Map<String,Object> updateRecord(String keyspaceName, String tableName, Map<String, Object> updateAttributes,
                                           Map<String, Object> compositeKey) {
        Map<String, Object> response = new HashMap<>();
        CqlSession session = null;
        try {
            session = connectionManager.getSession(keyspaceName);
            UpdateStart updateStart = com.datastax.oss.driver.api.querybuilder.QueryBuilder.update(keyspaceName, tableName);
            UpdateWithAssignments updateWithAssignments = updateStart.set(updateAttributes.entrySet().stream()
                    .map(entry -> Assignment.setColumn(entry.getKey(), com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal(entry.getValue())))
                    .toArray(Assignment[]::new));
            com.datastax.oss.driver.api.querybuilder.update.Update update = updateWithAssignments.where(compositeKey.entrySet().stream()
                    .map(entry -> Relation.column(entry.getKey()).isEqualTo(com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal(entry.getValue())))
                    .toArray(Relation[]::new));
            SimpleStatement statement = update.build();
            session.execute(statement);
            response.put(Constants.RESPONSE, Constants.SUCCESS);
        } catch (Exception e) {
            String errMsg = String.format("Exception occurred while updating record to %s: %s", tableName, e.getMessage());
            logger.error(errMsg, e);
            response.put(Constants.RESPONSE, Constants.FAILED);
            response.put(Constants.ERROR_MESSAGE, errMsg);
            throw e;
        }
        return response;
    }

}
