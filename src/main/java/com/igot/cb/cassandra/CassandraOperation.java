package com.igot.cb.cassandra;

import java.util.List;
import java.util.Map;

/**
 * @author Mahesh RV
 * @author Ruksana
 * Interface defining Cassandra operations for querying records.
 */

public interface CassandraOperation {
    /**
     * Inserts a record into Cassandra.
     *
     * @param keyspaceName The name of the keyspace containing the table.
     * @param tableName    The name of the table into which to insert the record.
     * @param request      A map representing the record to insert.
     * @return An object representing the result of the insertion operation.
     */
    public Object insertRecord(String keyspaceName, String tableName, Map<String, Object> request);

    public List<Map<String, Object>> getRecordsByProperties(String keyspaceName, String tableName,
                                                                            Map<String, Object> propertyMap, List<String> fields, Integer limit);

    public Map<String, Object> updateRecord(String keyspaceName, String tableName,
        Map<String, Object> updateAttributes,
        Map<String, Object> compositeKey
    );

}
