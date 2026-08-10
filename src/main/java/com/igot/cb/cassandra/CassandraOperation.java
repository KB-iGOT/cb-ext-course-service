package com.igot.cb.cassandra;

import com.igot.cb.model.ApiResponse;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

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

    /**
     * Updates a record only after a pre-commit validation (e.g. an ElasticSearch sync) succeeds,
     * so the Cassandra write and the external sync it depends on never diverge in the caller's favor.
     * If the Cassandra commit itself throws after validation already succeeded, onCommitFailureRollback
     * is invoked as a best-effort compensating action (e.g. reverting the external sync).
     *
     * @param preCommitValidator     run after the statement is built but before it is executed; commit is skipped if this returns false
     * @param onCommitFailureRollback best-effort compensation invoked if the commit throws after preCommitValidator returned true
     */
    public Map<String, Object> updateRecord(String keyspaceName, String tableName,
        Map<String, Object> updateAttributes,
        Map<String, Object> compositeKey,
        Supplier<Boolean> preCommitValidator,
        Runnable onCommitFailureRollback
    );

    ApiResponse insertBulkRecord(String keyspaceName, String tableName, List<Map<String, Object>> request);

    public void deleteRecord(String keyspaceName, String tableName, Map<String, Object> keyMap);

    /**
     * Inserts a record into Cassandra with a composite primary key.
     *
     * @param keyspaceName      The name of the keyspace containing the table.
     * @param tableName         The name of the table into which to insert the record.
     * @param primaryKeyColumn  The name of the primary key column.
     * @param primaryKeyValue   The value of the primary key.
     * @param compositeKey      A map representing the composite key fields and their values.
     * @param otherFields       A map representing other fields and their values to be inserted.
     * @return An object representing the result of the insertion operation.
     */
    Object insertRecord(
            String keyspaceName,
            String tableName,
            String primaryKeyColumn,
            String primaryKeyValue,
            Map<String, Object> compositeKey,
            Map<String, Object> otherFields);

    void forEachRecordByProperties(String keyspaceName, String tableName,
                                   Map<String, Object> propertyMap, List<String> fields,
                                   Integer pageSize, Integer maxRows, Consumer<Map<String, Object>> rowConsumer);


}
