package com.igot.cb.cassandra;

import lombok.Getter;

import java.util.Collections;
import java.util.List;

/**
 * Parameters for batch query with IN clause on clustering column.
 * Encapsulates query parameters to avoid excessive method parameters.
 */

@Getter
public class BatchQueryParams {
    private final String keyspaceName;
    private final String tableName;
    private final String partitionKeyColumn;
    private final Object partitionKeyValue;
    private final String clusteringColumn;
    private final List<String> clusteringValues;
    private final List<String> fields;
    private final Integer limit;

    private BatchQueryParams(Builder builder) {
        this.keyspaceName = builder.keyspaceName;
        this.tableName = builder.tableName;
        this.partitionKeyColumn = builder.partitionKeyColumn;
        this.partitionKeyValue = builder.partitionKeyValue;
        this.clusteringColumn = builder.clusteringColumn;
        this.clusteringValues = builder.clusteringValues;
        this.fields = builder.fields != null ? builder.fields : Collections.emptyList();
        this.limit = builder.limit;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String keyspaceName;
        private String tableName;
        private String partitionKeyColumn;
        private Object partitionKeyValue;
        private String clusteringColumn;
        private List<String> clusteringValues;
        private List<String> fields;
        private Integer limit;

        public Builder keyspaceName(String keyspaceName) {
            this.keyspaceName = keyspaceName;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder partitionKeyColumn(String column) {
            this.partitionKeyColumn = column;
            return this;
        }

        public Builder partitionKeyValue(Object value) {
            this.partitionKeyValue = value;
            return this;
        }

        public Builder clusteringColumn(String column) {
            this.clusteringColumn = column;
            return this;
        }

        public Builder clusteringValues(List<String> values) {
            this.clusteringValues = values;
            return this;
        }

        public Builder fields(List<String> fields) {
            this.fields = fields;
            return this;
        }

        public Builder limit(Integer limit) {
            this.limit = limit;
            return this;
        }

        public BatchQueryParams build() {
            return new BatchQueryParams(this);
        }
    }
}
