package com.booking.replication.augmenter.model.row;

import com.booking.replication.augmenter.model.event.AugmentedEventType;
import com.booking.replication.commons.util.CaseInsensitiveMap;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class AugmentedRow {

    private static final String NULL_STRING = "NULL";

    private String       transactionUUID;
    private Long         transactionXid;

    private Long commitTimestamp;
    private Long transactionSequenceNumber = 0L;
    private Long rowMicrosecondTimestamp = 0L;

    private List<String> primaryKeyColumns;

    private AugmentedEventType eventType;

    private String       tableName;
    private String       tableSchema;

    private Map<String, Object> values = new CaseInsensitiveMap<>();

    public AugmentedRow() { }

    public AugmentedRow(
                        AugmentedEventType eventType,
                        String schemaName,
                        String tableName,
                        Long commitTimestamp,
                        String transactionUUID,
                        Long transactionXid,
                        List<String> primaryKeyColumns,
                        Map<String, Object> values) {

        this.primaryKeyColumns  = primaryKeyColumns;
        this.commitTimestamp    = commitTimestamp;
        this.transactionUUID    = transactionUUID;
        this.transactionXid     = transactionXid;

        // time-bucketed binlogEventCounter is used to add a fake microsecond suffix for the timestamps
        // of all rows in the event. This way we keep the information about ordering of events
        // and the ordering of changes to their rows in case when the same row is changed multiple
        // times during one second, but in different events. The additional logic is added in
        // TimestampOrganizer which protects the ordering of changes in cases when the same row
        // is altered multiple times in the same event.
        // this.microsecondTransactionOffset = null; // transactionCounter * 100; // one inc <=> 0.1ms

        this.eventType      = eventType;
        this.values         = values;
        this.tableSchema    = schemaName;
        this.tableName      = tableName;
    }

    public void setTransactionSequenceNumber(Long transactionSequenceNumber) {
        this.transactionSequenceNumber = transactionSequenceNumber;
    }

    public void setCommitTimestamp(Long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    @JsonIgnore
    public String getTableSchema() {
        return this.tableSchema;
    }

    @JsonIgnore
    public String getTableName() {
        return this.tableName;
    }

    @JsonIgnore
    public List<String> getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

    public String getTransactionUUID() {
        return transactionUUID;
    }

    public Long getTransactionXid() {
        return transactionXid;
    }

    public Long getCommitTimestamp() {
        return commitTimestamp;
    }

    @JsonIgnore
    public Long getMicrosecondTransactionOffset() {
        return transactionSequenceNumber * 100;
    }

    @JsonIgnore
    public AugmentedEventType getEventType() {
        return eventType;
    }

    public void setRowMicrosecondTimestamp(Long rowMicrosecondTimestamp) {
        this.rowMicrosecondTimestamp = rowMicrosecondTimestamp;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Long getRowMicrosecondTimestamp() {
        return this.rowMicrosecondTimestamp;
    }

    public String getValueAsString(String column) {
        return getValueAsString(column, null);
    }
    public String getValueAsString(String column, String key) {

        Object value = null;

        if (values.containsKey(column)) {
            value = values.get(column);

            if (eventType == AugmentedEventType.UPDATE && value instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) value;
                if (map.containsKey(key)) {
                    value = map.get(key);
                } else {
                    value = null;
                }
            }
        }

        if (value == null) {
            return NULL_STRING;
        } else {
            return value.toString();
        }
    }

    public void setTransactionUUID(String transactionUUID) {
        this.transactionUUID = transactionUUID;
    }

    public void setPrimaryKeyColumns(List<String> primaryKeyColumns) {
        this.primaryKeyColumns = primaryKeyColumns;
    }

    public void setEventType(AugmentedEventType eventType) {
        this.eventType = eventType;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public void setValues(Map<String, Object> values) {
        this.values = values;
    }

    @Override
    public String toString() {
        return "AugmentedRow{"
                + "transactionUUID='" + transactionUUID
                + ", transactionXid=" + transactionXid
                + ", commitTimestamp=" + commitTimestamp
                + ", transactionSequenceNumber=" + transactionSequenceNumber
                + ", rowMicrosecondTimestamp=" + rowMicrosecondTimestamp
                + ", primaryKeyColumns=" + primaryKeyColumns
                + ", eventType=" + eventType
                + ", tableName='" + tableName
                + ", tableSchema='" + tableSchema
                + ", values=" + values
                + '}';
    }
}
