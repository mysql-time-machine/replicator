package com.booking.replication.augmenter.model.row;

import com.booking.replication.augmenter.model.AugmenterModel;
import com.booking.replication.commons.util.CaseInsensitiveMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class AugmentedRow {

    private UUID         transactionUUID;
    private Long         transactionXid;

    private Long         commitTimestamp;

    private List<String> primaryKeyColumns;

    private String       eventType;

    private String       tableName;
    private String       tableSchema;

    private final Long fakeMicrosecondCounter;
    private       Long rowMicrosecondTimestamp = null;

    // rowColumns format:
    // {
    //      $columnName => {
    //          value        => $value // <- null for UPDATE op
    //          value_before => $value // <- null for INSERT op
    //          value_after  => $value // <- null for DELETE op
    //      }
    // }
    private Map<String, Map<String,String>> rowColumns = new CaseInsensitiveMap<>();

    public AugmentedRow() {
        this.fakeMicrosecondCounter = null;
    }

    public AugmentedRow(
            String eventType,
            String schemaName,
            String tableName,

            UUID transactionUUID,
            Long transactionXid,

            Long commitTimestamp,
            Long binlogEventCounter,

            List<String> primaryKeyColumns,
            Map<String,Map<String, String>> rowColumnValues
    ) {


        this.primaryKeyColumns = primaryKeyColumns;

        this.transactionUUID = transactionUUID;

        this.transactionXid = transactionXid;

        this.commitTimestamp = commitTimestamp;

        // time-bucketed binlogEventCounter is used to add a fake microsecond suffix for the timestamps
        // of all rows in the event. This way we keep the information about ordering of events
        // and the ordering of changes to their rows in case when the same row is changed multiple
        // times during one second, but in different events. The additional logic is added in
        // TimestampOrganizer which protects the ordering of changes in cases when the same row
        // is altered multiple times in the same event.
        // TODO: merge these two steps in the TimestampOrganizer
        this.fakeMicrosecondCounter = binlogEventCounter * 100; // one inc <=> 0.1ms

        this.eventType = eventType;

        this.rowColumns = rowColumnValues;

        this.tableSchema = schemaName;
        this.tableName = tableName;

        initColumnDataSlots();
    }

    public void initColumnDataSlots() {
        rowColumns.put(AugmenterModel.Configuration.UUID_FIELD_NAME, new HashMap<String, String>());
        rowColumns.put(AugmenterModel.Configuration.XID_FIELD_NAME, new HashMap<String, String>());
    }

    public Map<String, Map<String, String>> getRowColumns() {
        return rowColumns;
    }

    public String getTableSchema() {
        return this.tableSchema;
    }

    public String getTableName() {
        return this.tableName;
    }

    public List<String> getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

    public UUID getTransactionUUID() {
        return transactionUUID;
    }

    public Long getTransactionXid() {
        return transactionXid;
    }

    public Long getCommitTimestamp() {
        return commitTimestamp;
    }

    public Long getFakeMicrosecondCounter() {
        return fakeMicrosecondCounter;
    }

    public String getEventType() {
        return eventType;
    }

    public void setTransactionUUID(UUID transactionUUID) {
        this.transactionUUID = transactionUUID;
    }

    public void setTransactionXid(Long transactionXid) {
        this.transactionXid = transactionXid;
    }

    public void setCommitTimestamp(Long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    public void setRowMicrosecondTimestamp(Long rowMicrosecondTimestamp) {
        this.rowMicrosecondTimestamp = rowMicrosecondTimestamp;
    }

    public void setPrimaryKeyColumns(List<String> primaryKeyColumns) {
        this.primaryKeyColumns = primaryKeyColumns;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public void setRowColumns(Map<String, Map<String, String>> rowColumns) {
        this.rowColumns = rowColumns;
    }

    public Long getRowMicrosecondTimestamp() {
        return this.rowMicrosecondTimestamp;
    }
}
