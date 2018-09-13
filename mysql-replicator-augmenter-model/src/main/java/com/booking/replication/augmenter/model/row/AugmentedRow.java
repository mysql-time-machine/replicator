package com.booking.replication.augmenter.model.row;

import com.booking.replication.augmenter.model.AugmenterModel;
import com.booking.replication.augmenter.model.schema.TableSchema;
import com.booking.replication.commons.util.CaseInsensitiveMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class AugmentedRow {

    private UUID transactionUUID;
    private Long         transactionXid;

    private Long         commitTimestamp;
    private Long         rowMicrosecondTimestamp;

    private List<String> primaryKeyColumns;

    private String       eventType;

    private String       tableName;
    private String       tableSchema;

    // rowColumns format:
    // {
    //      $columnName => {
    //          value        => $value // <- null for UPDATE op
    //          value_before => $value // <- null for INSERT op
    //          value_after  => $value // <- null for DELETE op
    //      }
    // }
    private Map<String, Map<String,String>> rowColumns = new CaseInsensitiveMap<>();

    public AugmentedRow(
            String eventType,
            String schemaName, String tableName,
            UUID transactionUUID, Long transactionXid,
            Long commitTimestamp, Long rowMicrosecondTimestamp,
            List<String> primaryKeyColumns,
            Map<String,Map<String, String>> rowColumnValues
    ) {

        this.primaryKeyColumns = primaryKeyColumns;

        this.transactionUUID = transactionUUID;
        this.transactionXid = transactionXid;

        this.commitTimestamp = commitTimestamp;
        this.rowMicrosecondTimestamp = rowMicrosecondTimestamp;

        this.eventType = eventType;

        this.rowColumns = rowColumnValues;

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

    public Long getRowMicrosecondTimestamp() {
        return rowMicrosecondTimestamp;
    }

    public String getEventType() {
        return eventType;
    }


}
