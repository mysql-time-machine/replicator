package com.booking.replication.augmenter.model.row;

import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.augmenter.model.event.WriteRowsAugmentedEventData;
import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.FullTableName;
import com.booking.replication.augmenter.model.schema.TableSchema;
import com.booking.replication.commons.util.CaseInsensitiveMap;

import java.util.*;

public class AugmentedRow {

    private TableSchema  tableSchema;
    private String       tableName;
    private UUID       transactionUUID;
    private Long         transactionXid;
    private Long         commitTimestamp;
    private Long         rowMicrosecondTimestamp;
    private String       eventType;

    private List<String> primaryKeyColumns = new ArrayList<>();
    private Map<String, Map<String,String>> rowColumns = new CaseInsensitiveMap<>(); // { columnName => { val_before/after => value } }

    public AugmentedRow(
            TableSchema tableSchema,
            String tableName,
            List<String> primaryKeyColumns,
            UUID transactionUUID,
            Long transactionXid,
            Long commitTimestamp,
            Long rowMicrosecondTimestamp,
            String eventType,
            Map<String,Map<String, String>> rowColumns
    ) {
        this.tableSchema = tableSchema;
        this.tableName = tableName;
        this.primaryKeyColumns = primaryKeyColumns;
        this.transactionUUID = transactionUUID;
        this.transactionXid = transactionXid;
        this.commitTimestamp = commitTimestamp;
        this.rowMicrosecondTimestamp = rowMicrosecondTimestamp;
        this.eventType = eventType;
        this.rowColumns = rowColumns;
    }

    public TableSchema getTableSchema() {
        return tableSchema;
    }

    public String getTableName() {
        return tableName;
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

    public Map<String, Map<String, String>> getRowColumns() {
        return rowColumns;
    }

    public static List<AugmentedRow> extractAugmentedRows(AugmentedEvent augmentedEvent) {

        List<AugmentedRow> augmentedRows = new ArrayList<>();

        switch (augmentedEvent.getHeader().getEventType()) {

            case WRITE_ROWS:

                WriteRowsAugmentedEventData writeRowsAugmentedEventData =
                        ((WriteRowsAugmentedEventData) augmentedEvent.getData());

                Collection<AugmentedRow> extractedAugmentedRows = writeRowsAugmentedEventData.getAugmentedRows();

                augmentedRows.addAll(extractedAugmentedRows);

                break;

            case UPDATE_ROWS:
                // TODO
                break;
            case DELETE_ROWS:
                // TODO
                break;
            default:
                break;
        }
        return augmentedRows;
    }
}
