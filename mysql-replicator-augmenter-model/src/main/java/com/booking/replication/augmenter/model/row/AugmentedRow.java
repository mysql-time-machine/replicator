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
    private long         rowBinlogEventOrdinal;
    private String       binlogFileName;
    private List<String> primaryKeyColumns = new ArrayList<>();
    private String       rowUUID;
    private String       rowBinlogPositionID;
    private UUID         transactionUUID;
    private Long         transactionXid;
    private Long         commitTimestamp;
    private Long         rowMicrosecondTimestamp;
    private String       eventType;

    private Map<String, Map<String,String>> eventColumns = new CaseInsensitiveMap<>();

    public AugmentedRow(
            TableSchema tableSchema,
            String tableName,
            long rowBinlogEventOrdinal,
            String binlogFileName,
            List<String> primaryKeyColumns,
            String rowUUID,
            String rowBinlogPositionID,
            UUID transactionUUID,
            Long transactionXid,
            Long commitTimestamp,
            Long rowMicrosecondTimestamp,
            String eventType,
            Map<String, Map<String, String>> eventColumns
        ) {
        this.tableSchema = tableSchema;
        this.tableName = tableName;
        this.rowBinlogEventOrdinal = rowBinlogEventOrdinal;
        this.binlogFileName = binlogFileName;
        this.primaryKeyColumns = primaryKeyColumns;
        this.rowUUID = rowUUID;
        this.rowBinlogPositionID = rowBinlogPositionID;
        this.transactionUUID = transactionUUID;
        this.transactionXid = transactionXid;
        this.commitTimestamp = commitTimestamp;
        this.rowMicrosecondTimestamp = rowMicrosecondTimestamp;
        this.eventType = eventType;
        this.eventColumns = eventColumns;
    }

    public TableSchema getTableSchema() {
        return tableSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public long getRowBinlogEventOrdinal() {
        return rowBinlogEventOrdinal;
    }

    public String getBinlogFileName() {
        return binlogFileName;
    }

    public List<String> getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

    public String getRowUUID() {
        return rowUUID;
    }

    public String getRowBinlogPositionID() {
        return rowBinlogPositionID;
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

    public Map<String, Map<String, String>> getEventColumns() {
        return eventColumns;
    }

    public static List<AugmentedRow> extractAugmentedRows(AugmentedEvent augmentedEvent) {

        List<AugmentedRow> augmentedRows = new ArrayList<>();

        switch (augmentedEvent.getHeader().getEventType()) {

            case WRITE_ROWS:

                WriteRowsAugmentedEventData writeRowsAugmentedEventData =
                        ((WriteRowsAugmentedEventData) augmentedEvent.getData());

                FullTableName eventTable = writeRowsAugmentedEventData.getEventTable();
                Collection<ColumnSchema> eventDataColumns = writeRowsAugmentedEventData.getColumns();
                Collection<Boolean> includedColumns = writeRowsAugmentedEventData.getIncludedColumns();
                Collection<Map<String, Object>> rows = writeRowsAugmentedEventData.getRows();

                List<AugmentedRow> extractedAugmentedRows = extractAugmentedRowsFromWriteRowsEventData(
                        eventTable,
                        eventDataColumns,
                        includedColumns,
                        rows
                );

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

    private static List<AugmentedRow> extractAugmentedRowsFromWriteRowsEventData(
        FullTableName eventTable,
        Collection<ColumnSchema> eventDataColumns,
        Collection<Boolean> includedColumns,
        Collection<Map<String, Object>> rows
    ) {
        List<AugmentedRow> augRows = new ArrayList<>();

        for (Map<String, Object> row : rows) {


        // TODO: get params and call
//            AugmentedRow ar = new AugmentedRow(
//                    null,
//
//                    null,
//                    0,
//                    null,
//                    null,
//                    null,
//                    null,
//                    null,
//                    null,
//                    null,
//                    null,
//                    null,
//                    null
//            );
        }
        return augRows;
    }

}
