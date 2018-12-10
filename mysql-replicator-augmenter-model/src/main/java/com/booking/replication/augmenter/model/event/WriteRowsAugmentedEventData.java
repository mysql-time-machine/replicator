package com.booking.replication.augmenter.model.event;

import com.booking.replication.augmenter.model.row.AugmentedRow;
import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.FullTableName;

import java.util.Collection;
import java.util.Map;

@SuppressWarnings("unused")
public class WriteRowsAugmentedEventData implements TableAugmentedEventData {
    private FullTableName eventTable;
    private Collection<Boolean> includedColumns;
    private Collection<ColumnSchema> columns;
    private Collection<Map<String, Object>> rows;
    private Collection<AugmentedRow> augmentedRows;

    public WriteRowsAugmentedEventData() {
    }

    public WriteRowsAugmentedEventData(
            FullTableName eventTable,
            Collection<Boolean> includedColumns,
            Collection<ColumnSchema> columns,
            Collection<AugmentedRow> augmentedRows
        ) {
        this.eventTable = eventTable;
        this.includedColumns = includedColumns;
        this.columns = columns;
        this.augmentedRows = augmentedRows;
    }

    @Override
    public FullTableName getEventTable() {
        return this.eventTable;
    }

    public void overrideEventTableName(String tableName) {
        this.eventTable.setName(tableName);
    }

    public Collection<Boolean> getIncludedColumns() {
        return this.includedColumns;
    }

    public Collection<ColumnSchema> getColumns() {
        return this.columns;
    }

    public Collection<AugmentedRow> getAugmentedRows() { return this.augmentedRows; }
}
