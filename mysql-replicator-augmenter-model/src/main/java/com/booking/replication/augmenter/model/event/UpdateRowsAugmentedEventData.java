package com.booking.replication.augmenter.model.event;

import com.booking.replication.augmenter.model.row.AugmentedRow;
import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.FullTableName;

import java.util.Collection;

@SuppressWarnings("unused")
public class UpdateRowsAugmentedEventData implements TableAugmentedEventData {
    private FullTableName eventTable;
    private Collection<Boolean> includedColumnsBeforeUpdate;
    private Collection<Boolean> includedColumns;
    private Collection<ColumnSchema> columns;
    private Collection<AugmentedRow> augmentedRows;

    public UpdateRowsAugmentedEventData() {

    }

    public UpdateRowsAugmentedEventData(
            FullTableName eventTable,
            Collection<Boolean> includedColumnsBeforeUpdate,
            Collection<Boolean> includedColumns,
            Collection<ColumnSchema> columns,
            Collection<AugmentedRow> augmentedRows
        ) {

        this.eventTable = eventTable;
        this.includedColumnsBeforeUpdate = includedColumnsBeforeUpdate;
        this.includedColumns = includedColumns;
        this.columns = columns;
        this.augmentedRows = augmentedRows;
    }

    @Override
    public FullTableName getEventTable() {
        return this.eventTable;
    }

    public Collection<Boolean> getIncludedColumnsBeforeUpdate() {
        return this.includedColumnsBeforeUpdate;
    }

    public Collection<Boolean> getIncludedColumns() {
        return this.includedColumns;
    }

    public Collection<ColumnSchema> getColumns() {
        return this.columns;
    }

    public Collection<AugmentedRow> getAugmentedRows() {
        return this.augmentedRows;
    }

    public void setEventTable(FullTableName eventTable) {
        this.eventTable = eventTable;
    }

    public void setIncludedColumnsBeforeUpdate(Collection<Boolean> includedColumnsBeforeUpdate) {
        this.includedColumnsBeforeUpdate = includedColumnsBeforeUpdate;
    }

    public void setIncludedColumns(Collection<Boolean> includedColumns) {
        this.includedColumns = includedColumns;
    }

    public void setColumns(Collection<ColumnSchema> columns) {
        this.columns = columns;
    }

    public void setAugmentedRows(Collection<AugmentedRow> augmentedRows) {
        this.augmentedRows = augmentedRows;
    }
}
