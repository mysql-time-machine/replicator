package com.booking.replication.augmenter.model.event;

import com.booking.replication.augmenter.model.row.AugmentedRow;
import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.FullTableName;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collection;

public abstract class RowsAugmentedEventData implements TableAugmentedEventData {

    @JsonIgnore
    protected FullTableName eventTable;

    @JsonIgnore
    protected Collection<Boolean> includedColumns;

    protected Collection<ColumnSchema> columns;
    protected Collection<AugmentedRow> augmentedRows;

    public RowsAugmentedEventData() { }

    public RowsAugmentedEventData(
            FullTableName eventTable,
            Collection<Boolean> includedColumns,
            Collection<ColumnSchema> columns,
            Collection<AugmentedRow> augmentedRows) {
        this.eventTable = eventTable;
        this.includedColumns = includedColumns;
        this.columns = columns;
        this.augmentedRows = augmentedRows;
    }

    @Override
    public FullTableName getEventTable() {
        return this.eventTable;
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
}