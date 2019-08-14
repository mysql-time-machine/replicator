package com.booking.replication.augmenter.model.event;

import com.booking.replication.augmenter.model.row.AugmentedRow;
import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.FullTableName;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collection;

public abstract class RowsAugmentedEventData implements TableAugmentedEventData {

    protected TableMetadata metadata;

    protected Collection<Boolean> includedColumns;
    protected Collection<AugmentedRow> rows;

    public RowsAugmentedEventData() { }

    public RowsAugmentedEventData(
            FullTableName eventTable,
            Collection<Boolean> includedColumns,
            Collection<ColumnSchema> columns,
            Collection<AugmentedRow> rows) {

        this.metadata = new TableMetadata(eventTable, columns);
        this.includedColumns = includedColumns;
        this.rows = rows;
    }

    @JsonIgnore
    public FullTableName getEventTable() {
        return this.metadata.getEventTable();
    }

    @JsonIgnore
    public Collection<Boolean> getIncludedColumns() {
        return this.includedColumns;
    }

    @JsonIgnore
    public Collection<ColumnSchema> getColumns() {
        return this.metadata.getColumns();
    }

    public Collection<AugmentedRow> getRows() {
        return this.rows;
    }

    public TableMetadata getMetadata() {
        return this.metadata;
    }
}