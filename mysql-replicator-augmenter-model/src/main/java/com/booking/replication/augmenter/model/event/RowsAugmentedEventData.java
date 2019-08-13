package com.booking.replication.augmenter.model.event;

import com.booking.replication.augmenter.model.row.AugmentedRow;
import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.FullTableName;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collection;

public abstract class RowsAugmentedEventData implements TableAugmentedEventData {

    protected TableMetadata tableMetadata;

    protected Collection<Boolean> includedColumns;
    protected Collection<AugmentedRow> rows;

    public RowsAugmentedEventData() { }

    public RowsAugmentedEventData(
            FullTableName eventTable,
            Collection<Boolean> includedColumns,
            Collection<ColumnSchema> columns,
            Collection<AugmentedRow> rows) {

        this.tableMetadata = new TableMetadata(eventTable, columns);
        this.includedColumns = includedColumns;
        this.rows = rows;
    }

    @JsonIgnore
    public FullTableName getEventTable() {
        return this.tableMetadata.getEventTable();
    }

    @JsonIgnore
    public Collection<Boolean> getIncludedColumns() {
        return this.includedColumns;
    }

    @JsonIgnore
    public Collection<ColumnSchema> getColumns() {
        return this.tableMetadata.getColumns();
    }

    public Collection<AugmentedRow> getRows() {
        return this.rows;
    }

    public TableMetadata getTableMetadata() {
        return this.tableMetadata;
    }
}