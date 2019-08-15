package com.booking.replication.augmenter.model.event;

import com.booking.replication.augmenter.model.row.AugmentedRow;
import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.FullTableName;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collection;

public abstract class RowsAugmentedEventData implements TableAugmentedEventData {

    protected EventMetadata metadata;

    protected Collection<Boolean> includedColumns;
    protected Collection<AugmentedRow> rows;

    public RowsAugmentedEventData() { }

    public RowsAugmentedEventData(
            AugmentedEventType eventType,
            FullTableName eventTable,
            Collection<Boolean> includedColumns,
            Collection<ColumnSchema> columns,
            Collection<AugmentedRow> rows) {
        this.metadata           = new RowEventMetadata(eventTable, eventType, columns);
        this.includedColumns    = includedColumns;
        this.rows               = rows;
    }

    @JsonIgnore
    public Collection<Boolean> getIncludedColumns() {
        return this.includedColumns;
    }

    public Collection<AugmentedRow> getRows() {
        return this.rows;
    }

    public EventMetadata getMetadata() {
        return this.metadata;
    }
}