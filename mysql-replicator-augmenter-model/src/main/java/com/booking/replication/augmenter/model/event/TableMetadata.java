package com.booking.replication.augmenter.model.event;

import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.FullTableName;

import java.util.Collection;

public class TableMetadata {
    private FullTableName eventTable;
    private Collection<ColumnSchema> columns;

    public TableMetadata(FullTableName eventTable, Collection<ColumnSchema> columns) {
        this.eventTable = eventTable;
        this.columns = columns;
    }

    public FullTableName getEventTable() {
        return eventTable;
    }

    public Collection<ColumnSchema> getColumns() {
        return columns;
    }
}