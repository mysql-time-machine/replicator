package com.booking.replication.augmenter.model.event;

import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.FullTableName;
import com.booking.replication.augmenter.model.schema.TableSchema;

import java.util.Collection;
import java.util.List;

public class TableMetadata {
    private FullTableName eventTable;
    private Collection<ColumnSchema> columns;

    List<String> primaryKeyColumns;

    public TableMetadata(FullTableName eventTable, Collection<ColumnSchema> columns) {
        this.eventTable = eventTable;
        this.columns    = columns;

        primaryKeyColumns = TableSchema.getPrimaryKeyColumns(columns);
    }

    public FullTableName getEventTable() {
        return eventTable;
    }

    public Collection<ColumnSchema> getColumns() {
        return columns;
    }

    public List<String> getPrimaryKeyColumns() {
        return this.primaryKeyColumns;
    }
}