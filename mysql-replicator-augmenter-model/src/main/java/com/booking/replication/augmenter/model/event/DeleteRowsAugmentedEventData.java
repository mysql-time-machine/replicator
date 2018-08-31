package com.booking.replication.augmenter.model.event;

import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.FullTableName;

import java.util.Collection;
import java.util.Map;

@SuppressWarnings("unused")
public class DeleteRowsAugmentedEventData implements TableAugmentedEventData {
    private FullTableName eventTable;
    private Collection<Boolean> includedColumns;
    private Collection<ColumnSchema> columns;
    private Collection<Map<String, Object>> rows;

    public DeleteRowsAugmentedEventData() {
    }

    public DeleteRowsAugmentedEventData(FullTableName eventTable, Collection<Boolean> includedColumns, Collection<ColumnSchema> columns, Collection<Map<String, Object>> rows) {
        this.eventTable = eventTable;
        this.includedColumns = includedColumns;
        this.columns = columns;
        this.rows = rows;
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

    public Collection<Map<String, Object>> getRows() {
        return this.rows;
    }
}
