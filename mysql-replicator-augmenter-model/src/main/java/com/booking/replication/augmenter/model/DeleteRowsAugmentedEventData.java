package com.booking.replication.augmenter.model;

import java.util.Collection;
import java.util.Map;

@SuppressWarnings("unused")
public class DeleteRowsAugmentedEventData implements TableAugmentedEventData {
    private AugmentedEventTable eventTable;
    private Collection<Boolean> includedColumns;
    private Collection<AugmentedEventColumn> columns;
    private Collection<Map<String, Object>> rows;

    public DeleteRowsAugmentedEventData() {
    }

    public DeleteRowsAugmentedEventData(AugmentedEventTable eventTable, Collection<Boolean> includedColumns, Collection<AugmentedEventColumn> columns, Collection<Map<String, Object>> rows) {
        this.eventTable = eventTable;
        this.includedColumns = includedColumns;
        this.columns = columns;
        this.rows = rows;
    }

    @Override
    public AugmentedEventTable getEventTable() {
        return this.eventTable;
    }

    public Collection<Boolean> getIncludedColumns() {
        return this.includedColumns;
    }

    public Collection<AugmentedEventColumn> getColumns() {
        return this.columns;
    }

    public Collection<Map<String, Object>> getRows() {
        return this.rows;
    }
}
