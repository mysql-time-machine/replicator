package com.booking.replication.augmenter.model;

import java.io.Serializable;
import java.util.List;

@SuppressWarnings("unused")
public class DeleteRowsAugmentedEventData implements TableAugmentedEventData {
    private AugmentedEventTable eventTable;
    private List<Boolean> includedColumns;
    private List<AugmentedEventColumn> columns;
    private List<Serializable[]> rows;

    public DeleteRowsAugmentedEventData() {
    }

    public DeleteRowsAugmentedEventData(AugmentedEventTable eventTable, List<Boolean> includedColumns, List<AugmentedEventColumn> columns, List<Serializable[]> rows) {
        this.eventTable = eventTable;
        this.includedColumns = includedColumns;
        this.columns = columns;
        this.rows = rows;
    }

    @Override
    public AugmentedEventTable getEventTable() {
        return this.eventTable;
    }

    public List<Boolean> getIncludedColumns() {
        return this.includedColumns;
    }

    public List<AugmentedEventColumn> getColumns() {
        return this.columns;
    }

    public List<Serializable[]> getRows() {
        return this.rows;
    }
}
