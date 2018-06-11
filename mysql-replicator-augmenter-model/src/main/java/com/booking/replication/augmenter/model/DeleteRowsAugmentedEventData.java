package com.booking.replication.augmenter.model;

import java.io.Serializable;
import java.util.List;

@SuppressWarnings("unused")
public class DeleteRowsAugmentedEventData implements TableAugmentedEventData {
    private AugmentedEventTable eventTable;
    private List<AugmentedEventColumn> includedColumns;
    private List<Serializable[]> rows;

    public DeleteRowsAugmentedEventData() {
    }

    public DeleteRowsAugmentedEventData(AugmentedEventTable eventTable, List<AugmentedEventColumn> includedColumns, List<Serializable[]> rows) {
        this.eventTable = eventTable;
        this.includedColumns = includedColumns;
        this.rows = rows;
    }

    @Override
    public AugmentedEventTable getEventTable() {
        return this.eventTable;
    }

    public List<AugmentedEventColumn> getIncludedColumns() {
        return this.includedColumns;
    }

    public List<Serializable[]> getRows() {
        return this.rows;
    }
}
