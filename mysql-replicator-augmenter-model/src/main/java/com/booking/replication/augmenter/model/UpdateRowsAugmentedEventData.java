package com.booking.replication.augmenter.model;

import java.util.List;

@SuppressWarnings("unused")
public class UpdateRowsAugmentedEventData implements AugmentedEventData {
    private AugmentedEventTable eventTable;
    private List<AugmentedEventColumn> includedColumnsBeforeUpdate;
    private List<AugmentedEventColumn> includedColumns;
    private List<AugmentedEventRow> rows;

    public UpdateRowsAugmentedEventData() {
    }

    public UpdateRowsAugmentedEventData(AugmentedEventTable eventTable, List<AugmentedEventColumn> includedColumnsBeforeUpdate, List<AugmentedEventColumn> includedColumns, List<AugmentedEventRow> rows) {
        this.eventTable = eventTable;
        this.includedColumnsBeforeUpdate = includedColumnsBeforeUpdate;
        this.includedColumns = includedColumns;
        this.rows = rows;
    }

    public AugmentedEventTable getEventTable() {
        return this.eventTable;
    }

    public List<AugmentedEventColumn> getIncludedColumnsBeforeUpdate() {
        return this.includedColumnsBeforeUpdate;
    }

    public List<AugmentedEventColumn> getIncludedColumns() {
        return this.includedColumns;
    }

    public List<AugmentedEventRow> getRows() {
        return this.rows;
    }
}
