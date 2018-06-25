package com.booking.replication.augmenter.model;

import java.util.List;

@SuppressWarnings("unused")
public class UpdateRowsAugmentedEventData implements TableAugmentedEventData {
    private AugmentedEventTable eventTable;
    private List<Boolean> includedColumnsBeforeUpdate;
    private List<Boolean> includedColumns;
    private List<AugmentedEventColumn> columns;
    private List<AugmentedEventUpdatedRow> rows;

    public UpdateRowsAugmentedEventData() {
    }

    public UpdateRowsAugmentedEventData(AugmentedEventTable eventTable, List<Boolean> includedColumnsBeforeUpdate, List<Boolean> includedColumns, List<AugmentedEventColumn> columns, List<AugmentedEventUpdatedRow> rows) {
        this.eventTable = eventTable;
        this.includedColumnsBeforeUpdate = includedColumnsBeforeUpdate;
        this.includedColumns = includedColumns;
        this.columns = columns;
        this.rows = rows;
    }

    @Override
    public AugmentedEventTable getEventTable() {
        return this.eventTable;
    }

    public List<Boolean> getIncludedColumnsBeforeUpdate() {
        return this.includedColumnsBeforeUpdate;
    }

    public List<Boolean> getIncludedColumns() {
        return this.includedColumns;
    }

    public List<AugmentedEventColumn> getColumns() {
        return this.columns;
    }

    public List<AugmentedEventUpdatedRow> getRows() {
        return this.rows;
    }
}
