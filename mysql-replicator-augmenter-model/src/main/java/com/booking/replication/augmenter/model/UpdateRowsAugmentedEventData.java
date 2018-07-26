package com.booking.replication.augmenter.model;

import java.util.Collection;

@SuppressWarnings("unused")
public class UpdateRowsAugmentedEventData implements TableAugmentedEventData {
    private AugmentedEventTable eventTable;
    private Collection<Boolean> includedColumnsBeforeUpdate;
    private Collection<Boolean> includedColumns;
    private Collection<AugmentedEventColumn> columns;
    private Collection<AugmentedEventUpdatedRow> rows;

    public UpdateRowsAugmentedEventData() {
    }

    public UpdateRowsAugmentedEventData(AugmentedEventTable eventTable, Collection<Boolean> includedColumnsBeforeUpdate, Collection<Boolean> includedColumns, Collection<AugmentedEventColumn> columns, Collection<AugmentedEventUpdatedRow> rows) {
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

    public Collection<Boolean> getIncludedColumnsBeforeUpdate() {
        return this.includedColumnsBeforeUpdate;
    }

    public Collection<Boolean> getIncludedColumns() {
        return this.includedColumns;
    }

    public Collection<AugmentedEventColumn> getColumns() {
        return this.columns;
    }

    public Collection<AugmentedEventUpdatedRow> getRows() {
        return this.rows;
    }
}
