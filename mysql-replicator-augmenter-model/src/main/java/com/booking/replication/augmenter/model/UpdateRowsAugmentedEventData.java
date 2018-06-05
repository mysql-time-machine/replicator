package com.booking.replication.augmenter.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class UpdateRowsAugmentedEventData implements AugmentedEventData {
    private List<AugmentedEventColumn> includedColumnsBeforeUpdate;
    private List<AugmentedEventColumn> includedColumns;
    private List<Map.Entry<Serializable[], Serializable[]>> rows;

    public UpdateRowsAugmentedEventData() {
    }

    public UpdateRowsAugmentedEventData(List<AugmentedEventColumn> includedColumnsBeforeUpdate, List<AugmentedEventColumn> includedColumns, List<Map.Entry<Serializable[], Serializable[]>> rows) {
        this.includedColumnsBeforeUpdate = includedColumnsBeforeUpdate;
        this.includedColumns = includedColumns;
        this.rows = rows;
    }

    public List<AugmentedEventColumn> getIncludedColumnsBeforeUpdate() {
        return this.includedColumnsBeforeUpdate;
    }

    public List<AugmentedEventColumn> getIncludedColumns() {
        return this.includedColumns;
    }

    public List<Map.Entry<Serializable[], Serializable[]>> getRows() {
        return this.rows;
    }
}
