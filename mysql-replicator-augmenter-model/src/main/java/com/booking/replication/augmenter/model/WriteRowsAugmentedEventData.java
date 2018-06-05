package com.booking.replication.augmenter.model;

import java.io.Serializable;
import java.util.List;

@SuppressWarnings("unused")
public class WriteRowsAugmentedEventData implements AugmentedEventData {
    private List<AugmentedEventColumn> includedColumns;
    private List<Serializable[]> rows;

    public WriteRowsAugmentedEventData() {
    }

    public WriteRowsAugmentedEventData(List<AugmentedEventColumn> includedColumns, List<Serializable[]> rows) {
        this.includedColumns = includedColumns;
        this.rows = rows;
    }

    public List<AugmentedEventColumn> getIncludedColumns() {
        return this.includedColumns;
    }

    public List<Serializable[]> getRows() {
        return this.rows;
    }
}
