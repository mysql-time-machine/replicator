package com.booking.replication.augmenter.model;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;

@SuppressWarnings("unused")
public class WriteRowsAugmentedEventData implements AugmentedEventData {
    private BitSet includedColumns;
    private List<Serializable[]> rows;

    public WriteRowsAugmentedEventData() {
    }

    public WriteRowsAugmentedEventData(BitSet includedColumns, List<Serializable[]> rows) {
        this.includedColumns = includedColumns;
        this.rows = rows;
    }

    public BitSet getIncludedColumns() {
        return this.includedColumns;
    }

    public List<Serializable[]> getRows() {
        return this.rows;
    }
}
