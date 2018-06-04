package com.booking.replication.augmenter.model;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class UpdateRowsAugmentedEventData implements AugmentedEventData {
    private BitSet includedColumnsBeforeUpdate;
    private BitSet includedColumns;
    private List<Map.Entry<Serializable[], Serializable[]>> rows;

    public UpdateRowsAugmentedEventData() {
    }

    public UpdateRowsAugmentedEventData(BitSet includedColumnsBeforeUpdate, BitSet includedColumns, List<Map.Entry<Serializable[], Serializable[]>> rows) {
        this.includedColumnsBeforeUpdate = includedColumnsBeforeUpdate;
        this.includedColumns = includedColumns;
        this.rows = rows;
    }

    public BitSet getIncludedColumnsBeforeUpdate() {
        return this.includedColumnsBeforeUpdate;
    }

    public BitSet getIncludedColumns() {
        return this.includedColumns;
    }

    public List<Map.Entry<Serializable[], Serializable[]>> getRows() {
        return this.rows;
    }
}
