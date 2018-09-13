package com.booking.replication.augmenter.model.row;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Optional;

public class RowBeforeAfter {

    BitSet includedColumns;
    Optional<Serializable[]> before;
    Optional<Serializable[]> after;

    public RowBeforeAfter(BitSet includedColumns, Serializable[] before, Serializable[] after) {
        this.includedColumns = includedColumns;
        this.before = Optional.ofNullable(before);
        this.after = Optional.ofNullable(after);
    }

    public Optional<Serializable[]> getBefore() {
        return before;
    }

    public Optional<Serializable[]> getAfter() {
        return after;
    }
}
