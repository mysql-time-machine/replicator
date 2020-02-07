package com.booking.replication.augmenter.model.row;

import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Optional;

public class RowBeforeAfter {

    BitSet includedColumns;
    Optional<Serializable[]> before;
    Optional<Serializable[]> after;

    @Override
    public String toString() {
        return "RowBeforeAfter{" +
                "includedColumns=" + includedColumns +
                ", before=" + ( before.isPresent() ? Arrays.toString(before.get()) : "empty" ) +
                ", after=" + ( after.isPresent() ? Arrays.toString(after.get()) : "empty" ) +
                '}';
    }

    public RowBeforeAfter(BitSet includedColumns, Serializable[] before, Serializable[] after) {
        this.includedColumns = includedColumns;
        this.before = (before != null) ? Optional.of(before) : Optional.empty();
        this.after = (after != null) ? Optional.of(after) : Optional.empty();
    }

    public Optional<Serializable[]> getBefore() {
        return before;
    }

    public Optional<Serializable[]> getAfter() {
        return after;
    }
}
