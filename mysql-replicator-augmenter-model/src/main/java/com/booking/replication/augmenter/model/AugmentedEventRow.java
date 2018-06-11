package com.booking.replication.augmenter.model;

import java.io.Serializable;

@SuppressWarnings("unused")
public class AugmentedEventRow {
    private Serializable[] before;
    private Serializable[] after;

    public AugmentedEventRow() {
    }

    public AugmentedEventRow(Serializable[] before, Serializable[] after) {
        this.before = before;
        this.after = after;
    }

    public Serializable getBefore() {
        return this.before;
    }

    public Serializable getAfter() {
        return this.after;
    }
}
