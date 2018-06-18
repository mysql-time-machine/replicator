package com.booking.replication.augmenter.model;

import java.io.Serializable;

@SuppressWarnings("unused")
public class AugmentedEventUpdatedRow implements Serializable {
    private Serializable[] before;
    private Serializable[] after;

    public AugmentedEventUpdatedRow() {
    }

    public AugmentedEventUpdatedRow(Serializable[] before, Serializable[] after) {
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
