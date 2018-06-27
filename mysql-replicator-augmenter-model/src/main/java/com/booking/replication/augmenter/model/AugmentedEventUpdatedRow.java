package com.booking.replication.augmenter.model;

import java.io.Serializable;
import java.util.Map;

@SuppressWarnings("unused")
public class AugmentedEventUpdatedRow implements Serializable {
    private Map<String, Serializable> before;
    private Map<String, Serializable> after;

    public AugmentedEventUpdatedRow() {
    }

    public AugmentedEventUpdatedRow(Map<String, Serializable> before, Map<String, Serializable> after) {
        this.before = before;
        this.after = after;
    }

    public Map<String, Serializable> getBefore() {
        return this.before;
    }

    public Map<String, Serializable> getAfter() {
        return this.after;
    }
}
