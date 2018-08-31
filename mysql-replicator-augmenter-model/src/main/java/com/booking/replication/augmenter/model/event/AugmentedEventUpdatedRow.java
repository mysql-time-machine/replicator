package com.booking.replication.augmenter.model.event;

import java.io.Serializable;
import java.util.Map;

@SuppressWarnings("unused")
public class AugmentedEventUpdatedRow implements Serializable {
    private Map<String, Object> before;
    private Map<String, Object> after;

    public AugmentedEventUpdatedRow() {
    }

    public AugmentedEventUpdatedRow(Map<String, Object> before, Map<String, Object> after) {
        this.before = before;
        this.after = after;
    }

    public Map<String, Object> getBefore() {
        return this.before;
    }

    public Map<String, Object> getAfter() {
        return this.after;
    }
}
