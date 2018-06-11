package com.booking.replication.augmenter.model;

import java.util.List;
import java.util.UUID;

@SuppressWarnings("unused")
public class TransactionAugmentedEventData implements AugmentedEventData {
    private String identifier;
    private long xxid;
    private List<AugmentedEvent> eventList;

    public TransactionAugmentedEventData() {
    }

    public TransactionAugmentedEventData(String identifier, long xxid, List<AugmentedEvent> eventList) {
        this.identifier = identifier;
        this.xxid = xxid;
        this.eventList = eventList;
    }

    public String getIdentifier() {
        return this.identifier;
    }

    public long getXXID() {
        return this.xxid;
    }

    public List<AugmentedEvent> getEventList() {
        return this.eventList;
    }
}
