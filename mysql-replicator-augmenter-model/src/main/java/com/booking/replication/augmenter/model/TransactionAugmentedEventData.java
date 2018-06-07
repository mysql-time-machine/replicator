package com.booking.replication.augmenter.model;

import java.util.List;
import java.util.UUID;

@SuppressWarnings("unused")
public class TransactionAugmentedEventData implements AugmentedEventData {
    private UUID identifier;
    private long xxid;
    private List<AugmentedEvent> eventList;

    public TransactionAugmentedEventData() {
    }

    public TransactionAugmentedEventData(UUID identifier, long xxid, List<AugmentedEvent> eventList) {
        this.eventList = eventList;
    }

    public UUID getIdentifier() {
        return this.identifier;
    }

    public long getXXID() {
        return this.xxid;
    }

    public List<AugmentedEvent> getEventList() {
        return this.eventList;
    }
}
