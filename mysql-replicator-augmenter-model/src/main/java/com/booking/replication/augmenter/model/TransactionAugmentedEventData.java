package com.booking.replication.augmenter.model;

import java.util.List;

@SuppressWarnings("unused")
public class TransactionAugmentedEventData implements TableAugmentedEventData {
    private String identifier;
    private long xxid;
    private AugmentedEventTable eventTable;
    private List<AugmentedEvent> eventList;

    public TransactionAugmentedEventData() {
    }

    public TransactionAugmentedEventData(String identifier, long xxid, AugmentedEventTable eventTable, List<AugmentedEvent> eventList) {
        this.identifier = identifier;
        this.xxid = xxid;
        this.eventTable = eventTable;
        this.eventList = eventList;
    }

    public String getIdentifier() {
        return this.identifier;
    }

    public long getXXID() {
        return this.xxid;
    }

    @Override
    public AugmentedEventTable getEventTable() {
        return this.eventTable;
    }

    public List<AugmentedEvent> getEventList() {
        return this.eventList;
    }
}
