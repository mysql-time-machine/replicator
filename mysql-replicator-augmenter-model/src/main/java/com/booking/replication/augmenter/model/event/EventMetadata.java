package com.booking.replication.augmenter.model.event;

import com.booking.replication.augmenter.model.schema.FullTableName;

public class EventMetadata {
    private FullTableName eventTable;
    private AugmentedEventType eventType;

    public EventMetadata(FullTableName eventTable, AugmentedEventType eventType) {
        this.eventTable = eventTable;
        this.eventType  = eventType;
    }

    public FullTableName getEventTable() {
        return eventTable;
    }

    public AugmentedEventType getEventType() {
        return this.eventType;
    }
}