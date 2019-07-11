package com.booking.replication;

import com.booking.replication.supplier.model.RawEvent;

public class RawEventInstance {

    private String gtidSet;

    private long timestamp;

    private String eventType;

    public RawEventInstance(long timestamp, String gtidSet, String eventType) {
        this.timestamp = timestamp;
        this.gtidSet = gtidSet;
        this.eventType = eventType;
    }

    public RawEventInstance(RawEvent rawEvent) {

        // TODO: serialize rawEvent
        this.gtidSet = rawEvent.getGTIDSet();
        this.eventType = rawEvent.getHeader().getEventType().getDefinition().getName();
        this.timestamp = rawEvent.getHeader().getTimestamp();

    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getGtidSet() {
        return gtidSet;
    }

    public void setGtidSet(String gtidSet) {
        this.gtidSet = gtidSet;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }
}
