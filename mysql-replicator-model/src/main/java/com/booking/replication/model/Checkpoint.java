package com.booking.replication.model;

@SuppressWarnings("unused")
public class Checkpoint {
    private long serverId;
    private String binlogFilename;
    private long binlogPosition;

    public Checkpoint() {
    }

    private Checkpoint(EventHeaderV4 eventHeader, RotateEventData eventData) {
        this.serverId = eventHeader.getServerId();
        this.binlogFilename = eventData.getBinlogFilename();
        this.binlogPosition = eventData.getBinlogPosition();
    }

    public long getServerId() {
        return this.serverId;
    }

    public String getBinlogFilename() {
        return this.binlogFilename;
    }

    public long getBinlogPosition() {
        return this.binlogPosition;
    }

    public static Checkpoint of(Event event) {
        if (event.getHeader().getEventType() == EventType.ROTATE) {
            return new Checkpoint(event.getHeader(), RotateEventData.class.cast(event.getData()));
        } else {
            return null;
        }
    }
}
