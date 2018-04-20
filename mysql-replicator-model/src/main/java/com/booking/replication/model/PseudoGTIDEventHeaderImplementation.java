package com.booking.replication.model;

public class PseudoGTIDEventHeaderImplementation implements PseudoGTIDEventHeader {
    private long serverId;
    private long eventLength;
    private long headerLength;
    private long dataLength;
    private long nextPosition;
    private int flags;
    private long timestamp;
    private EventType eventType;
    private Checkpoint checkpoint;

    public PseudoGTIDEventHeaderImplementation() {
    }

    public PseudoGTIDEventHeaderImplementation(long serverId, long eventLength, long headerLength, long dataLength, long nextPosition, int flags, long timestamp, EventType eventType, Checkpoint checkpoint) {
        this.serverId = serverId;
        this.eventLength = eventLength;
        this.headerLength = headerLength;
        this.dataLength = dataLength;
        this.nextPosition = nextPosition;
        this.flags = flags;
        this.timestamp = timestamp;
        this.eventType = eventType;
        this.checkpoint = checkpoint;
    }

    public PseudoGTIDEventHeaderImplementation(EventHeaderV4 eventHeader, Checkpoint checkpoint) {
        this(
                eventHeader.getServerId(),
                eventHeader.getEventLength(),
                eventHeader.getHeaderLength(),
                eventHeader.getDataLength(),
                eventHeader.getNextPosition(),
                eventHeader.getFlags(),
                eventHeader.getTimestamp(),
                eventHeader.getEventType(),
                checkpoint
        );
    }

    @Override
    public long getServerId() {
        return this.serverId;
    }

    @Override
    public long getEventLength() {
        return this.eventLength;
    }

    @Override
    public long getHeaderLength() {
        return this.headerLength;
    }

    @Override
    public long getDataLength() {
        return this.headerLength;
    }

    @Override
    public long getNextPosition() {
        return this.nextPosition;
    }

    @Override
    public int getFlags() {
        return this.flags;
    }

    @Override
    public long getTimestamp() {
        return this.timestamp;
    }

    @Override
    public EventType getEventType() {
        return this.eventType;
    }

    @Override
    public Checkpoint getCheckpoint() {
        return this.checkpoint;
    }
}
