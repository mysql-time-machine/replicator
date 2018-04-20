package com.booking.replication.supplier.model;

public class PseudoGTIDEventHeaderImplementation implements PseudoGTIDEventHeader {
    private long serverId;
    private long eventLength;
    private long headerLength;
    private long dataLength;
    private long nextPosition;
    private int flags;
    private long timestamp;
    private RawEventType rawEventType;
    private Checkpoint checkpoint;

    public PseudoGTIDEventHeaderImplementation() {
    }

    public PseudoGTIDEventHeaderImplementation(long serverId, long eventLength, long headerLength, long dataLength, long nextPosition, int flags, long timestamp, RawEventType rawEventType, Checkpoint checkpoint) {
        this.serverId = serverId;
        this.eventLength = eventLength;
        this.headerLength = headerLength;
        this.dataLength = dataLength;
        this.nextPosition = nextPosition;
        this.flags = flags;
        this.timestamp = timestamp;
        this.rawEventType = rawEventType;
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
                eventHeader.getRawEventType(),
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
    public RawEventType getRawEventType() {
        return this.rawEventType;
    }

    @Override
    public Checkpoint getCheckpoint() {
        return this.checkpoint;
    }
}
