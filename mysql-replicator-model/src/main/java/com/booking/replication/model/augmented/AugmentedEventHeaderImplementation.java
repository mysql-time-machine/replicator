package com.booking.replication.model.augmented;

import com.booking.replication.model.Checkpoint;
import com.booking.replication.model.EventHeaderV4;
import com.booking.replication.model.EventType;

public class AugmentedEventHeaderImplementation implements AugmentedEventHeader {
    private final long serverId;
    private final long eventLength;
    private final long nextPosition;
    private final int flags;
    private final long timestamp;
    private final EventType eventType;
    private final Checkpoint checkpoint;

    public AugmentedEventHeaderImplementation(long serverId, long eventLength, long nextPosition, int flags, long timestamp, EventType eventType, Checkpoint checkpoint) {
        this.serverId = serverId;
        this.eventLength = eventLength;
        this.nextPosition = nextPosition;
        this.flags = flags;
        this.timestamp = timestamp;
        this.eventType = eventType;
        this.checkpoint = checkpoint;
    }

    public AugmentedEventHeaderImplementation(EventHeaderV4 eventHeader, Checkpoint checkpoint) {
        this(
                eventHeader.getServerId(),
                eventHeader.getEventLength(),
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
