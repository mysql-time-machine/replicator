package com.booking.replication.augmenter.model;

import com.booking.replication.commons.checkpoint.Checkpoint;

import java.io.Serializable;

public class AugmentedEventHeader implements Serializable {
    private final long timestamp;
    private final Checkpoint checkpoint;
    private final AugmentedEventType eventType;
    private final String tableName;

    public AugmentedEventHeader(long timestamp, Checkpoint checkpoint, AugmentedEventType eventType, String tableName) {
        this.timestamp = timestamp;
        this.checkpoint = checkpoint;
        this.eventType = eventType;
        this.tableName = tableName;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public Checkpoint getCheckpoint() {
        return this.checkpoint;
    }

    public AugmentedEventType getEventType() {
        return this.eventType;
    }

    public String getTableName() {
        return this.tableName;
    }
}

