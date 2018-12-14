package com.booking.replication.augmenter.model.event.format.avro;

import java.io.Serializable;

public class AvroMessageKey implements Serializable {
    public final String tableName;
    public final String databaseName;
    public final String eventType;
    public final long timestamp;

    public AvroMessageKey(String tableName, String databaseName, String eventType, long timestamp) {
        this.tableName = tableName;
        this.databaseName = databaseName;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }
}
