package com.booking.replication.augmenter.model.event;

import com.booking.replication.commons.checkpoint.Checkpoint;

import java.io.Serializable;

@SuppressWarnings("unused")
public class AugmentedEventHeader implements Serializable {
    private String databaseName;
    private String tableName;
    private long timestamp;
    private Checkpoint checkpoint;
    private AugmentedEventType eventType;
    private AugmentedEventTransaction eventTransaction;

    public AugmentedEventHeader() {
    }

    public AugmentedEventHeader(long timestamp, Checkpoint checkpoint, AugmentedEventType eventType, String databaseName, String tableName) {
        this.timestamp = timestamp;
        this.checkpoint = checkpoint;
        this.eventType = eventType;
        this.eventTransaction = null;
        this.databaseName = databaseName;
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

    public AugmentedEventTransaction getEventTransaction() {
        return this.eventTransaction;
    }

    public void setEventTransaction(AugmentedEventTransaction eventTransaction) {
        this.eventTransaction = eventTransaction;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String schemaKey() {
        if(eventType == AugmentedEventType.QUERY){
            return "bigdata-ddl-schema";
        }
        return String.format("bigdata-%s-%s", databaseName, tableName);
    }

    public String headerString(){
        return String.format("%s-%s-%s", this.databaseName, this.tableName, timestamp);
    }

    @Override
    public String toString() {
        return String.format("timestamp: %s | checkpoint: %s | eventType: %s | db: %s | table: %s", timestamp, checkpoint.toString(), eventType, databaseName, tableName);
    }
}

