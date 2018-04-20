package com.booking.replication.augmenter.transaction;

import com.booking.replication.model.Event;

import java.util.List;
import java.util.UUID;

public class TransactionEventDataImplementation implements TransactionEventData {

    private List<Event> events;
    private UUID uuid;
    private long xid;
    private String tableName;

    @Override
    public List<Event> getEvents() {
        return this.events;
    }

    public void setEvents(List<Event> events) {
        this.events = events;
    }

    @Override
    public UUID getUuid() {
        return this.uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public long getXid() {
        return this.xid;
    }

    public void setXid(long xid) {
        this.xid = xid;
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
