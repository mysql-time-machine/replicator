package com.booking.replication.binlog.event;

/**
 * Created by bosko on 7/13/17.
 *
 * Taken from BinlogEventV4HeaderImpl
 */
public class RawBinlogEventHeader {
    private long timestamp;
    private int eventType;
    private long serverId;
    private long eventLength;
    private long nextPosition;
    private int flags;
    private long timestampOfReceipt;

    /**
     *
     */
    @Override
    public String toString() {
        return  "timestamp" +  timestamp
                + "eventType" + eventType
                + "serverId" + serverId
                + "eventLength" + eventLength
                + "nextPosition" + nextPosition
                + "flags" + flags
                + "timestampOfReceipt"+ timestampOfReceipt;
    }

    /**
     *
     */
    public int getHeaderLength() {
        return 19;
    }

    public long getPosition() {
        return this.nextPosition - this.eventLength;
    }

    /**
     *
     */
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getEventType() {
        return eventType;
    }

    public void setEventType(int eventType) {
        this.eventType = eventType;
    }

    public long getServerId() {
        return serverId;
    }

    public void setServerId(long serverId) {
        this.serverId = serverId;
    }

    public long getEventLength() {
        return eventLength;
    }

    public void setEventLength(long eventLength) {
        this.eventLength = eventLength;
    }

    public long getNextPosition() {
        return nextPosition;
    }

    public void setNextPosition(long nextPosition) {
        this.nextPosition = nextPosition;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public long getTimestampOfReceipt() {
        return timestampOfReceipt;
    }

    public void setTimestampOfReceipt(long timestampOfReceipt) {
        this.timestampOfReceipt = timestampOfReceipt;
    }
}
