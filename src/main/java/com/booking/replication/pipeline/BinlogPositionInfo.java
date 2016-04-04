package com.booking.replication.pipeline;

/**
 * Created by bdevetak on 26/11/15.
 */
public class BinlogPositionInfo {

    private String binlogFilename;
    private long   binlogPosition;

    private long fakeMicrosecondsCounter;

    public BinlogPositionInfo(String filename, long position) {
        this.binlogFilename = filename;
        this.binlogPosition = position;
    }

    public BinlogPositionInfo(String filename, long position, long fMCounter) {
        this.binlogFilename = filename;
        this.binlogPosition = position;
        this.fakeMicrosecondsCounter = fMCounter;
    }

    public String getBinlogFilename() {
        return binlogFilename;
    }

    public void setBinlogFilename(String binlogFilename) {
        this.binlogFilename = binlogFilename;
    }

    public long getBinlogPosition() {
        return binlogPosition;
    }

    public void setBinlogPosition(long binlogPosition) {
        this.binlogPosition = binlogPosition;
    }

    public long getFakeMicrosecondsCounter() {
        return fakeMicrosecondsCounter;
    }

    public void setFakeMicrosecondsCounter(long fakeMicrosecondsCounter) {
        this.fakeMicrosecondsCounter = fakeMicrosecondsCounter;
    }
}
