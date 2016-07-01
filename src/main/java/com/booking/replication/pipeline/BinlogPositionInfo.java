package com.booking.replication.pipeline;

/**
 * Created by bdevetak on 26/11/15.
 */
public class BinlogPositionInfo {

    private String binlogFilename;
    private long   binlogPosition;

    private long fakeMicrosecondsCounter;

    public BinlogPositionInfo() {}

    public BinlogPositionInfo(String filename, long position) {
        this.binlogFilename = filename;
        this.binlogPosition = position;
    }

    /**
     * Binlog position information.
     * @param filename          Binlog filename
     * @param position          Binlog position
     * @param fakeMsCounter     Fake microsecond counter
     */
    public BinlogPositionInfo(String filename, long position, long fakeMsCounter) {
        this.binlogFilename = filename;
        this.binlogPosition = position;
        this.fakeMicrosecondsCounter = fakeMsCounter;
    }

    public boolean equals(BinlogPositionInfo other) {
        return (this.getBinlogFilename().equals(other.getBinlogFilename()) && this.getBinlogPosition() == other.getBinlogPosition());
    }

    public boolean greaterThan(BinlogPositionInfo other) {
        int ourBinlogFile = Integer.parseInt(this.binlogFilename.split("\\.")[1]);
        int otherBinlogFile = Integer.parseInt(other.getBinlogFilename().split("\\.")[1]);

        return (
                ourBinlogFile > otherBinlogFile
                || (ourBinlogFile == otherBinlogFile && this.getBinlogPosition() > other.getBinlogPosition())
            );
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
