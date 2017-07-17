package com.booking.replication.pipeline;

/**
 * Created by bdevetak on 26/11/15.
 */
public class BinlogPositionInfo {

    private String binlogFilename;
    private long   binlogPosition;
    private int    serverID;
    private String host;

    private long   fakeMicrosecondsCounter;

    public BinlogPositionInfo() {}

    public BinlogPositionInfo(
        String host,
        Integer serverID,
        String filename,
        long position
    ) {
        this.host           = host;
        this.serverID = serverID;
        this.binlogFilename = filename;
        this.binlogPosition = position;
    }

    public BinlogPositionInfo(
            String filename,
            long   position
    ) {
        this.binlogFilename = filename;
        this.binlogPosition = position;
    }

    /**
     * Binlog position information.
     * @param filename          Binlog filename
     * @param position          Binlog position
     * @param fakeMsCounter     Fake microsecond counter
     */
    public BinlogPositionInfo(
        String host,
        Integer serverID,
        String filename,
        long   position,
        long   fakeMsCounter
    ) {
        this.host = host;
        this.serverID = serverID;
        this.binlogFilename = filename;
        this.binlogPosition = position;
        this.fakeMicrosecondsCounter = fakeMsCounter;
    }

    public BinlogPositionInfo(int serverID, String binlogFilename, long binlogPosition) {
        this.serverID = serverID;
        this.binlogFilename = binlogFilename;
        this.binlogPosition = binlogPosition;
    }

    public static int compare(BinlogPositionInfo binlogPostition1, BinlogPositionInfo binlogPostition2) throws BinlogPositionComparationException {
        if (binlogPostition1.getServerID() != null || binlogPostition2.getServerID() != null) {
            if (Integer.compare(binlogPostition1.getServerID(), binlogPostition2.getServerID()) != 0) {
                throw new BinlogPositionComparationException("Can't compare serverId " + binlogPostition1.getServerID() + " to serverId " + binlogPostition2.getServerID());
            }
        }
        int filenameComp = binlogPostition1.binlogFilename.compareTo(binlogPostition2.getBinlogFilename());
        if (filenameComp != 0) return filenameComp;
        return Long.compare(binlogPostition1.binlogPosition, binlogPostition2.getBinlogPosition());
    }

    @Override
    public String toString() {
        return "BinlogPositionInfo{" +
                "binlogFilename='" + binlogFilename + '\'' +
                ", binlogPosition=" + binlogPosition +
                ", serverID=" + serverID +
                ", host='" + host + '\'' +
                ", fakeMicrosecondsCounter=" + fakeMicrosecondsCounter +
                '}';
    }

    public int compareTo(BinlogPositionInfo binlogPostition) throws BinlogPositionComparationException {
        return compare(this, binlogPostition);
    }

    public boolean equals(BinlogPositionInfo other) throws Exception {
        if (!this.host.equals(other.host)) {
            throw new Exception("Can't compare binlog positions for equality between different hosts");
        }
        return (this.getBinlogFilename().equals(other.getBinlogFilename()) && this.getBinlogPosition() == other.getBinlogPosition());
    }

    public boolean greaterThan(BinlogPositionInfo other) throws Exception {
        if (!this.host.equals(other.host)) {
            throw new Exception("Can't compare binlog positions for equality between different hosts");
        }

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

    public Integer getServerID() {
        return serverID;
    }

    public void setServerID(int serverID) {
        this.serverID = serverID;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
