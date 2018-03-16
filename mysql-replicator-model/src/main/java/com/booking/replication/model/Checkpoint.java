package com.booking.replication.model;

@SuppressWarnings("unused")
public class Checkpoint {
    private long serverId;
    private String binlogFilename;
    private long binlogPosition;
    private String pseudoGTID;
    private int pseudoGTIDIndex;

    public Checkpoint() {
    }

    public Checkpoint(long serverId, String binlogFilename, long binlogPosition, String pseudoGTID, int pseudoGTIDIndex) {
        this.serverId = serverId;
        this.binlogFilename = binlogFilename;
        this.binlogPosition = binlogPosition;
        this.pseudoGTID = pseudoGTID;
        this.pseudoGTIDIndex = pseudoGTIDIndex;
    }

    public Checkpoint(Checkpoint checkpoint) {
        this(
                checkpoint.serverId,
                checkpoint.binlogFilename,
                checkpoint.binlogPosition,
                checkpoint.pseudoGTID,
                checkpoint.pseudoGTIDIndex
        );
    }

    public long getServerId() {
        return this.serverId;
    }

    public void setServerId(long serverId) {
        this.serverId = serverId;
    }

    public String getBinlogFilename() {
        return this.binlogFilename;
    }

    public void setBinlogFilename(String binlogFilename) {
        this.binlogFilename = binlogFilename;
    }

    public long getBinlogPosition() {
        return this.binlogPosition;
    }

    public void setBinlogPosition(long binlogPosition) {
        this.binlogPosition = binlogPosition;
    }

    public String getPseudoGTID() {
        return this.pseudoGTID;
    }

    public void setPseudoGTID(String pseudoGTID) {
        this.pseudoGTID = pseudoGTID;
    }

    public int getPseudoGTIDIndex() {
        return this.pseudoGTIDIndex;
    }

    public void setPseudoGTIDIndex(int pseudoGTIDIndex) {
        this.pseudoGTIDIndex = pseudoGTIDIndex;
    }
}
