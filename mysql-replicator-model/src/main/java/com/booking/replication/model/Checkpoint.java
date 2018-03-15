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

    public long getServerId() {
        return this.serverId;
    }

    public String getBinlogFilename() {
        return this.binlogFilename;
    }

    public long getBinlogPosition() {
        return this.binlogPosition;
    }

    public String getPseudoGTID() {
        return this.pseudoGTID;
    }

    public int getPseudoGTIDIndex() {
        return this.pseudoGTIDIndex;
    }
}
