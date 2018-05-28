package com.booking.replication.supplier.model.checkpoint;

@SuppressWarnings("unused")
public class Checkpoint implements Comparable<Checkpoint> {
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

    @Override
    public int compareTo(Checkpoint checkpoint) {
        if (this.getPseudoGTID() != null && checkpoint != null && checkpoint.getPseudoGTID() != null) {
            if (this.getPseudoGTID().equals(checkpoint.getPseudoGTID())) {
                return Integer.compare(this.getPseudoGTIDIndex(), checkpoint.getPseudoGTIDIndex());
            } else {
                return this.getPseudoGTID().compareTo(checkpoint.getPseudoGTID());
            }
        } else if (this.getPseudoGTID() != null) {
            return Integer.MAX_VALUE;
        } else if (checkpoint != null && checkpoint.getPseudoGTID() != null) {
            return Integer.MIN_VALUE;
        } else if (checkpoint != null) {
            if (this.getBinlogFilename() != null && checkpoint.getBinlogFilename() != null) {
                if (this.getBinlogFilename().equals(checkpoint.getBinlogFilename())) {
                    return Long.compare(this.getBinlogPosition(), checkpoint.getBinlogPosition());
                } else {
                    return this.getBinlogFilename().compareTo(checkpoint.getBinlogFilename());
                }
            } else if (this.getBinlogFilename() != null) {
                return Integer.MAX_VALUE;
            } else if (checkpoint.getBinlogFilename() != null) {
                return Integer.MIN_VALUE;
            } else {
                return 0;
            }
        } else {
            return Integer.MAX_VALUE;
        }
    }

    @Override
    public boolean equals(Object checkpoint) {
        if (Checkpoint.class.isInstance(checkpoint)) {
            return this.compareTo(Checkpoint.class.cast(checkpoint)) == 0;
        } else {
            return false;
        }
    }
}
