package com.booking.replication.commons.checkpoint;

import java.io.Serializable;

@SuppressWarnings("unused")
public class Checkpoint implements Serializable, Comparable<Checkpoint> {
    private long serverId;
    private String binlogFilename;
    private long binlogPosition;
    private GTID gtid;

    public Checkpoint() {
    }

    public Checkpoint(long serverId, String binlogFilename, long binlogPosition, GTID gtid) {
        this.serverId = serverId;
        this.binlogFilename = binlogFilename;
        this.binlogPosition = binlogPosition;
        this.gtid = gtid;
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

    public GTID getGTID() {
        return this.gtid;
    }

    @Override
    public int compareTo(Checkpoint checkpoint) {
        if (checkpoint != null) {
            if (this.gtid != null &&  checkpoint.gtid != null) {
                return this.gtid.compareTo(checkpoint.gtid);
            } else if (this.gtid != null) {
                return Integer.MAX_VALUE;
            } else if (checkpoint.gtid != null){
                return Integer.MIN_VALUE;
            } else if (this.binlogFilename != null && checkpoint.binlogFilename != null) {
                if (this.binlogFilename.equals(checkpoint.binlogFilename)) {
                    return Long.compare(this.binlogPosition, checkpoint.binlogPosition);
                } else {
                    return this.binlogFilename.compareTo(checkpoint.binlogFilename);
                }
            } else if (this.binlogFilename != null) {
                return Integer.MAX_VALUE;
            } else if (checkpoint.binlogFilename != null) {
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
