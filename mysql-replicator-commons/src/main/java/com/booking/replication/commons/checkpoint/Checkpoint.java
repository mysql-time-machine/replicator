package com.booking.replication.commons.checkpoint;

import java.io.Serializable;

@SuppressWarnings("unused")
public class Checkpoint implements Serializable, Comparable<Checkpoint> {
    private long serverId;
    private GTID gtid;
    private Binlog binlog;

    public Checkpoint() {
    }

    public Checkpoint(long serverId, GTID gtid, Binlog binlog) {
        this.serverId = serverId;
        this.gtid = gtid;
        this.binlog = binlog;
    }

    public long getServerId() {
        return this.serverId;
    }

    public Binlog getBinlog() {
        return this.binlog;
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
            } else if (this.binlog != null && checkpoint.binlog != null) {
                return this.binlog.compareTo(checkpoint.binlog);
            } else if (this.binlog != null) {
                return Integer.MAX_VALUE;
            } else if (checkpoint.binlog != null) {
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
