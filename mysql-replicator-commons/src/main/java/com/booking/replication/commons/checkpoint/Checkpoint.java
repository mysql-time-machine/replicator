package com.booking.replication.commons.checkpoint;

import java.io.Serializable;

@SuppressWarnings("unused")
public class Checkpoint implements Serializable, Comparable<Checkpoint> {
    private long timestamp;
    private long serverId;
    private GTID gtid;
    private Binlog binlog;

    public Checkpoint() {
    }

    public Checkpoint(long timestamp, long serverId, GTID gtid, Binlog binlog) {
        this.timestamp = timestamp;
        this.serverId = serverId;
        this.gtid = gtid;
        this.binlog = binlog;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public long getServerId() {
        return this.serverId;
    }

    public GTID getGTID() {
        return this.gtid;
    }

    public Binlog getBinlog() {
        return this.binlog;
    }

    @Override
    public int compareTo(Checkpoint checkpoint) {
        int comparison = 0;

        if (checkpoint != null) {
            if (this.gtid != null &&  checkpoint.gtid != null) {
                if (this.gtid.getType() != checkpoint.gtid.getType()) {
                    throw new UnsupportedOperationException(String.format(
                            "cannot compare checkpoints with distinct types: %s and %s",
                            this.gtid.getType().name(),
                            checkpoint.gtid.getType().name()
                    ));
                }

                comparison = this.gtid.compareTo(checkpoint.gtid);
            } else if (this.gtid != null) {
                comparison = Integer.MAX_VALUE;
            } else if (checkpoint.gtid != null){
                comparison = Integer.MIN_VALUE;
            }

            if (comparison == 0) {
                comparison = Long.compare(this.timestamp, checkpoint.timestamp);
            }

            if (comparison == 0 && this.serverId == checkpoint.serverId) {
                if (this.binlog != null && checkpoint.binlog != null) {
                    comparison = this.binlog.compareTo(checkpoint.binlog);
                } else if (this.binlog != null) {
                    comparison = Integer.MAX_VALUE;
                } else if (checkpoint.binlog != null) {
                    comparison = Integer.MIN_VALUE;
                }
            }
        } else {
            comparison = Integer.MAX_VALUE;
        }

        return comparison;
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
