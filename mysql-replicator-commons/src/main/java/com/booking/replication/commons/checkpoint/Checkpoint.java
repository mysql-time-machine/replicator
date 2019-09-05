package com.booking.replication.commons.checkpoint;

import java.io.Serializable;
import java.util.Objects;

@SuppressWarnings("unused")
public class Checkpoint implements Serializable, Comparable<Checkpoint> {
    private long timestamp;
    private long serverId;
    private GTID gtid;
    private Binlog binlog;
    private String gtidSet;

    public Checkpoint() {
    }

    public Checkpoint(String gtidSet) {
        this.gtidSet = gtidSet;
    }

    public Checkpoint( Binlog binlog) {
        this.binlog = binlog;
    }

    public Checkpoint( Binlog binlog, String gtidSet) {
        this.binlog = binlog;
        this.gtidSet = gtidSet;
    }

    public Checkpoint(long timestamp, long serverId, GTID gtid, Binlog binlog) {
        this.timestamp = timestamp;
        this.serverId = serverId;
        this.gtid = gtid;
        this.binlog = binlog;
    }

    public Checkpoint(long timestamp, long serverId, GTID gtid, Binlog binlog, String gtidSet) {
        this.timestamp = timestamp;
        this.serverId = serverId;
        this.gtid = gtid;
        this.binlog = binlog;
        this.gtidSet = gtidSet;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setServerId(long serverId) {
        this.serverId = serverId;
    }

    public GTID getGtid() {
        return gtid;
    }

    public void setGtid(GTID gtid) {
        this.gtid = gtid;
    }

    public void setGtidSet(String gtidSet) {
        this.gtidSet = gtidSet;
    }

    public String getGtidSet() {
        return gtidSet;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public long getServerId() {
        return this.serverId;
    }

    public Binlog getBinlog() {
        return this.binlog;
    }

    @Override
    public int compareTo(Checkpoint checkpoint) {
        int comparison = 0;

        if (checkpoint != null) {
            if (this.gtid != null && checkpoint.gtid != null) {
                if (this.gtid.getType() != checkpoint.gtid.getType()) {
                    throw new UnsupportedOperationException(String.format(
                            "cannot compare checkpoints with distinct types: %s and %s",
                            this.gtid.getType().name(),
                            checkpoint.gtid.getType().name()
                    ));
                }

                comparison = this.gtid.compareTo(checkpoint.gtid);
            } else if (this.gtid != null) {
                comparison = 1;
            } else if (checkpoint.gtid != null) {
                comparison = -1;
            }

            if (comparison == 0) {
                comparison = Long.compare(this.timestamp, checkpoint.timestamp);
            }

            if (comparison == 0 && this.serverId == checkpoint.serverId) {
                if (this.binlog != null && checkpoint.binlog != null) {
                    comparison = this.binlog.compareTo(checkpoint.binlog);
                } else if (this.binlog != null) {
                    comparison = 1;
                } else if (checkpoint.binlog != null) {
                    comparison = -1;
                }
            }
        } else {
            comparison = 1;
        }

        return comparison;
    }

    @Override
    public boolean equals(Object checkpoint) {
        if (checkpoint instanceof Checkpoint) {
            return this.compareTo((Checkpoint) checkpoint) == 0;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, serverId, gtid, binlog, gtidSet);
    }

    @Override
    public String toString() {
        return String.format("timestamp: %s | serverid: %s | gitid: %s | binlog: %s | gtidSet: %s", timestamp, serverId, gtid, binlog, gtidSet);
    }
}
