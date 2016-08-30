package com.booking.replication.checkpoints;

/**
 * Created by bosko on 5/30/16.
 */
public class SafeCheckpointType {
    public static final int BINLOG_POSITION    = 1;
    public static final int GLOBAL_PSEUDO_GTID = 2;
}
