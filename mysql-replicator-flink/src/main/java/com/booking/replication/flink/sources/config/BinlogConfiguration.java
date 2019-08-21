package com.booking.replication.flink.sources.config;

public class BinlogConfiguration {
    public static final String CHECKPOINT_PATH = "checkpoint.path";
    public static final String CHECKPOINT_DEFAULT = "checkpoint.default";
    public static final String REPLICATOR_THREADS = "replicator.threads";
    public static final String REPLICATOR_TASKS = "replicator.tasks";
    public static final String REPLICATOR_QUEUE_SIZE = "replicator.queue.size";
    public static final String REPLICATOR_QUEUE_TIMEOUT = "replicator.queue.timeout";
    public static final String OVERRIDE_CHECKPOINT_START_POSITION = "override.checkpoint.start.position";
    public static final String OVERRIDE_CHECKPOINT_BINLOG_FILENAME = "override.checkpoint.binLog.filename";
    public static final String OVERRIDE_CHECKPOINT_BINLOG_POSITION = "override.checkpoint.binLog.position";
    public static final String OVERRIDE_CHECKPOINT_GTID_SET = "override.checkpoint.gtidSet";

    public BinlogConfiguration() {
    }
}
