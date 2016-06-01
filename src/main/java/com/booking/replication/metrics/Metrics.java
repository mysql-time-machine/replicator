package com.booking.replication.metrics;

/**
 * Created by bosko on 12/24/15.
 */
public class Metrics {

    // =========================================================
    // I. Pipeline Stats
    //
    // I.a Event counters
    public static final int INSERT_EVENTS_COUNTER           = 0;
    public static final int UPDATE_EVENTS_COUNTER           = 1;
    public static final int DELETE_EVENTS_COUNTER           = 2;
    public static final int COMMIT_COUNTER                  = 3;
    public static final int XID_COUNTER                     = 4;

    public static final int EVENTS_PROCESSED                = 5;
    public static final int EVENTS_SKIPPED                  = 6;
    public static final int EVENTS_RECEIVED                 = 7;

    public static final int TOTAL_INSERT_EVENTS_COUNTER     = 50;
    public static final int TOTAL_UPDATE_EVENTS_COUNTER     = 51;
    public static final int TOTAL_DELETE_EVENTS_COUNTER     = 52;
    public static final int TOTAL_COMMIT_COUNTER            = 53;
    public static final int TOTAL_XID_COUNTER               = 54;

    public static final int TOTAL_EVENTS_PROCESSED          = 55;
    public static final int TOTAL_EVENTS_SKIPPED            = 56;
    public static final int TOTAL_EVENTS_RECEIVED           = 57;

    // I.b Row counters
    public static final int ROWS_PROCESSED                  = 101;
    public static final int ROWS_FOR_INSERT_PROCESSED       = 102;
    public static final int ROWS_FOR_UPDATE_PROCESSED       = 103;
    public static final int ROWS_FOR_DELETE_PROCESSED       = 104;

    public static final int TOTAL_ROWS_PROCESSED            = 501;
    public static final int TOTAL_ROWS_FOR_INSERT_PROCESSED = 502;
    public static final int TOTAL_ROWS_FOR_UPDATE_PROCESSED = 503;
    public static final int TOTAL_ROWS_FOR_DELETE_PROCESSED = 504;


    // ============================================================
    // II. Applier metrics
    //
    // II.a Task stats
    public static final int APPLIER_TASKS_SUBMITTED         = 3002;
    public static final int APPLIER_TASKS_IN_PROGRESS       = 3003;
    public static final int APPLIER_TASKS_SUCCEEDED         = 3004;
    public static final int APPLIER_TASKS_FAILED            = 3005;

    public static final int TOTAL_APPLIER_TASKS_SUBMITTED   = 4002;
    public static final int TOTAL_APPLIER_TASKS_IN_PROGRESS = 4003;
    public static final int TOTAL_APPLIER_TASKS_SUCCEEDED   = 4004;
    public static final int TOTAL_APPLIER_TASKS_FAILED      = 4005;

    public static final int TASK_QUEUE_SIZE                = 5002;

    // III.b Row stats
    public static final int HBASE_ROWS_AFFECTED = 6001;
    public static final int TOTAL_HBASE_ROWS_AFFECTED = 6002;


    // ============================================================
    // III. General Metrics:
    //
    // hearth beat:
    public static final int HEART_BEAT_COUNTER              = 7001;
    public static final int TOTAL_HEART_BEAT_COUNTER        = 7002;

    // replication delay
    public static final int REPLICATION_DELAY_MS            = 7003;

    // Name catalog
    public static String getCounterName(int metricID) {
        if (metricID == INSERT_EVENTS_COUNTER) {
            return "INSERT_EVENTS_COUNT";
        }
        else if (metricID == UPDATE_EVENTS_COUNTER) {
            return  "UPDATE_EVENTS_COUNT";
        }
        else if (metricID == DELETE_EVENTS_COUNTER) {
            return "DELETE_EVENTS_COUNT";
        }
        else if (metricID == COMMIT_COUNTER) {
            return "COMMIT_QUERIES_COUNT";
        }
        else if (metricID == XID_COUNTER ) {
            return "XID_COUNT";
        }
        else if (metricID == EVENTS_RECEIVED) {
            return "EVENTS_RECEIVED";
        }
        else if (metricID == EVENTS_PROCESSED) {
            return "EVENTS_PROCESSED";
        }
        else if (metricID == EVENTS_SKIPPED) {
            return "EVENTS_SKIPPED";
        }
        else if (metricID == ROWS_PROCESSED) {
            return "ROWS_PROCESSED";
        }
        else if (metricID == ROWS_FOR_INSERT_PROCESSED) {
            return "ROWS_FOR_INSERT_PROCESSED";
        }
        else if (metricID == ROWS_FOR_UPDATE_PROCESSED) {
            return "ROWS_FOR_UPDATE_PROCESSED";
        }
        else if (metricID == ROWS_FOR_DELETE_PROCESSED) {
            return "ROWS_FOR_DELETE_PROCESSED";
        }
        else if (metricID == HBASE_ROWS_AFFECTED) {
            return "HBASE_ROWS_AFFECTED";
        }
        else if (metricID == HEART_BEAT_COUNTER ) {
            return "HEART_BEAT_COUNTER";
        }
        else if (metricID == APPLIER_TASKS_SUBMITTED) {
            return "APPLIER_TASKS_SUBMITTED";
        }
        else if (metricID == APPLIER_TASKS_IN_PROGRESS) {
            return "APPLIER_TASKS_IN_PROGRESS";
        }
        else if (metricID == APPLIER_TASKS_SUCCEEDED ) {
            return "APPLIER_TASKS_SUCCEEDED";
        }
        else if (metricID == APPLIER_TASKS_FAILED ) {
            return "APPLIER_TASKS_FAILED";
        }
        else if (metricID == REPLICATION_DELAY_MS) {
            return "REPLICATION_DELAY_MS";
        }
        else if (metricID == TOTAL_INSERT_EVENTS_COUNTER) {
            return "TOTAL_INSERT_EVENTS_COUNT";
        }
        else if (metricID == TOTAL_UPDATE_EVENTS_COUNTER) {
            return "TOTAL_UPDATE_EVENTS_COUNT";
        }
        else if (metricID == TOTAL_DELETE_EVENTS_COUNTER) {
            return "TOTAL_DELETE_EVENTS_COUNT";
        }
        else if (metricID == TOTAL_COMMIT_COUNTER) {
            return "TOTAL_COMMIT_QUERIES_COUNT";
        }
        else if (metricID == TOTAL_XID_COUNTER ) {
            return "TOTAL_XID_COUNT";
        }
        else if (metricID == TOTAL_EVENTS_RECEIVED) {
            return "TOTAL_EVENTS_RECEIVED";
        }
        else if (metricID == TOTAL_EVENTS_PROCESSED) {
            return "TOTAL_EVENTS_PROCESSED";
        }
        else if (metricID == TOTAL_EVENTS_SKIPPED) {
            return "TOTAL_EVENTS_SKIPPED";
        }
        else if (metricID == TOTAL_ROWS_PROCESSED) {
            return "TOTAL_ROWS_PROCESSED";
        }
        else if (metricID == TOTAL_ROWS_FOR_INSERT_PROCESSED) {
            return "TOTAL_ROWS_FOR_INSERT_PROCESSED";
        }
        else if (metricID == TOTAL_ROWS_FOR_UPDATE_PROCESSED) {
            return "TOTAL_ROWS_FOR_UPDATE_PROCESSED";
        }
        else if (metricID == TOTAL_ROWS_FOR_DELETE_PROCESSED) {
            return "TOTAL_ROWS_FOR_DELETE_PROCESSED";
        }
        else if (metricID == TOTAL_HBASE_ROWS_AFFECTED) {
            return "TOTAL_HBASE_ROWS_AFFECTED";
        }
        else if (metricID == TOTAL_HEART_BEAT_COUNTER ) {
            return "TOTAL_HEART_BEAT_COUNTER";
        }
        else if (metricID == TOTAL_APPLIER_TASKS_SUBMITTED) {
            return "TOTAL_APPLIER_TASKS_SUBMITTED";
        }
        else if (metricID == TOTAL_APPLIER_TASKS_IN_PROGRESS) {
            return "TOTAL_APPLIER_TASKS_IN_PROGRESS";
        }
        else if (metricID == TOTAL_APPLIER_TASKS_SUCCEEDED ) {
            return "TOTAL_APPLIER_TASKS_SUCCEEDED";
        }
        else if (metricID == TOTAL_APPLIER_TASKS_FAILED ) {
            return "TOTAL_APPLIER_TASKS_FAILED";
        }
        else if (metricID == TASK_QUEUE_SIZE ) {
            return "TASK_QUEUE_SIZE";
        }
        else {
            return "NA";
        }
    }
}
