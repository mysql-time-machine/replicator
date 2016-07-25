package com.booking.replication;

public class Constants {

    // Queue sizes
    // TODO: move this to config
    public static final int MAX_RAW_QUEUE_SIZE       = 10000;

    public static final String BLACKLISTED_DB = "meta"; // TODO: make this a list & add to config

    public static final String HEART_BEAT_TABLE = "db_heartbeat";

}
