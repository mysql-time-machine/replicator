package com.booking.replication;

public class Constants {

    public static final int LAST_KNOWN_BINLOG_POSITION    = 1;
    public static final int LAST_KNOWN_MAP_EVENT_POSITION = 2;
    public static final int LAST_KNOWN_MAP_EVENT_POSITION_FAKE_MICROSECONDS_COUNTER = 3;

    public static final int MAX_QUEUE_SIZE = 10000;

    public static final String BLACKLISTED_DB = "meta"; // TODO: make this a list & add to config

    public static final String ACTIVE_SCHEMA_SUFIX = "active_schema";

    public static final String HEART_BEAT_TABLE = "db_heartbeat";

}
