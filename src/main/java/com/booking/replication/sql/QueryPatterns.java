package com.booking.replication.sql;

/**
 * Created by bosko on 8/30/16.
 */
public class QueryPatterns {
    public static final String isBEGIN  = "(begin)";
    public static final String isCOMMIT = "(commit)";
    public static final String isDDL    = "(alter|drop|create|rename|truncate|modify)\\s+(table|column|view)";
}
