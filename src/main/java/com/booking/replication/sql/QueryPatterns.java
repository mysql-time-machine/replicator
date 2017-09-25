package com.booking.replication.sql;

/**
 * Created by bosko on 8/30/16.
 */
public class QueryPatterns {
    public static final String isBEGIN    = "^(/\\*.*?\\*/\\s*)?(begin)";
    public static final String isCOMMIT   = "^(/\\*.*?\\*/\\s*)?(commit)";
    public static final String isDDLDefiner = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+definer\\s*=";
    public static final String isDDLTable = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(table)";
    public static final String isDDLTemporaryTable = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+temporary\\s+(table)";
    public static final String isDDLView  = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(view)";
    public static final String isANALYZE  = "^(/\\*.*?\\*/\\s*)?(analyze)\\s+(table)";
    public static final String isGTIDPattern  = "(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})";
}
