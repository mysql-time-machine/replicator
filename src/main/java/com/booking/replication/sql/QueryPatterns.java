package com.booking.replication.sql;

/**
 * Created by bosko on 8/30/16.
 */
public class QueryPatterns {
    public static final String isBEGIN    = "^(begin)";
    public static final String isCOMMIT   = "^(commit)";
    public static final String isDDLDefiner = "^(alter|drop|create|rename|truncate|modify)\\s+definer\\s*=";
    public static final String isDDLTable = "^(alter|drop|create|rename|truncate|modify)\\s+(table)";
    public static final String isDDLTemporaryTable = "^(alter|drop|create|rename|truncate|modify)\\s+temporary\\s+(table)";
    public static final String isDDLView  = "^(alter|drop|create|rename|truncate|modify)\\s+(view)";
    public static final String isANALYZE  = "^(analyze)\\s+(table)";
    public static final String isGTIDPattern  = "(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})";
}
