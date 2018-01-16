package com.booking.replication.sql;

import com.booking.replication.binlog.event.QueryEventType;
import com.booking.replication.sql.exception.QueryInspectorException;
import com.google.code.or.binlog.impl.event.QueryEvent;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bosko on 8/26/16.
 */
public class QueryInspector {

    private static final Pattern isDDLTemporaryTablePattern = Pattern.compile(QueryPatterns.isDDLTemporaryTable, Pattern.CASE_INSENSITIVE);
    private static final Pattern isDDLDefinerPattern = Pattern.compile(QueryPatterns.isDDLDefiner, Pattern.CASE_INSENSITIVE);
    private static final Pattern isDDLTablePattern = Pattern.compile(QueryPatterns.isDDLTable, Pattern.CASE_INSENSITIVE);
    private static final Pattern isDDLViewPattern = Pattern.compile(QueryPatterns.isDDLView, Pattern.CASE_INSENSITIVE);
    private static final Pattern isBeginPattern = Pattern.compile(QueryPatterns.isBEGIN, Pattern.CASE_INSENSITIVE);
    private static final Pattern isCommitPattern = Pattern.compile(QueryPatterns.isCOMMIT, Pattern.CASE_INSENSITIVE);
    private static final Pattern isAnalyzePattern = Pattern.compile(QueryPatterns.isANALYZE, Pattern.CASE_INSENSITIVE);
    private static Pattern isPseudoGTIDPattern = null;

    public static void setIsPseudoGTIDPattern(String isPseudoGTIDPattern) throws IllegalStateException {
        if (QueryInspector.isPseudoGTIDPattern != null) {
            throw new IllegalStateException("Failed to reassign isPseudoGTIDPattern. Not null");
        }
        QueryInspector.isPseudoGTIDPattern = Pattern.compile(isPseudoGTIDPattern, Pattern.CASE_INSENSITIVE);
    }

    public static boolean isDDLTemporaryTable(String querySQL) {

        // optimization
        if (querySQL.equals("BEGIN")) {
            return false;
        }

        Matcher matcher = isDDLTemporaryTablePattern.matcher(querySQL);

        return matcher.find();
    }

    public static boolean isDDLDefiner(String querySQL) {

        // optimization
        if (querySQL.equals("BEGIN")) {
            return false;
        }

        Matcher matcher = isDDLDefinerPattern.matcher(querySQL);

        return matcher.find();
    }

    public static boolean isDDLTable(String querySQL) {

        // optimization
        if (querySQL.equals("BEGIN")) {
            return false;
        }

        Matcher matcher = isDDLTablePattern.matcher(querySQL);

        return matcher.find();
    }

    public static boolean isDDLView(String querySQL) {

        // optimization
        if (querySQL.equals("BEGIN")) {
            return false;
        }

        Matcher matcher = isDDLViewPattern.matcher(querySQL);

        return matcher.find();
    }

    public static boolean isBegin(String querySQL, boolean isDDL) {

        boolean hasBegin;

        // optimization
        if (querySQL.equals("COMMIT")) {
            hasBegin = false;
        } else {
            Matcher matcher = isBeginPattern.matcher(querySQL);
            hasBegin = matcher.find();
        }

        return (hasBegin && !isDDL);
    }

    public static boolean isCommit(String querySQL, boolean isDDL) {

        boolean hasCommit;

        // optimization
        if (querySQL.equals("BEGIN")) {
            hasCommit = false;
        } else {
            Matcher matcher = isCommitPattern.matcher(querySQL);
            hasCommit = matcher.find();
        }
        return (hasCommit && !isDDL);
    }

    public static boolean isPseudoGTID(String querySQL) {

        // optimization
        if (querySQL.equals("BEGIN") || querySQL.equals("COMMIT")) {
            return false;
        }

        Matcher matcher = isPseudoGTIDPattern.matcher(querySQL);

        boolean found = matcher.find();

        return found;
    }

    public static boolean isAnalyze(String querySQL) {

        // optimization
        if (querySQL.equals("BEGIN") || querySQL.equals("COMMIT")) {
            return false;
        }

        Matcher matcher = isAnalyzePattern.matcher(querySQL);

        boolean found = matcher.find();

        return found;
    }

    public static String extractPseudoGTID(String querySQL) throws QueryInspectorException {

        Matcher matcher = isPseudoGTIDPattern.matcher(querySQL);

        boolean found = matcher.find();
        if (found) {
            if (!(matcher.groupCount() == 1)) {
                throw new QueryInspectorException("Invalid PseudoGTID query. Could not extract PseudoGTID from: " + querySQL);
            }
            String pseudoGTID = matcher.group(0);
            return  pseudoGTID;

        } else {
            throw new QueryInspectorException("Invalid PseudoGTID query. Could not extract PseudoGTID from: " + querySQL);
        }
    }

    public static QueryEventType getQueryEventType(QueryEvent event) {
        String querySQL = event.getSql().toString();
        boolean isDDLTable = isDDLTable(querySQL);
        boolean isDDLView = isDDLView(querySQL);

        if (isCommit(querySQL, isDDLTable)) {
            return QueryEventType.COMMIT;
        } else if (isDDLDefiner(querySQL)) {
            return QueryEventType.DDLDEFINER;
        } else if (isBegin(querySQL, isDDLTable)) {
            return QueryEventType.BEGIN;
        } else if (isPseudoGTID(querySQL)) {
            return QueryEventType.PSEUDOGTID;
        } else if (isDDLTable) {
            return QueryEventType.DDLTABLE;
        } else if (isDDLTemporaryTable(querySQL)) {
            return QueryEventType.DDLTEMPORARYTABLE;
        } else if (isDDLView) {
            return QueryEventType.DDLVIEW;
        } else if (isAnalyze(querySQL)) {
            return QueryEventType.ANALYZE;
        }
        return QueryEventType.UNKNOWN;
    }
}
