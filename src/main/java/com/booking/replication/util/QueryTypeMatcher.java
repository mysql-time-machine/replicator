package com.booking.replication.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bosko on 8/26/16.
 */
public class QueryTypeMatcher {

    // TODO: pre-compile patterns and reuse

    public static boolean isDDL(String querySQL) {

        // optimization
        if (querySQL.equals("BEGIN")) {
            return false;
        }

        String ddlPattern = "(alter|drop|create|rename|truncate|modify)\\s+(table|column)";

        Pattern pattern = Pattern.compile(ddlPattern, Pattern.CASE_INSENSITIVE);

        Matcher matcher = pattern.matcher(querySQL);

        return matcher.find();
    }

    public static boolean isBegin(String querySQL, boolean isDDL) {

        boolean hasBegin;

        // optimization
        if (querySQL.equals("COMMIT")) {
            hasBegin = false;
        } else {

            String beginPattern = "(begin)";

            Pattern pattern = Pattern.compile(beginPattern, Pattern.CASE_INSENSITIVE);

            Matcher matcher = pattern.matcher(querySQL);

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

            String commitPattern = "(commit)";

            Pattern pattern = Pattern.compile(commitPattern, Pattern.CASE_INSENSITIVE);

            Matcher matcher = pattern.matcher(querySQL);

            hasCommit = matcher.find();
        }

        return (hasCommit && !isDDL);
    }
}
