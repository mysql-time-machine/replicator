package com.booking.replication.augmenter.util;

import java.util.Calendar;

/**
 * Extracted from com.google.code.or.common.util.MySQLUtils
 */
public class MySQLUtils {
    public static java.sql.Date toDate(int value) {
        final int d = value % 32; value >>>= 5;
        final int m = value % 16;
        final int y = value >> 4;
        final Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.set(y, m - 1, d);
        return new java.sql.Date(cal.getTimeInMillis());
    }
}
