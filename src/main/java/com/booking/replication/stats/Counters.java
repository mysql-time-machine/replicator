package com.booking.replication.stats;

/**
 * Created by bosko on 12/24/15.
 */
public class Counters {
    public static final int INSERT_COUNTER = 0;
    public static final int UPDATE_COUNTER = 1;
    public static final int DELETE_COUNTER = 2;
    public static final int COMMIT_COUNTER = 3;
    public static final int XID_COUNTER    = 4;

    public static String getCounterName(int counterID) {
        if (counterID == INSERT_COUNTER) {
            return "INSERTS";
        }
        else if (counterID == UPDATE_COUNTER) {
            return  "UPDATES";
        }
        else if (counterID == DELETE_COUNTER) {
            return "DELETES";
        }
        else if (counterID == COMMIT_COUNTER) {
            return "COMMIT_QUERIES";
        }
        else if (counterID == XID_COUNTER ) {
            return "TRANSACTIONS_COUNT";
        }
        else {
            return "NA";
        }
    }
}
