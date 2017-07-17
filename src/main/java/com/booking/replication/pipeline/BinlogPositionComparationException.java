package com.booking.replication.pipeline;

/**
 * Created by edmitriev on 7/14/17.
 */
public class BinlogPositionComparationException extends Exception {
    public BinlogPositionComparationException(String string) {
        super(string);
    }
    public BinlogPositionComparationException(String string, Throwable throwable) {
        super(string, throwable);
    }
}
