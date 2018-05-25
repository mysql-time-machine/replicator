package com.booking.replication.augmenter.pipeline;

public class BinlogPositionComparationException extends Exception {
    public BinlogPositionComparationException(String string) {
        super(string);
    }
    public BinlogPositionComparationException(String string, Throwable throwable) {
        super(string, throwable);
    }
}
