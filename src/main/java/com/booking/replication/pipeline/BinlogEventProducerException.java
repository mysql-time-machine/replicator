package com.booking.replication.pipeline;

/**
 * Created by edmitriev on 7/14/17.
 */
public class BinlogEventProducerException extends Exception {
    public BinlogEventProducerException(String string) {
        super(string);
    }
    public BinlogEventProducerException(String string, Throwable throwable) {
        super(string, throwable);
    }
}
