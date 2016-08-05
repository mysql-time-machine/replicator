package com.booking.replication.applier.kafka;

/**
 * Created by bosko on 8/5/16.
 */
public class KafkaMessageBufferException extends Exception {

    private Exception originalException;

    public KafkaMessageBufferException() {
        this("", null);
    }

    public KafkaMessageBufferException(String message) {
        this(message, null);
    }

    public KafkaMessageBufferException(String message, Exception exception) {
        super(message);
        originalException = exception;
    }

    public Exception getOriginalException() {
        return originalException;
    }

}
