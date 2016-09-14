package com.booking.replication.applier.hbase;

/**
 * Created by bosko on 9/15/16.
 */
public class TaskAccountingException extends Exception {
    private Exception originalException;

    public TaskAccountingException() {
        this("", null);
    }

    public TaskAccountingException(String message) {
        this(message, null);
    }

    public TaskAccountingException(String message, Exception exception) {
        super(message);
        originalException = exception;
    }

    public Exception getOriginalException() {
        return originalException;
    }
}
