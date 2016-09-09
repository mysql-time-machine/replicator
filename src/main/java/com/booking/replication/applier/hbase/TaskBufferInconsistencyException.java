package com.booking.replication.applier.hbase;

/**
 * Created by bosko on 9/9/16.
 */
public class TaskBufferInconsistencyException extends  Exception {
    private Exception originalException;

    public TaskBufferInconsistencyException() {
        this("", null);
    }

    public TaskBufferInconsistencyException(String message) {
        this(message, null);
    }

    public TaskBufferInconsistencyException(String message, Exception exception) {
        super(message);
        originalException = exception;
    }

    public Exception getOriginalException() {
        return originalException;
    }
}
