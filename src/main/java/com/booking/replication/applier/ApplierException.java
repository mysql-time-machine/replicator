package com.booking.replication.applier;

import com.booking.replication.applier.hbase.TaskBufferInconsistencyException;

/**
 * Created by bosko on 9/9/16.
 */
public class ApplierException extends Exception {

    private Exception originalException;

    public ApplierException() {
        this("", null);
    }

    public ApplierException(String message) {
        this(message, null);
    }

    public ApplierException(String message, Exception exception) {
        super(message);
        originalException = exception;
    }

    public Exception getOriginalException() {
        return originalException;
    }

    public ApplierException(TaskBufferInconsistencyException exception) {
        originalException = exception;
    }
}
