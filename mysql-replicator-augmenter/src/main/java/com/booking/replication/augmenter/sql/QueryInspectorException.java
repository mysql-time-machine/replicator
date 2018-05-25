package com.booking.replication.augmenter.sql;

public class QueryInspectorException extends Exception {

    private Exception originalException;

    public QueryInspectorException() {
        this("", null);
    }

    public QueryInspectorException(String message) {
        this(message, null);
    }

    public QueryInspectorException(String message, Exception exception) {
        super(message);
        originalException = exception;
    }

    public Exception getOriginalException() {
        return originalException;
    }
}
