package com.booking.replication.sql.exception;

/**
 * Created by bosko on 8/30/16.
 */
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
