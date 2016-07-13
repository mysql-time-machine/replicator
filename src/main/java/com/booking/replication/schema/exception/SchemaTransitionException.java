package com.booking.replication.schema.exception;

/**
 * Created by bosko on 11/1/15.
 */
public class SchemaTransitionException extends Exception {

    private Exception originalException;

    public SchemaTransitionException() {
        this("", null);
    }

    public SchemaTransitionException(String message) {
        this(message, null);
    }

    public SchemaTransitionException(String message, Exception exception) {
        super(message);
        originalException = exception;
    }

    public Exception getOriginalException() {
        return originalException;
    }
}
