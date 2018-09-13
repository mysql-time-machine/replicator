package com.booking.replication.applier.hbase.schema;

/**
 * Created by bosko on 11/1/15.
 */
public class SchemaTransitionException extends Exception {

    public SchemaTransitionException() {
        super("");
    }

    public SchemaTransitionException(String message) {
        super(message);
    }

    public SchemaTransitionException(String message, Exception exception) {
        super(message, exception);
    }
}