package com.booking.replication.schema.exception;

import java.io.IOException;

/**
 * Created by bosko on 11/1/15.
 */
public class SchemaTransitionException extends IOException {

    public SchemaTransitionException() {

    }

    public SchemaTransitionException(String message) {
        super(message);
    }
}
