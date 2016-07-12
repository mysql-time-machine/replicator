package com.booking.replication.schema.exception;

import java.sql.SQLException;

/**
 * Created by bosko on 11/1/15.
 */
public class SchemaTransitionException extends Exception {

    public SchemaTransitionException() {

    }

    public SchemaTransitionException(String message) {
        super(message);
    }

    public SchemaTransitionException(String message, SQLException ex) {

    }
}
