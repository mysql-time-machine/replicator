package com.booking.replication.exceptions;

public class RowListMessageSerializationException extends Exception {

    public RowListMessageSerializationException() {
        super("");
    }

    public RowListMessageSerializationException(String message) {
        super(message);
    }

    public RowListMessageSerializationException(Exception e, String message) {
        super(message, e);
    }
}
