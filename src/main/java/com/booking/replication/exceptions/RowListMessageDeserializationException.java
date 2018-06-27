package com.booking.replication.exceptions;

public class RowListMessageDeserializationException extends Exception {

    public RowListMessageDeserializationException() {
        super("");
    }

    public RowListMessageDeserializationException(String message) {
        super(message);
    }

    public RowListMessageDeserializationException(Exception e, String message) {
        super(message, e);
    }
}
